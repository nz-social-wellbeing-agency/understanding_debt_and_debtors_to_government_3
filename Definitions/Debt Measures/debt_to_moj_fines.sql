/**************************************************************************************************
Title: Fines debt to MOJ Phase 3
Author: Freya Li and Simon Anastasiadis

Inputs & Dependencies:
- [IDI_Adhoc].[clean_read_DEBT].[moj_debt_full_summary_trimmed]
- [IDI_Adhoc].[clean_read_DEBT].[moj_debt_identity_fml_and_adhoc]
- [IDI_Adhoc].[clean_read_DEBT].[moj_debt_data_link] 
- [IDI_Clean].[security].[concordance]
Outputs:
- [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fines_transactions_ready]

Description: 
Debt, debt balances, and repayment for debtors owing Fines debt to MOJ.

Intended purpose:
Identifying debtors.
Calculating number of debtors and total value of debts.
Calculating change in debts - due to borrowing or repayment.
Determining persistence of debt

Notes:
1. Date range for table [moj_debt_full_summary_trimmed] is Dec2010 - Dec2020.
	But the balance in 2011 has no change for every month, we discard all data pre-2012.

2. For orginal data, numbers represent the amount of the money, they are all positive.
	Delta = penalty + impositions - payment - remittals + payment_reversal +remittal_reversal
	delta is calculated correctly.

3. If the first outstanding_balance for an identity may not equal to the delta, which means that 
	the outstanding_balance include the debt before 2012  

4. This table contains debts from fines and infringements which have been imposed. These are not charges,
	which are more courts and tribunals related. (Email Stephanie Simpson, MoJ data SME, 2021-04-08)

5. FML for the MoJ debt data is specific to the 20201020 refresh. To use the data on a different refresh, 
	researchers have to first join it to the 20201020 refresh and then use agency uid's to link to other refreshes.
	We used the security concordances from two refreshes to link the data to refreshes other than 20201020.
	Data exploration showed that only 69% of debtors have a snz_jus_uid. So instead of depending on a single
	agency uid we create a hybrid uid from IR, MOH, and DOL (immigration). This has >99.5% coverage.
	
6. The case of total repaid is more than total incurred has been observed.

7. For some individuals they have debt prior to 2012, but the first record we observe for them is not Jan 2012.
	The vast majority of such people have their first transaction in 2012 or 2013, hence we are comfortable
	assuming that they owe this debt in December 2011.
	When constructing this openning debt balance, we exclude people who's first transaction is a remittal
	leaving them with zero outstanding balance. While such people will have been in debt prior to the remittal,
	these kinds of remittals tend to occur in bunk and so probably do not reflect a person carrying debt.

8. We want every output identity to have an snz_uid. As around a quarter of moj_ppn can be linked to an snz_uid
	We create fill-in / artificial identities using -moj_ppn.
	This is a crude/workaround approach. We can only use it because none of the other data sources (MSD, IR, FCCO)
	require the creation of artifical snz_uids.

9. Legal aid data is not included as a part of this analysis (not provided)

Issue:

Parameters & Present values:
  Fast Match Loader (FML) refresh = 20201020 can not be changed without SNZ redo-ing linking
  Current refresh = 20211020
  Prefix = defn_
  Project schema = [DL-MAA2020-01]

History (reverse order):
2021-11-17 SA cross-refresh linking
2021-10-12 SA revise for phase 3 persistence work
2021-10-04 SA review
2021-08-13 FL insert records when there are no transactions
2021-06-17 FL comment out the active debt duration as it's out of scope; add repayment indicator; debt persisitence
2021-05-07 FL including monthly incurred debt and repayment
2021-03-03 FL work begun
**************************************************************************************************/

/**************************************************************************************************
Prior to use, copy to sandpit and index
**************************************************************************************************/

DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_full_summary_trimmed];
GO

SELECT *
      ,EOMONTH(DATEFROMPARTS(year_nbr, month_of_year_nbr, 1)) AS month_date
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_full_summary_trimmed]
FROM [IDI_Adhoc].[clean_read_DEBT].[moj_debt_full_summary_trimmed]
WHERE year_nbr > 2011
GO

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_full_summary_trimmed] ([moj_ppn]);
GO

/**************************************************************************************************
Handle start dates of outstanding balances
**************************************************************************************************/

WITH
first_record_ind AS (
	SELECT *
		,IIF(month_date = MIN(month_date) OVER (PARTITION BY moj_ppn), 1, 0) AS first_record_ind
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_full_summary_trimmed]
),
initial_balances AS (
	SELECT *
		,outstanding_balance - delta AS pre_2012_balance
	FROM first_record_ind
	WHERE first_record_ind = 1
	AND NOT (ABS(delta + remittals) < 1 AND ABS(outstanding_balance) < 1) -- first record is solely remittal of current balance
	AND ABS(outstanding_balance - delta) > 1
)
INSERT INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_full_summary_trimmed] (moj_ppn, month_date, impositions, delta, outstanding_balance)
SELECT moj_ppn
	,'2011-12-31' AS month_date
	,pre_2012_balance AS impositions
	,pre_2012_balance AS delta
	,pre_2012_balance AS outstanding_balance
FROM initial_balances

/**************************************************************************************************
Recalculate running balances
**************************************************************************************************/

DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_prep];
GO

SELECT moj_ppn
	,month_date
	,penalty
	,impositions
	,payment
	,remittals
	,payment_reversal
	,remittal_reversal
	,delta
	,outstanding_balance
	,SUM(delta) OVER (PARTITION BY moj_ppn ORDER BY month_date) AS balance_correct
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_prep]
FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_full_summary_trimmed]

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_prep] ([moj_ppn]);
GO

/**************************************************************************************************
fill in records where balance is non-zero but transactions are zero
**************************************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_non_transactions];
GO

WITH 
/* list of 120 numbers of 1:120 - spt_values is a n admin table chosen as it is at least 120 row long */
n AS (
	SELECT TOP 120 ROW_NUMBER() OVER (ORDER BY type) AS x
	FROM master.dbo.spt_values
),
/* list of dates, constructed by adding list of numbers to initial date */
my_dates AS (
	SELECT TOP (DATEDIFF(MONTH, '2011-12-01', '2020-12-31') + 1) /* number of dates required */
		EOMONTH(DATEADD(MONTH, x-1, '2012-01-01')) AS my_dates
	FROM n
	ORDER BY x
),
/* get the next date for each record */
debt_source AS(
SELECT *
	  ,LEAD(month_date, 1, '9999-01-01') OVER (PARTITION BY moj_ppn ORDER BY month_date) AS lead_date_cor
FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_prep]
),
joined AS (
	SELECT *
	FROM debt_source
	INNER JOIN my_dates
	ON month_date < my_dates 
	AND my_dates < lead_date_cor
)
/* combine original and additional records into same table */
SELECT * 
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_non_transactions]
FROM (
	/* original records */
	SELECT moj_ppn 
	      ,month_date
		  ,ROUND(impositions, 2) AS impositions
		  ,ROUND(penalty, 2) AS penalty
		  ,ROUND(payment, 2) AS payment
		  ,ROUND(remittals, 2) AS remittals
		  ,ROUND(payment_reversal, 2) AS payment_reversal
		  ,ROUND(remittal_reversal, 2) AS remittal_reversal
		  ,ROUND(balance_correct, 2) AS balance_correct
		  ,ROUND(delta, 2) AS delta
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_prep]

	UNION ALL

	/* additional records */
	SELECT moj_ppn 
	      ,my_dates AS month_date
		  ,NULL AS impositions
		  ,NULL AS penalty
		  ,NULL AS payment
		  ,NULL AS remittals
		  ,NULL AS payment_reversal
		  ,NULL AS remittal_reversal
		  ,ROUND(balance_correct, 2) AS balance_correct
		  ,0 AS delta
	FROM joined
	WHERE NOT balance_correct BETWEEN - 1 AND 0 --exclude small netative balances
) k

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_non_transactions] ([moj_ppn]);
GO

/*************************************************************************
Establish indexes for identity linking table
(the identity linking table is very slow without indexes)
*************************************************************************/
-- delete before creation
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_fml]
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_dl]
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_base]
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_current]
GO

-- temporary tables to enable correct indexing
SELECT [snz_jus_uid],[moj_ppn],[snz_fml_7_uid]
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_fml]
FROM [IDI_Adhoc].[clean_read_DEBT].[moj_debt_identity_fml_and_adhoc]

SELECT [rhs_nbr],[lhs_nbr],near_exact_ind,weight_nbr,run_key
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_dl]
FROM [IDI_Adhoc].[clean_read_DEBT].[moj_debt_data_link]
WHERE run_key = 941  -- there are two run_keys as FML used twice for MoJ data
-- runkey = 941 for fines & charges, runkey = 943 for FCCO
AND (near_exact_ind = 1 OR weight_nbr > 17) -- exclude only low weight, non-exact links

SELECT snz_uid,snz_spine_uid
	,CASE
		WHEN snz_ird_uid IS NOT NULL THEN 1
		WHEN snz_moh_uid IS NOT NULL THEN 2
		WHEN snz_dol_uid IS NOT NULL THEN 3
	END AS type_uid
	,COALESCE(snz_ird_uid, snz_moh_uid, snz_dol_uid) AS hybrid_uid
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_base]
FROM [IDI_Clean_20201020].[security].[concordance]
WHERE snz_ird_uid IS NOT NULL
OR snz_moh_uid IS NOT NULL
OR snz_dol_uid IS NOT NULL

SELECT snz_uid
	,CASE
		WHEN snz_ird_uid IS NOT NULL THEN 1
		WHEN snz_moh_uid IS NOT NULL THEN 2
		WHEN snz_dol_uid IS NOT NULL THEN 3
	END AS type_uid
	,COALESCE(snz_ird_uid, snz_moh_uid, snz_dol_uid) AS hybrid_uid
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_current]
FROM [IDI_Clean_20211020].[security].[concordance]
WHERE snz_ird_uid IS NOT NULL
OR snz_moh_uid IS NOT NULL
OR snz_dol_uid IS NOT NULL
GO

-- add indexing
CREATE NONCLUSTERED INDEX fml_index ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_fml] (snz_fml_7_uid);
CREATE NONCLUSTERED INDEX lhs_rhs_index ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_dl] (rhs_nbr, lhs_nbr); -- we join by both these columns hence both need indexing
CREATE NONCLUSTERED INDEX spine_index ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_base] (snz_spine_uid) INCLUDE (hybrid_uid, type_uid); -- we join by both these columns hence both need indexing
CREATE NONCLUSTERED INDEX jus_index ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_current] (hybrid_uid, type_uid);
GO

/*************************************************************************
Create identity linking table
*************************************************************************/
--Linking MoJ debt data with fast match loader
--FML for the MoJ debt data is specific to the 20201020 refresh. 
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_id];
GO

SELECT fml.[snz_jus_uid] AS adhoc_moj_uid -- agency uid for some adhoc loaded records
      ,fml.[moj_ppn] -- uid on debt input table
	  ,fml.[snz_fml_7_uid] -- links to dl.rhs_nbr
	  ,dl.[rhs_nbr] -- links to fml.snz_fml_7_uid
	  ,dl.[lhs_nbr] -- links to sc.snz_spine_uid
      ,sc.snz_spine_uid -- links to dl.lhr_nbr
	  --,sc.hybrid -- combined uid for cross-refresh linking
	  ,sc2.snz_uid -- desired output uid
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_id]
FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_fml] AS fml
LEFT JOIN [IDI_Sandpit].[DL-MAA2020-01].[tmp_dl] AS dl
ON fml.snz_fml_7_uid = dl.rhs_nbr 
LEFT JOIN [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_base] AS sc
ON dl.lhs_nbr = sc.snz_spine_uid
LEFT JOIN [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_current] AS sc2
ON sc.type_uid = sc2.type_uid
AND sc.hybrid_uid = sc2.hybrid_uid
GO

CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_id] ([moj_ppn]);
GO

-- delete temporary tables
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_fml]
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_dl]
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_base]
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_current]
GO

/*************************************************************************
Join MoJ data to spine
*************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fines_transactions_ready];
GO

SELECT COALESCE(b.snz_uid, -a.moj_ppn) AS snz_uid /* negative uids are used as fill ins for identities without snz_uids */
       ,a.*
INTO  [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fines_transactions_ready]
FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_non_transactions] a
LEFT JOIN [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_id] b
ON a.moj_ppn = b.moj_ppn

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fines_transactions_ready] (snz_uid);
GO
/* Compress */
ALTER TABLE [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fines_transactions_ready] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
GO

/*****************************************************************************
Remove temporary tables
*****************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_full_summary_trimmed];
GO

DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_prep];
GO

DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_non_transactions];
GO

DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_id];
GO
