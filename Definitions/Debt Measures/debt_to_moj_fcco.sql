/**************************************************************************************************
Title: FCCO debt to MOJ Phase 3
Author: Freya Li and Simon Anastasiadis

Inputs & Dependencies:
- [IDI_Adhoc].[clean_read_DEBT].[moj_debt_fcco_monthly_balances]
- [IDI_Adhoc].[clean_read_DEBT].[moj_debt_identity_fml_and_adhoc]
- [IDI_Adhoc].[clean_read_DEBT].[moj_debt_data_link] 
- [IDI_Clean].[security].[concordance]
Outputs:
- [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fcco_transactions_ready]

Description: 
Debt, debt balances, and repayment for debtors owing
Family Court Contribution Order debt to MOJ.

Intended purpose:
Identifying debtors.
Calculating number of debtors and total value of debts.
Calculating change in debts - due to borrowing or repayment.
Determining persistence of debt

Notes:
1. Date range for table [moj_debt_fcco_monthly_balances] is Jul2014 to Jan2021
	Family Court Contribution Orders were introduced in the 2014 family justice reforms.
	Hence there is no FCCO debt prior to 2014.

2. The first record closing_balance for each identity is equal to the sum of all the conponent:
   closing_balance[1] = new_debt_established[1] + repayments[1] + write_offs[1], the rest of the
   closing_balance (except first record) is the running balnce (which means that:
   closing_balance[i+1] = closing_balance[i] + new_debt_established[i+1] + repayments[i+1] + write_offs[i+1] 
   where i = 2, 3,...).

3. 12% of the fcco_file_nbr couldn't be link to the corresponding snz_uid in table [moj_fcco_debt_cases]
   14% of the fcco_file_nbr couldn't be link to the corresponding snz_uid in table [moj_fcco_debt_by_month]

4. A single person may have multiple fcco_fiel_nbrs for multiple debts or debt roles.
   After link the monthly debt to spine, only keep one record for each snz_uid (which means combine those rows with
   same snz_uid but different fcco_file_nbr)
   For debt case data, we would keep it as it is, as different fcco_file_nbr refer to deffierent debt or debt roles.

5. FML for the MoJ debt data is specific to the 20201020 refresh. To use the data on a different refresh, 
	researchers have to first join it to the 20201020 refresh and then use agency uid's to link to other refreshes.
	We used the security concordances from two refreshes to link the data to refreshes other than 20201020.
	Data exploration showed that only 69% of debtors have a snz_jus_uid. So instead of depending on a single
	agency uid we create a hybrid uid from IR, MOH, and DOL (immigration). This has >99.5% coverage.

6. Fast match loader:
   Run_key is a number that is specific to the linking of a specific dataset.
   Lhs_nbr is the let hand side identifier, also known as the snz_spine _uid.
   Rhs_nbr is the right_hand side identifier, also known as node. This is the uid that was created in the primary 
   series table related to the primary series key. 

7. Legal aid data is not included as a part of this analysis (not provided)
Issue:

Parameters & Present values:
  Fast Match Loader (FML) refresh = 20201020 can not be changed without SNZ redo-ing linking
  Current refresh = 20211020
  Prefix = defn_
  Project schema = [DL-MAA2020-01]

History (reverse order):
2021-11-12 SA revise for phase 3 persistence work
2021-09-12 FL restructure 
2021-06-18 FL comment out active debt deration period as it's out of scope; repayment indicator; persistence indicator
2021-05-07 FL including monthly incured debt and repayment 
2021-03-08 FL work begun
**************************************************************************************************/

/**************************************************************************************************
Prior to use, copy to sandpit and index
**************************************************************************************************/

DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_fcco_monthly_balances];
GO

WITH debt_source AS(
	SELECT *
		  ,EOMONTH(DATEFROMPARTS(calendar_year, RIGHT(month_nbr,2), 1)) AS month_date
		  ,COALESCE(new_debt_established, 0) + COALESCE(repayments, 0) + COALESCE(write_offs, 0)  AS  delta
	FROM [IDI_Adhoc].[clean_read_DEBT].[moj_debt_fcco_monthly_balances]
)
SELECT *
      ,SUM(delta) OVER (PARTITION BY fcco_file_nbr ORDER BY month_date) AS balance_correct
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_fcco_monthly_balances]
FROM debt_source
WHERE month_date <= '2020-12-31'
GO

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_fcco_monthly_balances] ([fcco_file_nbr]);
GO

/**************************************************************************************************
fill in records where balance is non-zero but transactions are zero
**************************************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_fcco_non_transactions];
GO

WITH 
/* list of 120 numbers of 1:120 - spt_values is a n admin table chosen as it is at least 120 row long */
n AS (
	SELECT TOP 120 ROW_NUMBER() OVER (ORDER BY type) AS x
	FROM master.dbo.spt_values
),
/* list of dates, constructed by adding list of numbers to initial date */
my_dates AS (
	SELECT TOP (DATEDIFF(MONTH, '2014-07-01', '2020-12-31') + 1) /* number of dates required */
		EOMONTH(DATEADD(MONTH, x-1, '2014-07-01')) AS my_dates
	FROM n
	ORDER BY x
),
/* get the date for each record */
debt_source AS(
	SELECT *
		,LEAD(month_date, 1, '9999-01-01') OVER (PARTITION BY fcco_file_nbr ORDER BY month_date) AS lead_date_cor
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_fcco_monthly_balances]
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
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_fcco_non_transactions]
FROM (
	/* original records */
	SELECT fcco_file_nbr
	      ,month_date
		  ,new_debt_established
		  ,repayments
		  ,write_offs
		  ,balance_correct
		  ,delta
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_fcco_monthly_balances]

	UNION ALL

	/* additional records */
	SELECT fcco_file_nbr
	      ,my_dates AS month_date
		  ,NULL AS new_debt_established
		  ,NULL AS repaymets
		  ,NULL AS write_offs
		  ,balance_correct
		  ,0 AS delta
	FROM joined
	WHERE NOT balance_correct BETWEEN -1 AND 0 -- exclude small negative balances
) k

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_fcco_non_transactions] ([fcco_file_nbr]);
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
SELECT [fcco_file_nbr],[snz_fml_8_uid]
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_fml]
FROM [IDI_Adhoc].[clean_read_DEBT].[moj_debt_fcco_identities]

SELECT [rhs_nbr],[lhs_nbr],near_exact_ind,weight_nbr,run_key
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_dl]
FROM [IDI_Adhoc].[clean_read_DEBT].[moj_debt_data_link]
WHERE run_key = 943  -- there are two run_keys as FML used twice for MoJ data
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
CREATE NONCLUSTERED INDEX fml_index ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_fml] (snz_fml_8_uid);
CREATE NONCLUSTERED INDEX lhs_rhs_index ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_dl] (rhs_nbr, lhs_nbr); -- we join by both these columns hence both need indexing
CREATE NONCLUSTERED INDEX spine_index ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_base] (snz_spine_uid) INCLUDE (hybrid_uid, type_uid); -- we join by both these columns hence both need indexing
CREATE NONCLUSTERED INDEX jus_index ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_current] (hybrid_uid, type_uid);
GO

/*************************************************************************
Create identity linking table
*************************************************************************/
--Linking MoJ debt data with fast match loader
--FML for the MoJ debt data is specific to the 20201020 refresh. 
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_fcco_debt_id];
GO

SELECT fml.[fcco_file_nbr] -- uid on debt input table
	  ,fml.[snz_fml_8_uid] -- links to dl.rhs_nbr
	  ,dl.[rhs_nbr] -- links to fml.snz_fml_8_uid
	  ,dl.[lhs_nbr] -- links to sc.snz_spine_uid
      ,sc.snz_spine_uid -- links to dl.lhr_nbr
	  --,sc.hybrid_uid -- agency uid for cross-refresh linking
	  ,sc2.snz_uid -- desired output uid
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_fcco_debt_id]
FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_fml] AS fml
LEFT JOIN [IDI_Sandpit].[DL-MAA2020-01].[tmp_dl] AS dl
ON fml.snz_fml_8_uid = dl.rhs_nbr 
LEFT JOIN [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_base] AS sc
ON dl.lhs_nbr = sc.snz_spine_uid
LEFT JOIN [IDI_Sandpit].[DL-MAA2020-01].[tmp_sc_base] AS sc2
ON sc.hybrid_uid = sc2.hybrid_uid
AND sc.type_uid = sc2.type_uid

CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_fcco_debt_id] ([fcco_file_nbr]);
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
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fcco_transactions_ready];
GO

SELECT b.snz_uid
       ,a.*
INTO  [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fcco_transactions_ready]
FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_fcco_non_transactions] a
LEFT JOIN [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_fcco_debt_id] b
ON a.fcco_file_nbr = b.fcco_file_nbr

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fcco_transactions_ready] (snz_uid);
GO
/* Compress */
ALTER TABLE [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fcco_transactions_ready] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
GO

/*****************************************************************************
Remove temporary tables
*****************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_debt_fcco_monthly_balances];
GO
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_fcco_non_transactions];
GO
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_moj_fcco_debt_id];
GO
