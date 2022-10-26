/**************************************************************************************************
Title: Debt to IRD Phase 2
Author: Freya Li
Reviewer: Simon Anastasiadis

Acknowledgement: Part of the code is took from Simon's code for D2G phase 1.

Inputs & Dependencies:
- [IDI_Clean].[security].[concordance]
- [IDI_Adhoc].[clean_read_DEBT].[ir_debt_transactions]
- [IDI_Adhoc].[clean_read_DEBT].[ir_debt_transactions_student_202105]
Outputs:
- [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_post_2018]

Description:
Debt, debt balances, and repayment for debtors owing money to IRD.

Intended purpose:
Identifying debtors.
Calculating number of debts and total value of debts.
Calculating change in debts - due to borrowing or repayment.

Notes:
1. As tables are adhoc they need to be linked to IDI_Clean via agency specific uid's. Both IRD tables exhibit excellent correspondence
	to one-another and both link very well to the spine.

2. We observe student loan debt provided by IR in the table [ir_debt_transactions] to contain errors.
	- Double the amount of debt as reported outside the IDI and as observed in [sla_clean].[ird_overdue_debt]
    - About half of all people with overdue student loan debt have multiple debt cases (much higher than all the other debt types).
    However, the number of people with debt is approximately consistent.

	Cause - due to a processing error IR included both overdue AND non-overdue balance for some debtors.
	Fix - IR provided fresh overdue student loan information in table [ir_debt_transactions_student_202105].
		We replace student loan debt in [ir_debt_transactions] with the debt in this table.
	Outstanding concerns - fix address majority of problem but still some differences from numbers reported outside the IDI.

3. Some of the different debt types have different starting points.
   This is because of changes in IR's computer system between 2018 and 2020.
   It is not due to data missing from the table.
   Jordan's (IRD) response about this: There have been several releases as Inland Revenue are migrating completely over to our new tax system, 
   START. One of these key release dates was in April-2020 and specifically that was when the Income Tax type product was converted over.
   The collection case begin dates are correct if they were opened from before Jan-2019 but any specific historical transaction data is 
   essentially rolled up into a single converted transaction line item dated 20th April 2019. This is why it appears to have data missing 
   and only start from then.
      tax_type_group		start date  end date
	Donation Tax Credits	2019-04-30	2020-11-30
	Employment Activities	2019-01-31	2020-11-30
	Families				2019-04-30	2020-11-30
	GST						2019-01-31	2020-11-30
	Income Tax				2019-04-30	2020-11-30
	Liable Parent			2019-01-31	2020-11-30
	Other					2019-01-31	2020-11-30
	Receiving Carer			2019-01-31	2020-11-30
	Student Loans			2020-04-30	2020-11-30

4. Date range for dataset is 2019-01-31 to 2020-11-30. Debt existing prior to the start date (of the tax-type group) appears in different ways:
	- In some cases (e.g. Liable Parent) existing debt appears in the running balance and in none of the transaction records.
	- In some cases (e.g. Income tax) existing debt appears as amounts assessed
	Hence the data is not well suited to identifying 2018 debt balances or how much debt is new principle in 2019.

5. Numbers represent changes in debt owing. So principle is positive, repayments are negative, interest is positive when charges and negative
	if reversed/waived. Around 2% assesses are negative. delta is the sum of the components

6. Each identity can have multiple cases, of different tax types, and case numbers are reused between tax types and individuals. 

7. Outliers:
	Very large positive and negative balances and delta values are observed in the data. Defining 'large values' as magnitudes in excess
	of $1 million, then a tiny fraction of people <0.1% have very large balances at any given time

8. We consider a debt case is closed if the balance for a snz_uid within a snz_case_number and tax_type_group turns to 0.
	2% of the records has negative balance, only 0.4% of debt cases have negative debt balances of at least $100
	Negative balances are assumed to indicate that a person has overpaid. They may be repaid by IR, or may allocate their
	repayment against a different debt.

9. There are around 3000 cases where the same identity has two (or more) records for the same month, tax type, and case number.
	This primariy affects Liable Parent debt. Our approach keeps all records, which is equivalent to assuming that the case
	numbers should be different.

Issues:
1. [running_balance_case] from table [ir_debt_transactions] is not correctly calculated, it doesn't sum up the running
   balance across the different tax type group with in a debt case. We depend on [running_balance_tax_type] instead. 

Parameters & Present values:
  Current refresh = 20211020
  Prefix = defn_
  Project schema = [DL-MAA2020-01]
  Earliest debt date = '2019-01-01'
  Latest debt date = '2020-12-31'

History (reverse order):
2021-11-22 MR Update latest refresh (20211020)
2021-11-16 SA update for phase 3 persistence
2021-07-13 SA insert records when there are no transactions
2021-06-18 FL comment out active cases part as it is out of scope; add repayment indicator, debt persistence indicator
2021-06-11 SA update notes on student loan debt and faster loading/setup
2021-05-26 FL IR reload the student loan debt, replace the student loan records with the new table
2021-04-13 FL removing wrong student loan debt
2021-02-02 FL v3
2021-01-26 SA QA
2021-01-13 FL v2 Create monthly time series
2021-01-06 FL (update to the latest refresh, make necessary changes to the code)
2020-11-25 FL QA
2020-07-02 SA update following input from SME
2020-06-23 SA v1
**************************************************************************************************/

/**************************************************************************************************
set location for views once at start
**************************************************************************************************/
USE [IDI_UserCode]
GO

/**************************************************************************************************
Prior to use, copy to sandpit and index
(runtime 2 minutes)
**************************************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions];
GO

SELECT *
	,REPLACE(tax_type_group, ' ', '_') AS tax_type_label
	,CAST(REPLACE(REPLACE([running_balance_tax_type],'$',''), ',' , '') AS NUMERIC(10,2)) AS [running_balance_tax_type_num] -- convert $text to numeric
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions]
FROM (

	/* All debt in base table excluding faulty student loan records */
	SELECT *
	FROM [IDI_Adhoc].[clean_read_DEBT].[ir_debt_transactions]
	WHERE tax_type_group <> 'Student Loans'

	UNION ALL

	/* Corrected student loan debt records */
	SELECT *
	FROM [IDI_Adhoc].[clean_read_DEBT].[ir_debt_transactions_student_202105]
) k

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions] ([snz_ird_uid], [tax_type_group]);
GO

/**************************************************************************************************
fill in records where balance is non-zero but transactions are zero
**************************************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_ird_non_transactions];
GO

WITH
/* list of 100 numbers 1:100 - spt_values is an admin table chosen as it is at least 100 rows long */
n AS (
	SELECT TOP 100 ROW_NUMBER() OVER (ORDER BY type) AS x
	FROM master.dbo.spt_values
),
/* list of dates, constructed by adding list of numbers to initial date */
my_dates AS (
	SELECT TOP (DATEDIFF(MONTH, '2019-01-01', '2020-12-31') + 1) /* number of dates required */
		EOMONTH(DATEADD(MONTH, x-1, '2019-01-01')) AS my_dates
	FROM n
	ORDER BY x
),
/* get the next date for each record */
debt_source AS (
	SELECT *
		,LEAD(month_end, 1, '9999-01-01') OVER (PARTITION BY snz_ird_uid, tax_type_label, snz_case_number ORDER BY month_end) AS lead_month_end
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions]
),
/* join where dates from list are between current and next date --> hence dates from list are missing */
joined AS (
	SELECT *
	FROM debt_source
	INNER JOIN my_dates
	ON month_end < my_dates
	AND my_dates < lead_month_end
)
/* combine original and additional records into same table */
SELECT *
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_ird_non_transactions]
FROM (
	/* original records */
	SELECT snz_ird_uid
		,snz_case_number
		,month_end
		,tax_type_label
		,assess
		,penalty
		,interest
		,account_maintenance
		,payment
		,remiss
		,delta
		,running_balance_tax_type_num
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions]

	UNION ALL

	/* additional records */
	SELECT snz_ird_uid
		,snz_case_number
		,my_dates AS month_end
		,tax_type_label
		,NULL AS assess
		,NULL AS penalty
		,NULL AS interest
		,NULL AS account_maintenance
		,NULL AS payment
		,NULL AS remiss
		,0 AS delta
		,running_balance_tax_type_num
	FROM joined
	WHERE NOT running_balance_tax_type_num BETWEEN -10 AND 0 -- exclude small negative balances
) k

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_ird_non_transactions] ([snz_ird_uid]);
GO

/**************************************************************************************************
join on snz_uid
**************************************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_post_2018];
GO

SELECT b.snz_uid
	,a.*
INTO [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_post_2018]
FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_ird_non_transactions] AS a
LEFT JOIN [IDI_Clean_20211020].[security].[concordance] AS b
ON a.snz_ird_uid = b.snz_ird_uid

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_post_2018] (snz_uid);
GO
/* Compress */
ALTER TABLE [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_post_2018] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
GO

/**************************************************************************************************
remove temporary tables
**************************************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions];
GO
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_ird_non_transactions];
GO