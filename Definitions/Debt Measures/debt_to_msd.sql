/**************************************************************************************************
Title: Debt to MSD Phase 3
Authors: Freya Li and Simon Anastasiadis

Inputs & Dependencies:
- [IDI_Adhoc].[clean_read_DEBT].[msd_debt_30sep]
- [IDI_Clean].[security].[concordance]
Outputs:
- [IDI_Sandpit].[DL-MAA2020-01].[defn_msd_non_transactions]

Description:
Debt, debt balances, and repayment for debtors owing money to MSD.

Intended purpose:
Identifying debtors.
Calculating number of debts and total value of debts.
Calculating change in debts - due to borrowing or repayment.
Determining persistence of debt

Notes:
1. Date range for table [msd_debt_30sep] is 2009-01-01 to 2020-09-01. Existing balances
	are created as new principle (amount incurred) on the opening date.

2. Numbers represent changes in debt owing. So principle is positive, repayments and
	write-offs are negative. A small number of repayments and write-offs are positive
	we assume these are reversals - and they increase debt in the same way as principle.

3. MSD debt data available at time of analysis does not classify MSD debt by type. Our method for classifying MSD debt into
	overpayment and recoverable assistance using T3 payments can be seen in the code for phase 2.
	We are not currently classifying MSD debt by type for phase 3.

4. Outlier values
	Large transactions (>$10k) make up a tiny proportion of transactions (less than 0.1%) and effect a small number of people
	(less than 3%) but are a significant amount of total amounts incurred (22%). Current hypothesis is that such amounts are
	fraud (which is part of the dataset).
	Conversation with MSD data experts suggests these amounts are most likely related to fraud.

5. Values approx 0 that should be zero is of minimal concern. Less than 0.5% of people ever have an absolute debt balance of
	1-5 cents. As values are dollars, all numbers should be rounded to 2 decimal places.
	
6. Spikes - Yes there are occurrences where people's balances change in a spike pattern (a large change, immediately reversed)
	and the value of the change exceeds $5k. There are less than 6,000 instances of this in the dataset, about 0.01% of records.
	The total amount of money involved is less than 0.2% of total debt. Hence this pattern is not of concern.

7. We would expect that a debtor's balance is always non-negative. But, some identities have dates on which their net amount owing
	is negative. About 6000 people have negative amounts owing at some point. Inspection of the data suggests that this happens
	when repayments exceed debt, and rather than withdraw the excess, the amount is of debt is left negative until the individual
	borrows again. It is common to observe negative balances that later on become zero balances. We would not observe this if the
	negative balances implied that some debt incurred was not recorded in the dataset.

Issues:

Parameters & Present values:
  Current refresh = 20211020
  Prefix = defn_
  Project schema = [DL-MAA2020-01]

History (reverse order):
2021-11-22 MR Update latest refresh (20211020)
2021-10-12 SA revise for phase 3 persistence work
2021-10-06 SA merge relevant part of debt_to_msd_p2_split.sql in to reduce duplication
2021-09-08 FL restructure MSD debt data begun
**************************************************************************************************/

/**************************************************************************************************
Prior to use, copy to sandpit and index
**************************************************************************************************/

IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_30sep]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_30sep];
GO

SELECT b.snz_uid
	  ,a.[snz_msd_uid]
      ,a.[debt_as_at_date]
	  ,SUM(ISNULL(a.[amount_incurred], 0)) AS [amount_incurred]
	  ,SUM(ISNULL(a.[amount_repaid], 0)) AS [amount_repaid]
	  ,SUM(ISNULL(a.[amount_written_off], 0)) AS [amount_written_off]
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_30sep]
FROM [IDI_Adhoc].[clean_read_DEBT].[msd_debt_30sep] AS a
INNER JOIN [IDI_Clean_20211020].[security].[concordance] AS b -- adding snz_uid 
ON a.snz_msd_uid = b.snz_msd_uid
GROUP BY b.snz_uid, a.snz_msd_uid, a.debt_as_at_date -- some identities have more than one record per month, hence collapse

/* Index by snz_uid (improve efficiency) */
CREATE NONCLUSTERED INDEX snz_uid_index ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_30sep] (snz_uid); 

/**************************************************************************************************
Recalculate running balances
**************************************************************************************************/

IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_prep]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_prep];
GO

WITH calculate_delta AS (
	SELECT *
		  ,amount_incurred + amount_repaid + amount_written_off AS delta
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_30sep]
)
SELECT *
	  ,SUM(delta) OVER (PARTITION BY snz_uid ORDER BY debt_as_at_date) AS balance_correct
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_prep]
FROM calculate_delta

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_prep] (snz_uid);
GO

/**************************************************************************************************
fill in records where balance is non-zero but transactions are zero
**************************************************************************************************/
IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[defn_msd_non_transactions]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[defn_msd_non_transactions];
GO

WITH
/* list of 1000 numbers 1:1000 - spt_values is an admin table chosen as it is at least 1000 row long */
n AS (
	SELECT TOP 1000 ROW_NUMBER() OVER (ORDER BY type) AS x
	FROM master.dbo.spt_values
),
/* list of dates, constructed by adding list of numbers to initial date */
my_dates AS (
	SELECT TOP (DATEDIFF(MONTH, '2009-01-01', '2020-09-01') + 1) /* number of dates required */
		 EOMONTH(DATEADD(MONTH, x-1, '2009-01-01')) AS my_dates
	FROM n
	ORDER BY x
),
/* get the next date for each record */
debt_source AS (
	SELECT *
		,LEAD(debt_as_at_date, 1, '9999-01-01') OVER (PARTITION BY snz_uid  ORDER BY debt_as_at_date) AS lead_date
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_prep]
),
/* join where dates from list are between current and next date --> hence dates from list are missing */
joined AS (
	SELECT *
	FROM debt_source
	INNER JOIN my_dates
	ON EOMONTH(debt_as_at_date) < my_dates
	AND my_dates < EOMONTH(lead_date)
)
/* combine original and additional records into same table */
SELECT *
INTO [IDI_Sandpit].[DL-MAA2020-01].[defn_msd_non_transactions]
FROM (
	/* original records */
	SELECT snz_uid
		,snz_msd_uid
		,debt_as_at_date
		,ROUND(amount_incurred, 2) AS amount_incurred
		,ROUND(amount_repaid, 2) AS amount_repaid
		,ROUND(amount_written_off, 2) AS amount_written_off
		,ROUND(delta, 2) AS delta
		,ROUND(balance_correct, 2) AS balance_correct
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_prep]

	UNION ALL

	/* additional records */
	SELECT snz_uid
		,snz_msd_uid
		,my_dates AS debt_as_at_date
		,NULL AS amount_incurred
		,NULL AS amount_repaid
		,NULL AS amount_written_off
		,0 AS delta
		,ROUND(balance_correct, 2) AS balance_correct
	FROM joined
	WHERE NOT balance_correct BETWEEN -10 AND 2 -- exclude small negative balances, the BETWEEN operator is inclusive
) k

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[defn_msd_non_transactions] ([snz_uid]);
GO

/**************************************************************************************************
remove temporary tables
**************************************************************************************************/

IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_30sep]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_30sep];
GO

IF OBJECT_ID('[IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_prep]','U') IS NOT NULL
DROP TABLE [IDI_Sandpit].[DL-MAA2020-01].[tmp_msd_debt_prep];
GO
