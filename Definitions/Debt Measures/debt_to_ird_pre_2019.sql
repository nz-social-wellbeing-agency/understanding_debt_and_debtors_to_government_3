/**************************************************************************************************
Title: Debt to IRD Phase 3
Authors: Freya Li and Simon Anastasiadis

Inputs & Dependencies:
- [IDI_Clean].[security].[concordance]
- [IDI_Adhoc].[clean_read_DEBT].[ir_debt_transactions_202004]
Outputs:
- [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_pre_2019]

Description:
Debt, debt balances, and repayment for debtors owing money to IRD.

Intended purpose:
Identifying debtors.
Calculating number of debts and total value of debts.
Calculating change in debts - due to borrowing or repayment.
Determining persistence of debt

Notes:
1. Date range for dataset is 2010Q1 to 2018Q4. About one-tenth of debt cases arise prior to 2009. Existing balances are
	created as [dbt_running_bal] amounts without new assessments. This is consistent with debt cases beginning pre-2010.
	This table can not be easily linked to post-2018 records due to a change in IT system at IR.

2. Adhoc tables need to be linked to IDI_Clean via agency specific uid's. The IR tables link very well to the spine.

3. Numbers represent changes in debt owing. So principle is positive, repayments are negative, interest is positive when
	charges and negative if reversed/waived. Delta is the sum of the components. However, the running balances are not
	consistently the cumulative sum of the deltas. Hence the running balances have been recalculated.

4. Each identity can have multiple cases, of different tax types, and case numbers are reused between tax types and
	individuals. Hence for a unique join three columns must be used: snz_ird_uid, snz_case_number, and tax_type.

5. Debt balances are not reported every quarter. A row/record only appears in the dataset if there were transactions that
	quarter. Hence where a balance is non-zero in the absence of future records, the balance should be considered unchanged.

6. Values approximately zero that are treated as zero are minimal: More than 80% of the values under $10 are exactly $0.
	Inspection of individual records suggests that small positive balances (e.g. $4) are adjusted away to zero using negative
	account maintenance, with the account closed at $0. The account is also treated as closed on a small negative balance when
	there are no further transactions (e.g. the payment is not reversed, or penalty/interest is not applied).

7. Outliers:
	Very large positive and negative balances and delta values are observed in the data. Defining 'large values' as magnitudes
	in excess of $1 million, then a tiny fraction of people <0.1% have very large balances at any given time.

8. Spikes in balances do occur (e.g. assessed for debt of $100k one quarter, and the assessment is reversed the next quarter).
	This is most notable between 2017Q1 and 2017Q2 where it effects about 20% of people. However, it is difficult to distinguish
	these from people who are assessed one quarter and repay the next, or penalties that are applied and then reversed (as an
	incentive for repayment). Hence we have not attempted to control for such spikes.

9. There are almost 30,000 records where the same identity, case number, and tax type has multiple records on the same day.
	However, at this is less than 0.15% of our data preparation keeps all records (this is roughly equivalent to assuming that
	the case numbers should be different).

Issues:
1. There may be data missing from the table.
	- Total debt is lower than expected. An MSD & IRD debt comparison from 30 September 2016 reports total IRD debt of $6.3 billion.
		A similar estimate for 2019 was given in the cross-agency working group. However, simple calculations of debt from this data
		give values closer to $5.8 billion. (Some of this might be explained by business vs. individual customers.)

	- The number of debtors is comparable between the Sept 2016 report (350k) and the number observed in the IDI data.

	- A significant proportion of debt cases have a negative balance (-$100 or less) while the case is open. This suggests overpayment
		of debt. Many of the cases with negative balances are open at the start of 2010 when the dataset begins. And hence might be
		missing data pre-2010 or are longer duration debts with more chance to miss information.

	- Many negative balances occur at the end of a debt case where there continue to be repayments even after the debt balance goes negative.
		Fewer than 0.5% of negative debt balances have a magnitude greater than $100.

	- Our initial approach (phase 1) was to add back negative values to increase the balances. However, as we are less confident of the
		correctness of this approach and the focus of our current analysis is persistence more than total balance, we leave the values as is.

Parameters & Present values:
  Current refresh = 20211020
  Prefix = defn_
  Project schema = [DL-MAA2020-01]

History (reverse order):
2021-11-22 MR Update latest refresh (20211020)
2021-10-12 SA revise for phase 3 persistence work
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
Prior to use, copy to sandpit and index
**************************************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions];
GO

SELECT *
	,DATEFROMPARTS(
		2000 + CAST(RIGHT([date_processed],2) AS INT)
		,CASE LEFT([date_processed],3) WHEN 'JAN' THEN 1 WHEN 'APR' THEN 4 WHEN 'JUL' THEN 7 WHEN 'OCT' THEN 10 END
		,1
	) AS debt_date
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions]
FROM [IDI_Adhoc].[clean_read_DEBT].[ir_debt_transactions_202004]
GO

/* Add index  -  Use this index as matches subsequent partition by. Other indexes tried performed significantly worse. */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions] ([snz_ird_uid], [snz_case_number], [tax_type]);
GO

/**************************************************************************************************
recalculate running balances & join in snz_uid
**************************************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions_corrected];
GO

SELECT b.snz_uid
	,a.*
	,SUM([delta]) OVER (PARTITION BY a.[snz_ird_uid], [snz_case_number], [tax_type] ORDER BY debt_date) AS dbt_running_bal_corrected
INTO [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions_corrected]
FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions] AS a
LEFT JOIN [IDI_Clean_20211020].[security].[concordance] AS b
ON a.snz_ird_uid = b.snz_ird_uid
GO

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions_corrected] ([snz_uid], [snz_ird_uid], [snz_case_number], [tax_type]);
GO

/**************************************************************************************************
fill in records where balance is non-zero but transactions are zero
**************************************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_pre_2019];
GO

WITH
/* list of 200 numbers 1:200 - spt_values is an admin table chosen as it is at least 200 rows long */
n AS (
	SELECT TOP 200 ROW_NUMBER() OVER (ORDER BY type) AS x
	FROM master.dbo.spt_values
),
/* list of dates, constructed by adding list of numbers to initial date */
my_dates AS (
	SELECT TOP ((DATEDIFF(MONTH, '2010-01-01', '2018-12-31') + 1) / 3) /* number of dates required */
		CAST(DATEADD(MONTH, x-1, '2010-01-01') AS DATE) AS my_dates
	FROM n
	WHERE MONTH(DATEADD(MONTH, x-1, '2010-01-01')) IN (1, 4, 7, 10)
	ORDER BY x
),
/* get the next date for each record */
debt_source AS (
	SELECT *
		,LEAD(debt_date, 1, '9999-01-01') OVER (PARTITION BY snz_uid, snz_ird_uid, tax_type, snz_case_number ORDER BY debt_date) AS lead_month_end
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions_corrected]
),
/* join where dates from list are between current and next date --> hence dates from list are missing */
joined AS (
	SELECT *
	FROM debt_source
	INNER JOIN my_dates
	ON debt_date < my_dates
	AND my_dates < lead_month_end
)
/* combine original and additional records into same table */
SELECT *
INTO [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_pre_2019]
FROM (
	/* original records */
	SELECT snz_uid
		,snz_ird_uid
		,debt_date
		,tax_type
		,snz_case_number
		,assess
		,penalty
		,interest
		,account_maintenance
		,payment
		,remiss
		,delta
		,dbt_running_bal_corrected AS balance_corrected
	FROM [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions_corrected]

	UNION ALL

	/* additional records */
	SELECT snz_uid
		,snz_ird_uid
		,my_dates AS debt_date
		,tax_type
		,snz_case_number
		,NULL AS assess
		,NULL AS penalty
		,NULL AS interest
		,NULL AS account_maintenance
		,NULL AS payment
		,NULL AS remiss
		,0 AS delta
		,dbt_running_bal_corrected AS balance_corrected
	FROM joined
	WHERE NOT dbt_running_bal_corrected BETWEEN -10 AND 0 -- exclude small negative balances
) k

/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_pre_2019] ([snz_uid]);
GO
/* Compress */
ALTER TABLE [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_pre_2019] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
GO

/**************************************************************************************************
remove temporary tables
**************************************************************************************************/
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions];
GO
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[tmp_ir_debt_transactions_corrected];
GO
