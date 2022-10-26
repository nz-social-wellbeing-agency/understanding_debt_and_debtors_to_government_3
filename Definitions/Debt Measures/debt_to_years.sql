/**************************************************************************************************
Title: Debt to yearly views Phase 3
Authors: Simon Anastasiadis

Inputs & Dependencies:
- debt_to_msd.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_msd_non_transactions]
- debt_to_moj_fines.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fines_transactions_ready]
- debt_to_moj_fcco.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fcco_transactions_ready]
- debt_to_ird_pre_2019.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_pre_2019]
- debt_to_ird_post_2018.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_post_2018]
Outputs:
- [IDI_UserCode].[DL-MAA2020-01].[defn_msd_debt_by_years]
- [IDI_UserCode].[DL-MAA2020-01].[defn_moj_fines_debt_by_years]
- [IDI_UserCode].[DL-MAA2020-01].[defn_moj_fcco_debt_by_years]
- [IDI_UserCode].[DL-MAA2020-01].[defn_ird_pre_2019_debt_by_years]
- [IDI_UserCode].[DL-MAA2020-01].[defn_ird_post_2018_debt_by_years]

Description:
Debt, debt balances, and transactions by year for each debt type.

Intended purpose:
Identifying debtors.
Calculating number of debts and total value of debts.
Calculating change in debts - due to borrowing or repayment.
Determining persistence of debt

Notes:
1. For collapsing to years we sum all transaction amounts and keep the balance at the end of
	the year.

Issues:

Parameters & Present values:
  Prefix = defn_
  Project schema = [DL-MAA2020-01]

History (reverse order):
2021-12-15 MR Updated the labels
2021-11-29 MR Revise for phase 3 persistence work
2021-10-19 SA v1
**************************************************************************************************/

Use IDI_UserCode
GO

/**************************************************************************************************
MSD debt
**************************************************************************************************/

DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_msd_debt_by_years]
GO

CREATE VIEW [DL-MAA2020-01].[defn_msd_debt_by_years] AS
SELECT snz_uid
	,snz_msd_uid
	,DATEFROMPARTS(YEAR(debt_as_at_date), 12, 01) AS year_date
	,SUM(ISNULL(amount_incurred, 0)) AS amount_incurred
	,SUM(ISNULL(amount_repaid, 0)) AS amount_repaid
	,SUM(ISNULL(amount_written_off, 0)) AS amount_written_off
	,SUM(ISNULL(delta, 0)) AS delta
	,SUM(IIF(
		(YEAR(debt_as_at_date) <> 2020 AND MONTH(debt_as_at_date) = 12) /* take December value for each year except 2020 */
		OR (YEAR(debt_as_at_date) = 2020 AND MONTH(debt_as_at_date) = 9) /* take September value of 2020 as Dec value not available */
		, balance_correct, 0)) AS balance_correct
	,MAX(CONCAT('msd_repaid_', YEAR(debt_as_at_date))) AS repayment_label
	,MAX(CONCAT('msd_principle_', YEAR(debt_as_at_date))) AS principle_label
	--,MAX(CONCAT('msd_delta_', YEAR(debt_as_at_date))) AS delta_label
	,MAX(CONCAT('msd_balance_', YEAR(debt_as_at_date))) AS balance_label
FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_msd_non_transactions]
GROUP BY snz_uid, snz_msd_uid, YEAR(debt_as_at_date)
GO

/**************************************************************************************************
MoJ Fines debt 
**************************************************************************************************/

DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_moj_fines_debt_by_years]
GO

CREATE VIEW [DL-MAA2020-01].[defn_moj_fines_debt_by_years] AS
SELECT snz_uid
	,moj_ppn
	,DATEFROMPARTS(YEAR(month_date), 12 , 01) AS year_date
	,SUM(ISNULL(impositions, 0)) AS impositions
	,SUM(ISNULL(penalty, 0)) AS penalty
	,SUM(ISNULL(payment, 0)) AS payment
	,SUM(ISNULL(remittals, 0)) AS remittals
	,SUM(ISNULL(payment_reversal, 0)) AS payment_reversal
	,SUM(ISNULL(remittal_reversal, 0)) AS remittal_reversal
	,SUM(ISNULL(delta, 0)) AS delta
	,SUM(IIF(MONTH(month_date) = 12, balance_correct, 0)) AS balance_correct
	,MAX(CONCAT('moj_fine_repaid_', YEAR(month_date))) AS repayment_label
	,MAX(CONCAT('moj_fine_principle_', YEAR(month_date))) AS principle_label
	--,MAX(CONCAT('moj_fine_delta_', YEAR(month_date))) AS delta_label
	,MAX(CONCAT('moj_fine_balance_', YEAR(month_date))) AS balance_label
FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fines_transactions_ready]
GROUP BY snz_uid, moj_ppn, YEAR(month_date)
GO

/**************************************************************************************************
MoJ FCCO debt 
**************************************************************************************************/

DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_moj_fcco_debt_by_years]
GO

CREATE VIEW [DL-MAA2020-01].[defn_moj_fcco_debt_by_years] AS
SELECT snz_uid
	,fcco_file_nbr
	,DATEFROMPARTS(YEAR(month_date), 12 , 01) AS year_date
	,SUM(ISNULL(new_debt_established, 0)) AS impositions
	--,SUM(ISNULL(penalty, 0)) AS penalty
	,SUM(ISNULL(repayments, 0)) AS payment
	,SUM(ISNULL(write_offs, 0)) AS remittals
	--,SUM(ISNULL(payment_reversal, 0)) AS payment_reversal
	--,SUM(ISNULL(remittal_reversal, 0)) AS remittal_reversal
	,SUM(ISNULL(delta, 0)) AS delta
	,SUM(IIF(MONTH(month_date) = 12, balance_correct, 0)) AS balance_correct
	,MAX(CONCAT('moj_fcco_repaid_', YEAR(month_date))) AS repayment_label
	,MAX(CONCAT('moj_fcco_principle_', YEAR(month_date))) AS principle_label
	--,MAX(CONCAT('moj_fcco_delta_', YEAR(month_date))) AS delta_label
	,MAX(CONCAT('moj_fcco_balance_', YEAR(month_date))) AS balance_label
FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fcco_transactions_ready]
GROUP BY snz_uid, fcco_file_nbr, YEAR(month_date)
GO

/**************************************************************************************************
IRD debt (Pre 2019)
**************************************************************************************************/

DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_ird_pre_2019_debt_by_years]
GO

CREATE VIEW [DL-MAA2020-01].[defn_ird_pre_2019_debt_by_years] AS
WITH recode_tax_types AS (
	SELECT *
		,CASE tax_type
			WHEN 'Child Support' THEN 'child'
			WHEN 'Families' THEN 'wff'
			WHEN 'GST' THEN 'oth'
			WHEN 'Income Tax' THEN 'income'
			WHEN 'Other' THEN 'oth'
			WHEN 'PAYE' THEN 'oth'
			WHEN 'Student Loan' THEN 'student'
			ELSE 'mistake' END
		AS new_tax_type
	FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_pre_2019]
)
SELECT snz_uid
	,snz_ird_uid
	,DATEFROMPARTS(YEAR(debt_date), 12 , 01) AS year_date
	,tax_type
	,new_tax_type
	,ISNULL(assess, 0) AS assess
	,ISNULL(penalty, 0) AS penalty
	,ISNULL(interest, 0) AS interest
	,ISNULL(account_maintenance, 0) AS account_maintenance
	,ISNULL(payment, 0) AS payment
	,ISNULL(remiss, 0) AS remiss
	,ISNULL(delta, 0) AS delta
	,IIF(MONTH(debt_date) = 10, balance_corrected, 0) AS balance_corrected /* use Nov as pre2019 is quarterly */
	,CONCAT('ird_pr19_principle_', new_tax_type, '_', YEAR(debt_date)) AS principle_label
	,CONCAT('ird_pr19_repaid_', new_tax_type, '_', YEAR(debt_date)) AS repayment_label
	--,CONCAT('ird_pr19_delta', new_tax_type, '_', YEAR(debt_date)) AS delta_label
	,CONCAT('ird_pr19_balance_', new_tax_type, '_', YEAR(debt_date)) AS balance_label
FROM recode_tax_types;
GO
/**************************************************************************************************
IRD debt (Post 2018)
**************************************************************************************************/

DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_ird_post_2018_debt_by_years]
GO

CREATE VIEW [DL-MAA2020-01].[defn_ird_post_2018_debt_by_years] AS
WITH recode_tax_types AS (
	SELECT *
		,CASE tax_type_label
			--WHEN 'Child Support' THEN 'child'
			WHEN 'Families' THEN 'wff'
			WHEN 'GST' THEN 'oth'
			WHEN 'Income_Tax' THEN 'income'
			WHEN 'Other' THEN 'oth'
			--WHEN 'PAYE' THEN 'oth'
			WHEN 'Student_Loans' THEN 'student'
			WHEN 'Donation_Tax_Credits' THEN 'oth'
			WHEN 'Employment_Activities' THEN 'oth'
			WHEN 'Receiving_Carer' THEN 'oth'
			WHEN 'Liable_Parent' THEN 'child'
			ELSE 'mistake' END
		AS new_tax_type
	FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_post_2018]
)
SELECT snz_uid
	,snz_ird_uid
	,DATEFROMPARTS(YEAR(month_end), 12 , 01) AS year_date
	,tax_type_label
	,new_tax_type
	,ISNULL(assess, 0) AS assess
	,ISNULL(penalty, 0) AS penalty
	,ISNULL(interest, 0) AS interest
	,ISNULL(account_maintenance, 0) AS account_maintenance
	,ISNULL(payment, 0) AS payment
	,ISNULL(remiss, 0) AS remiss
	,ISNULL(delta, 0) AS delta
	,IIF(MONTH(month_end) = 12, running_balance_tax_type_num, 0) AS balance_corrected
	,CONCAT('ird_ps18_principle_', new_tax_type, '_', YEAR(month_end)) AS principle_label
	,CONCAT('ird_ps18_repayment_', new_tax_type, '_', YEAR(month_end)) AS repayment_label
	--,CONCAT('ird_ps18_delta_', new_tax_type, '_', YEAR(month_end)) AS delta_label
	,CONCAT('ird_ps18_balance_', new_tax_type, '_', YEAR(month_end)) AS balance_label
FROM recode_tax_types;
GO