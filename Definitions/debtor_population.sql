/**************************************************************************************************
Title: Debtor population
Author: Simon Anastasiadis

Inputs & Dependencies:
- debt_to_msd.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_msd_non_transactions]
- debt_to_moj_fines.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fines_transactions_ready]
- debt_to_moj_fcco.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fcco_transactions_ready]
- debt_to_ird_pre_2019.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_pre_2019]
- debt_to_ird_post_2018.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_post_2018]

Outputs:
- [IDI_Sandpit].[DL-MAA2020-01].[pop_all_debtors]

Description:
List of snz_uid values for those identities that own debt to MSD, MoJ, or IR
between 2012 and 2020 inclusive.

Intended purpose:
Producing summary statistics for all debtors.

Notes:

Parameters & Present values:
  Current refresh = 20211020
  Prefix = pop_
  Project schema = [DL-MAA2020-01]
   
Issues:
 
History (reverse order):
2021-11-29 MR Revise for phase 3 persistence work
2021-10-12 SA v1
**************************************************************************************************/

DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[pop_all_debtors];
GO

SELECT DISTINCT snz_uid
INTO [IDI_Sandpit].[DL-MAA2020-01].[pop_all_debtors]
FROM (
	SELECT snz_uid
	FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_msd_non_transactions]
	WHERE YEAR(debt_as_at_date) BETWEEN 2012 AND 2020

	UNION ALL

	SELECT snz_uid
	FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fines_transactions_ready]
	WHERE YEAR(month_date) BETWEEN 2012 AND 2020

	UNION ALL

	SELECT snz_uid
	FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_moj_fcco_transactions_ready]
	WHERE YEAR(month_date) BETWEEN 2012 AND 2020

	UNION ALL 

	SELECT snz_uid
	FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_pre_2019]
	WHERE YEAR(debt_date) BETWEEN 2012 AND 2020

	UNION ALL 

	SELECT snz_uid
	FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_ird_non_transactions_post_2018]
	WHERE YEAR(month_end) BETWEEN 2012 AND 2020
) AS k

CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[pop_all_debtors] ([snz_uid]);
GO
