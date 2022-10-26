/**************************************************************************************************
Title: Beneficiary indicator
Author:  Freya Li
Reviewer: Simon Anastasiadis

Inputs & Dependencies:
- [IDI_Clean].[ir_clean].[ird_ems]
Outputs:
- [IDI_UserCode].[DL-MAA2020-01].[defn_benefit_ind]

Description:
Main benefit receipt indicator on a specific date.

Intended purpose:
Creating indicators of individuals who receiving benefit on August 2020.
 
Notes:
1) 

Parameters & Present values:
  Refresh = 20211020
  Prefix = defn_
  Project schema = [DL-MAA2020-01]
  Earliest start date = '2011-12-01'
  Latest end date = '2019-01-31'

Issues:
 
History (reverse order):
2021-06-10 SA QA
2021-04-16 FL v1
**************************************************************************************************/

/* Set database for writing views */
USE IDI_UserCode
GO

/* Clear existing view */
DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_benefit_ind];
GO

/* Create view */
CREATE VIEW [DL-MAA2020-01].[defn_benefit_ind] AS
SELECT snz_uid
       ,[ir_ems_return_period_date]
FROM [IDI_Clean_20211020].[ir_clean].[ird_ems]
WHERE ir_ems_income_source_code = 'BEN'      -- income source is benefit
AND YEAR([ir_ems_return_period_date]) BETWEEN 2011 AND 2019
GO
