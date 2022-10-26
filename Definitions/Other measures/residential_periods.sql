/**************************************************************************************************
Title: Residential periods
Author: Simon Anastasiadis

Inputs & Dependencies:
- [IDI_Clean].[data].[snz_res_pop]
Outputs:
- [IDI_UserCode].[DL-MAA2020-01].[defn_resident_year]

Description:
Years that a person was (estimated) redisent in New Zealand.

Intended purpose:
Indication of residency.
Filtering to residents.
 
Notes:
1) As residential population table contains residency in mid-year for each year, we assume
	people resident mid-year are residents for the whole year.

Parameters & Present values:
  Current refresh = 20211020
  Prefix = defn_
  Project schema = [DL-MAA2020-01]

Issues:
 
History (reverse order):
2021-11-22 MR Update latest refresh (20211020)
2021-10-14 SA v1
**************************************************************************************************/

/* Set database for writing views */
USE IDI_UserCode
GO

/* Clear existing view */
DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_resident_year];
GO

/* Create view */
CREATE VIEW [DL-MAA2020-01].[defn_resident_year] AS


SELECT [snz_uid]
	,DATEFROMPARTS(YEAR([srp_ref_date]), 01, 01) AS residence_start_date
	,DATEFROMPARTS(YEAR([srp_ref_date]), 12, 31) AS residence_end_date
FROM [IDI_Clean_20211020].[data].[snz_res_pop];
GO
