/*******************************************************************************************************************************************************
Title: Dependent Children Indicator - DIA and Census-2018
Author: Manjusha Radhakrishnan 
Reviewer: Simon Anastasiadis

Inputs & Dependencies:
- [data].[personal_detail]
- [data].[address_notification]
- [cen_clean].[census_individual_2018]

Outputs:
- [IDI_Sandpit].[DL-MAA2020-01].[defn_dep_child]

Description:
The time period during which a person has living children under the age of 18.

Intended purpose:
- Counting the number of dependent children that a person has at the specified point in time.

Notes:
1) DIA births do not capture overseas births and so relying on only DIA will miss many parent-child relationships. 
   Hence it is important to do this extra work.
2) Record Type - Census 18:
	3 NZ Adult
	4 NZ Child
3) Family Role Code - Census 18:
	11 Parent (other than grandparent) and spouse/partner in a family nucleus
	21 Sole parent (other than sole grandparent) in a family nucleus
	31 Spouse/partner only in a family nucleus
	41 Child in a family nucleus (birth/biological, step, adopted and foster parent)
	42 Child in a family nucleus (other parent)
	51 Person under 15 in a household of people under 15
4) Role 31 is excluded as they dont have any dependent child linked in a family nucleus; 
   Role 51 is excluded because they are not our point of interest.
5) Family ID is prefered over the Dwell ID to link the parent and child because
   it was found that multiple families with dependent children were living in the 
   same dwelling; Hence, linking using dwell ID generated a huge list of dependent 
   children with multiple unrelated parents.
6) It is noted that the number of dependent children in a household exceeds 100. This could be because of linkage error.
   Hence the maximum number of dependent children in a household is limited to 5 as it covers 98.7% of the household population.
7) Duplicates for correctness is ensured when DIA and census data is combined.
8) A proxy birthdate where birth month and year are retained but the middle day of each month is set as the proxy day i.e. on the 15th or 14th (for February). 
   This column makes age calculation at specific points in time easier. (Sourced from the IDI Derived Population Data Dictionary)

Limitations:
- There is no control for whether a person is involved in the care of their dependent child. 

Parameters & Present values:
  Current refresh = 20211020
  Prefix = defn_
  Project schema = [DL-MAA2020-01]
  Age no longer dependent = 18
  Max no of dependent children in a household = 5
  Date of interest = 2020-12-15

History (reverse order):
2022-01-11 MR v1 refine and merge
2021-12-22 SA review
2021-12-20 MR v0 initial design
*******************************************************************************************************************************************************/
/*** DIA BIRTHS ***/
USE [IDI_UserCode]
GO

/* Clear before creation */
DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_dia_chld]
GO

CREATE VIEW [DL-MAA2020-01].[defn_dia_chld] AS
	WITH temp_dia_dep_child AS (
		SELECT [snz_parent1_uid]
			,[snz_parent2_uid]
			,[snz_uid] AS [snz_child_uid]
		FROM [IDI_Clean_20211020].[data].[personal_detail]
		WHERE [snz_uid] IS NOT NULL
	)
	SELECT *
		, 'DIA' AS data_src
	FROM (
		SELECT [snz_parent1_uid] AS [snz_parent_uid]
			,[snz_child_uid]
		FROM temp_dia_dep_child
		WHERE [snz_parent1_uid] IS NOT NULL

		UNION ALL

		SELECT [snz_parent2_uid] AS [snz_parent_uid]
			,[snz_child_uid]
		FROM temp_dia_dep_child
		WHERE [snz_parent2_uid] IS NOT NULL
		AND [snz_parent1_uid] <> [snz_parent2_uid] -- parents are different
	) k	
GO
/*******************************************************************************************************************************************************/
/*** CENSUS18 BIRTHS ***/
/* Clear before creation */
DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_cen18_chld]
GO

CREATE VIEW [DL-MAA2020-01].[defn_cen18_chld] AS
	WITH 
	/* Families with dependent children */
	temp_fam_with_child AS(
		SELECT snz_cen_fam_uid
				, snz_uid
				, cen_ind_family_role_code 
		FROM [IDI_Clean_20211020].[cen_clean].[census_individual_2018]
		WHERE cen_ind_record_type_code IN (3,4) -- Only people who were available in NZ on the census night
			AND cen_ind_family_role_code IN (11,21,41,42) -- Only parent, child roles
			AND snz_uid IS NOT NULL
	),

	/* List of all parents with dependent children */
	parents AS (
		SELECT snz_cen_fam_uid
		, snz_uid AS snz_parent_uid
		FROM temp_fam_with_child
		WHERE cen_ind_family_role_code IN (11,21)
	),

	/* list of all dependent children */
	dep_children AS (
		SELECT snz_cen_fam_uid
		, snz_uid AS snz_child_uid
		FROM temp_fam_with_child
		WHERE cen_ind_family_role_code IN (41,42)
	)

	/* Link the parent and their respective dependent children */
	SELECT a.snz_parent_uid
		, b.snz_child_uid
		, 'CEN' AS data_src
	FROM parents AS a
	LEFT JOIN dep_children AS b
	ON a.snz_cen_fam_uid = b.snz_cen_fam_uid
GO
/*******************************************************************************************************************************************************/
/* Clear before creation */
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[temp_dep_child]
GO

/* Parent - Child not born in NZ added to DIA births */
SELECT * 
INTO [IDI_Sandpit].[DL-MAA2020-01].[temp_dep_child]
FROM [IDI_UserCode].[DL-MAA2020-01].[defn_dia_chld]
	
UNION ALL
	
SELECT * 
FROM [IDI_UserCode].[DL-MAA2020-01].[defn_cen18_chld];
GO

/*******************************************************************************************************************************************************/
/* Remove all the census records which were already recorded in DIA */
WITH cte AS (
	SELECT *
		, ROW_NUMBER() OVER (PARTITION BY snz_parent_uid, snz_child_uid ORDER BY snz_parent_uid) AS row_num
	FROM [IDI_Sandpit].[DL-MAA2020-01].[temp_dep_child]
)
DELETE FROM cte WHERE row_num > 1
/*******************************************************************************************************************************************************/
/* Clear before creation */
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[defn_dep_child]
GO

WITH dependency AS (
	SELECT an.snz_idi_address_register_uid 
		, a.snz_parent_uid
		, a.snz_child_uid
		, b.snz_birth_date_proxy AS [start_date]
		, IIF(EOMONTH(DATEFROMPARTS(b.[snz_birth_year_nbr] + 18, b.[snz_birth_month_nbr], 15)) -- [18th_birthday]
				> EOMONTH(DATEFROMPARTS(b.[snz_deceased_year_nbr], b.[snz_deceased_month_nbr], 28)), -- [death_day]
				EOMONTH(DATEFROMPARTS(b.[snz_deceased_year_nbr], b.[snz_deceased_month_nbr], 28))
		, EOMONTH(DATEFROMPARTS(b.[snz_birth_year_nbr] + 18, b.[snz_birth_month_nbr], 15))) AS [end_date]
		, IIF(DATEDIFF(MONTH, b.[snz_birth_date_proxy], '2020-12-15') <= 12*18, 1, 0) AS dependency_ind
		, a.data_src
	FROM [IDI_Sandpit].[DL-MAA2020-01].[temp_dep_child] AS a
	LEFT JOIN [IDI_Clean_20211020].[data].[address_notification] AS an
	ON an.snz_uid = a.snz_parent_uid
	LEFT JOIN [IDI_Clean_20211020].[data].[personal_detail] AS b
	ON a.snz_child_uid = b.snz_uid
	WHERE an.[snz_idi_address_register_uid] IS NOT NULL 
		AND '2020-12-15' BETWEEN an.[ant_notification_date] AND an.[ant_replacement_date]
		AND [snz_birth_year_nbr] IS NOT NULL
		AND [snz_birth_month_nbr] IS NOT NULL
		AND [snz_birth_year_nbr] <> 9999
),

temp_dep_child AS (
	SELECT a.snz_idi_address_register_uid 
	, a.snz_parent_uid
	, a.snz_child_uid
	, a.[start_date] AS start_dependency
	, a.end_date AS end_dependency
	--, SUM(a.dependency_ind) OVER(PARTITION BY a.snz_parent_uid) AS total_dep_child_ind -- Number of dependent children for each parent
	--, MAX(YEAR(a.start_date)) OVER(PARTITION BY a.snz_parent_uid) AS max_child_yr -- Youngest Child - Birth Year
	--, MIN(YEAR(a.start_date)) OVER(PARTITION BY a.snz_parent_uid) AS min_child_yr -- Oldest Child - Birth Year
	, a.data_src
	FROM dependency AS a
	WHERE dependency_ind = 1 -- Include only dependent children at 2020-12-15 
	AND a.[start_date] <= a.end_date
),

temp_dep_child_hhld AS (
	SELECT snz_idi_address_register_uid
		   , COUNT(DISTINCT snz_child_uid) AS tot_dep_child_hhld
	FROM temp_dep_child
	GROUP BY snz_idi_address_register_uid
)

SELECT a.snz_idi_address_register_uid 
	, a.snz_parent_uid AS snz_uid
	, a.snz_child_uid
	, a.start_dependency
	, a.end_dependency
	--, a.total_dep_child_ind
	--, a.max_child_yr
	--, a.min_child_yr
	--, b.tot_dep_child_hhld
	, a.data_src
INTO [IDI_Sandpit].[DL-MAA2020-01].[defn_dep_child]
FROM temp_dep_child AS a
LEFT JOIN temp_dep_child_hhld AS b
ON a.snz_idi_address_register_uid = b.snz_idi_address_register_uid
WHERE b.tot_dep_child_hhld <= 5

/*******************************************************************************************************************************************************/
/* Add index */
CREATE NONCLUSTERED INDEX my_index_name ON [IDI_Sandpit].[DL-MAA2020-01].[defn_dep_child] (snz_uid);
GO

/* Compress final table to save space */
ALTER TABLE [IDI_Sandpit].[DL-MAA2020-01].[defn_dep_child] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE);
GO

/* Delete intermediate tables */
DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_dia_chld]
DROP VIEW IF EXISTS [DL-MAA2020-01].[defn_cen18_chld]
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[temp_dep_child]
GO

