/**************************************************************************************************
Title: Recent, but not current, beneficiary indicator
Author: Simon Anastasiadis
Reviewer: 

Inputs & Dependencies:
- main_benefits_by_type_and_partner_status.sql --> [IDI_Sandpit].[DL-MAA2020-01].[defn_abt_main_benefit_final]
Outputs:
- [IDI_Sandpit].[DL-MAA2020-01].[defn_recent_noncurrent_benefit_ind]

Description:
Indicator for recent, but not current, benefit receipt.
With controls for how recent and how much time off benefit.

Intended purpose:
Allows for comparisons of beneficiaries to people who where recently beneficiaries.
 
Notes:
1) Four parameters are used to determine how recent a person must be off benefit. These are:
	- as at date: the specific date a person must be off benefit
	- minimum and maximum days since last benefit receipt
	- minimum days until the person is next on benefit
	Using these control we can identify people who: are not receiving benefit on the current
	date, transitioned off benefit within the last 30-90 days, and will remain off benefit
	for at least another 120 days.

2) The definition assumes benefit data is available on either side of the 'as at' date.
	It may produce unexpected results if run outside the available data range (for example
	setting the as-at date to September 2020 when benefit data is availabe only up to June 2020).

3) A naive approach to this definition would simply check benefit receipt at several points in
	time. But this would fail if we considered  very long time periods or very short benefit spells.
	Hence we need to consider overlaps between spells, not just single points in time.

Parameters & Present values:
  Prefix = defn_
  Project schema = [DL-MAA2020-01]
  As at date = '2020-09-01'
  Minimum days since last benefit receipt = 30
  Maximum days since last benefit receipt = 90
  Minimum days until next benefit receipt = 120

Issues:
 
History (reverse order):
2021-11-12 SA v1
**************************************************************************************************/

/* Clear existing table */
DROP TABLE IF EXISTS [IDI_Sandpit].[DL-MAA2020-01].[defn_recent_noncurrent_benefit_ind];
GO

/* Create definition */
WITH setup_indicators AS (
	SELECT snz_uid
		/* specific points in time */
		,IIF('2020-09-01' BETWEEN [start_date] AND [end_date], 1, 0) AS benefit_at_asat_date
		,IIF(DATEADD(DAY, -30, '2020-09-01') BETWEEN [start_date] AND [end_date], 1, 0) AS benefit_at_past_30_days
		,IIF(DATEADD(DAY, 120, '2020-09-01') BETWEEN [start_date] AND [end_date], 1, 0) AS benefit_at_next_120_days
		/* overlaps */
		,IIF([end_date] BETWEEN DATEADD(DAY, -90, '2020-09-01') AND DATEADD(DAY, -30, '2020-09-01'), 1, 0) AS off_benefit_past_30_to_90_days
		,IIF([start_date] BETWEEN DATEADD(DAY, -30, '2020-09-01') AND DATEADD(DAY, 120, '2020-09-01'), 1, 0) AS on_benefit_past_30_to_next_120_days
	FROM [IDI_Sandpit].[DL-MAA2020-01].[defn_abt_main_benefit_final]
)
SELECT snz_uid
INTO [IDI_Sandpit].[DL-MAA2020-01].[defn_recent_noncurrent_benefit_ind]
FROM setup_indicators
GROUP BY snz_uid
HAVING SUM(benefit_at_asat_date) = 0 -- no benefit at as-at date
AND SUM(benefit_at_past_30_days) = 0 -- no benefit 30 days in past
AND SUM(benefit_at_next_120_days) = 0 -- no benefit 120 days in future
AND SUM(off_benefit_past_30_to_90_days) > 0 -- moved off benefit 30-90 days in past
AND SUM(on_benefit_past_30_to_next_120_days) = 0 -- does not move on benefit until at least 120 days in future
GO

/* index */
CREATE NONCLUSTERED INDEX my_index ON [IDI_Sandpit].[DL-MAA2020-01].[defn_recent_noncurrent_benefit_ind] ([snz_uid])
GO
/* compress */
ALTER TABLE [IDI_Sandpit].[DL-MAA2020-01].[defn_recent_noncurrent_benefit_ind] REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = PAGE)
GO
