#####################################################################################################
#' Description: Tidy assembled data
#'
#' Input: Rectangular debt table produced by run_assembly
#'
#' Output: Tidied debt table
#' 
#' Author: Manjusha Radhakrishnan
#' 
#' Dependencies: dbplyr_helper_functions.R, utility_functions.R, table_consistency_checks.R,
#' overview_dataset.R, summary_confidential.R
#' 
#' Notes: 
#' Living wage is calculated as 21.625 x 40 x 52 = 44980. Where 21.625 is the average hourly
#' living wage calculated as the average of the living wage rate for 2019 ($21.15) and the
#' living wage rate for 2020 ($22.10)
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2022-02-16 MR complete
#' 2022-02-15 SA review & QA
#' 2022-01-28 MR v1
#####################################################################################################
## parameters -------------------------------------------------------------------------------------

# locations
ABSOLUTE_PATH_TO_TOOL <- "~/Network-Shares/DataLabNas/MAA/MAA2020-01/debt to government_Phase3/tools"
ABSOLUTE_PATH_TO_ANALYSIS <- "~/Network-Shares/DataLabNas/MAA/MAA2020-01/debt to government_Phase3/analysis"
SANDPIT = "[IDI_Sandpit]"
USERCODE = "[IDI_UserCode]"
OUR_SCHEMA = "[DL-MAA2020-01]"

# inputs
ASSEMBLED_TABLE = "[d2gP3_rectangular]"
LOW_INCOME_THRESHOLD = 44980

# outputs
TIDY_TABLE = "[d2gP3_tidy_table]"
INTERIM_TABLE_1 ="[d2gP3_tidy_table_interim_1]"
INTERIM_TABLE_2 ="[d2gP3_tidy_table_interim_2]"
INTERIM_TABLE_3 ="[d2gP3_tidy_table_interim_3]"
INTERIM_TABLE_4 ="[d2gP3_tidy_table_interim_4]"

# controls
DEVELOPMENT_MODE = FALSE
VERBOSE = "details" # {"all", "details", "heading", "none"}

# Coeffecients from - Prediction Modelling
INTERCEPT = -6.55
ALPHA = 6.19
BETA = 6.70

# Minimum balance required to be considered as a debtor
MIN_CHECK = 10

# Summary of the model
# Call:
#   glm(formula = curr ~ prev + post, family = binomial, data = train)
# 
# Deviance Residuals: 
#   Min       1Q   Median       3Q      Max  
# -3.5628   0.0592   0.0592   0.0592   3.6199  
# 
# Coefficients:
#   Estimate Std.  Error          z   value   Pr(>|z|)    
#   (Intercept) -6.55049    0.02348  -279.0   <2e-16 ***
#   prevnodebt   6.18762    0.01935   319.8   <2e-16 ***
#   postnodebt   6.70779    0.02103   318.9   <2e-16 ***
#   ---
#   Signif. codes:  0 ‘***’ 0.001 ‘**’ 0.01 ‘*’ 0.05 ‘.’ 0.1 ‘ ’ 1
# 
# (Dispersion parameter for binomial family taken to be 1)
# 
# Null deviance: 1044117  on 2291671  degrees of freedom
# Residual deviance:  127648  on 2291669  degrees of freedom
# AIC: 127654
# 
# Number of Fisher Scoring iterations: 9


## setup ------------------------------------------------------------------------------------------

setwd(ABSOLUTE_PATH_TO_TOOL)
source("utility_functions.R")
source("dbplyr_helper_functions.R")
source("table_consistency_checks.R")
source("overview_dataset.R")
source("summary_confidential.R")
setwd(ABSOLUTE_PATH_TO_ANALYSIS)

## access dataset ---------------------------------------------------------------------------------
run_time_inform_user("GRAND START", context = "heading", print_level = VERBOSE)

db_con = create_database_connection(database = "IDI_Sandpit")

working_table = create_access_point(db_con, SANDPIT, OUR_SCHEMA, ASSEMBLED_TABLE)

if(DEVELOPMENT_MODE){
  working_table = working_table %>% 
    filter(identity_column %% 100 == 0)
}
## error checking ---------------------------------------------------------------------------------
run_time_inform_user("error checks begun", context = "heading", print_level = VERBOSE)

# one person per period
assert_all_unique(working_table, c('identity_column', 'label_summary_period'))

# at least 1000 rows
if(!DEVELOPMENT_MODE){
  assert_size(working_table, ">", 1000)
}

run_time_inform_user("error checks complete", context = "heading", print_level = VERBOSE)
## check dataset variables ------------------------------------------------------------------------

# explore_report(working_table, id_column = "identity_column", output_file = "raw_table_report")
# explore_report(working_table, id_column = "identity_column", target = "label_summary_period", output_file = "raw_table_report")

# sort columns
working_table = working_table %>%
	select(sort(colnames(working_table)))

# during development inspect state of table
# explore::explore_shiny(collect(working_table))

## prep / cleaning in SQL -------------------------------------------------------------------------
working_table = working_table %>%
  # drop unrequired columns -------------------------------------------------------------------------
  select(-label_identity, 
         -summary_period_start_date, 
         -summary_period_end_date, 
         -label_summary_period, 
         -intersect(starts_with('moj_fine'),ends_with('2011')),
         -intersect(starts_with('moj_fine'),ends_with('2021')),
         -intersect(starts_with('ird'),ends_with('2010')),
         -intersect(starts_with('ird'),ends_with('2011')),
         -intersect(starts_with('msd'),ends_with('2009')),
         -intersect(starts_with('msd'),ends_with('2010')),
         -intersect(starts_with('msd'),ends_with('2011'))
  ) %>%
  # whole population ----------------------------------------------------------------------
  mutate(everybody = 1) %>%
  # Group residents and non-residents -------------------------------------------------------------------------
  mutate(res_indicator = ifelse((is_alive == 1 & resident == 1 & spine_indicator == 1), 1, 0)) %>%
  # combine some histogram'ed variables -------------------------------------------------------------------------
  collapse_indicator_columns(prefix = "sex_code=", yes_values = 1, label = "sex_code") %>%
  collapse_indicator_columns(prefix = "region_code=", yes_values = 1, label = "region_code") %>%
  collapse_indicator_columns(prefix = "area_type=", yes_values = 1, label = "area_type") %>%
  # Scaling 2020 MSD Repayment and Principle amounts to annual as MSD data ends in Sep -------------------------------------------------------------------------
    # (1.33 = Inverse of 3/4 (1 - 0.75))
    mutate(
      msd_repaid_2020 = round(1.33 * msd_repaid_2020, 2),
      msd_principle_2020 = round(1.33 * msd_principle_2020, 2)
    ) %>%
  # Is beneficiary ---------------------------------------------------------------------
    mutate(
      is_beneficiary = ifelse(days_benefit > 185, 1, 0)
    )
## Handling gaps --------------------------------------------------------------------------------------------
working_table = working_table %>%
  # handling missings ----------------------------------------------------------------------
    mutate(
      res_indicator = ifelse(is.na(res_indicator), 0, res_indicator),
      eth_asian  = ifelse(is.na(eth_asian), 0, eth_asian),
      eth_european = ifelse(is.na(eth_european), 0, eth_european),
      eth_maori = ifelse(is.na(eth_maori), 0, eth_maori),
      eth_MELAA = ifelse(is.na(eth_MELAA), 0, eth_MELAA),
      eth_other = ifelse(is.na(eth_other), 0, eth_other),
      eth_pacific = ifelse(is.na(eth_pacific), 0, eth_pacific),
      is_beneficiary = ifelse(is.na(is_beneficiary), 0, is_beneficiary)
    ) %>%
  # negative value check ----------------------------------------------------------------------------------
    mutate(
      msd_repaid_2012 = ifelse(msd_repaid_2012 < 0, -1.0 * msd_repaid_2012, 0),
      msd_repaid_2013 = ifelse(msd_repaid_2013 < 0, -1.0 * msd_repaid_2013, 0),
      msd_repaid_2014 = ifelse(msd_repaid_2014 < 0, -1.0 * msd_repaid_2014, 0),
      msd_repaid_2015 = ifelse(msd_repaid_2015 < 0, -1.0 * msd_repaid_2015, 0),
      msd_repaid_2016 = ifelse(msd_repaid_2016 < 0, -1.0 * msd_repaid_2016, 0),
      msd_repaid_2017 = ifelse(msd_repaid_2017 < 0, -1.0 * msd_repaid_2017, 0),
      msd_repaid_2018 = ifelse(msd_repaid_2018 < 0, -1.0 * msd_repaid_2018, 0),
      msd_repaid_2019 = ifelse(msd_repaid_2019 < 0, -1.0 * msd_repaid_2019, 0),
      msd_repaid_2020 = ifelse(msd_repaid_2020 < 0, -1.0 * msd_repaid_2020, 0),
      
      # All the repayment amounts for MOJ - Fines are positive; the code below ensures to remove reversals (negatives) if exists
      
      moj_fine_repaid_2012 = ifelse(moj_fine_repaid_2012 > 0, moj_fine_repaid_2012, 0),
      moj_fine_repaid_2013 = ifelse(moj_fine_repaid_2013 > 0, moj_fine_repaid_2013, 0),
      moj_fine_repaid_2014 = ifelse(moj_fine_repaid_2014 > 0, moj_fine_repaid_2014, 0),
      moj_fine_repaid_2015 = ifelse(moj_fine_repaid_2015 > 0, moj_fine_repaid_2015, 0),
      moj_fine_repaid_2016 = ifelse(moj_fine_repaid_2016 > 0, moj_fine_repaid_2016, 0),
      moj_fine_repaid_2017 = ifelse(moj_fine_repaid_2017 > 0, moj_fine_repaid_2017, 0),
      moj_fine_repaid_2018 = ifelse(moj_fine_repaid_2018 > 0, moj_fine_repaid_2018, 0),
      moj_fine_repaid_2019 = ifelse(moj_fine_repaid_2019 > 0, moj_fine_repaid_2019, 0),
      moj_fine_repaid_2020 = ifelse(moj_fine_repaid_2020 > 0, moj_fine_repaid_2020, 0),
      
      moj_fcco_repaid_2014 = ifelse(moj_fcco_repaid_2014 < 0, -1.0 * moj_fcco_repaid_2014, 0),
      moj_fcco_repaid_2015 = ifelse(moj_fcco_repaid_2015 < 0, -1.0 * moj_fcco_repaid_2015, 0),
      moj_fcco_repaid_2016 = ifelse(moj_fcco_repaid_2016 < 0, -1.0 * moj_fcco_repaid_2016, 0),
      moj_fcco_repaid_2017 = ifelse(moj_fcco_repaid_2017 < 0, -1.0 * moj_fcco_repaid_2017, 0),
      moj_fcco_repaid_2018 = ifelse(moj_fcco_repaid_2018 < 0, -1.0 * moj_fcco_repaid_2018, 0),
      moj_fcco_repaid_2019 = ifelse(moj_fcco_repaid_2019 < 0, -1.0 * moj_fcco_repaid_2019, 0),
      moj_fcco_repaid_2020 = ifelse(moj_fcco_repaid_2020 < 0, -1.0 * moj_fcco_repaid_2020, 0),
      
      ird_pr19_repaid_child_2012 = ifelse(ird_pr19_repaid_child_2012 < 0, -1.0 * ird_pr19_repaid_child_2012, 0),
      ird_pr19_repaid_child_2013 = ifelse(ird_pr19_repaid_child_2013 < 0, -1.0 * ird_pr19_repaid_child_2013, 0),
      ird_pr19_repaid_child_2014 = ifelse(ird_pr19_repaid_child_2014 < 0, -1.0 * ird_pr19_repaid_child_2014, 0),
      ird_pr19_repaid_child_2015 = ifelse(ird_pr19_repaid_child_2015 < 0, -1.0 * ird_pr19_repaid_child_2015, 0),
      ird_pr19_repaid_child_2016 = ifelse(ird_pr19_repaid_child_2016 < 0, -1.0 * ird_pr19_repaid_child_2016, 0),
      ird_pr19_repaid_child_2017 = ifelse(ird_pr19_repaid_child_2017 < 0, -1.0 * ird_pr19_repaid_child_2017, 0),
      ird_pr19_repaid_child_2018 = ifelse(ird_pr19_repaid_child_2018 < 0, -1.0 * ird_pr19_repaid_child_2018, 0),
      ird_ps18_repayment_child_2019 = ifelse(ird_ps18_repayment_child_2019 < 0, -1.0 * ird_ps18_repayment_child_2019, 0),
      ird_ps18_repayment_child_2020 = ifelse(ird_ps18_repayment_child_2020 < 0, -1.0 * ird_ps18_repayment_child_2020, 0),
      
      ird_pr19_repaid_student_2012 = ifelse(ird_pr19_repaid_student_2012 < 0, -1.0 * ird_pr19_repaid_student_2012, 0),
      ird_pr19_repaid_student_2013 = ifelse(ird_pr19_repaid_student_2013 < 0, -1.0 * ird_pr19_repaid_student_2013, 0),
      ird_pr19_repaid_student_2014 = ifelse(ird_pr19_repaid_student_2014 < 0, -1.0 * ird_pr19_repaid_student_2014, 0),
      ird_pr19_repaid_student_2015 = ifelse(ird_pr19_repaid_student_2015 < 0, -1.0 * ird_pr19_repaid_student_2015, 0),
      ird_pr19_repaid_student_2016 = ifelse(ird_pr19_repaid_student_2016 < 0, -1.0 * ird_pr19_repaid_student_2016, 0),
      ird_pr19_repaid_student_2017 = ifelse(ird_pr19_repaid_student_2017 < 0, -1.0 * ird_pr19_repaid_student_2017, 0),
      ird_pr19_repaid_student_2018 = ifelse(ird_pr19_repaid_student_2018 < 0, -1.0 * ird_pr19_repaid_student_2018, 0),
      
      # No existing data for IRD - Overdue Student Loan - 2019
      # ird_ps18_repayment_student_2019 = ifelse(ird_ps18_repayment_student_2019 < 0, -1.0 * ird_ps18_repayment_student_2019, 0),
      
      ird_ps18_repayment_student_2020 = ifelse(ird_ps18_repayment_student_2020 < 0, -1.0 * ird_ps18_repayment_student_2020, 0),
      
      ird_pr19_repaid_income_2012 = ifelse(ird_pr19_repaid_income_2012 < 0, -1.0 * ird_pr19_repaid_income_2012, 0),
      ird_pr19_repaid_income_2013 = ifelse(ird_pr19_repaid_income_2013 < 0, -1.0 * ird_pr19_repaid_income_2013, 0),
      ird_pr19_repaid_income_2014 = ifelse(ird_pr19_repaid_income_2014 < 0, -1.0 * ird_pr19_repaid_income_2014, 0),
      ird_pr19_repaid_income_2015 = ifelse(ird_pr19_repaid_income_2015 < 0, -1.0 * ird_pr19_repaid_income_2015, 0),
      ird_pr19_repaid_income_2016 = ifelse(ird_pr19_repaid_income_2016 < 0, -1.0 * ird_pr19_repaid_income_2016, 0),
      ird_pr19_repaid_income_2017 = ifelse(ird_pr19_repaid_income_2017 < 0, -1.0 * ird_pr19_repaid_income_2017, 0),
      ird_pr19_repaid_income_2018 = ifelse(ird_pr19_repaid_income_2018 < 0, -1.0 * ird_pr19_repaid_income_2018, 0),
      ird_ps18_repayment_income_2019 = ifelse(ird_ps18_repayment_income_2019 < 0, -1.0 * ird_ps18_repayment_income_2019, 0),
      ird_ps18_repayment_income_2020 = ifelse(ird_ps18_repayment_income_2020 < 0, -1.0 * ird_ps18_repayment_income_2020, 0),
      
      ird_pr19_repaid_wff_2012 = ifelse(ird_pr19_repaid_wff_2012 < 0, -1.0 * ird_pr19_repaid_wff_2012, 0),
      ird_pr19_repaid_wff_2013 = ifelse(ird_pr19_repaid_wff_2013 < 0, -1.0 * ird_pr19_repaid_wff_2013, 0),
      ird_pr19_repaid_wff_2014 = ifelse(ird_pr19_repaid_wff_2014 < 0, -1.0 * ird_pr19_repaid_wff_2014, 0),
      ird_pr19_repaid_wff_2015 = ifelse(ird_pr19_repaid_wff_2015 < 0, -1.0 * ird_pr19_repaid_wff_2015, 0),
      ird_pr19_repaid_wff_2016 = ifelse(ird_pr19_repaid_wff_2016 < 0, -1.0 * ird_pr19_repaid_wff_2016, 0),
      ird_pr19_repaid_wff_2017 = ifelse(ird_pr19_repaid_wff_2017 < 0, -1.0 * ird_pr19_repaid_wff_2017, 0),
      ird_pr19_repaid_wff_2018 = ifelse(ird_pr19_repaid_wff_2018 < 0, -1.0 * ird_pr19_repaid_wff_2018, 0),
      ird_ps18_repayment_wff_2019 = ifelse(ird_ps18_repayment_wff_2019 < 0, -1.0 * ird_ps18_repayment_wff_2019, 0),
      ird_ps18_repayment_wff_2020 = ifelse(ird_ps18_repayment_wff_2020 < 0, -1.0 * ird_ps18_repayment_wff_2020, 0),
      
      ird_pr19_repaid_oth_2012 = ifelse(ird_pr19_repaid_oth_2012 < 0, -1.0 * ird_pr19_repaid_oth_2012, 0),
      ird_pr19_repaid_oth_2013 = ifelse(ird_pr19_repaid_oth_2013 < 0, -1.0 * ird_pr19_repaid_oth_2013, 0),
      ird_pr19_repaid_oth_2014 = ifelse(ird_pr19_repaid_oth_2014 < 0, -1.0 * ird_pr19_repaid_oth_2014, 0),
      ird_pr19_repaid_oth_2015 = ifelse(ird_pr19_repaid_oth_2015 < 0, -1.0 * ird_pr19_repaid_oth_2015, 0),
      ird_pr19_repaid_oth_2016 = ifelse(ird_pr19_repaid_oth_2016 < 0, -1.0 * ird_pr19_repaid_oth_2016, 0),
      ird_pr19_repaid_oth_2017 = ifelse(ird_pr19_repaid_oth_2017 < 0, -1.0 * ird_pr19_repaid_oth_2017, 0),
      ird_pr19_repaid_oth_2018 = ifelse(ird_pr19_repaid_oth_2018 < 0, -1.0 * ird_pr19_repaid_oth_2018, 0),
      ird_ps18_repayment_oth_2019 = ifelse(ird_ps18_repayment_oth_2019 < 0, -1.0 * ird_ps18_repayment_oth_2019, 0),
      ird_ps18_repayment_oth_2020 = ifelse(ird_ps18_repayment_oth_2020 < 0, -1.0 * ird_ps18_repayment_oth_2020, 0)
    )%>%
  # handling nulls ----------------------------------------------------------------------
    mutate(
      # Dependent Children indicator
      dep_chld_hhld = coalesce(dep_chld_hhld,0),
      dep_chld_indv = coalesce(dep_chld_indv,0),
      
      # Days benefit
      days_benefit = coalesce(days_benefit,0),
      
      # Income variables
      income_taxible = coalesce(income_taxible,0),
      income_t2_benefits = coalesce(income_t2_benefits,0),
      income_t3_benefits = coalesce(income_t3_benefits,0),
      income_wff = coalesce(income_wff,0),
      
      ## Balance variables
      # MSD
      msd_balance_2012 = coalesce(ifelse(msd_balance_2012 > MIN_CHECK,msd_balance_2012,0),0),
      msd_balance_2013 = coalesce(ifelse(msd_balance_2013 > MIN_CHECK,msd_balance_2013,0),0),
      msd_balance_2014 = coalesce(ifelse(msd_balance_2014 > MIN_CHECK,msd_balance_2014,0),0),
      msd_balance_2015 = coalesce(ifelse(msd_balance_2015 > MIN_CHECK,msd_balance_2015,0),0),
      msd_balance_2016 = coalesce(ifelse(msd_balance_2016 > MIN_CHECK,msd_balance_2016,0),0),
      msd_balance_2017 = coalesce(ifelse(msd_balance_2017 > MIN_CHECK,msd_balance_2017,0),0),
      msd_balance_2018 = coalesce(ifelse(msd_balance_2018 > MIN_CHECK,msd_balance_2018,0),0),
      msd_balance_2019 = coalesce(ifelse(msd_balance_2019 > MIN_CHECK,msd_balance_2019,0),0),
      msd_balance_2020 = coalesce(ifelse(msd_balance_2020 > MIN_CHECK,msd_balance_2020,0),0),
      
      # MOJ - Fine
      moj_fine_balance_2012 = coalesce(ifelse(moj_fine_balance_2012 > MIN_CHECK,moj_fine_balance_2012,0),0),
      moj_fine_balance_2013 = coalesce(ifelse(moj_fine_balance_2013 > MIN_CHECK,moj_fine_balance_2013,0),0),
      moj_fine_balance_2014 = coalesce(ifelse(moj_fine_balance_2014 > MIN_CHECK,moj_fine_balance_2014,0),0),
      moj_fine_balance_2015 = coalesce(ifelse(moj_fine_balance_2015 > MIN_CHECK,moj_fine_balance_2015,0),0),
      moj_fine_balance_2016 = coalesce(ifelse(moj_fine_balance_2016 > MIN_CHECK,moj_fine_balance_2016,0),0),
      moj_fine_balance_2017 = coalesce(ifelse(moj_fine_balance_2017 > MIN_CHECK,moj_fine_balance_2017,0),0),
      moj_fine_balance_2018 = coalesce(ifelse(moj_fine_balance_2018 > MIN_CHECK,moj_fine_balance_2018,0),0),
      moj_fine_balance_2019 = coalesce(ifelse(moj_fine_balance_2019 > MIN_CHECK,moj_fine_balance_2019,0),0),
      moj_fine_balance_2020 = coalesce(ifelse(moj_fine_balance_2020 > MIN_CHECK,moj_fine_balance_2020,0),0),
      
      # MOJ - FCCO
      moj_fcco_balance_2014 = coalesce(ifelse(moj_fcco_balance_2014 > MIN_CHECK,moj_fcco_balance_2014,0),0),
      moj_fcco_balance_2015 = coalesce(ifelse(moj_fcco_balance_2015 > MIN_CHECK,moj_fcco_balance_2015,0),0),
      moj_fcco_balance_2016 = coalesce(ifelse(moj_fcco_balance_2016 > MIN_CHECK,moj_fcco_balance_2016,0),0),
      moj_fcco_balance_2017 = coalesce(ifelse(moj_fcco_balance_2017 > MIN_CHECK,moj_fcco_balance_2017,0),0),
      moj_fcco_balance_2018 = coalesce(ifelse(moj_fcco_balance_2018 > MIN_CHECK,moj_fcco_balance_2018,0),0),
      moj_fcco_balance_2019 = coalesce(ifelse(moj_fcco_balance_2019 > MIN_CHECK,moj_fcco_balance_2019,0),0),
      moj_fcco_balance_2020 = coalesce(ifelse(moj_fcco_balance_2020 > MIN_CHECK,moj_fcco_balance_2020,0),0),
      
      # IR - Child Support
      ird_pr19_balance_child_2012 = coalesce(ifelse(ird_pr19_balance_child_2012 > MIN_CHECK,ird_pr19_balance_child_2012,0),0),
      ird_pr19_balance_child_2013 = coalesce(ifelse(ird_pr19_balance_child_2013 > MIN_CHECK,ird_pr19_balance_child_2013,0),0),
      ird_pr19_balance_child_2014 = coalesce(ifelse(ird_pr19_balance_child_2014 > MIN_CHECK,ird_pr19_balance_child_2014,0),0),
      ird_pr19_balance_child_2015 = coalesce(ifelse(ird_pr19_balance_child_2015 > MIN_CHECK,ird_pr19_balance_child_2015,0),0),
      ird_pr19_balance_child_2016 = coalesce(ifelse(ird_pr19_balance_child_2016 > MIN_CHECK,ird_pr19_balance_child_2016,0),0),
      ird_pr19_balance_child_2017 = coalesce(ifelse(ird_pr19_balance_child_2017 > MIN_CHECK,ird_pr19_balance_child_2017,0),0),
      ird_pr19_balance_child_2018 = coalesce(ifelse(ird_pr19_balance_child_2018 > MIN_CHECK,ird_pr19_balance_child_2018,0),0),
      ird_ps18_balance_child_2019 = coalesce(ifelse(ird_ps18_balance_child_2019 > MIN_CHECK,ird_ps18_balance_child_2019,0),0),
      ird_ps18_balance_child_2020 = coalesce(ifelse(ird_ps18_balance_child_2020 > MIN_CHECK,ird_ps18_balance_child_2020,0),0),
      
      # IR - Overdue Student Loan
      ird_pr19_balance_student_2012 = coalesce(ifelse(ird_pr19_balance_student_2012 > MIN_CHECK,ird_pr19_balance_student_2012,0),0),
      ird_pr19_balance_student_2013 = coalesce(ifelse(ird_pr19_balance_student_2013 > MIN_CHECK,ird_pr19_balance_student_2013,0),0),
      ird_pr19_balance_student_2014 = coalesce(ifelse(ird_pr19_balance_student_2014 > MIN_CHECK,ird_pr19_balance_student_2014,0),0),
      ird_pr19_balance_student_2015 = coalesce(ifelse(ird_pr19_balance_student_2015 > MIN_CHECK,ird_pr19_balance_student_2015,0),0),
      ird_pr19_balance_student_2016 = coalesce(ifelse(ird_pr19_balance_student_2016 > MIN_CHECK,ird_pr19_balance_student_2016,0),0),
      ird_pr19_balance_student_2017 = coalesce(ifelse(ird_pr19_balance_student_2017 > MIN_CHECK,ird_pr19_balance_student_2017,0),0),
      ird_pr19_balance_student_2018 = coalesce(ifelse(ird_pr19_balance_student_2018 > MIN_CHECK,ird_pr19_balance_student_2018,0),0),
      # ird_ps18_balance_student_2019 = coalesce(ifelse(ird_ps18_balance_student_2019 > MIN_CHECK,ird_ps18_balance_student_2019,0),0),
      ird_ps18_balance_student_2020 = coalesce(ifelse(ird_ps18_balance_student_2020 > MIN_CHECK,ird_ps18_balance_student_2020,0),0),
      
      # IR - Income Tax
      ird_pr19_balance_income_2012 = coalesce(ifelse(ird_pr19_balance_income_2012 > MIN_CHECK,ird_pr19_balance_income_2012,0),0),
      ird_pr19_balance_income_2013 = coalesce(ifelse(ird_pr19_balance_income_2013 > MIN_CHECK,ird_pr19_balance_income_2013,0),0),
      ird_pr19_balance_income_2014 = coalesce(ifelse(ird_pr19_balance_income_2014 > MIN_CHECK,ird_pr19_balance_income_2014,0),0),
      ird_pr19_balance_income_2015 = coalesce(ifelse(ird_pr19_balance_income_2015 > MIN_CHECK,ird_pr19_balance_income_2015,0),0),
      ird_pr19_balance_income_2016 = coalesce(ifelse(ird_pr19_balance_income_2016 > MIN_CHECK,ird_pr19_balance_income_2016,0),0),
      ird_pr19_balance_income_2017 = coalesce(ifelse(ird_pr19_balance_income_2017 > MIN_CHECK,ird_pr19_balance_income_2017,0),0),
      ird_pr19_balance_income_2018 = coalesce(ifelse(ird_pr19_balance_income_2018 > MIN_CHECK,ird_pr19_balance_income_2018,0),0),
      ird_ps18_balance_income_2019 = coalesce(ifelse(ird_ps18_balance_income_2019 > MIN_CHECK,ird_ps18_balance_income_2019,0),0),
      ird_ps18_balance_income_2020 = coalesce(ifelse(ird_ps18_balance_income_2020 > MIN_CHECK,ird_ps18_balance_income_2020,0),0),
      
      # IR - Working For Families
      ird_pr19_balance_wff_2012 = coalesce(ifelse(ird_pr19_balance_wff_2012 > MIN_CHECK,ird_pr19_balance_wff_2012,0),0),
      ird_pr19_balance_wff_2013 = coalesce(ifelse(ird_pr19_balance_wff_2013 > MIN_CHECK,ird_pr19_balance_wff_2013,0),0),
      ird_pr19_balance_wff_2014 = coalesce(ifelse(ird_pr19_balance_wff_2014 > MIN_CHECK,ird_pr19_balance_wff_2014,0),0),
      ird_pr19_balance_wff_2015 = coalesce(ifelse(ird_pr19_balance_wff_2015 > MIN_CHECK,ird_pr19_balance_wff_2015,0),0),
      ird_pr19_balance_wff_2016 = coalesce(ifelse(ird_pr19_balance_wff_2016 > MIN_CHECK,ird_pr19_balance_wff_2016,0),0),
      ird_pr19_balance_wff_2017 = coalesce(ifelse(ird_pr19_balance_wff_2017 > MIN_CHECK,ird_pr19_balance_wff_2017,0),0),
      ird_pr19_balance_wff_2018 = coalesce(ifelse(ird_pr19_balance_wff_2018 > MIN_CHECK,ird_pr19_balance_wff_2018,0),0),
      ird_ps18_balance_wff_2019 = coalesce(ifelse(ird_ps18_balance_wff_2019 > MIN_CHECK,ird_ps18_balance_wff_2019,0),0),
      ird_ps18_balance_wff_2020 = coalesce(ifelse(ird_ps18_balance_wff_2020 > MIN_CHECK,ird_ps18_balance_wff_2020,0),0),
      
      # IR - Others
      ird_pr19_balance_oth_2012 = coalesce(ifelse(ird_pr19_balance_oth_2012 > MIN_CHECK,ird_pr19_balance_oth_2012,0),0),
      ird_pr19_balance_oth_2013 = coalesce(ifelse(ird_pr19_balance_oth_2013 > MIN_CHECK,ird_pr19_balance_oth_2013,0),0),
      ird_pr19_balance_oth_2014 = coalesce(ifelse(ird_pr19_balance_oth_2014 > MIN_CHECK,ird_pr19_balance_oth_2014,0),0),
      ird_pr19_balance_oth_2015 = coalesce(ifelse(ird_pr19_balance_oth_2015 > MIN_CHECK,ird_pr19_balance_oth_2015,0),0),
      ird_pr19_balance_oth_2016 = coalesce(ifelse(ird_pr19_balance_oth_2016 > MIN_CHECK,ird_pr19_balance_oth_2016,0),0),
      ird_pr19_balance_oth_2017 = coalesce(ifelse(ird_pr19_balance_oth_2017 > MIN_CHECK,ird_pr19_balance_oth_2017,0),0),
      ird_pr19_balance_oth_2018 = coalesce(ifelse(ird_pr19_balance_oth_2018 > MIN_CHECK,ird_pr19_balance_oth_2018,0),0),
      ird_ps18_balance_oth_2019 = coalesce(ifelse(ird_ps18_balance_oth_2019 > MIN_CHECK,ird_ps18_balance_oth_2019,0),0),
      ird_ps18_balance_oth_2020 = coalesce(ifelse(ird_ps18_balance_oth_2020 > MIN_CHECK,ird_ps18_balance_oth_2020,0),0),
      
      ## Principle variables
      ## principle variables
      # MSD
      msd_principle_2012 = coalesce(ifelse(msd_principle_2012 > MIN_CHECK,msd_principle_2012,0),0),
      msd_principle_2013 = coalesce(ifelse(msd_principle_2013 > MIN_CHECK,msd_principle_2013,0),0),
      msd_principle_2014 = coalesce(ifelse(msd_principle_2014 > MIN_CHECK,msd_principle_2014,0),0),
      msd_principle_2015 = coalesce(ifelse(msd_principle_2015 > MIN_CHECK,msd_principle_2015,0),0),
      msd_principle_2016 = coalesce(ifelse(msd_principle_2016 > MIN_CHECK,msd_principle_2016,0),0),
      msd_principle_2017 = coalesce(ifelse(msd_principle_2017 > MIN_CHECK,msd_principle_2017,0),0),
      msd_principle_2018 = coalesce(ifelse(msd_principle_2018 > MIN_CHECK,msd_principle_2018,0),0),
      msd_principle_2019 = coalesce(ifelse(msd_principle_2019 > MIN_CHECK,msd_principle_2019,0),0),
      msd_principle_2020 = coalesce(ifelse(msd_principle_2020 > MIN_CHECK,msd_principle_2020,0),0),
      
      # MOJ - Fine
      moj_fine_principle_2012 = coalesce(ifelse(moj_fine_principle_2012 > MIN_CHECK,moj_fine_principle_2012,0),0),
      moj_fine_principle_2013 = coalesce(ifelse(moj_fine_principle_2013 > MIN_CHECK,moj_fine_principle_2013,0),0),
      moj_fine_principle_2014 = coalesce(ifelse(moj_fine_principle_2014 > MIN_CHECK,moj_fine_principle_2014,0),0),
      moj_fine_principle_2015 = coalesce(ifelse(moj_fine_principle_2015 > MIN_CHECK,moj_fine_principle_2015,0),0),
      moj_fine_principle_2016 = coalesce(ifelse(moj_fine_principle_2016 > MIN_CHECK,moj_fine_principle_2016,0),0),
      moj_fine_principle_2017 = coalesce(ifelse(moj_fine_principle_2017 > MIN_CHECK,moj_fine_principle_2017,0),0),
      moj_fine_principle_2018 = coalesce(ifelse(moj_fine_principle_2018 > MIN_CHECK,moj_fine_principle_2018,0),0),
      moj_fine_principle_2019 = coalesce(ifelse(moj_fine_principle_2019 > MIN_CHECK,moj_fine_principle_2019,0),0),
      moj_fine_principle_2020 = coalesce(ifelse(moj_fine_principle_2020 > MIN_CHECK,moj_fine_principle_2020,0),0),
      
      # MOJ - FCCO
      moj_fcco_principle_2014 = coalesce(ifelse(moj_fcco_principle_2014 > MIN_CHECK,moj_fcco_principle_2014,0),0),
      moj_fcco_principle_2015 = coalesce(ifelse(moj_fcco_principle_2015 > MIN_CHECK,moj_fcco_principle_2015,0),0),
      moj_fcco_principle_2016 = coalesce(ifelse(moj_fcco_principle_2016 > MIN_CHECK,moj_fcco_principle_2016,0),0),
      moj_fcco_principle_2017 = coalesce(ifelse(moj_fcco_principle_2017 > MIN_CHECK,moj_fcco_principle_2017,0),0),
      moj_fcco_principle_2018 = coalesce(ifelse(moj_fcco_principle_2018 > MIN_CHECK,moj_fcco_principle_2018,0),0),
      moj_fcco_principle_2019 = coalesce(ifelse(moj_fcco_principle_2019 > MIN_CHECK,moj_fcco_principle_2019,0),0),
      moj_fcco_principle_2020 = coalesce(ifelse(moj_fcco_principle_2020 > MIN_CHECK,moj_fcco_principle_2020,0),0),
      
      # IR - Child Support
      ird_pr19_principle_child_2012 = coalesce(ifelse(ird_pr19_principle_child_2012 > MIN_CHECK,ird_pr19_principle_child_2012,0),0),
      ird_pr19_principle_child_2013 = coalesce(ifelse(ird_pr19_principle_child_2013 > MIN_CHECK,ird_pr19_principle_child_2013,0),0),
      ird_pr19_principle_child_2014 = coalesce(ifelse(ird_pr19_principle_child_2014 > MIN_CHECK,ird_pr19_principle_child_2014,0),0),
      ird_pr19_principle_child_2015 = coalesce(ifelse(ird_pr19_principle_child_2015 > MIN_CHECK,ird_pr19_principle_child_2015,0),0),
      ird_pr19_principle_child_2016 = coalesce(ifelse(ird_pr19_principle_child_2016 > MIN_CHECK,ird_pr19_principle_child_2016,0),0),
      ird_pr19_principle_child_2017 = coalesce(ifelse(ird_pr19_principle_child_2017 > MIN_CHECK,ird_pr19_principle_child_2017,0),0),
      ird_pr19_principle_child_2018 = coalesce(ifelse(ird_pr19_principle_child_2018 > MIN_CHECK,ird_pr19_principle_child_2018,0),0),
      ird_ps18_principle_child_2019 = coalesce(ifelse(ird_ps18_principle_child_2019 > MIN_CHECK,ird_ps18_principle_child_2019,0),0),
      ird_ps18_principle_child_2020 = coalesce(ifelse(ird_ps18_principle_child_2020 > MIN_CHECK,ird_ps18_principle_child_2020,0),0),
      
      # IR - Overdue Student Loan
      ird_pr19_principle_student_2012 = coalesce(ifelse(ird_pr19_principle_student_2012 > MIN_CHECK,ird_pr19_principle_student_2012,0),0),
      ird_pr19_principle_student_2013 = coalesce(ifelse(ird_pr19_principle_student_2013 > MIN_CHECK,ird_pr19_principle_student_2013,0),0),
      ird_pr19_principle_student_2014 = coalesce(ifelse(ird_pr19_principle_student_2014 > MIN_CHECK,ird_pr19_principle_student_2014,0),0),
      ird_pr19_principle_student_2015 = coalesce(ifelse(ird_pr19_principle_student_2015 > MIN_CHECK,ird_pr19_principle_student_2015,0),0),
      ird_pr19_principle_student_2016 = coalesce(ifelse(ird_pr19_principle_student_2016 > MIN_CHECK,ird_pr19_principle_student_2016,0),0),
      ird_pr19_principle_student_2017 = coalesce(ifelse(ird_pr19_principle_student_2017 > MIN_CHECK,ird_pr19_principle_student_2017,0),0),
      ird_pr19_principle_student_2018 = coalesce(ifelse(ird_pr19_principle_student_2018 > MIN_CHECK,ird_pr19_principle_student_2018,0),0),
      # ird_ps18_principle_student_2019 = coalesce(ifelse(ird_ps18_principle_student_2019 > MIN_CHECK,ird_ps18_principle_student_2019,0),0),
      ird_ps18_principle_student_2020 = coalesce(ifelse(ird_ps18_principle_student_2020 > MIN_CHECK,ird_ps18_principle_student_2020,0),0),
      
      # IR - Income Tax
      ird_pr19_principle_income_2012 = coalesce(ifelse(ird_pr19_principle_income_2012 > MIN_CHECK,ird_pr19_principle_income_2012,0),0),
      ird_pr19_principle_income_2013 = coalesce(ifelse(ird_pr19_principle_income_2013 > MIN_CHECK,ird_pr19_principle_income_2013,0),0),
      ird_pr19_principle_income_2014 = coalesce(ifelse(ird_pr19_principle_income_2014 > MIN_CHECK,ird_pr19_principle_income_2014,0),0),
      ird_pr19_principle_income_2015 = coalesce(ifelse(ird_pr19_principle_income_2015 > MIN_CHECK,ird_pr19_principle_income_2015,0),0),
      ird_pr19_principle_income_2016 = coalesce(ifelse(ird_pr19_principle_income_2016 > MIN_CHECK,ird_pr19_principle_income_2016,0),0),
      ird_pr19_principle_income_2017 = coalesce(ifelse(ird_pr19_principle_income_2017 > MIN_CHECK,ird_pr19_principle_income_2017,0),0),
      ird_pr19_principle_income_2018 = coalesce(ifelse(ird_pr19_principle_income_2018 > MIN_CHECK,ird_pr19_principle_income_2018,0),0),
      ird_ps18_principle_income_2019 = coalesce(ifelse(ird_ps18_principle_income_2019 > MIN_CHECK,ird_ps18_principle_income_2019,0),0),
      ird_ps18_principle_income_2020 = coalesce(ifelse(ird_ps18_principle_income_2020 > MIN_CHECK,ird_ps18_principle_income_2020,0),0),
      
      # IR - Working For Families
      ird_pr19_principle_wff_2012 = coalesce(ifelse(ird_pr19_principle_wff_2012 > MIN_CHECK,ird_pr19_principle_wff_2012,0),0),
      ird_pr19_principle_wff_2013 = coalesce(ifelse(ird_pr19_principle_wff_2013 > MIN_CHECK,ird_pr19_principle_wff_2013,0),0),
      ird_pr19_principle_wff_2014 = coalesce(ifelse(ird_pr19_principle_wff_2014 > MIN_CHECK,ird_pr19_principle_wff_2014,0),0),
      ird_pr19_principle_wff_2015 = coalesce(ifelse(ird_pr19_principle_wff_2015 > MIN_CHECK,ird_pr19_principle_wff_2015,0),0),
      ird_pr19_principle_wff_2016 = coalesce(ifelse(ird_pr19_principle_wff_2016 > MIN_CHECK,ird_pr19_principle_wff_2016,0),0),
      ird_pr19_principle_wff_2017 = coalesce(ifelse(ird_pr19_principle_wff_2017 > MIN_CHECK,ird_pr19_principle_wff_2017,0),0),
      ird_pr19_principle_wff_2018 = coalesce(ifelse(ird_pr19_principle_wff_2018 > MIN_CHECK,ird_pr19_principle_wff_2018,0),0),
      ird_ps18_principle_wff_2019 = coalesce(ifelse(ird_ps18_principle_wff_2019 > MIN_CHECK,ird_ps18_principle_wff_2019,0),0),
      ird_ps18_principle_wff_2020 = coalesce(ifelse(ird_ps18_principle_wff_2020 > MIN_CHECK,ird_ps18_principle_wff_2020,0),0),
      
      # IR - Others
      ird_pr19_principle_oth_2012 = coalesce(ifelse(ird_pr19_principle_oth_2012 > MIN_CHECK,ird_pr19_principle_oth_2012,0),0),
      ird_pr19_principle_oth_2013 = coalesce(ifelse(ird_pr19_principle_oth_2013 > MIN_CHECK,ird_pr19_principle_oth_2013,0),0),
      ird_pr19_principle_oth_2014 = coalesce(ifelse(ird_pr19_principle_oth_2014 > MIN_CHECK,ird_pr19_principle_oth_2014,0),0),
      ird_pr19_principle_oth_2015 = coalesce(ifelse(ird_pr19_principle_oth_2015 > MIN_CHECK,ird_pr19_principle_oth_2015,0),0),
      ird_pr19_principle_oth_2016 = coalesce(ifelse(ird_pr19_principle_oth_2016 > MIN_CHECK,ird_pr19_principle_oth_2016,0),0),
      ird_pr19_principle_oth_2017 = coalesce(ifelse(ird_pr19_principle_oth_2017 > MIN_CHECK,ird_pr19_principle_oth_2017,0),0),
      ird_pr19_principle_oth_2018 = coalesce(ifelse(ird_pr19_principle_oth_2018 > MIN_CHECK,ird_pr19_principle_oth_2018,0),0),
      ird_ps18_principle_oth_2019 = coalesce(ifelse(ird_ps18_principle_oth_2019 > MIN_CHECK,ird_ps18_principle_oth_2019,0),0),
      ird_ps18_principle_oth_2020 = coalesce(ifelse(ird_ps18_principle_oth_2020 > MIN_CHECK,ird_ps18_principle_oth_2020,0),0),
      
      ## repaid variables
      # MSD
      msd_repaid_2012 = coalesce(ifelse(msd_repaid_2012 > MIN_CHECK,msd_repaid_2012,0),0),
      msd_repaid_2013 = coalesce(ifelse(msd_repaid_2013 > MIN_CHECK,msd_repaid_2013,0),0),
      msd_repaid_2014 = coalesce(ifelse(msd_repaid_2014 > MIN_CHECK,msd_repaid_2014,0),0),
      msd_repaid_2015 = coalesce(ifelse(msd_repaid_2015 > MIN_CHECK,msd_repaid_2015,0),0),
      msd_repaid_2016 = coalesce(ifelse(msd_repaid_2016 > MIN_CHECK,msd_repaid_2016,0),0),
      msd_repaid_2017 = coalesce(ifelse(msd_repaid_2017 > MIN_CHECK,msd_repaid_2017,0),0),
      msd_repaid_2018 = coalesce(ifelse(msd_repaid_2018 > MIN_CHECK,msd_repaid_2018,0),0),
      msd_repaid_2019 = coalesce(ifelse(msd_repaid_2019 > MIN_CHECK,msd_repaid_2019,0),0),
      msd_repaid_2020 = coalesce(ifelse(msd_repaid_2020 > MIN_CHECK,msd_repaid_2020,0),0),
      
      # MOJ - Fine
      moj_fine_repaid_2012 = coalesce(ifelse(moj_fine_repaid_2012 > MIN_CHECK,moj_fine_repaid_2012,0),0),
      moj_fine_repaid_2013 = coalesce(ifelse(moj_fine_repaid_2013 > MIN_CHECK,moj_fine_repaid_2013,0),0),
      moj_fine_repaid_2014 = coalesce(ifelse(moj_fine_repaid_2014 > MIN_CHECK,moj_fine_repaid_2014,0),0),
      moj_fine_repaid_2015 = coalesce(ifelse(moj_fine_repaid_2015 > MIN_CHECK,moj_fine_repaid_2015,0),0),
      moj_fine_repaid_2016 = coalesce(ifelse(moj_fine_repaid_2016 > MIN_CHECK,moj_fine_repaid_2016,0),0),
      moj_fine_repaid_2017 = coalesce(ifelse(moj_fine_repaid_2017 > MIN_CHECK,moj_fine_repaid_2017,0),0),
      moj_fine_repaid_2018 = coalesce(ifelse(moj_fine_repaid_2018 > MIN_CHECK,moj_fine_repaid_2018,0),0),
      moj_fine_repaid_2019 = coalesce(ifelse(moj_fine_repaid_2019 > MIN_CHECK,moj_fine_repaid_2019,0),0),
      moj_fine_repaid_2020 = coalesce(ifelse(moj_fine_repaid_2020 > MIN_CHECK,moj_fine_repaid_2020,0),0),
      
      # MOJ - FCCO
      moj_fcco_repaid_2014 = coalesce(ifelse(moj_fcco_repaid_2014 > MIN_CHECK,moj_fcco_repaid_2014,0),0),
      moj_fcco_repaid_2015 = coalesce(ifelse(moj_fcco_repaid_2015 > MIN_CHECK,moj_fcco_repaid_2015,0),0),
      moj_fcco_repaid_2016 = coalesce(ifelse(moj_fcco_repaid_2016 > MIN_CHECK,moj_fcco_repaid_2016,0),0),
      moj_fcco_repaid_2017 = coalesce(ifelse(moj_fcco_repaid_2017 > MIN_CHECK,moj_fcco_repaid_2017,0),0),
      moj_fcco_repaid_2018 = coalesce(ifelse(moj_fcco_repaid_2018 > MIN_CHECK,moj_fcco_repaid_2018,0),0),
      moj_fcco_repaid_2019 = coalesce(ifelse(moj_fcco_repaid_2019 > MIN_CHECK,moj_fcco_repaid_2019,0),0),
      moj_fcco_repaid_2020 = coalesce(ifelse(moj_fcco_repaid_2020 > MIN_CHECK,moj_fcco_repaid_2020,0),0),
      
      # IR - Child Support
      ird_pr19_repaid_child_2012 = coalesce(ifelse(ird_pr19_repaid_child_2012 > MIN_CHECK,ird_pr19_repaid_child_2012,0),0),
      ird_pr19_repaid_child_2013 = coalesce(ifelse(ird_pr19_repaid_child_2013 > MIN_CHECK,ird_pr19_repaid_child_2013,0),0),
      ird_pr19_repaid_child_2014 = coalesce(ifelse(ird_pr19_repaid_child_2014 > MIN_CHECK,ird_pr19_repaid_child_2014,0),0),
      ird_pr19_repaid_child_2015 = coalesce(ifelse(ird_pr19_repaid_child_2015 > MIN_CHECK,ird_pr19_repaid_child_2015,0),0),
      ird_pr19_repaid_child_2016 = coalesce(ifelse(ird_pr19_repaid_child_2016 > MIN_CHECK,ird_pr19_repaid_child_2016,0),0),
      ird_pr19_repaid_child_2017 = coalesce(ifelse(ird_pr19_repaid_child_2017 > MIN_CHECK,ird_pr19_repaid_child_2017,0),0),
      ird_pr19_repaid_child_2018 = coalesce(ifelse(ird_pr19_repaid_child_2018 > MIN_CHECK,ird_pr19_repaid_child_2018,0),0),
      ird_ps18_repayment_child_2019 = coalesce(ifelse(ird_ps18_repayment_child_2019 > MIN_CHECK,ird_ps18_repayment_child_2019,0),0),
      ird_ps18_repayment_child_2020 = coalesce(ifelse(ird_ps18_repayment_child_2020 > MIN_CHECK,ird_ps18_repayment_child_2020,0),0),
      
      # IR - Overdue Student Loan
      ird_pr19_repaid_student_2012 = coalesce(ifelse(ird_pr19_repaid_student_2012 > MIN_CHECK,ird_pr19_repaid_student_2012,0),0),
      ird_pr19_repaid_student_2013 = coalesce(ifelse(ird_pr19_repaid_student_2013 > MIN_CHECK,ird_pr19_repaid_student_2013,0),0),
      ird_pr19_repaid_student_2014 = coalesce(ifelse(ird_pr19_repaid_student_2014 > MIN_CHECK,ird_pr19_repaid_student_2014,0),0),
      ird_pr19_repaid_student_2015 = coalesce(ifelse(ird_pr19_repaid_student_2015 > MIN_CHECK,ird_pr19_repaid_student_2015,0),0),
      ird_pr19_repaid_student_2016 = coalesce(ifelse(ird_pr19_repaid_student_2016 > MIN_CHECK,ird_pr19_repaid_student_2016,0),0),
      ird_pr19_repaid_student_2017 = coalesce(ifelse(ird_pr19_repaid_student_2017 > MIN_CHECK,ird_pr19_repaid_student_2017,0),0),
      ird_pr19_repaid_student_2018 = coalesce(ifelse(ird_pr19_repaid_student_2018 > MIN_CHECK,ird_pr19_repaid_student_2018,0),0),
      # ird_ps18_repayment_student_2019 = coalesce(ifelse(ird_ps18_repayment_student_2019 > MIN_CHECK,ird_ps18_repayment_student_2019,0),0),
      ird_ps18_repayment_student_2020 = coalesce(ifelse(ird_ps18_repayment_student_2020 > MIN_CHECK,ird_ps18_repayment_student_2020,0),0),
      
      # IR - Income Tax
      ird_pr19_repaid_income_2012 = coalesce(ifelse(ird_pr19_repaid_income_2012 > MIN_CHECK,ird_pr19_repaid_income_2012,0),0),
      ird_pr19_repaid_income_2013 = coalesce(ifelse(ird_pr19_repaid_income_2013 > MIN_CHECK,ird_pr19_repaid_income_2013,0),0),
      ird_pr19_repaid_income_2014 = coalesce(ifelse(ird_pr19_repaid_income_2014 > MIN_CHECK,ird_pr19_repaid_income_2014,0),0),
      ird_pr19_repaid_income_2015 = coalesce(ifelse(ird_pr19_repaid_income_2015 > MIN_CHECK,ird_pr19_repaid_income_2015,0),0),
      ird_pr19_repaid_income_2016 = coalesce(ifelse(ird_pr19_repaid_income_2016 > MIN_CHECK,ird_pr19_repaid_income_2016,0),0),
      ird_pr19_repaid_income_2017 = coalesce(ifelse(ird_pr19_repaid_income_2017 > MIN_CHECK,ird_pr19_repaid_income_2017,0),0),
      ird_pr19_repaid_income_2018 = coalesce(ifelse(ird_pr19_repaid_income_2018 > MIN_CHECK,ird_pr19_repaid_income_2018,0),0),
      ird_ps18_repayment_income_2019 = coalesce(ifelse(ird_ps18_repayment_income_2019 > MIN_CHECK,ird_ps18_repayment_income_2019,0),0),
      ird_ps18_repayment_income_2020 = coalesce(ifelse(ird_ps18_repayment_income_2020 > MIN_CHECK,ird_ps18_repayment_income_2020,0),0),
      
      # IR - Working For Families
      ird_pr19_repaid_wff_2012 = coalesce(ifelse(ird_pr19_repaid_wff_2012 > MIN_CHECK,ird_pr19_repaid_wff_2012,0),0),
      ird_pr19_repaid_wff_2013 = coalesce(ifelse(ird_pr19_repaid_wff_2013 > MIN_CHECK,ird_pr19_repaid_wff_2013,0),0),
      ird_pr19_repaid_wff_2014 = coalesce(ifelse(ird_pr19_repaid_wff_2014 > MIN_CHECK,ird_pr19_repaid_wff_2014,0),0),
      ird_pr19_repaid_wff_2015 = coalesce(ifelse(ird_pr19_repaid_wff_2015 > MIN_CHECK,ird_pr19_repaid_wff_2015,0),0),
      ird_pr19_repaid_wff_2016 = coalesce(ifelse(ird_pr19_repaid_wff_2016 > MIN_CHECK,ird_pr19_repaid_wff_2016,0),0),
      ird_pr19_repaid_wff_2017 = coalesce(ifelse(ird_pr19_repaid_wff_2017 > MIN_CHECK,ird_pr19_repaid_wff_2017,0),0),
      ird_pr19_repaid_wff_2018 = coalesce(ifelse(ird_pr19_repaid_wff_2018 > MIN_CHECK,ird_pr19_repaid_wff_2018,0),0),
      ird_ps18_repayment_wff_2019 = coalesce(ifelse(ird_ps18_repayment_wff_2019 > MIN_CHECK,ird_ps18_repayment_wff_2019,0),0),
      ird_ps18_repayment_wff_2020 = coalesce(ifelse(ird_ps18_repayment_wff_2020 > MIN_CHECK,ird_ps18_repayment_wff_2020,0),0),
      
      # IR - Others
      ird_pr19_repaid_oth_2012 = coalesce(ifelse(ird_pr19_repaid_oth_2012 > MIN_CHECK,ird_pr19_repaid_oth_2012,0),0),
      ird_pr19_repaid_oth_2013 = coalesce(ifelse(ird_pr19_repaid_oth_2013 > MIN_CHECK,ird_pr19_repaid_oth_2013,0),0),
      ird_pr19_repaid_oth_2014 = coalesce(ifelse(ird_pr19_repaid_oth_2014 > MIN_CHECK,ird_pr19_repaid_oth_2014,0),0),
      ird_pr19_repaid_oth_2015 = coalesce(ifelse(ird_pr19_repaid_oth_2015 > MIN_CHECK,ird_pr19_repaid_oth_2015,0),0),
      ird_pr19_repaid_oth_2016 = coalesce(ifelse(ird_pr19_repaid_oth_2016 > MIN_CHECK,ird_pr19_repaid_oth_2016,0),0),
      ird_pr19_repaid_oth_2017 = coalesce(ifelse(ird_pr19_repaid_oth_2017 > MIN_CHECK,ird_pr19_repaid_oth_2017,0),0),
      ird_pr19_repaid_oth_2018 = coalesce(ifelse(ird_pr19_repaid_oth_2018 > MIN_CHECK,ird_pr19_repaid_oth_2018,0),0),
      ird_ps18_repayment_oth_2019 = coalesce(ifelse(ird_ps18_repayment_oth_2019 > MIN_CHECK,ird_ps18_repayment_oth_2019,0),0),
      ird_ps18_repayment_oth_2020 = coalesce(ifelse(ird_ps18_repayment_oth_2020 > MIN_CHECK,ird_ps18_repayment_oth_2020,0),0)
)

## INTERIM SAVE 1 -------------------------------------------------------------------------------------------------------------------
working_table = write_for_reuse(db_con, SANDPIT, OUR_SCHEMA, INTERIM_TABLE_1, working_table)
## Debtor Indicator -------------------------------------------------------------------------
working_table = working_table %>%
  # MSD Debtors ------------------------------------------------------------------------------
    mutate(
      msd_12_debtor = ifelse(msd_balance_2012 > 0, 1, 0),  
      msd_13_debtor = ifelse(msd_balance_2013 > 0, 1, 0),
      msd_14_debtor = ifelse(msd_balance_2014 > 0, 1, 0),
      msd_15_debtor = ifelse(msd_balance_2015 > 0, 1, 0),
      msd_16_debtor = ifelse(msd_balance_2016 > 0, 1, 0),
      msd_17_debtor = ifelse(msd_balance_2017 > 0, 1, 0),
      msd_18_debtor = ifelse(msd_balance_2018 > 0, 1, 0),
      msd_19_debtor = ifelse(msd_balance_2019 > 0, 1, 0),
      msd_20_debtor = ifelse(msd_balance_2020 > 0, 1, 0)
    ) %>%
  # MOJ Fine Debtors -------------------------------------------------------------------------
    mutate(
      moj_fine_12_debtor = ifelse(moj_fine_balance_2012 > 0, 1, 0),  
      moj_fine_13_debtor = ifelse(moj_fine_balance_2013 > 0, 1, 0),
      moj_fine_14_debtor = ifelse(moj_fine_balance_2014 > 0, 1, 0),
      moj_fine_15_debtor = ifelse(moj_fine_balance_2015 > 0, 1, 0),
      moj_fine_16_debtor = ifelse(moj_fine_balance_2016 > 0, 1, 0),
      moj_fine_17_debtor = ifelse(moj_fine_balance_2017 > 0, 1, 0),
      moj_fine_18_debtor = ifelse(moj_fine_balance_2018 > 0, 1, 0),
      moj_fine_19_debtor = ifelse(moj_fine_balance_2019 > 0, 1, 0),
      moj_fine_20_debtor = ifelse(moj_fine_balance_2020 > 0, 1, 0)
    ) %>%
  # MOJ FCCO Debtors -------------------------------------------------------------------------
    mutate(
      moj_fcco_14_debtor = ifelse(moj_fcco_balance_2014 > 0, 1, 0),
      moj_fcco_15_debtor = ifelse(moj_fcco_balance_2015 > 0, 1, 0),
      moj_fcco_16_debtor = ifelse(moj_fcco_balance_2016 > 0, 1, 0),
      moj_fcco_17_debtor = ifelse(moj_fcco_balance_2017 > 0, 1, 0),
      moj_fcco_18_debtor = ifelse(moj_fcco_balance_2018 > 0, 1, 0),
      moj_fcco_19_debtor = ifelse(moj_fcco_balance_2019 > 0, 1, 0),
      moj_fcco_20_debtor = ifelse(moj_fcco_balance_2020 > 0, 1, 0)
    ) %>%
  # IR - Child Support Debtors  ------------------------------------------------------------------------- 
    mutate(
      ir_child_12_debtor = ifelse(ird_pr19_balance_child_2012 > 0, 1, 0),  
      ir_child_13_debtor = ifelse(ird_pr19_balance_child_2013 > 0, 1, 0),
      ir_child_14_debtor = ifelse(ird_pr19_balance_child_2014 > 0, 1, 0),
      ir_child_15_debtor = ifelse(ird_pr19_balance_child_2015 > 0, 1, 0),
      ir_child_16_debtor = ifelse(ird_pr19_balance_child_2016 > 0, 1, 0),
      ir_child_17_debtor = ifelse(ird_pr19_balance_child_2017 > 0, 1, 0),
      ir_child_18_debtor = ifelse(ird_pr19_balance_child_2018 > 0, 1, 0),
      ir_child_19_debtor = ifelse(ird_ps18_balance_child_2019 > 0, 1, 0),
      ir_child_20_debtor = ifelse(ird_ps18_balance_child_2020 > 0, 1, 0)
    ) %>%
  # IR - Income Tax Debtors -------------------------------------------------------------------------  
    mutate(
      ir_income_12_debtor = ifelse(ird_pr19_balance_income_2012 > 0, 1, 0),  
      ir_income_13_debtor = ifelse(ird_pr19_balance_income_2013 > 0, 1, 0),
      ir_income_14_debtor = ifelse(ird_pr19_balance_income_2014 > 0, 1, 0),
      ir_income_15_debtor = ifelse(ird_pr19_balance_income_2015 > 0, 1, 0),
      ir_income_16_debtor = ifelse(ird_pr19_balance_income_2016 > 0, 1, 0),
      ir_income_17_debtor = ifelse(ird_pr19_balance_income_2017 > 0, 1, 0),
      ir_income_18_debtor = ifelse(ird_pr19_balance_income_2018 > 0, 1, 0),
      ir_income_19_debtor = ifelse(ird_ps18_balance_income_2019 > 0, 1, 0),
      ir_income_20_debtor = ifelse(ird_ps18_balance_income_2020 > 0, 1, 0)
    ) %>%
  # IR - Overdue Student Loan Debtors -------------------------------------------------------------------------
    mutate(
      ir_student_12_debtor = ifelse(ird_pr19_balance_student_2012 > 0, 1, 0),  
      ir_student_13_debtor = ifelse(ird_pr19_balance_student_2013 > 0, 1, 0),
      ir_student_14_debtor = ifelse(ird_pr19_balance_student_2014 > 0, 1, 0),
      ir_student_15_debtor = ifelse(ird_pr19_balance_student_2015 > 0, 1, 0),
      ir_student_16_debtor = ifelse(ird_pr19_balance_student_2016 > 0, 1, 0),
      ir_student_17_debtor = ifelse(ird_pr19_balance_student_2017 > 0, 1, 0),
      ir_student_18_debtor = ifelse(ird_pr19_balance_student_2018 > 0, 1, 0),
      # ir_student_19_debtor = ifelse(ird_ps18_balance_student_2019 > 0, 1, 0),
      ir_student_20_debtor = ifelse(ird_ps18_balance_student_2020 > 0, 1, 0)
    ) %>%
  # IR - Working For Families Debtors -------------------------------------------------------------------------
    mutate(
      ir_wff_12_debtor = ifelse(ird_pr19_balance_wff_2012 > 0, 1, 0),  
      ir_wff_13_debtor = ifelse(ird_pr19_balance_wff_2013 > 0, 1, 0),
      ir_wff_14_debtor = ifelse(ird_pr19_balance_wff_2014 > 0, 1, 0),
      ir_wff_15_debtor = ifelse(ird_pr19_balance_wff_2015 > 0, 1, 0),
      ir_wff_16_debtor = ifelse(ird_pr19_balance_wff_2016 > 0, 1, 0),
      ir_wff_17_debtor = ifelse(ird_pr19_balance_wff_2017 > 0, 1, 0),
      ir_wff_18_debtor = ifelse(ird_pr19_balance_wff_2018 > 0, 1, 0),
      ir_wff_19_debtor = ifelse(ird_ps18_balance_wff_2019 > 0, 1, 0),
      ir_wff_20_debtor = ifelse(ird_ps18_balance_wff_2020 > 0, 1, 0)
    ) %>%
  # IR - Other Debtors -------------------------------------------------------------------------
    mutate(
      ir_oth_12_debtor = ifelse(ird_pr19_balance_oth_2012 > 0, 1, 0),  
      ir_oth_13_debtor = ifelse(ird_pr19_balance_oth_2013 > 0, 1, 0),
      ir_oth_14_debtor = ifelse(ird_pr19_balance_oth_2014 > 0, 1, 0),
      ir_oth_15_debtor = ifelse(ird_pr19_balance_oth_2015 > 0, 1, 0),
      ir_oth_16_debtor = ifelse(ird_pr19_balance_oth_2016 > 0, 1, 0),
      ir_oth_17_debtor = ifelse(ird_pr19_balance_oth_2017 > 0, 1, 0),
      ir_oth_18_debtor = ifelse(ird_pr19_balance_oth_2018 > 0, 1, 0),
      ir_oth_19_debtor = ifelse(ird_ps18_balance_oth_2019 > 0, 1, 0),
      ir_oth_20_debtor = ifelse(ird_ps18_balance_oth_2020 > 0, 1, 0)
    ) %>%
  # Imputing IR - Overdure Student Loan - 2019 using the coeffecients from the prediction modelling --------
  mutate(ir_student_19_debtor = ifelse((INTERCEPT + 
                                          (ALPHA * ir_student_18_debtor) + 
                                          (BETA *ir_student_20_debtor)) > 0, 1, 0))
  
## INTERIM SAVE 2 -------------------------------------------------------------------------------------------------------------------
working_table = write_for_reuse(db_con, SANDPIT, OUR_SCHEMA, INTERIM_TABLE_2, working_table)
## Total variables -------------------------------------------------------------------------------------------------------  
working_table = working_table %>%
  # Total Debt - Combined Debt Types ---------------------------------------------------------------
    mutate(msd_total_debt_20 = msd_balance_2020,
           moj_total_debt_20 = (moj_fcco_balance_2020 + 
                                moj_fine_balance_2020),
           ir_total_debt_20 = (ird_ps18_balance_child_2020 + 
                               ird_ps18_balance_income_2020 +
                               ird_ps18_balance_student_2020 +
                               ird_ps18_balance_wff_2020 +
                               ird_ps18_balance_oth_2020)
           ) %>%
  
    mutate(total_debt_group = case_when(msd_total_debt_20 + moj_total_debt_20 + ir_total_debt_20 > 10000 ~ ">10k",
                                        msd_total_debt_20 + moj_total_debt_20 + ir_total_debt_20 >  5000 ~ "5-10k",
                                        msd_total_debt_20 + moj_total_debt_20 + ir_total_debt_20 >  1000 ~ "1-5k",
                                        msd_total_debt_20 + moj_total_debt_20 + ir_total_debt_20 >    10 ~ "0-1k")
           )%>%
  # Total MOJ Repayments - Combined Debt Types -----------------------------------------------------
    mutate(
      moj_total_repayment_14 = (moj_fcco_repaid_2014 + moj_fine_repaid_2014),
      moj_total_repayment_15 = (moj_fcco_repaid_2015 + moj_fine_repaid_2015),
      moj_total_repayment_16 = (moj_fcco_repaid_2016 + moj_fine_repaid_2016),
      moj_total_repayment_17 = (moj_fcco_repaid_2017 + moj_fine_repaid_2017),
      moj_total_repayment_18 = (moj_fcco_repaid_2018 + moj_fine_repaid_2018),
      moj_total_repayment_19 = (moj_fcco_repaid_2019 + moj_fine_repaid_2019),
      moj_total_repayment_20 = (moj_fcco_repaid_2020 + moj_fine_repaid_2020)
    ) %>%
  # Total IR Repayments - Combined Debt Types -------------------------------------------------------------------------
    mutate(
      ir_total_repayment_12 = (ird_pr19_repaid_child_2012 + 
                                 ird_pr19_repaid_income_2012 +
                                 ird_pr19_repaid_student_2012 +
                                 ird_pr19_repaid_wff_2012 +
                                 ird_pr19_repaid_oth_2012),
      ir_total_repayment_13 = (ird_pr19_repaid_child_2013 + 
                                 ird_pr19_repaid_income_2013 +
                                 ird_pr19_repaid_student_2013 +
                                 ird_pr19_repaid_wff_2013 +
                                 ird_pr19_repaid_oth_2013),
      ir_total_repayment_14 = (ird_pr19_repaid_child_2014 + 
                                 ird_pr19_repaid_income_2014 +
                                 ird_pr19_repaid_student_2014 +
                                 ird_pr19_repaid_wff_2014 +
                                 ird_pr19_repaid_oth_2014),
      ir_total_repayment_15 = (ird_pr19_repaid_child_2015 + 
                                 ird_pr19_repaid_income_2015 +
                                 ird_pr19_repaid_student_2015 +
                                 ird_pr19_repaid_wff_2015 +
                                 ird_pr19_repaid_oth_2015),
      ir_total_repayment_16 = (ird_pr19_repaid_child_2016 + 
                                 ird_pr19_repaid_income_2016 +
                                 ird_pr19_repaid_student_2016 +
                                 ird_pr19_repaid_wff_2016 +
                                 ird_pr19_repaid_oth_2016),
      ir_total_repayment_17 = (ird_pr19_repaid_child_2017 + 
                                 ird_pr19_repaid_income_2017 +
                                 ird_pr19_repaid_student_2017 +
                                 ird_pr19_repaid_wff_2017 +
                                 ird_pr19_repaid_oth_2017),
      ir_total_repayment_18 = (ird_pr19_repaid_child_2018 + 
                                 ird_pr19_repaid_income_2018 +
                                 ird_pr19_repaid_student_2018 +
                                 ird_pr19_repaid_wff_2018 +
                                 ird_pr19_repaid_oth_2018),
      ir_total_repayment_19 = (ird_ps18_repayment_child_2019 + 
                                 ird_ps18_repayment_income_2019 +
                                 # ird_ps18_repayment_student_2019 +
                                 ird_ps18_repayment_wff_2019 +
                                 ird_ps18_repayment_oth_2019),
      ir_total_repayment_20 = (ird_ps18_repayment_child_2020 + 
                                 ird_ps18_repayment_income_2020 +
                                 ird_ps18_repayment_student_2020 +
                                 ird_ps18_repayment_wff_2020 +
                                 ird_ps18_repayment_oth_2020)
    ) %>%
  # Total number of debt types in 2020 ----------------------------------------------------------
  mutate( total_debt_type_20 = msd_20_debtor +
            moj_fine_20_debtor +
            moj_fcco_20_debtor +
            ir_child_20_debtor +
            ir_income_20_debtor +
            ir_student_20_debtor +
            ir_wff_20_debtor +
            ir_oth_20_debtor)%>%
## Age variables --------------------------------------------------------------------------------------
  # age at end of 2020 -------------------------------------------------------------------------
  mutate(age = 2020 - birth_year) %>%
  # age categories -------------------------------------------------------------------------
  mutate(
    age_cat = case_when(00 <= age & age < 10 ~ "00_to_09",
                        10 <= age & age < 20 ~ "10_to_19",
                        20 <= age & age < 30 ~ "20_to_29",
                        30 <= age & age < 40 ~ "30_to_39",
                        40 <= age & age < 50 ~ "40_to_49",
                        50 <= age & age < 60 ~ "50_to_59",
                        60 <= age & age < 70 ~ "60_to_69",
                        70 <= age & age < 80 ~ "70_to_79",
                        80 <= age ~ "80_up"),
    yngst_child_indv_age = case_when(2020 - yngst_dep_chld_indv >= 19 ~ NA,
                                     2020 - yngst_dep_chld_indv >= 13 ~ "13_to_18",
                                     2020 - yngst_dep_chld_indv >=  7 ~ "07_to_12",
                                     2020 - yngst_dep_chld_indv >=  4 ~ "04_to_06",
                                     2020 - yngst_dep_chld_indv >=  0 ~ "00_to_03"),
    yngst_child_hhld_age = case_when(2020 - yngst_dep_chld_hhld >= 19 ~ NA,
                                     2020 - yngst_dep_chld_hhld >= 13 ~ "13_to_18",
                                     2020 - yngst_dep_chld_hhld >=  7 ~ "07_to_12",
                                     2020 - yngst_dep_chld_hhld >=  4 ~ "04_to_06",
                                     2020 - yngst_dep_chld_hhld >=  0 ~ "00_to_03")) %>%
## Income variables --------------------------------------------------------------------------------------
  # Total Income -------------------------------------------------------------------------
  mutate(total_income = round(income_taxible,2) + 
           round(income_t2_benefits,2) + 
           round(income_t3_benefits,2) + 
           round(income_wff,2)) %>%
  # Low Income indicator -------------------------------------------------------------------------
  mutate(low_income = ifelse(total_income < LOW_INCOME_THRESHOLD, 1, 0)) %>%
  # Income post repayments ----------------------------------------------------------
  mutate(
    income_post_all_repayment = total_income - (msd_repaid_2020 + moj_total_repayment_20 + ir_total_repayment_20),
    income_post_msd_repayment = total_income - msd_repaid_2020,
    income_post_moj_repayment = total_income - moj_total_repayment_20,
    income_post_ir_repayment = total_income - ir_total_repayment_20
  )%>%
  # Income categories ---------------------------------------------------------------------------------------------------------------------
  mutate(
    total_income_cat = case_when(    0 <= total_income & total_income < 10000 ~ "00-10k",
                                     10000 <= total_income & total_income < 20000 ~ "10-20k",
                                     20000 <= total_income & total_income < 30000 ~ "20-30k",
                                     30000 <= total_income & total_income < 40000 ~ "30-40k",
                                     40000 <= total_income & total_income < 50000 ~ "40-50k",
                                     50000 <= total_income & total_income < 60000 ~ "50-60k",
                                     60000 <= total_income & total_income < 70000 ~ "60-70k",
                                     70000 <= total_income & total_income < 80000 ~ "70-80k",
                                     80000 <= total_income & total_income < 90000 ~ "80-90k",
                                     90000 <= total_income ~ "90k+"),
    income_post_all_repayment_cat = case_when(    0 <= income_post_all_repayment & income_post_all_repayment < 10000 ~ "00-10k",
                                                  10000 <= income_post_all_repayment & income_post_all_repayment < 20000 ~ "10-20k",
                                                  20000 <= income_post_all_repayment & income_post_all_repayment < 30000 ~ "20-30k",
                                                  30000 <= income_post_all_repayment & income_post_all_repayment < 40000 ~ "30-40k",
                                                  40000 <= income_post_all_repayment & income_post_all_repayment < 50000 ~ "40-50k",
                                                  50000 <= income_post_all_repayment & income_post_all_repayment < 60000 ~ "50-60k",
                                                  60000 <= income_post_all_repayment & income_post_all_repayment < 70000 ~ "60-70k",
                                                  70000 <= income_post_all_repayment & income_post_all_repayment < 80000 ~ "70-80k",
                                                  80000 <= income_post_all_repayment & income_post_all_repayment < 90000 ~ "80-90k",
                                                  90000 <= income_post_all_repayment ~ "90k+"),
    income_post_msd_repayment_cat = case_when(    0 <= income_post_msd_repayment & income_post_msd_repayment < 10000 ~ "00-10k",
                                                  10000 <= income_post_msd_repayment & income_post_msd_repayment < 20000 ~ "10-20k",
                                                  20000 <= income_post_msd_repayment & income_post_msd_repayment < 30000 ~ "20-30k",
                                                  30000 <= income_post_msd_repayment & income_post_msd_repayment < 40000 ~ "30-40k",
                                                  40000 <= income_post_msd_repayment & income_post_msd_repayment < 50000 ~ "40-50k",
                                                  50000 <= income_post_msd_repayment & income_post_msd_repayment < 60000 ~ "50-60k",
                                                  60000 <= income_post_msd_repayment & income_post_msd_repayment < 70000 ~ "60-70k",
                                                  70000 <= income_post_msd_repayment & income_post_msd_repayment < 80000 ~ "70-80k",
                                                  80000 <= income_post_msd_repayment & income_post_msd_repayment < 90000 ~ "80-90k",
                                                  90000 <= income_post_msd_repayment ~ "90k+"),
    income_post_moj_repayment_cat = case_when(    0 <= income_post_moj_repayment & income_post_moj_repayment < 10000 ~ "00-10k",
                                                  10000 <= income_post_moj_repayment & income_post_moj_repayment < 20000 ~ "10-20k",
                                                  20000 <= income_post_moj_repayment & income_post_moj_repayment < 30000 ~ "20-30k",
                                                  30000 <= income_post_moj_repayment & income_post_moj_repayment < 40000 ~ "30-40k",
                                                  40000 <= income_post_moj_repayment & income_post_moj_repayment < 50000 ~ "40-50k",
                                                  50000 <= income_post_moj_repayment & income_post_moj_repayment < 60000 ~ "50-60k",
                                                  60000 <= income_post_moj_repayment & income_post_moj_repayment < 70000 ~ "60-70k",
                                                  70000 <= income_post_moj_repayment & income_post_moj_repayment < 80000 ~ "70-80k",
                                                  80000 <= income_post_moj_repayment & income_post_moj_repayment < 90000 ~ "80-90k",
                                                  90000 <= income_post_moj_repayment ~ "90k+"),
    income_post_ir_repayment_cat = case_when(    0 <= income_post_ir_repayment & income_post_ir_repayment < 10000 ~ "00-10k",
                                                 10000 <= income_post_ir_repayment & income_post_ir_repayment < 20000 ~ "10-20k",
                                                 20000 <= income_post_ir_repayment & income_post_ir_repayment < 30000 ~ "20-30k",
                                                 30000 <= income_post_ir_repayment & income_post_ir_repayment < 40000 ~ "30-40k",
                                                 40000 <= income_post_ir_repayment & income_post_ir_repayment < 50000 ~ "40-50k",
                                                 50000 <= income_post_ir_repayment & income_post_ir_repayment < 60000 ~ "50-60k",
                                                 60000 <= income_post_ir_repayment & income_post_ir_repayment < 70000 ~ "60-70k",
                                                 70000 <= income_post_ir_repayment & income_post_ir_repayment < 80000 ~ "70-80k",
                                                 80000 <= income_post_ir_repayment & income_post_ir_repayment < 90000 ~ "80-90k",
                                                 90000 <= income_post_ir_repayment ~ "90k+")
    
  )
## INTERIM SAVE 3 -------------------------------------------------------------------------------------------------------------------
working_table = write_for_reuse(db_con, SANDPIT, OUR_SCHEMA, INTERIM_TABLE_3, working_table)
## Previous years of debt observed -------------------------------------------------------------------------
working_table = working_table %>%
  # MSD ----------------------------------------------------------
      mutate(
        msd_persistence_2yrs = ifelse(msd_20_debtor == 1 & 
                                      msd_19_debtor == 1, 1, NA),
        msd_persistence_3yrs = ifelse(msd_20_debtor == 1 & 
                                      msd_19_debtor == 1 & 
                                      msd_18_debtor == 1, 1, NA),
        msd_persistence_4yrs = ifelse(msd_20_debtor == 1 & 
                                      msd_19_debtor == 1 & 
                                      msd_18_debtor == 1 & 
                                      msd_17_debtor == 1, 1, NA),
        msd_persistence_5yrs = ifelse(msd_20_debtor == 1 & 
                                      msd_19_debtor == 1 & 
                                      msd_18_debtor == 1 & 
                                      msd_17_debtor == 1 & 
                                      msd_16_debtor == 1, 1, NA),
        msd_persistence_6yrs = ifelse(msd_20_debtor == 1 & 
                                      msd_19_debtor == 1 & 
                                      msd_18_debtor == 1 & 
                                      msd_17_debtor == 1 & 
                                      msd_16_debtor == 1 & 
                                      msd_15_debtor == 1, 1, NA),
        msd_persistence_7yrs = ifelse(msd_20_debtor == 1 & 
                                      msd_19_debtor == 1 & 
                                      msd_18_debtor == 1 & 
                                      msd_17_debtor == 1 & 
                                      msd_16_debtor == 1 & 
                                      msd_15_debtor == 1 & 
                                      msd_14_debtor == 1, 1, NA),
        msd_persistence_8yrs = ifelse(msd_20_debtor == 1 & 
                                      msd_19_debtor == 1 &
                                      msd_18_debtor == 1 & 
                                      msd_17_debtor == 1 & 
                                      msd_16_debtor == 1 & 
                                      msd_15_debtor == 1 &
                                      msd_14_debtor == 1 &
                                      msd_13_debtor == 1, 1, NA),
        msd_persistence_9yrs = ifelse(msd_20_debtor == 1 & 
                                        msd_19_debtor == 1 &
                                        msd_18_debtor == 1 & 
                                        msd_17_debtor == 1 & 
                                        msd_16_debtor == 1 & 
                                        msd_15_debtor == 1 &
                                        msd_14_debtor == 1 &
                                        msd_13_debtor == 1 &
                                        msd_12_debtor == 1, 1, NA)
      ) %>%
  # MOJ (Fines) ----------------------------------------------------------
      mutate(
        moj_fine_persistence_2yrs = ifelse(moj_fine_20_debtor == 1 & 
                                           moj_fine_19_debtor == 1, 1, NA),
        moj_fine_persistence_3yrs = ifelse(moj_fine_20_debtor == 1 & 
                                           moj_fine_19_debtor == 1 & 
                                           moj_fine_18_debtor == 1, 1, NA),
        moj_fine_persistence_4yrs = ifelse(moj_fine_20_debtor == 1 & 
                                           moj_fine_19_debtor == 1 & 
                                           moj_fine_18_debtor == 1 & 
                                           moj_fine_17_debtor == 1, 1, NA),
        moj_fine_persistence_5yrs = ifelse(moj_fine_20_debtor == 1 & 
                                           moj_fine_19_debtor == 1 & 
                                           moj_fine_18_debtor == 1 & 
                                           moj_fine_17_debtor == 1 & 
                                           moj_fine_16_debtor == 1, 1, NA),
        moj_fine_persistence_6yrs = ifelse(moj_fine_20_debtor == 1 & 
                                        moj_fine_19_debtor == 1 & 
                                        moj_fine_18_debtor == 1 & 
                                        moj_fine_17_debtor == 1 & 
                                        moj_fine_16_debtor == 1 & 
                                        moj_fine_15_debtor == 1, 1, NA),
        moj_fine_persistence_7yrs = ifelse(moj_fine_20_debtor == 1 & 
                                        moj_fine_19_debtor == 1 & 
                                        moj_fine_18_debtor == 1 & 
                                        moj_fine_17_debtor == 1 & 
                                        moj_fine_16_debtor == 1 & 
                                        moj_fine_15_debtor == 1 & 
                                        moj_fine_14_debtor == 1, 1, NA),
        moj_fine_persistence_8yrs = ifelse(moj_fine_20_debtor == 1 & 
                                        moj_fine_19_debtor == 1 &
                                        moj_fine_18_debtor == 1 & 
                                        moj_fine_17_debtor == 1 & 
                                        moj_fine_16_debtor == 1 & 
                                        moj_fine_15_debtor == 1 &
                                        moj_fine_14_debtor == 1 &
                                        moj_fine_13_debtor == 1, 1, NA),
        moj_fine_persistence_9yrs = ifelse(moj_fine_20_debtor == 1 & 
                                        moj_fine_19_debtor == 1 &
                                        moj_fine_18_debtor == 1 & 
                                        moj_fine_17_debtor == 1 & 
                                        moj_fine_16_debtor == 1 & 
                                        moj_fine_15_debtor == 1 &
                                        moj_fine_14_debtor == 1 &
                                        moj_fine_13_debtor == 1 &
                                        moj_fine_12_debtor == 1, 1, NA)
      ) %>%
    
  # MOJ (FCCO) ----------------------------------------------------------
      mutate(
        moj_fcco_persistence_2yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                        moj_fcco_19_debtor == 1, 1, NA),
        moj_fcco_persistence_3yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                        moj_fcco_19_debtor == 1 & 
                                        moj_fcco_18_debtor == 1, 1, NA),
        moj_fcco_persistence_4yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                        moj_fcco_19_debtor == 1 & 
                                        moj_fcco_18_debtor == 1 & 
                                        moj_fcco_17_debtor == 1, 1, NA),
        moj_fcco_persistence_5yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                        moj_fcco_19_debtor == 1 & 
                                        moj_fcco_18_debtor == 1 & 
                                        moj_fcco_17_debtor == 1 & 
                                        moj_fcco_16_debtor == 1, 1, NA),
        moj_fcco_persistence_6yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                        moj_fcco_19_debtor == 1 & 
                                        moj_fcco_18_debtor == 1 & 
                                        moj_fcco_17_debtor == 1 & 
                                        moj_fcco_16_debtor == 1 & 
                                        moj_fcco_15_debtor == 1, 1, NA),
        moj_fcco_persistence_7yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                        moj_fcco_19_debtor == 1 & 
                                        moj_fcco_18_debtor == 1 & 
                                        moj_fcco_17_debtor == 1 & 
                                        moj_fcco_16_debtor == 1 & 
                                        moj_fcco_15_debtor == 1 & 
                                        moj_fcco_14_debtor == 1, 1, NA)
      ) %>%
    
  # IR (Child) ----------------------------------------------------------
      mutate(
        ir_child_persistence_2yrs = ifelse(ir_child_20_debtor == 1 & 
                                             ir_child_19_debtor == 1, 1, NA),
        ir_child_persistence_3yrs = ifelse(ir_child_20_debtor == 1 & 
                                             ir_child_19_debtor == 1 & 
                                             ir_child_18_debtor == 1, 1, NA),
        ir_child_persistence_4yrs = ifelse(ir_child_20_debtor == 1 & 
                                             ir_child_19_debtor == 1 & 
                                             ir_child_18_debtor == 1 & 
                                             ir_child_17_debtor == 1, 1, NA),
        ir_child_persistence_5yrs = ifelse(ir_child_20_debtor == 1 & 
                                             ir_child_19_debtor == 1 & 
                                             ir_child_18_debtor == 1 & 
                                             ir_child_17_debtor == 1 & 
                                             ir_child_16_debtor == 1, 1, NA),
        ir_child_persistence_6yrs = ifelse(ir_child_20_debtor == 1 & 
                                             ir_child_19_debtor == 1 & 
                                             ir_child_18_debtor == 1 & 
                                             ir_child_17_debtor == 1 & 
                                             ir_child_16_debtor == 1 & 
                                             ir_child_15_debtor == 1, 1, NA),
        ir_child_persistence_7yrs = ifelse(ir_child_20_debtor == 1 & 
                                             ir_child_19_debtor == 1 & 
                                             ir_child_18_debtor == 1 & 
                                             ir_child_17_debtor == 1 & 
                                             ir_child_16_debtor == 1 & 
                                             ir_child_15_debtor == 1 & 
                                             ir_child_14_debtor == 1, 1, NA),
        ir_child_persistence_8yrs = ifelse(ir_child_20_debtor == 1 & 
                                        ir_child_19_debtor == 1 & 
                                        ir_child_18_debtor == 1 & 
                                        ir_child_17_debtor == 1 & 
                                        ir_child_16_debtor == 1 & 
                                        ir_child_15_debtor == 1 & 
                                        ir_child_14_debtor == 1 &
                                        ir_child_13_debtor == 1, 1, NA),
        ir_child_persistence_9yrs = ifelse(ir_child_20_debtor == 1 & 
                                        ir_child_19_debtor == 1 & 
                                        ir_child_18_debtor == 1 & 
                                        ir_child_17_debtor == 1 & 
                                        ir_child_16_debtor == 1 & 
                                        ir_child_15_debtor == 1 & 
                                        ir_child_14_debtor == 1 &
                                        ir_child_13_debtor == 1 &
                                        ir_child_12_debtor == 1, 1, NA)
      )%>%
  # IR (Income) ----------------------------------------------------------
      mutate(
        ir_income_persistence_2yrs = ifelse(ir_income_20_debtor == 1 & 
                                              ir_income_19_debtor == 1, 1, NA),
        ir_income_persistence_3yrs = ifelse(ir_income_20_debtor == 1 & 
                                              ir_income_19_debtor == 1 & 
                                              ir_income_18_debtor == 1, 1, NA),
        ir_income_persistence_4yrs = ifelse(ir_income_20_debtor == 1 & 
                                              ir_income_19_debtor == 1 & 
                                              ir_income_18_debtor == 1 & 
                                              ir_income_17_debtor == 1, 1, NA),
        ir_income_persistence_5yrs = ifelse(ir_income_20_debtor == 1 & 
                                              ir_income_19_debtor == 1 & 
                                              ir_income_18_debtor == 1 & 
                                              ir_income_17_debtor == 1 & 
                                              ir_income_16_debtor == 1, 1, NA),
        ir_income_persistence_6yrs = ifelse(ir_income_20_debtor == 1 & 
                                              ir_income_19_debtor == 1 & 
                                              ir_income_18_debtor == 1 & 
                                              ir_income_17_debtor == 1 & 
                                              ir_income_16_debtor == 1 & 
                                              ir_income_15_debtor == 1, 1, NA),
        ir_income_persistence_7yrs = ifelse(ir_income_20_debtor == 1 & 
                                              ir_income_19_debtor == 1 & 
                                              ir_income_18_debtor == 1 & 
                                              ir_income_17_debtor == 1 & 
                                              ir_income_16_debtor == 1 & 
                                              ir_income_15_debtor == 1 & 
                                              ir_income_14_debtor == 1, 1, NA),
        ir_income_persistence_8yrs = ifelse(ir_income_20_debtor == 1 & 
                                              ir_income_19_debtor == 1 & 
                                              ir_income_18_debtor == 1 & 
                                              ir_income_17_debtor == 1 & 
                                              ir_income_16_debtor == 1 & 
                                              ir_income_15_debtor == 1 & 
                                              ir_income_14_debtor == 1 &
                                              ir_income_13_debtor == 1, 1, NA),
        ir_income_persistence_9yrs = ifelse(ir_income_20_debtor == 1 & 
                                              ir_income_19_debtor == 1 & 
                                              ir_income_18_debtor == 1 & 
                                              ir_income_17_debtor == 1 & 
                                              ir_income_16_debtor == 1 & 
                                              ir_income_15_debtor == 1 & 
                                              ir_income_14_debtor == 1 &
                                              ir_income_13_debtor == 1 &
                                              ir_income_12_debtor == 1, 1, NA)
      ) %>%
    
  # IR (Student) ----------------------------------------------------------
      mutate(
        ir_student_persistence_2yrs = ifelse(ir_student_20_debtor == 1 & 
                                              ir_student_19_debtor == 1, 1, NA),
        ir_student_persistence_3yrs = ifelse(ir_student_20_debtor == 1 & 
                                              ir_student_19_debtor == 1 & 
                                              ir_student_18_debtor == 1, 1, NA),
        ir_student_persistence_4yrs = ifelse(ir_student_20_debtor == 1 & 
                                               ir_student_19_debtor == 1 & 
                                               ir_student_18_debtor == 1 & 
                                               ir_student_17_debtor == 1, 1, NA),
        ir_student_persistence_5yrs = ifelse(ir_student_20_debtor == 1 & 
                                               ir_student_19_debtor == 1 & 
                                               ir_student_18_debtor == 1 & 
                                               ir_student_17_debtor == 1 & 
                                               ir_student_16_debtor == 1, 1, NA),
        ir_student_persistence_6yrs = ifelse(ir_student_20_debtor == 1 & 
                                               ir_student_19_debtor == 1 & 
                                               ir_student_18_debtor == 1 & 
                                               ir_student_17_debtor == 1 & 
                                               ir_student_16_debtor == 1 & 
                                               ir_student_15_debtor == 1, 1, NA),
        ir_student_persistence_7yrs = ifelse(ir_student_20_debtor == 1 & 
                                               ir_student_19_debtor == 1 & 
                                               ir_student_18_debtor == 1 & 
                                               ir_student_17_debtor == 1 & 
                                               ir_student_16_debtor == 1 & 
                                               ir_student_15_debtor == 1 & 
                                               ir_student_14_debtor == 1, 1, NA),
        ir_student_persistence_8yrs = ifelse(ir_student_20_debtor == 1 & 
                                               ir_student_19_debtor == 1 & 
                                               ir_student_18_debtor == 1 & 
                                               ir_student_17_debtor == 1 & 
                                               ir_student_16_debtor == 1 & 
                                               ir_student_15_debtor == 1 & 
                                               ir_student_14_debtor == 1 &
                                               ir_student_13_debtor == 1, 1, NA),
        ir_student_persistence_9yrs = ifelse(ir_student_20_debtor == 1 & 
                                               ir_student_19_debtor == 1 & 
                                               ir_student_18_debtor == 1 & 
                                               ir_student_17_debtor == 1 & 
                                               ir_student_16_debtor == 1 & 
                                               ir_student_15_debtor == 1 & 
                                               ir_student_14_debtor == 1 &
                                               ir_student_13_debtor == 1 &
                                               ir_student_12_debtor == 1, 1, NA)
      )%>%
  # IR (WFF) ----------------------------------------------------------
      mutate(
        ir_wff_persistence_2yrs = ifelse(ir_wff_20_debtor == 1 & 
                                           ir_wff_19_debtor == 1, 1, NA),
        ir_wff_persistence_3yrs = ifelse(ir_wff_20_debtor == 1 & 
                                           ir_wff_19_debtor == 1 & 
                                           ir_wff_18_debtor == 1, 1, NA),
        ir_wff_persistence_4yrs = ifelse(ir_wff_20_debtor == 1 & 
                                           ir_wff_19_debtor == 1 & 
                                           ir_wff_18_debtor == 1 & 
                                           ir_wff_17_debtor == 1, 1, NA),
        ir_wff_persistence_5yrs = ifelse(ir_wff_20_debtor == 1 & 
                                           ir_wff_19_debtor == 1 & 
                                           ir_wff_18_debtor == 1 & 
                                           ir_wff_17_debtor == 1 & 
                                           ir_wff_16_debtor == 1, 1, NA),
        ir_wff_persistence_6yrs = ifelse(ir_wff_20_debtor == 1 & 
                                           ir_wff_19_debtor == 1 & 
                                           ir_wff_18_debtor == 1 & 
                                           ir_wff_17_debtor == 1 & 
                                           ir_wff_16_debtor == 1 & 
                                           ir_wff_15_debtor == 1, 1, NA),
        ir_wff_persistence_7yrs = ifelse(ir_wff_20_debtor == 1 & 
                                           ir_wff_19_debtor == 1 & 
                                           ir_wff_18_debtor == 1 & 
                                           ir_wff_17_debtor == 1 & 
                                           ir_wff_16_debtor == 1 & 
                                           ir_wff_15_debtor == 1 & 
                                           ir_wff_14_debtor == 1, 1, NA),
        ir_wff_persistence_8yrs = ifelse(ir_wff_20_debtor == 1 & 
                                           ir_wff_19_debtor == 1 & 
                                           ir_wff_18_debtor == 1 & 
                                           ir_wff_17_debtor == 1 & 
                                           ir_wff_16_debtor == 1 & 
                                           ir_wff_15_debtor == 1 & 
                                           ir_wff_14_debtor == 1 &
                                           ir_wff_13_debtor == 1, 1, NA),
        ir_wff_persistence_9yrs = ifelse(ir_wff_20_debtor == 1 & 
                                           ir_wff_19_debtor == 1 & 
                                           ir_wff_18_debtor == 1 & 
                                           ir_wff_17_debtor == 1 & 
                                           ir_wff_16_debtor == 1 & 
                                           ir_wff_15_debtor == 1 & 
                                           ir_wff_14_debtor == 1 &
                                           ir_wff_13_debtor == 1 &
                                           ir_wff_12_debtor == 1, 1, NA)
      )%>%
  # IR (Others) ----------------------------------------------------------
      mutate(
        ir_oth_persistence_2yrs = ifelse(ir_oth_20_debtor == 1 & 
                                           ir_oth_19_debtor == 1, 1, NA),
        ir_oth_persistence_3yrs = ifelse(ir_oth_20_debtor == 1 & 
                                           ir_oth_19_debtor == 1 & 
                                           ir_oth_18_debtor == 1, 1, NA),
        ir_oth_persistence_4yrs = ifelse(ir_oth_20_debtor == 1 & 
                                           ir_oth_19_debtor == 1 & 
                                           ir_oth_18_debtor == 1 & 
                                           ir_oth_17_debtor == 1, 1, NA),
        ir_oth_persistence_5yrs = ifelse(ir_oth_20_debtor == 1 & 
                                           ir_oth_19_debtor == 1 & 
                                           ir_oth_18_debtor == 1 & 
                                           ir_oth_17_debtor == 1 & 
                                           ir_oth_16_debtor == 1, 1, NA),
        ir_oth_persistence_6yrs = ifelse(ir_oth_20_debtor == 1 & 
                                           ir_oth_19_debtor == 1 & 
                                           ir_oth_18_debtor == 1 & 
                                           ir_oth_17_debtor == 1 & 
                                           ir_oth_16_debtor == 1 & 
                                           ir_oth_15_debtor == 1, 1, NA),
        ir_oth_persistence_7yrs = ifelse(ir_oth_20_debtor == 1 & 
                                           ir_oth_19_debtor == 1 & 
                                           ir_oth_18_debtor == 1 & 
                                           ir_oth_17_debtor == 1 & 
                                           ir_oth_16_debtor == 1 & 
                                           ir_oth_15_debtor == 1 & 
                                           ir_oth_14_debtor == 1, 1, NA),
        ir_oth_persistence_8yrs = ifelse(ir_oth_20_debtor == 1 & 
                                           ir_oth_19_debtor == 1 & 
                                           ir_oth_18_debtor == 1 & 
                                           ir_oth_17_debtor == 1 & 
                                           ir_oth_16_debtor == 1 & 
                                           ir_oth_15_debtor == 1 & 
                                           ir_oth_14_debtor == 1 &
                                           ir_oth_13_debtor == 1, 1, NA),
        ir_oth_persistence_9yrs = ifelse(ir_oth_20_debtor == 1 & 
                                           ir_oth_19_debtor == 1 & 
                                           ir_oth_18_debtor == 1 & 
                                           ir_oth_17_debtor == 1 & 
                                           ir_oth_16_debtor == 1 & 
                                           ir_oth_15_debtor == 1 & 
                                           ir_oth_14_debtor == 1 &
                                           ir_oth_13_debtor == 1 &
                                           ir_oth_12_debtor == 1, 1, NA)
      ) %>%
  # Any previous debt observed ---------------------------------------------------------------------------
  mutate(
    any_msd_persistence = ifelse(msd_persistence_5yrs == 1 | 
                                   msd_persistence_6yrs == 1 |
                                   msd_persistence_7yrs == 1 | 
                                   msd_persistence_8yrs == 1 | 
                                   msd_persistence_9yrs == 1 , 1, 0),
    any_moj_fine_persistence = ifelse(moj_fine_persistence_5yrs == 1 | 
                                        moj_fine_persistence_6yrs == 1 |
                                        moj_fine_persistence_7yrs == 1 | 
                                        moj_fine_persistence_8yrs == 1 | 
                                        moj_fine_persistence_9yrs == 1 , 1, 0),
    any_moj_fcco_persistence = ifelse(moj_fcco_persistence_5yrs == 1 | 
                                        moj_fcco_persistence_6yrs == 1 | 
                                        moj_fcco_persistence_7yrs == 1 , 1, 0),
    any_ir_child_persistence = ifelse(ir_child_persistence_5yrs == 1 | 
                                        ir_child_persistence_6yrs == 1 | 
                                        ir_child_persistence_7yrs == 1 | 
                                        ir_child_persistence_8yrs == 1 |
                                        ir_child_persistence_9yrs == 1 , 1, 0),
    any_ir_income_persistence = ifelse(ir_income_persistence_5yrs == 1 | 
                                         ir_income_persistence_6yrs == 1 | 
                                         ir_income_persistence_7yrs == 1 | 
                                         ir_income_persistence_8yrs == 1 |
                                         ir_income_persistence_9yrs == 1 , 1, 0),
    any_ir_student_persistence = ifelse(ir_student_persistence_5yrs == 1 | 
                                          ir_student_persistence_6yrs == 1 | 
                                          ir_student_persistence_7yrs == 1 | 
                                          ir_student_persistence_8yrs == 1 |
                                          ir_student_persistence_9yrs == 1 , 1, 0),
    any_ir_wff_persistence = ifelse(ir_wff_persistence_5yrs == 1 | 
                                      ir_wff_persistence_6yrs == 1 | 
                                      ir_wff_persistence_7yrs == 1 | 
                                      ir_wff_persistence_8yrs == 1 |
                                      ir_wff_persistence_9yrs == 1 , 1, 0),
    any_ir_oth_persistence = ifelse(ir_oth_persistence_5yrs == 1 | 
                                      ir_oth_persistence_6yrs == 1 | 
                                      ir_oth_persistence_7yrs == 1 | 
                                      ir_oth_persistence_8yrs == 1 |
                                      ir_oth_persistence_9yrs == 1 , 1, 0)
  )%>%
  mutate(
    any_persistence = ifelse(any_msd_persistence == 1 | 
                               any_moj_fine_persistence == 1 | 
                               any_moj_fcco_persistence == 1 | 
                               any_ir_child_persistence == 1 | 
                               any_ir_income_persistence == 1 |
                               any_ir_student_persistence == 1 |
                               any_ir_wff_persistence == 1 |
                               any_ir_oth_persistence == 1, 1, 0)
  )
## INTERIM SAVE 4 ----------------------------------------------------------------------------------------------------
working_table = write_for_reuse(db_con, SANDPIT, OUR_SCHEMA, INTERIM_TABLE_4, working_table)
## Ratios -----------------------------------------------------------------------------------------------
working_table = working_table %>%
  # Debt to Income ratio ----------------------------------------------------------
      mutate(
        debt_vs_income_moj = ifelse(moj_total_debt_20 > MIN_CHECK & total_income > 1,
                                    round(moj_total_debt_20 / total_income, 2), NA),
        debt_vs_income_msd = ifelse(msd_total_debt_20 > MIN_CHECK & total_income > 1,
                                    round(msd_total_debt_20 / total_income, 2), NA),
        debt_vs_income_ird = ifelse(ir_total_debt_20 > MIN_CHECK & total_income > 1,
                                    round(ir_total_debt_20 / total_income, 2), NA),
        debt_vs_income_all = ifelse(moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20 > MIN_CHECK & total_income > 1,
                                    round((moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20) / total_income, 2), NA)
      ) %>%
  # Repayment to Income ratio ----------------------------------------------------------
      mutate(
        repayment_vs_income_moj = ifelse(moj_total_repayment_20 > 0 & total_income > 1,
                                    round(moj_total_repayment_20 / total_income, 2), NA),
        
        repayment_vs_income_msd = ifelse(msd_repaid_2020 > 0 & total_income > 1,
                                    round(msd_repaid_2020 / total_income, 2), NA),
        
        repayment_vs_income_ird = ifelse(ir_total_repayment_20 > 0 & total_income > 1,
                                    round(ir_total_repayment_20 / total_income, 2), NA),
        
        repayment_vs_income_all = ifelse(moj_total_repayment_20 + msd_repaid_2020 + ir_total_repayment_20 > 0 & total_income > 1,
                                    round((moj_total_repayment_20 + msd_repaid_2020 + ir_total_repayment_20) / total_income, 2), NA)
      ) %>%
  
  # Repayment to debt ratio ----------------------------------------------------------
      mutate(
        repayment_vs_debt_moj = ifelse(moj_total_repayment_20 > 0 & moj_total_debt_20 > MIN_CHECK,
                                    round(moj_total_repayment_20 / moj_total_debt_20, 2), NA),
        
        repayment_vs_debt_msd = ifelse(msd_repaid_2020 > 0 & msd_total_debt_20 > MIN_CHECK,
                                    round(msd_repaid_2020 / msd_total_debt_20, 2), NA),
        
        repayment_vs_debt_ird = ifelse(ir_total_repayment_20 > 0 & ir_total_debt_20 > MIN_CHECK,
                                    round(ir_total_repayment_20 / ir_total_debt_20, 2), NA),
        
        repayment_vs_debt_all = ifelse(moj_total_repayment_20 + msd_repaid_2020 + ir_total_repayment_20 > 0 & moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20 > MIN_CHECK,
                                    round((moj_total_repayment_20 + msd_repaid_2020 + ir_total_repayment_20) / (moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20), 2), NA)
      )%>%
  # Ratio categories -----------------------------------------------------------------------------------------------------
  mutate(
    debt_vs_income_moj = case_when(0 <= debt_vs_income_moj & debt_vs_income_moj < 0.005 ~ "0-0.5%",
                                   0.005 <= debt_vs_income_moj & debt_vs_income_moj < 0.01 ~ "0.5-1%",
                                   0.01 <= debt_vs_income_moj & debt_vs_income_moj < 0.05 ~ "1-5%",
                                   0.05 <= debt_vs_income_moj & debt_vs_income_moj < 0.1 ~ "5-10%",
                                   0.1 <= debt_vs_income_moj & debt_vs_income_moj < 0.2 ~ "10-20%",
                                   0.2 <= debt_vs_income_moj & debt_vs_income_moj < 0.4 ~ "20-40%",
                                   0.4 <= debt_vs_income_moj ~ ">40%"),
    debt_vs_income_msd = case_when(0 <= debt_vs_income_msd & debt_vs_income_msd < 0.005 ~ "0-0.5%",
                                   0.005 <= debt_vs_income_msd & debt_vs_income_msd < 0.01 ~ "0.5-1%",
                                   0.01 <= debt_vs_income_msd & debt_vs_income_msd < 0.05 ~ "1-5%",
                                   0.05 <= debt_vs_income_msd & debt_vs_income_msd < 0.1 ~ "5-10%",
                                   0.1 <= debt_vs_income_msd & debt_vs_income_msd < 0.2 ~ "10-20%",
                                   0.2 <= debt_vs_income_msd & debt_vs_income_msd < 0.4 ~ "20-40%",
                                   0.4 <= debt_vs_income_msd ~ ">40%"),
    debt_vs_income_ird = case_when(0 <= debt_vs_income_ird & debt_vs_income_ird < 0.005 ~ "0-0.5%",
                                   0.005 <= debt_vs_income_ird & debt_vs_income_ird < 0.01 ~ "0.5-1%",
                                   0.01 <= debt_vs_income_ird & debt_vs_income_ird < 0.05 ~ "1-5%",
                                   0.05 <= debt_vs_income_ird & debt_vs_income_ird < 0.1 ~ "5-10%",
                                   0.1 <= debt_vs_income_ird & debt_vs_income_ird < 0.2 ~ "10-20%",
                                   0.2 <= debt_vs_income_ird & debt_vs_income_ird < 0.4 ~ "20-40%",
                                   0.4 <= debt_vs_income_ird ~ ">40%"),
    debt_vs_income_all = case_when(0 <= debt_vs_income_all & debt_vs_income_all < 0.005 ~ "0-0.5%",
                                   0.005 <= debt_vs_income_all & debt_vs_income_all < 0.01 ~ "0.5-1%",
                                   0.01 <= debt_vs_income_all & debt_vs_income_all < 0.05 ~ "1-5%",
                                   0.05 <= debt_vs_income_all & debt_vs_income_all < 0.1 ~ "5-10%",
                                   0.1 <= debt_vs_income_all & debt_vs_income_all < 0.2 ~ "10-20%",
                                   0.2 <= debt_vs_income_all & debt_vs_income_all < 0.4 ~ "20-40%",
                                   0.4 <= debt_vs_income_all ~ ">40%"),
    repayment_vs_income_moj = case_when(0 <= repayment_vs_income_moj & repayment_vs_income_moj < 0.005 ~ "0-0.5%",
                                        0.005 <= repayment_vs_income_moj & repayment_vs_income_moj < 0.01 ~ "0.5-1%",
                                        0.01 <= repayment_vs_income_moj & repayment_vs_income_moj < 0.05 ~ "1-5%",
                                        0.05 <= repayment_vs_income_moj & repayment_vs_income_moj < 0.1 ~ "5-10%",
                                        0.1 <= repayment_vs_income_moj & repayment_vs_income_moj < 0.2 ~ "10-20%",
                                        0.2 <= repayment_vs_income_moj & repayment_vs_income_moj < 0.4 ~ "20-40%",
                                        0.4 <= repayment_vs_income_moj ~ ">40%"),
    repayment_vs_income_msd = case_when(0 <= repayment_vs_income_msd & repayment_vs_income_msd < 0.005 ~ "0-0.5%",
                                        0.005 <= repayment_vs_income_msd & repayment_vs_income_msd < 0.01 ~ "0.5-1%",
                                        0.01 <= repayment_vs_income_msd & repayment_vs_income_msd < 0.05 ~ "1-5%",
                                        0.05 <= repayment_vs_income_msd & repayment_vs_income_msd < 0.1 ~ "5-10%",
                                        0.1 <= repayment_vs_income_msd & repayment_vs_income_msd < 0.2 ~ "10-20%",
                                        0.2 <= repayment_vs_income_msd & repayment_vs_income_msd < 0.4 ~ "20-40%",
                                        0.4 <= repayment_vs_income_msd ~ ">40%"),
    repayment_vs_income_ird = case_when(0 <= repayment_vs_income_ird & repayment_vs_income_ird < 0.005 ~ "0-0.5%",
                                        0.005 <= repayment_vs_income_ird & repayment_vs_income_ird < 0.01 ~ "0.5-1%",
                                        0.01 <= repayment_vs_income_ird & repayment_vs_income_ird < 0.05 ~ "1-5%",
                                        0.05 <= repayment_vs_income_ird & repayment_vs_income_ird < 0.1 ~ "5-10%",
                                        0.1 <= repayment_vs_income_ird & repayment_vs_income_ird < 0.2 ~ "10-20%",
                                        0.2 <= repayment_vs_income_ird & repayment_vs_income_ird < 0.4 ~ "20-40%",
                                        0.4 <= repayment_vs_income_ird ~ ">40%"),
    repayment_vs_income_all = case_when(0 <= repayment_vs_income_all & repayment_vs_income_all < 0.005 ~ "0-0.5%",
                                        0.005 <= repayment_vs_income_all & repayment_vs_income_all < 0.01 ~ "0.5-1%",
                                        0.01 <= repayment_vs_income_all & repayment_vs_income_all < 0.05 ~ "1-5%",
                                        0.05 <= repayment_vs_income_all & repayment_vs_income_all < 0.1 ~ "5-10%",
                                        0.1 <= repayment_vs_income_all & repayment_vs_income_all < 0.2 ~ "10-20%",
                                        0.2 <= repayment_vs_income_all & repayment_vs_income_all < 0.4 ~ "20-40%",
                                        0.4 <= repayment_vs_income_all ~ ">40%"),
    repayment_vs_debt_moj = case_when(0 <= repayment_vs_debt_moj & repayment_vs_debt_moj < 0.005 ~ "0-0.5%",
                                      0.005 <= repayment_vs_debt_moj & repayment_vs_debt_moj < 0.01 ~ "0.5-1%",
                                      0.01 <= repayment_vs_debt_moj & repayment_vs_debt_moj < 0.05 ~ "1-5%",
                                      0.05 <= repayment_vs_debt_moj & repayment_vs_debt_moj < 0.1 ~ "5-10%",
                                      0.1 <= repayment_vs_debt_moj & repayment_vs_debt_moj < 0.2 ~ "10-20%",
                                      0.2 <= repayment_vs_debt_moj & repayment_vs_debt_moj < 0.4 ~ "20-40%",
                                      0.4 <= repayment_vs_debt_moj ~ ">40%"),
    repayment_vs_debt_msd = case_when(0 <= repayment_vs_debt_msd & repayment_vs_debt_msd < 0.005 ~ "0-0.5%",
                                      0.005 <= repayment_vs_debt_msd & repayment_vs_debt_msd < 0.01 ~ "0.5-1%",
                                      0.01 <= repayment_vs_debt_msd & repayment_vs_debt_msd < 0.05 ~ "1-5%",
                                      0.05 <= repayment_vs_debt_msd & repayment_vs_debt_msd < 0.1 ~ "5-10%",
                                      0.1 <= repayment_vs_debt_msd & repayment_vs_debt_msd < 0.2 ~ "10-20%",
                                      0.2 <= repayment_vs_debt_msd & repayment_vs_debt_msd < 0.4 ~ "20-40%",
                                      0.4 <= repayment_vs_debt_msd ~ ">40%"),
    repayment_vs_debt_ird = case_when(0 <= repayment_vs_debt_ird & repayment_vs_debt_ird < 0.005 ~ "0-0.5%",
                                      0.005 <= repayment_vs_debt_ird & repayment_vs_debt_ird < 0.01 ~ "0.5-1%",
                                      0.01 <= repayment_vs_debt_ird & repayment_vs_debt_ird < 0.05 ~ "1-5%",
                                      0.05 <= repayment_vs_debt_ird & repayment_vs_debt_ird < 0.1 ~ "5-10%",
                                      0.1 <= repayment_vs_debt_ird & repayment_vs_debt_ird < 0.2 ~ "10-20%",
                                      0.2 <= repayment_vs_debt_ird & repayment_vs_debt_ird < 0.4 ~ "20-40%",
                                      0.4 <= repayment_vs_debt_ird ~ ">40%"),
    repayment_vs_debt_all = case_when(0 <= repayment_vs_debt_all & repayment_vs_debt_all < 0.005 ~ "0-0.5%",
                                      0.005 <= repayment_vs_debt_all & repayment_vs_debt_all < 0.01 ~ "0.5-1%",
                                      0.01 <= repayment_vs_debt_all & repayment_vs_debt_all < 0.05 ~ "1-5%",
                                      0.05 <= repayment_vs_debt_all & repayment_vs_debt_all < 0.1 ~ "5-10%",
                                      0.1 <= repayment_vs_debt_all & repayment_vs_debt_all < 0.2 ~ "10-20%",
                                      0.2 <= repayment_vs_debt_all & repayment_vs_debt_all < 0.4 ~ "20-40%",
                                      0.4 <= repayment_vs_debt_all ~ ">40%")
  ) %>%
  # Debt to income post repayment ----------------------------------------------------------
      mutate(
        debt_vs_ipr_moj = ifelse(moj_total_debt_20 > MIN_CHECK & income_post_moj_repayment > 1,
                                    round(moj_total_debt_20 / income_post_moj_repayment, 2), NA),
    
        debt_vs_ipr_msd = ifelse(msd_total_debt_20 > MIN_CHECK & income_post_msd_repayment > 1,
                                    round(msd_total_debt_20 / income_post_msd_repayment, 2), NA),
    
        debt_vs_ipr_ird = ifelse(ir_total_debt_20 > MIN_CHECK & income_post_ir_repayment > 1,
                                    round(ir_total_debt_20 / income_post_ir_repayment, 2), NA),
    
        debt_vs_ipr_all = ifelse(moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20 > MIN_CHECK & income_post_all_repayment > 1,
                                    round((moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20) / income_post_all_repayment, 2), NA)
      )%>%
  # Repayment to income post repayment ----------------------------------------------------------
      mutate(
        repayment_vs_ipr_moj = ifelse(moj_total_repayment_20 > 0 & income_post_moj_repayment > 1,
                                    round(moj_total_repayment_20 / income_post_moj_repayment, 2), NA),
        
        repayment_vs_ipr_msd = ifelse(msd_repaid_2020 > 0 & income_post_msd_repayment > 1,
                                    round(msd_repaid_2020 / income_post_msd_repayment, 2), NA),
        
        repayment_vs_ipr_ird = ifelse(ir_total_repayment_20 > 0 & income_post_ir_repayment > 1,
                                    round(ir_total_repayment_20 / income_post_ir_repayment, 2), NA),
        
        repayment_vs_ipr_all = ifelse(moj_total_repayment_20 + msd_repaid_2020 + ir_total_repayment_20 > 0 & income_post_all_repayment > 1,
                                    round((moj_total_repayment_20 + msd_repaid_2020 + ir_total_repayment_20) / income_post_all_repayment, 2), NA)
      )%>%
  # Income post repayment to income ratio ----------------------------------------------------------
      mutate(
        ipr_vs_income_moj = ifelse(income_post_moj_repayment > 0 & total_income > 1,
                                    round(income_post_moj_repayment / total_income, 2), NA),
        
        ipr_vs_income_msd = ifelse(income_post_msd_repayment > 0 & total_income > 1,
                                    round(income_post_msd_repayment / total_income, 2), NA),
        
        ipr_vs_income_ird = ifelse(income_post_ir_repayment > 0 & total_income > 1,
                                    round(income_post_ir_repayment / total_income, 2), NA),
        
        ipr_vs_income_all = ifelse(income_post_all_repayment > 0 & total_income > 1,
                                    round(income_post_all_repayment / total_income, 2), NA)
        )%>%

  # IPR - Ratio categories -----------------------------------------------------------------------------------------------------
  mutate(
    debt_vs_ipr_moj = case_when(0 <= debt_vs_ipr_moj & debt_vs_ipr_moj < 0.005 ~ "0-0.5%",
                                0.005 <= debt_vs_ipr_moj & debt_vs_ipr_moj < 0.01 ~ "0.5-1%",
                                0.01 <= debt_vs_ipr_moj & debt_vs_ipr_moj < 0.05 ~ "1-5%",
                                0.05 <= debt_vs_ipr_moj & debt_vs_ipr_moj < 0.1 ~ "5-10%",
                                0.1 <= debt_vs_ipr_moj & debt_vs_ipr_moj < 0.2 ~ "10-20%",
                                0.2 <= debt_vs_ipr_moj & debt_vs_ipr_moj < 0.4 ~ "20-40%",
                                0.4 <= debt_vs_ipr_moj ~ ">40%"),
    debt_vs_ipr_msd = case_when(0 <= debt_vs_ipr_msd & debt_vs_ipr_msd < 0.005 ~ "0-0.5%",
                                0.005 <= debt_vs_ipr_msd & debt_vs_ipr_msd < 0.01 ~ "0.5-1%",
                                0.01 <= debt_vs_ipr_msd & debt_vs_ipr_msd < 0.05 ~ "1-5%",
                                0.05 <= debt_vs_ipr_msd & debt_vs_ipr_msd < 0.1 ~ "5-10%",
                                0.1 <= debt_vs_ipr_msd & debt_vs_ipr_msd < 0.2 ~ "10-20%",
                                0.2 <= debt_vs_ipr_msd & debt_vs_ipr_msd < 0.4 ~ "20-40%",
                                0.4 <= debt_vs_ipr_msd ~ ">40%"),
    debt_vs_ipr_ird = case_when(0 <= debt_vs_ipr_ird & debt_vs_ipr_ird < 0.005 ~ "0-0.5%",
                                0.005 <= debt_vs_ipr_ird & debt_vs_ipr_ird < 0.01 ~ "0.5-1%",
                                0.01 <= debt_vs_ipr_ird & debt_vs_ipr_ird < 0.05 ~ "1-5%",
                                0.05 <= debt_vs_ipr_ird & debt_vs_ipr_ird < 0.1 ~ "5-10%",
                                0.1 <= debt_vs_ipr_ird & debt_vs_ipr_ird < 0.2 ~ "10-20%",
                                0.2 <= debt_vs_ipr_ird & debt_vs_ipr_ird < 0.4 ~ "20-40%",
                                0.4 <= debt_vs_ipr_ird ~ ">40%"),
    debt_vs_ipr_all = case_when(0 <= debt_vs_ipr_all & debt_vs_ipr_all < 0.005 ~ "0-0.5%",
                                0.005 <= debt_vs_ipr_all & debt_vs_ipr_all < 0.01 ~ "0.5-1%",
                                0.01 <= debt_vs_ipr_all & debt_vs_ipr_all < 0.05 ~ "1-5%",
                                0.05 <= debt_vs_ipr_all & debt_vs_ipr_all < 0.1 ~ "5-10%",
                                0.1 <= debt_vs_ipr_all & debt_vs_ipr_all < 0.2 ~ "10-20%",
                                0.2 <= debt_vs_ipr_all & debt_vs_ipr_all < 0.4 ~ "20-40%",
                                0.4 <= debt_vs_ipr_all ~ ">40%"),
    repayment_vs_ipr_moj = case_when(0 <= repayment_vs_ipr_moj & repayment_vs_ipr_moj < 0.005 ~ "0-0.5%",
                                     0.005 <= repayment_vs_ipr_moj & repayment_vs_ipr_moj < 0.01 ~ "0.5-1%",
                                     0.01 <= repayment_vs_ipr_moj & repayment_vs_ipr_moj < 0.05 ~ "1-5%",
                                     0.05 <= repayment_vs_ipr_moj & repayment_vs_ipr_moj < 0.1 ~ "5-10%",
                                     0.1 <= repayment_vs_ipr_moj & repayment_vs_ipr_moj < 0.2 ~ "10-20%",
                                     0.2 <= repayment_vs_ipr_moj & repayment_vs_ipr_moj < 0.4 ~ "20-40%",
                                     0.4 <= repayment_vs_ipr_moj ~ ">40%"),
    repayment_vs_ipr_msd = case_when(0 <= repayment_vs_ipr_msd & repayment_vs_ipr_msd < 0.005 ~ "0-0.5%",
                                     0.005 <= repayment_vs_ipr_msd & repayment_vs_ipr_msd < 0.01 ~ "0.5-1%",
                                     0.01 <= repayment_vs_ipr_msd & repayment_vs_ipr_msd < 0.05 ~ "1-5%",
                                     0.05 <= repayment_vs_ipr_msd & repayment_vs_ipr_msd < 0.1 ~ "5-10%",
                                     0.1 <= repayment_vs_ipr_msd & repayment_vs_ipr_msd < 0.2 ~ "10-20%",
                                     0.2 <= repayment_vs_ipr_msd & repayment_vs_ipr_msd < 0.4 ~ "20-40%",
                                     0.4 <= repayment_vs_ipr_msd ~ ">40%"),
    repayment_vs_ipr_ird = case_when(0 <= repayment_vs_ipr_ird & repayment_vs_ipr_ird < 0.005 ~ "0-0.5%",
                                     0.005 <= repayment_vs_ipr_ird & repayment_vs_ipr_ird < 0.01 ~ "0.5-1%",
                                     0.01 <= repayment_vs_ipr_ird & repayment_vs_ipr_ird < 0.05 ~ "1-5%",
                                     0.05 <= repayment_vs_ipr_ird & repayment_vs_ipr_ird < 0.1 ~ "5-10%",
                                     0.1 <= repayment_vs_ipr_ird & repayment_vs_ipr_ird < 0.2 ~ "10-20%",
                                     0.2 <= repayment_vs_ipr_ird & repayment_vs_ipr_ird < 0.4 ~ "20-40%",
                                     0.4 <= repayment_vs_ipr_ird ~ ">40%"),
    repayment_vs_ipr_all = case_when(0 <= repayment_vs_ipr_all & repayment_vs_ipr_all < 0.005 ~ "0-0.5%",
                                     0.005 <= repayment_vs_ipr_all & repayment_vs_ipr_all < 0.01 ~ "0.5-1%",
                                     0.01 <= repayment_vs_ipr_all & repayment_vs_ipr_all < 0.05 ~ "1-5%",
                                     0.05 <= repayment_vs_ipr_all & repayment_vs_ipr_all < 0.1 ~ "5-10%",
                                     0.1 <= repayment_vs_ipr_all & repayment_vs_ipr_all < 0.2 ~ "10-20%",
                                     0.2 <= repayment_vs_ipr_all & repayment_vs_ipr_all < 0.4 ~ "20-40%",
                                     0.4 <= repayment_vs_ipr_all ~ ">40%"),
    ipr_vs_income_moj = case_when(0 <= ipr_vs_income_moj & ipr_vs_income_moj < 0.005 ~ "0-0.5%",
                                  0.005 <= ipr_vs_income_moj & ipr_vs_income_moj < 0.01 ~ "0.5-1%",
                                  0.01 <= ipr_vs_income_moj & ipr_vs_income_moj < 0.05 ~ "1-5%",
                                  0.05 <= ipr_vs_income_moj & ipr_vs_income_moj < 0.1 ~ "5-10%",
                                  0.1 <= ipr_vs_income_moj & ipr_vs_income_moj < 0.2 ~ "10-20%",
                                  0.2 <= ipr_vs_income_moj & ipr_vs_income_moj < 0.4 ~ "20-40%",
                                  0.4 <= ipr_vs_income_moj ~ ">40%"),
    ipr_vs_income_msd = case_when(0 <= ipr_vs_income_msd & ipr_vs_income_msd < 0.005 ~ "0-0.5%",
                                  0.005 <= ipr_vs_income_msd & ipr_vs_income_msd < 0.01 ~ "0.5-1%",
                                  0.01 <= ipr_vs_income_msd & ipr_vs_income_msd < 0.05 ~ "1-5%",
                                  0.05 <= ipr_vs_income_msd & ipr_vs_income_msd < 0.1 ~ "5-10%",
                                  0.1 <= ipr_vs_income_msd & ipr_vs_income_msd < 0.2 ~ "10-20%",
                                  0.2 <= ipr_vs_income_msd & ipr_vs_income_msd < 0.4 ~ "20-40%",
                                  0.4 <= ipr_vs_income_msd ~ ">40%"),
    ipr_vs_income_ird = case_when(0 <= ipr_vs_income_ird & ipr_vs_income_ird < 0.005 ~ "0-0.5%",
                                  0.005 <= ipr_vs_income_ird & ipr_vs_income_ird < 0.01 ~ "0.5-1%",
                                  0.01 <= ipr_vs_income_ird & ipr_vs_income_ird < 0.05 ~ "1-5%",
                                  0.05 <= ipr_vs_income_ird & ipr_vs_income_ird < 0.1 ~ "5-10%",
                                  0.1 <= ipr_vs_income_ird & ipr_vs_income_ird < 0.2 ~ "10-20%",
                                  0.2 <= ipr_vs_income_ird & ipr_vs_income_ird < 0.4 ~ "20-40%",
                                  0.4 <= ipr_vs_income_ird ~ ">40%"),
    ipr_vs_income_all = case_when(0 <= ipr_vs_income_all & ipr_vs_income_all < 0.005 ~ "0-0.5%",
                                  0.005 <= ipr_vs_income_all & ipr_vs_income_all < 0.01 ~ "0.5-1%",
                                  0.01 <= ipr_vs_income_all & ipr_vs_income_all < 0.05 ~ "1-5%",
                                  0.05 <= ipr_vs_income_all & ipr_vs_income_all < 0.1 ~ "5-10%",
                                  0.1 <= ipr_vs_income_all & ipr_vs_income_all < 0.2 ~ "10-20%",
                                  0.2 <= ipr_vs_income_all & ipr_vs_income_all < 0.4 ~ "20-40%",
                                  0.4 <= ipr_vs_income_all ~ ">40%")
  ) %>%
## Grouping debtors by debt type and agencies ----------------------------------------------------------------------------------
  # grouping indicators ----------------------------------------------------------------------
    mutate(
      msd_debtor = ifelse(msd_20_debtor == 1, 1, 0),
      moj_debtor = ifelse(moj_fine_20_debtor == 1 | moj_fcco_20_debtor == 1, 1, 0),
      ird_debtor = ifelse(ir_child_20_debtor == 1 |
                            ir_income_20_debtor == 1 |
                            ir_student_20_debtor == 1 |
                            ir_wff_20_debtor == 1|
                            ir_oth_20_debtor == 1, 1, 0),
      is_dep_child_hhld = ifelse(res_indicator == 1 & dep_chld_hhld > 0, 1, 0),
      is_dep_child_indv = ifelse(res_indicator == 1 & dep_chld_indv > 0, 1, 0),
      low_income = ifelse(res_indicator == 1, low_income, 0),
      is_beneficiary = ifelse(res_indicator == 1, is_beneficiary, 0)
    ) %>%
  # subgrouping indicators ----------------------------------------------------------------------
    mutate(
      fine_debtor = ifelse(moj_fine_20_debtor == 1, 1, 0),
      fcco_debtor = ifelse(moj_fcco_20_debtor == 1, 1, 0),
      child_debtor = ifelse(ir_child_20_debtor == 1, 1, 0),
      income_debtor = ifelse(ir_income_20_debtor == 1, 1, 0),
      student_debtor = ifelse(ir_student_20_debtor == 1, 1, 0),
      wff_debtor = ifelse(ir_wff_20_debtor == 1, 1, 0),
      other_debtor = ifelse(ir_oth_20_debtor == 1, 1, 0)
    ) %>%
  # Current debtor ------------------------------------------------------------------------------
    mutate(
      current_debtor = ifelse(msd_debtor == 1 | 
                      moj_debtor == 1 | 
                      ird_debtor == 1,1,0)
    )%>%
## Handling nulls --------------------------------------------------------------------------------------------------------------
  # This is to reassure that there all the grouping variables are not null; else the null rows will be excluded when summarised
  mutate(
    msd_debtor = coalesce(msd_debtor,0),
    moj_debtor = coalesce(moj_debtor,0),
    ird_debtor = coalesce(ird_debtor,0),
    low_income = coalesce(low_income,0),
    is_beneficiary = coalesce(is_beneficiary,0),
    res_indicator = coalesce(res_indicator,0),
    any_persistence = coalesce(any_persistence,0),
    
    any_msd_persistence = coalesce(any_msd_persistence,0),
    any_moj_fine_persistence = coalesce(any_moj_fine_persistence,0),
    any_moj_fcco_persistence = coalesce(any_moj_fcco_persistence,0),
    any_ir_child_persistence = coalesce(any_ir_child_persistence,0),
    any_ir_income_persistence = coalesce(any_ir_income_persistence,0),
    any_ir_student_persistence = coalesce(any_ir_student_persistence,0),
    any_ir_wff_persistence = coalesce(any_ir_wff_persistence,0),
    any_ir_oth_persistence = coalesce(any_ir_oth_persistence,0),
    
    fine_debtor = coalesce(fine_debtor,0),
    fcco_debtor = coalesce(fcco_debtor,0),
    child_debtor = coalesce(child_debtor,0),
    income_debtor = coalesce(income_debtor,0),
    student_debtor = coalesce(student_debtor,0),
    wff_debtor = coalesce(wff_debtor,0),
    other_debtor = coalesce(other_debtor,0)
  )
## review tidied dataset --------------------------------------------------------------------------
# during development inspect state of table
# explore::explore_shiny(collect(working_table))
## write for output -------------------------------------------------------------------------------

run_time_inform_user("saving output table", context = "heading", print_level = VERBOSE)
written_tbl = write_to_database(working_table, db_con, SANDPIT, OUR_SCHEMA, TIDY_TABLE, OVERWRITE = TRUE)

# index
run_time_inform_user("indexing", context = "details", print_level = VERBOSE)
create_nonclustered_index(db_con, SANDPIT, OUR_SCHEMA, TIDY_TABLE, "identity_column")

# compress
run_time_inform_user("compressing", context = "details", print_level = VERBOSE)
compress_table(db_con, SANDPIT, OUR_SCHEMA, TIDY_TABLE)

## close connection -------------------------------------------------------------------------------
close_database_connection(db_con)
run_time_inform_user("grand completion", context = "heading", print_level = VERBOSE)
