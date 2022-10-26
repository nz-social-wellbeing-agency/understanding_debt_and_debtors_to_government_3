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
TIDY_TABLE = "[d2gP3_tidy_table_v2]"

# controls
DEVELOPMENT_MODE = FALSE
VERBOSE = "details" # {"all", "details", "heading", "none"}

# Coeffecients from - Prediction Modelling
INTERCEPT = -6.55
ALPHA = 6.19
BETA = 6.70

# Minimum balance required to be considered as a debtor
MIN_DEBT_BAL = 10

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
# Null deviance:  1044117  on 2291671  degrees of freedom
# Residual deviance:  127648   on 2291669    degrees of freedom
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
  # drop unrequired columns
  select(-label_identity, 
         -summary_period_start_date, 
         -summary_period_end_date, 
         -label_summary_period, 
         -intersect(starts_with('moj_fine'),ends_with('2011')),
         -intersect(starts_with('moj_fine'),ends_with('2021')),
         -intersect(starts_with('ird'),ends_with('2010')),
         -intersect(starts_with('ird'),ends_with('2011'))
  ) %>%
  
  # Group residents and non-residents
  mutate(res_indicator = ifelse((is_alive == 1 & resident == 1 & spine_indicator == 1), 1, 0)) %>%
  
  # combine some histogram'ed variables
  collapse_indicator_columns(prefix = "sex_code=", yes_values = 1, label = "sex_code") %>%
  collapse_indicator_columns(prefix = "region_code=", yes_values = 1, label = "region_code") %>%
  collapse_indicator_columns(prefix = "area_type=", yes_values = 1, label = "area_type") %>%

  # age at end of 2020
  mutate(age = 2020 - birth_year) %>%

  # age categories
  mutate(age_cat = case_when(
    00 <= age & age < 10 ~ "00_to_09",
    10 <= age & age < 20 ~ "10_to_19",
    20 <= age & age < 30 ~ "20_to_29",
    30 <= age & age < 40 ~ "30_to_39",
    40 <= age & age < 50 ~ "40_to_49",
    50 <= age & age < 60 ~ "50_to_59",
    60 <= age & age < 70 ~ "60_to_69",
    70 <= age & age < 80 ~ "70_to_79",
    80 <= age ~ "80_up"
  )) %>%
  
  # Total Income
  mutate(total_income = round(coalesce(income_taxible,0),2) + 
                        round(coalesce(income_t2_benefits,0),2) + 
                        round(coalesce(income_t3_benefits,0),2) + 
                        round(coalesce(income_wff,0),2)) %>%
  
  # Scaling 2020 MSD Repayment and Principle amounts to annual as MSD data ends in Sep
  # (1.33 = Inverse of 3/4 (1 - 0.75))
  mutate(
    msd_repaid_2020 = round(1.33 * msd_repaid_2020, 2),
    msd_principle_2020 = round(1.33 * msd_principle_2020, 2)
  ) %>%
  
  # Negative balance check
  mutate(
    msd_balance_2020 = ifelse(msd_balance_2020 < 0, 0, msd_balance_2020),
    moj_fine_balance_2020 = ifelse(moj_fine_balance_2020 < 0, 0, moj_fine_balance_2020),
    moj_fcco_balance_2020 = ifelse(moj_fcco_balance_2020 < 0, 0, moj_fcco_balance_2020),
    ird_ps18_balance_child_2020 = ifelse(ird_ps18_balance_child_2020 < 0, 0, ird_ps18_balance_child_2020),
    ird_ps18_balance_student_2020 = ifelse(ird_ps18_balance_student_2020 < 0, 0, ird_ps18_balance_student_2020),
    ird_ps18_balance_income_2020 = ifelse(ird_ps18_balance_income_2020 < 0, 0, ird_ps18_balance_income_2020),
    ird_ps18_balance_wff_2020 = ifelse(ird_ps18_balance_wff_2020 < 0, 0, ird_ps18_balance_wff_2020),
    ird_ps18_balance_oth_2020 = ifelse(ird_ps18_balance_oth_2020 < 0, 0, ird_ps18_balance_oth_2020)
  )%>%
  
  mutate(
    msd_principle_2020 = ifelse(msd_principle_2020 < 0, 0, msd_principle_2020),
    moj_fine_principle_2020 = ifelse(moj_fine_principle_2020 < 0, 0, moj_fine_principle_2020),
    moj_fcco_principle_2020 = ifelse(moj_fcco_principle_2020 < 0, 0, moj_fcco_principle_2020),
    ird_ps18_principle_child_2020 = ifelse(ird_ps18_principle_child_2020 < 0, 0, ird_ps18_principle_child_2020),
    ird_ps18_principle_student_2020 = ifelse(ird_ps18_principle_student_2020 < 0, 0, ird_ps18_principle_student_2020),
    ird_ps18_principle_income_2020 = ifelse(ird_ps18_principle_income_2020 < 0, 0, ird_ps18_principle_income_2020),
    ird_ps18_principle_wff_2020 = ifelse(ird_ps18_principle_wff_2020 < 0, 0, ird_ps18_principle_wff_2020),
    ird_ps18_principle_oth_2020 = ifelse(ird_ps18_principle_oth_2020 < 0, 0, ird_ps18_principle_oth_2020)
    
  )%>%
  
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
  )
## Low income indicator -------------------------------------------------------------------------
  working_table = working_table %>%
    mutate(low_income = ifelse(total_income < LOW_INCOME_THRESHOLD, 1, 0)) %>%
  
## Is beneficiary ---------------------------------------------------------------------
    mutate(
      is_beneficiary = ifelse(coalesce(days_benefit,0) > 185, 1, 0)
    ) %>%
## Debtors Indicator -------------------------------------------------------------------------
# MSD Debtors
mutate(
  msd_12_debtor = ifelse(coalesce(msd_balance_2012,0) > MIN_DEBT_BAL, 1, 0),
  msd_13_debtor = ifelse(coalesce(msd_balance_2013,0) > MIN_DEBT_BAL, 1, 0),
  msd_14_debtor = ifelse(coalesce(msd_balance_2014,0) > MIN_DEBT_BAL, 1, 0),
  msd_15_debtor = ifelse(coalesce(msd_balance_2015,0) > MIN_DEBT_BAL, 1, 0),
  msd_16_debtor = ifelse(coalesce(msd_balance_2016,0) > MIN_DEBT_BAL, 1, 0),
  msd_17_debtor = ifelse(coalesce(msd_balance_2017,0) > MIN_DEBT_BAL, 1, 0),
  msd_18_debtor = ifelse(coalesce(msd_balance_2018,0) > MIN_DEBT_BAL, 1, 0),
  msd_19_debtor = ifelse(coalesce(msd_balance_2019,0) > MIN_DEBT_BAL, 1, 0),
  msd_20_debtor = ifelse(coalesce(msd_balance_2020,0) > MIN_DEBT_BAL, 1, 0)
) %>%
  
  # MOJ Fine Debtors
  mutate(
    moj_fine_12_debtor = ifelse(coalesce(moj_fine_balance_2012,0) > MIN_DEBT_BAL, 1, 0),
    moj_fine_13_debtor = ifelse(coalesce(moj_fine_balance_2013,0) > MIN_DEBT_BAL, 1, 0),
    moj_fine_14_debtor = ifelse(coalesce(moj_fine_balance_2014,0) > MIN_DEBT_BAL, 1, 0),
    moj_fine_15_debtor = ifelse(coalesce(moj_fine_balance_2015,0) > MIN_DEBT_BAL, 1, 0),
    moj_fine_16_debtor = ifelse(coalesce(moj_fine_balance_2016,0) > MIN_DEBT_BAL, 1, 0),
    moj_fine_17_debtor = ifelse(coalesce(moj_fine_balance_2017,0) > MIN_DEBT_BAL, 1, 0),
    moj_fine_18_debtor = ifelse(coalesce(moj_fine_balance_2018,0) > MIN_DEBT_BAL, 1, 0),
    moj_fine_19_debtor = ifelse(coalesce(moj_fine_balance_2019,0) > MIN_DEBT_BAL, 1, 0),
    moj_fine_20_debtor = ifelse(coalesce(moj_fine_balance_2020,0) > MIN_DEBT_BAL, 1, 0)
  ) %>%
  
  # MOJ FCCO Debtors  
  mutate(
    moj_fcco_14_debtor = ifelse(coalesce(moj_fcco_balance_2014,0) > MIN_DEBT_BAL, 1, 0),
    moj_fcco_15_debtor = ifelse(coalesce(moj_fcco_balance_2015,0) > MIN_DEBT_BAL, 1, 0),
    moj_fcco_16_debtor = ifelse(coalesce(moj_fcco_balance_2016,0) > MIN_DEBT_BAL, 1, 0),
    moj_fcco_17_debtor = ifelse(coalesce(moj_fcco_balance_2017,0) > MIN_DEBT_BAL, 1, 0),
    moj_fcco_18_debtor = ifelse(coalesce(moj_fcco_balance_2018,0) > MIN_DEBT_BAL, 1, 0),
    moj_fcco_19_debtor = ifelse(coalesce(moj_fcco_balance_2019,0) > MIN_DEBT_BAL, 1, 0),
    moj_fcco_20_debtor = ifelse(coalesce(moj_fcco_balance_2020,0) > MIN_DEBT_BAL, 1, 0)
  ) %>%
  
  # IR - Child Support Debtors  
  mutate(
    ir_child_12_debtor = ifelse(coalesce(ird_pr19_balance_child_2012,0) > MIN_DEBT_BAL, 1, 0),
    ir_child_13_debtor = ifelse(coalesce(ird_pr19_balance_child_2013,0) > MIN_DEBT_BAL, 1, 0),
    ir_child_14_debtor = ifelse(coalesce(ird_pr19_balance_child_2014,0) > MIN_DEBT_BAL, 1, 0),
    ir_child_15_debtor = ifelse(coalesce(ird_pr19_balance_child_2015,0) > MIN_DEBT_BAL, 1, 0),
    ir_child_16_debtor = ifelse(coalesce(ird_pr19_balance_child_2016,0) > MIN_DEBT_BAL, 1, 0),
    ir_child_17_debtor = ifelse(coalesce(ird_pr19_balance_child_2017,0) > MIN_DEBT_BAL, 1, 0),
    ir_child_18_debtor = ifelse(coalesce(ird_pr19_balance_child_2018,0) > MIN_DEBT_BAL, 1, 0),
    ir_child_19_debtor = ifelse(coalesce(ird_ps18_balance_child_2019,0) > MIN_DEBT_BAL, 1, 0),
    ir_child_20_debtor = ifelse(coalesce(ird_ps18_balance_child_2020,0) > MIN_DEBT_BAL, 1, 0)
  ) %>%
  
  # IR - Income Tax Debtors  
  mutate(
    ir_income_12_debtor = ifelse(coalesce(ird_pr19_balance_income_2012,0) > MIN_DEBT_BAL, 1, 0),
    ir_income_13_debtor = ifelse(coalesce(ird_pr19_balance_income_2013,0) > MIN_DEBT_BAL, 1, 0),
    ir_income_14_debtor = ifelse(coalesce(ird_pr19_balance_income_2014,0) > MIN_DEBT_BAL, 1, 0),
    ir_income_15_debtor = ifelse(coalesce(ird_pr19_balance_income_2015,0) > MIN_DEBT_BAL, 1, 0),
    ir_income_16_debtor = ifelse(coalesce(ird_pr19_balance_income_2016,0) > MIN_DEBT_BAL, 1, 0),
    ir_income_17_debtor = ifelse(coalesce(ird_pr19_balance_income_2017,0) > MIN_DEBT_BAL, 1, 0),
    ir_income_18_debtor = ifelse(coalesce(ird_pr19_balance_income_2018,0) > MIN_DEBT_BAL, 1, 0),
    ir_income_19_debtor = ifelse(coalesce(ird_ps18_balance_income_2019,0) > MIN_DEBT_BAL, 1, 0),
    ir_income_20_debtor = ifelse(coalesce(ird_ps18_balance_income_2020,0) > MIN_DEBT_BAL, 1, 0)
  ) %>%
  
  # IR - Overdue Student Loan Debtors
  mutate(
    ir_student_12_debtor = ifelse(coalesce(ird_pr19_balance_student_2012,0) > MIN_DEBT_BAL, 1, 0),
    ir_student_13_debtor = ifelse(coalesce(ird_pr19_balance_student_2013,0) > MIN_DEBT_BAL, 1, 0),
    ir_student_14_debtor = ifelse(coalesce(ird_pr19_balance_student_2014,0) > MIN_DEBT_BAL, 1, 0),
    ir_student_15_debtor = ifelse(coalesce(ird_pr19_balance_student_2015,0) > MIN_DEBT_BAL, 1, 0),
    ir_student_16_debtor = ifelse(coalesce(ird_pr19_balance_student_2016,0) > MIN_DEBT_BAL, 1, 0),
    ir_student_17_debtor = ifelse(coalesce(ird_pr19_balance_student_2017,0) > MIN_DEBT_BAL, 1, 0),
    ir_student_18_debtor = ifelse(coalesce(ird_pr19_balance_student_2018,0) > MIN_DEBT_BAL, 1, 0),
    ir_student_20_debtor = ifelse(coalesce(ird_ps18_balance_student_2020,0) > MIN_DEBT_BAL, 1, 0)
  ) %>%
  
  # IR - Working For Families Debtors  
  mutate(
    ir_wff_12_debtor = ifelse(coalesce(ird_pr19_balance_wff_2012,0) > MIN_DEBT_BAL, 1, 0),
    ir_wff_13_debtor = ifelse(coalesce(ird_pr19_balance_wff_2013,0) > MIN_DEBT_BAL, 1, 0),
    ir_wff_14_debtor = ifelse(coalesce(ird_pr19_balance_wff_2014,0) > MIN_DEBT_BAL, 1, 0),
    ir_wff_15_debtor = ifelse(coalesce(ird_pr19_balance_wff_2015,0) > MIN_DEBT_BAL, 1, 0),
    ir_wff_16_debtor = ifelse(coalesce(ird_pr19_balance_wff_2016,0) > MIN_DEBT_BAL, 1, 0),
    ir_wff_17_debtor = ifelse(coalesce(ird_pr19_balance_wff_2017,0) > MIN_DEBT_BAL, 1, 0),
    ir_wff_18_debtor = ifelse(coalesce(ird_pr19_balance_wff_2018,0) > MIN_DEBT_BAL, 1, 0),
    ir_wff_19_debtor = ifelse(coalesce(ird_ps18_balance_wff_2019,0) > MIN_DEBT_BAL, 1, 0),
    ir_wff_20_debtor = ifelse(coalesce(ird_ps18_balance_wff_2020,0) > MIN_DEBT_BAL, 1, 0)
  ) %>%
  
  # IR - Other Debtors
  mutate(
    ir_oth_12_debtor = ifelse(coalesce(ird_pr19_balance_oth_2012,0) > MIN_DEBT_BAL, 1, 0),
    ir_oth_13_debtor = ifelse(coalesce(ird_pr19_balance_oth_2013,0) > MIN_DEBT_BAL, 1, 0),
    ir_oth_14_debtor = ifelse(coalesce(ird_pr19_balance_oth_2014,0) > MIN_DEBT_BAL, 1, 0),
    ir_oth_15_debtor = ifelse(coalesce(ird_pr19_balance_oth_2015,0) > MIN_DEBT_BAL, 1, 0),
    ir_oth_16_debtor = ifelse(coalesce(ird_pr19_balance_oth_2016,0) > MIN_DEBT_BAL, 1, 0),
    ir_oth_17_debtor = ifelse(coalesce(ird_pr19_balance_oth_2017,0) > MIN_DEBT_BAL, 1, 0),
    ir_oth_18_debtor = ifelse(coalesce(ird_pr19_balance_oth_2018,0) > MIN_DEBT_BAL, 1, 0),
    ir_oth_19_debtor = ifelse(coalesce(ird_ps18_balance_oth_2019,0) > MIN_DEBT_BAL, 1, 0),
    ir_oth_20_debtor = ifelse(coalesce(ird_ps18_balance_oth_2020,0) > MIN_DEBT_BAL, 1, 0)
  ) %>%
  
## Imputing IR - Overdure Student Loan - 2019 using the coeffecients from the prediction modelling --------
  mutate(ir_student_19_debtor = ifelse((INTERCEPT + 
                                        (ALPHA * coalesce(ir_student_18_debtor,0)) + 
                                        (BETA *coalesce( ir_student_20_debtor,0))) > 0, 1, 0)) %>%
## Total Debt - Combined Debt Types ---------------------------------------------------------------
    mutate(msd_total_debt_20 = (coalesce(msd_balance_2020,0))) %>%
    mutate(moj_total_debt_20 = (coalesce(moj_fcco_balance_2020,0) + coalesce(moj_fine_balance_2020,0))) %>%
    mutate(ir_total_debt_20 = (coalesce(ird_ps18_balance_child_2020,0) + 
                                      coalesce(ird_ps18_balance_income_2020,0) +
                                      coalesce(ird_ps18_balance_student_2020,0) +
                                      coalesce(ird_ps18_balance_wff_2020,0) +
                                      coalesce(ird_ps18_balance_oth_2020,0))) %>%
  
## Total MOJ Repayments - Combined Debt Types -----------------------------------------------------
  mutate(
    moj_total_repayment_14 = (coalesce(moj_fcco_repaid_2014,0) + coalesce(moj_fine_repaid_2014,0)),
    moj_total_repayment_15 = (coalesce(moj_fcco_repaid_2015,0) + coalesce(moj_fine_repaid_2015,0)),
    moj_total_repayment_16 = (coalesce(moj_fcco_repaid_2016,0) + coalesce(moj_fine_repaid_2016,0)),
    moj_total_repayment_17 = (coalesce(moj_fcco_repaid_2017,0) + coalesce(moj_fine_repaid_2017,0)),
    moj_total_repayment_18 = (coalesce(moj_fcco_repaid_2018,0) + coalesce(moj_fine_repaid_2018,0)),
    moj_total_repayment_19 = (coalesce(moj_fcco_repaid_2019,0) + coalesce(moj_fine_repaid_2019,0)),
    moj_total_repayment_20 = (coalesce(moj_fcco_repaid_2020,0) + coalesce(moj_fine_repaid_2020,0))
  ) %>%

## Total IR Repayments - Combined Debt Types -------------------------------------------------------------------------
    mutate(
      ir_total_repayment_12 = (coalesce(ird_pr19_repaid_child_2012,0) + 
                                 coalesce(ird_pr19_repaid_income_2012,0) +
                                 coalesce(ird_pr19_repaid_student_2012,0) +
                                 coalesce(ird_pr19_repaid_wff_2012,0) +
                                 coalesce(ird_pr19_repaid_oth_2012,0)),
      ir_total_repayment_13 = (coalesce(ird_pr19_repaid_child_2013,0) + 
                                 coalesce(ird_pr19_repaid_income_2013,0) +
                                 coalesce(ird_pr19_repaid_student_2013,0) +
                                 coalesce(ird_pr19_repaid_wff_2013,0) +
                                 coalesce(ird_pr19_repaid_oth_2013,0)),
      ir_total_repayment_14 = (coalesce(ird_pr19_repaid_child_2014,0) + 
                                 coalesce(ird_pr19_repaid_income_2014,0) +
                                 coalesce(ird_pr19_repaid_student_2014,0) +
                                 coalesce(ird_pr19_repaid_wff_2014,0) +
                                 coalesce(ird_pr19_repaid_oth_2014,0)),
      ir_total_repayment_15 = (coalesce(ird_pr19_repaid_child_2015,0) + 
                                 coalesce(ird_pr19_repaid_income_2015,0) +
                                 coalesce(ird_pr19_repaid_student_2015,0) +
                                 coalesce(ird_pr19_repaid_wff_2015,0) +
                                 coalesce(ird_pr19_repaid_oth_2015,0)),
      ir_total_repayment_16 = (coalesce(ird_pr19_repaid_child_2016,0) + 
                                 coalesce(ird_pr19_repaid_income_2016,0) +
                                 coalesce(ird_pr19_repaid_student_2016,0) +
                                 coalesce(ird_pr19_repaid_wff_2016,0) +
                                 coalesce(ird_pr19_repaid_oth_2016,0)),
      ir_total_repayment_17 = (coalesce(ird_pr19_repaid_child_2017,0) + 
                                 coalesce(ird_pr19_repaid_income_2017,0) +
                                 coalesce(ird_pr19_repaid_student_2017,0) +
                                 coalesce(ird_pr19_repaid_wff_2017,0) +
                                 coalesce(ird_pr19_repaid_oth_2017,0)),
      ir_total_repayment_18 = (coalesce(ird_pr19_repaid_child_2018,0) + 
                                 coalesce(ird_pr19_repaid_income_2018,0) +
                                 coalesce(ird_pr19_repaid_student_2018,0) +
                                 coalesce(ird_pr19_repaid_wff_2018,0) +
                                 coalesce(ird_pr19_repaid_oth_2018,0)),
      ir_total_repayment_19 = (coalesce(ird_ps18_repayment_child_2019,0) + 
                                 coalesce(ird_ps18_repayment_income_2019,0) +
                                 # ird_ps18_repayment_student_2019 +
                                 coalesce(ird_ps18_repayment_wff_2019,0) +
                                 coalesce(ird_ps18_repayment_oth_2019,0)),
      ir_total_repayment_20 = (coalesce(ird_ps18_repayment_child_2020,0) + 
                                 coalesce(ird_ps18_repayment_income_2020,0) +
                                 coalesce(ird_ps18_repayment_student_2020,0) +
                                 coalesce(ird_ps18_repayment_wff_2020,0) +
                                 coalesce(ird_ps18_repayment_oth_2020,0))
    ) %>%
  
## Total number of debt types in 2020 ----------------------------------------------------------
    mutate( total_debt_type_20 = msd_20_debtor +
              moj_fine_20_debtor +
              moj_fcco_20_debtor +
              ir_child_20_debtor +
              ir_income_20_debtor +
              ir_student_20_debtor +
              ir_wff_20_debtor +
              ir_oth_20_debtor
    )%>%
  
## Previous years of debt observed - MSD ----------------------------------------------------------
    mutate(
      msd_persistence_2yrs = ifelse(msd_20_debtor == 1 & 
                                    msd_19_debtor == 1, 1, 0),
      msd_persistence_3yrs = ifelse(msd_20_debtor == 1 & 
                                    msd_19_debtor == 1 & 
                                    msd_18_debtor == 1, 1, 0),
      msd_persistence_4yrs = ifelse(msd_20_debtor == 1 & 
                                    msd_19_debtor == 1 & 
                                    msd_18_debtor == 1 & 
                                    msd_17_debtor == 1, 1, 0),
      msd_persistence_5yrs = ifelse(msd_20_debtor == 1 & 
                                    msd_19_debtor == 1 & 
                                    msd_18_debtor == 1 & 
                                    msd_17_debtor == 1 & 
                                    msd_16_debtor == 1, 1, 0),
      msd_persistence_6yrs = ifelse(msd_20_debtor == 1 & 
                                    msd_19_debtor == 1 & 
                                    msd_18_debtor == 1 & 
                                    msd_17_debtor == 1 & 
                                    msd_16_debtor == 1 & 
                                    msd_15_debtor == 1, 1, 0),
      msd_persistence_7yrs = ifelse(msd_20_debtor == 1 & 
                                    msd_19_debtor == 1 & 
                                    msd_18_debtor == 1 & 
                                    msd_17_debtor == 1 & 
                                    msd_16_debtor == 1 & 
                                    msd_15_debtor == 1 & 
                                    msd_14_debtor == 1, 1, 0),
      msd_persistence_8yrs = ifelse(msd_20_debtor == 1 & 
                                    msd_19_debtor == 1 &
                                    msd_18_debtor == 1 & 
                                    msd_17_debtor == 1 & 
                                    msd_16_debtor == 1 & 
                                    msd_15_debtor == 1 &
                                    msd_14_debtor == 1 &
                                    msd_13_debtor == 1, 1, 0),
      msd_persistence_9yrs = ifelse(msd_20_debtor == 1 & 
                                      msd_19_debtor == 1 &
                                      msd_18_debtor == 1 & 
                                      msd_17_debtor == 1 & 
                                      msd_16_debtor == 1 & 
                                      msd_15_debtor == 1 &
                                      msd_14_debtor == 1 &
                                      msd_13_debtor == 1 &
                                      msd_12_debtor == 1, 1, 0),
      msd_persistence_tot_yrs = (msd_20_debtor + 
                                msd_19_debtor + 
                                msd_18_debtor + 
                                msd_17_debtor + 
                                msd_16_debtor + 
                                msd_15_debtor + 
                                msd_14_debtor + 
                                msd_13_debtor + 
                                msd_12_debtor)
    ) %>%
  
## Previous years of debt observed - MOJ (Fines) ----------------------------------------------------------
    mutate(
      moj_fine_persistence_2yrs = ifelse(moj_fine_20_debtor == 1 & 
                                         moj_fine_19_debtor == 1, 1, 0),
      moj_fine_persistence_3yrs = ifelse(moj_fine_20_debtor == 1 & 
                                         moj_fine_19_debtor == 1 & 
                                         moj_fine_18_debtor == 1, 1, 0),
      moj_fine_persistence_4yrs = ifelse(moj_fine_20_debtor == 1 & 
                                         moj_fine_19_debtor == 1 & 
                                         moj_fine_18_debtor == 1 & 
                                         moj_fine_17_debtor == 1, 1, 0),
      moj_fine_persistence_5yrs = ifelse(moj_fine_20_debtor == 1 & 
                                         moj_fine_19_debtor == 1 & 
                                         moj_fine_18_debtor == 1 & 
                                         moj_fine_17_debtor == 1 & 
                                         moj_fine_16_debtor == 1, 1, 0),
      moj_fine_persistence_6yrs = ifelse(moj_fine_20_debtor == 1 & 
                                      moj_fine_19_debtor == 1 & 
                                      moj_fine_18_debtor == 1 & 
                                      moj_fine_17_debtor == 1 & 
                                      moj_fine_16_debtor == 1 & 
                                      moj_fine_15_debtor == 1, 1, 0),
      moj_fine_persistence_7yrs = ifelse(moj_fine_20_debtor == 1 & 
                                      moj_fine_19_debtor == 1 & 
                                      moj_fine_18_debtor == 1 & 
                                      moj_fine_17_debtor == 1 & 
                                      moj_fine_16_debtor == 1 & 
                                      moj_fine_15_debtor == 1 & 
                                      moj_fine_14_debtor == 1, 1, 0),
      moj_fine_persistence_8yrs = ifelse(moj_fine_20_debtor == 1 & 
                                      moj_fine_19_debtor == 1 &
                                      moj_fine_18_debtor == 1 & 
                                      moj_fine_17_debtor == 1 & 
                                      moj_fine_16_debtor == 1 & 
                                      moj_fine_15_debtor == 1 &
                                      moj_fine_14_debtor == 1 &
                                      moj_fine_13_debtor == 1, 1, 0),
      moj_fine_persistence_9yrs = ifelse(moj_fine_20_debtor == 1 & 
                                      moj_fine_19_debtor == 1 &
                                      moj_fine_18_debtor == 1 & 
                                      moj_fine_17_debtor == 1 & 
                                      moj_fine_16_debtor == 1 & 
                                      moj_fine_15_debtor == 1 &
                                      moj_fine_14_debtor == 1 &
                                      moj_fine_13_debtor == 1 &
                                      moj_fine_12_debtor == 1, 1, 0),
      moj_fine_persistence_tot_yrs = (moj_fine_20_debtor + 
                                   moj_fine_19_debtor + 
                                   moj_fine_18_debtor + 
                                   moj_fine_17_debtor + 
                                   moj_fine_16_debtor + 
                                   moj_fine_15_debtor + 
                                   moj_fine_14_debtor + 
                                   moj_fine_13_debtor + 
                                   moj_fine_12_debtor)
    ) %>%
  
## Previous years of debt observed - MOJ (FCCO) ----------------------------------------------------------
    mutate(
      moj_fcco_persistence_2yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                      moj_fcco_19_debtor == 1, 1, 0),
      moj_fcco_persistence_3yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                      moj_fcco_19_debtor == 1 & 
                                      moj_fcco_18_debtor == 1, 1, 0),
      moj_fcco_persistence_4yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                      moj_fcco_19_debtor == 1 & 
                                      moj_fcco_18_debtor == 1 & 
                                      moj_fcco_17_debtor == 1, 1, 0),
      moj_fcco_persistence_5yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                      moj_fcco_19_debtor == 1 & 
                                      moj_fcco_18_debtor == 1 & 
                                      moj_fcco_17_debtor == 1 & 
                                      moj_fcco_16_debtor == 1, 1, 0),
      moj_fcco_persistence_6yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                      moj_fcco_19_debtor == 1 & 
                                      moj_fcco_18_debtor == 1 & 
                                      moj_fcco_17_debtor == 1 & 
                                      moj_fcco_16_debtor == 1 & 
                                      moj_fcco_15_debtor == 1, 1, 0),
      moj_fcco_persistence_7yrs = ifelse(moj_fcco_20_debtor == 1 & 
                                      moj_fcco_19_debtor == 1 & 
                                      moj_fcco_18_debtor == 1 & 
                                      moj_fcco_17_debtor == 1 & 
                                      moj_fcco_16_debtor == 1 & 
                                      moj_fcco_15_debtor == 1 & 
                                      moj_fcco_14_debtor == 1, 1, 0),
      moj_fcco_persistence_tot_yrs = (moj_fcco_20_debtor + 
                                   moj_fcco_19_debtor + 
                                   moj_fcco_18_debtor + 
                                   moj_fcco_17_debtor + 
                                   moj_fcco_16_debtor + 
                                   moj_fcco_15_debtor + 
                                   moj_fcco_14_debtor)
    ) %>%
  
## Previous years of debt observed - IR (Child) ----------------------------------------------------------
    mutate(
      ir_child_persistence_2yrs = ifelse(ir_child_20_debtor == 1 & 
                                           ir_child_19_debtor == 1, 1, 0),
      ir_child_persistence_3yrs = ifelse(ir_child_20_debtor == 1 & 
                                           ir_child_19_debtor == 1 & 
                                           ir_child_18_debtor == 1, 1, 0),
      ir_child_persistence_4yrs = ifelse(ir_child_20_debtor == 1 & 
                                           ir_child_19_debtor == 1 & 
                                           ir_child_18_debtor == 1 & 
                                           ir_child_17_debtor == 1, 1, 0),
      ir_child_persistence_5yrs = ifelse(ir_child_20_debtor == 1 & 
                                           ir_child_19_debtor == 1 & 
                                           ir_child_18_debtor == 1 & 
                                           ir_child_17_debtor == 1 & 
                                           ir_child_16_debtor == 1, 1, 0),
      ir_child_persistence_6yrs = ifelse(ir_child_20_debtor == 1 & 
                                           ir_child_19_debtor == 1 & 
                                           ir_child_18_debtor == 1 & 
                                           ir_child_17_debtor == 1 & 
                                           ir_child_16_debtor == 1 & 
                                           ir_child_15_debtor == 1, 1, 0),
      ir_child_persistence_7yrs = ifelse(ir_child_20_debtor == 1 & 
                                           ir_child_19_debtor == 1 & 
                                           ir_child_18_debtor == 1 & 
                                           ir_child_17_debtor == 1 & 
                                           ir_child_16_debtor == 1 & 
                                           ir_child_15_debtor == 1 & 
                                           ir_child_14_debtor == 1, 1, 0),
      ir_child_persistence_8yrs = ifelse(ir_child_20_debtor == 1 & 
                                      ir_child_19_debtor == 1 & 
                                      ir_child_18_debtor == 1 & 
                                      ir_child_17_debtor == 1 & 
                                      ir_child_16_debtor == 1 & 
                                      ir_child_15_debtor == 1 & 
                                      ir_child_14_debtor == 1 &
                                      ir_child_13_debtor == 1, 1, 0),
      ir_child_persistence_9yrs = ifelse(ir_child_20_debtor == 1 & 
                                      ir_child_19_debtor == 1 & 
                                      ir_child_18_debtor == 1 & 
                                      ir_child_17_debtor == 1 & 
                                      ir_child_16_debtor == 1 & 
                                      ir_child_15_debtor == 1 & 
                                      ir_child_14_debtor == 1 &
                                      ir_child_13_debtor == 1 &
                                      ir_child_12_debtor == 1, 1, 0),
      ir_child_persistence_tot_yrs = (ir_child_20_debtor + 
                                        ir_child_19_debtor + 
                                        ir_child_18_debtor + 
                                        ir_child_17_debtor + 
                                        ir_child_16_debtor + 
                                        ir_child_15_debtor + 
                                        ir_child_14_debtor +
                                        ir_child_13_debtor +
                                        ir_child_12_debtor)
    ) %>%
## Previous years of debt observed - IR (Income) ----------------------------------------------------------
    mutate(
      ir_income_persistence_2yrs = ifelse(ir_income_20_debtor == 1 & 
                                            ir_income_19_debtor == 1, 1, 0),
      ir_income_persistence_3yrs = ifelse(ir_income_20_debtor == 1 & 
                                            ir_income_19_debtor == 1 & 
                                            ir_income_18_debtor == 1, 1, 0),
      ir_income_persistence_4yrs = ifelse(ir_income_20_debtor == 1 & 
                                            ir_income_19_debtor == 1 & 
                                            ir_income_18_debtor == 1 & 
                                            ir_income_17_debtor == 1, 1, 0),
      ir_income_persistence_5yrs = ifelse(ir_income_20_debtor == 1 & 
                                            ir_income_19_debtor == 1 & 
                                            ir_income_18_debtor == 1 & 
                                            ir_income_17_debtor == 1 & 
                                            ir_income_16_debtor == 1, 1, 0),
      ir_income_persistence_6yrs = ifelse(ir_income_20_debtor == 1 & 
                                            ir_income_19_debtor == 1 & 
                                            ir_income_18_debtor == 1 & 
                                            ir_income_17_debtor == 1 & 
                                            ir_income_16_debtor == 1 & 
                                            ir_income_15_debtor == 1, 1, 0),
      ir_income_persistence_7yrs = ifelse(ir_income_20_debtor == 1 & 
                                            ir_income_19_debtor == 1 & 
                                            ir_income_18_debtor == 1 & 
                                            ir_income_17_debtor == 1 & 
                                            ir_income_16_debtor == 1 & 
                                            ir_income_15_debtor == 1 & 
                                            ir_income_14_debtor == 1, 1, 0),
      ir_income_persistence_8yrs = ifelse(ir_income_20_debtor == 1 & 
                                            ir_income_19_debtor == 1 & 
                                            ir_income_18_debtor == 1 & 
                                            ir_income_17_debtor == 1 & 
                                            ir_income_16_debtor == 1 & 
                                            ir_income_15_debtor == 1 & 
                                            ir_income_14_debtor == 1 &
                                            ir_income_13_debtor == 1, 1, 0),
      ir_income_persistence_9yrs = ifelse(ir_income_20_debtor == 1 & 
                                            ir_income_19_debtor == 1 & 
                                            ir_income_18_debtor == 1 & 
                                            ir_income_17_debtor == 1 & 
                                            ir_income_16_debtor == 1 & 
                                            ir_income_15_debtor == 1 & 
                                            ir_income_14_debtor == 1 &
                                            ir_income_13_debtor == 1 &
                                            ir_income_12_debtor == 1, 1, 0),
      ir_income_persistence_tot_yrs = (ir_income_20_debtor + 
                                         ir_income_19_debtor + 
                                         ir_income_18_debtor + 
                                         ir_income_17_debtor + 
                                         ir_income_16_debtor + 
                                         ir_income_15_debtor + 
                                         ir_income_14_debtor +
                                         ir_income_13_debtor +
                                         ir_income_12_debtor)
    ) %>%
  
## Previous years of debt observed - IR (Student) ----------------------------------------------------------
    mutate(
      ir_student_persistence_2yrs = ifelse(ir_student_20_debtor == 1 & 
                                            ir_student_19_debtor == 1, 1, 0),
      ir_student_persistence_3yrs = ifelse(ir_student_20_debtor == 1 & 
                                            ir_student_19_debtor == 1 & 
                                            ir_student_18_debtor == 1, 1, 0),
      ir_student_persistence_4yrs = ifelse(ir_student_20_debtor == 1 & 
                                             ir_student_19_debtor == 1 & 
                                             ir_student_18_debtor == 1 & 
                                             ir_student_17_debtor == 1, 1, 0),
      ir_student_persistence_5yrs = ifelse(ir_student_20_debtor == 1 & 
                                             ir_student_19_debtor == 1 & 
                                             ir_student_18_debtor == 1 & 
                                             ir_student_17_debtor == 1 & 
                                             ir_student_16_debtor == 1, 1, 0),
      ir_student_persistence_6yrs = ifelse(ir_student_20_debtor == 1 & 
                                             ir_student_19_debtor == 1 & 
                                             ir_student_18_debtor == 1 & 
                                             ir_student_17_debtor == 1 & 
                                             ir_student_16_debtor == 1 & 
                                             ir_student_15_debtor == 1, 1, 0),
      ir_student_persistence_7yrs = ifelse(ir_student_20_debtor == 1 & 
                                             ir_student_19_debtor == 1 & 
                                             ir_student_18_debtor == 1 & 
                                             ir_student_17_debtor == 1 & 
                                             ir_student_16_debtor == 1 & 
                                             ir_student_15_debtor == 1 & 
                                             ir_student_14_debtor == 1, 1, 0),
      ir_student_persistence_8yrs = ifelse(ir_student_20_debtor == 1 & 
                                             ir_student_19_debtor == 1 & 
                                             ir_student_18_debtor == 1 & 
                                             ir_student_17_debtor == 1 & 
                                             ir_student_16_debtor == 1 & 
                                             ir_student_15_debtor == 1 & 
                                             ir_student_14_debtor == 1 &
                                             ir_student_13_debtor == 1, 1, 0),
      ir_student_persistence_9yrs = ifelse(ir_student_20_debtor == 1 & 
                                             ir_student_19_debtor == 1 & 
                                             ir_student_18_debtor == 1 & 
                                             ir_student_17_debtor == 1 & 
                                             ir_student_16_debtor == 1 & 
                                             ir_student_15_debtor == 1 & 
                                             ir_student_14_debtor == 1 &
                                             ir_student_13_debtor == 1 &
                                             ir_student_12_debtor == 1, 1, 0),
      ir_student_persistence_tot_yrs = (ir_student_20_debtor + 
                                          ir_student_19_debtor + 
                                          ir_student_18_debtor + 
                                          ir_student_17_debtor + 
                                          ir_student_16_debtor + 
                                          ir_student_15_debtor + 
                                          ir_student_14_debtor +
                                          ir_student_13_debtor +
                                          ir_student_12_debtor)
    ) %>%
## Previous years of debt observed - IR (WFF) ----------------------------------------------------------
    mutate(
      ir_wff_persistence_2yrs = ifelse(ir_wff_20_debtor == 1 & 
                                         ir_wff_19_debtor == 1, 1, 0),
      ir_wff_persistence_3yrs = ifelse(ir_wff_20_debtor == 1 & 
                                         ir_wff_19_debtor == 1 & 
                                         ir_wff_18_debtor == 1, 1, 0),
      ir_wff_persistence_4yrs = ifelse(ir_wff_20_debtor == 1 & 
                                         ir_wff_19_debtor == 1 & 
                                         ir_wff_18_debtor == 1 & 
                                         ir_wff_17_debtor == 1, 1, 0),
      ir_wff_persistence_5yrs = ifelse(ir_wff_20_debtor == 1 & 
                                         ir_wff_19_debtor == 1 & 
                                         ir_wff_18_debtor == 1 & 
                                         ir_wff_17_debtor == 1 & 
                                         ir_wff_16_debtor == 1, 1, 0),
      ir_wff_persistence_6yrs = ifelse(ir_wff_20_debtor == 1 & 
                                         ir_wff_19_debtor == 1 & 
                                         ir_wff_18_debtor == 1 & 
                                         ir_wff_17_debtor == 1 & 
                                         ir_wff_16_debtor == 1 & 
                                         ir_wff_15_debtor == 1, 1, 0),
      ir_wff_persistence_7yrs = ifelse(ir_wff_20_debtor == 1 & 
                                         ir_wff_19_debtor == 1 & 
                                         ir_wff_18_debtor == 1 & 
                                         ir_wff_17_debtor == 1 & 
                                         ir_wff_16_debtor == 1 & 
                                         ir_wff_15_debtor == 1 & 
                                         ir_wff_14_debtor == 1, 1, 0),
      ir_wff_persistence_8yrs = ifelse(ir_wff_20_debtor == 1 & 
                                         ir_wff_19_debtor == 1 & 
                                         ir_wff_18_debtor == 1 & 
                                         ir_wff_17_debtor == 1 & 
                                         ir_wff_16_debtor == 1 & 
                                         ir_wff_15_debtor == 1 & 
                                         ir_wff_14_debtor == 1 &
                                         ir_wff_13_debtor == 1, 1, 0),
      ir_wff_persistence_9yrs = ifelse(ir_wff_20_debtor == 1 & 
                                         ir_wff_19_debtor == 1 & 
                                         ir_wff_18_debtor == 1 & 
                                         ir_wff_17_debtor == 1 & 
                                         ir_wff_16_debtor == 1 & 
                                         ir_wff_15_debtor == 1 & 
                                         ir_wff_14_debtor == 1 &
                                         ir_wff_13_debtor == 1 &
                                         ir_wff_12_debtor == 1, 1, 0),
      ir_wff_persistence_tot_yrs = (ir_wff_20_debtor + 
                                      ir_wff_19_debtor + 
                                      ir_wff_18_debtor + 
                                      ir_wff_17_debtor + 
                                      ir_wff_16_debtor + 
                                      ir_wff_15_debtor + 
                                      ir_wff_14_debtor +
                                      ir_wff_13_debtor +
                                      ir_wff_12_debtor)
    ) %>%
## Previous years of debt observed - IR (Others) ----------------------------------------------------------
    mutate(
      ir_oth_persistence_2yrs = ifelse(ir_oth_20_debtor == 1 & 
                                         ir_oth_19_debtor == 1, 1, 0),
      ir_oth_persistence_3yrs = ifelse(ir_oth_20_debtor == 1 & 
                                         ir_oth_19_debtor == 1 & 
                                         ir_oth_18_debtor == 1, 1, 0),
      ir_oth_persistence_4yrs = ifelse(ir_oth_20_debtor == 1 & 
                                         ir_oth_19_debtor == 1 & 
                                         ir_oth_18_debtor == 1 & 
                                         ir_oth_17_debtor == 1, 1, 0),
      ir_oth_persistence_5yrs = ifelse(ir_oth_20_debtor == 1 & 
                                         ir_oth_19_debtor == 1 & 
                                         ir_oth_18_debtor == 1 & 
                                         ir_oth_17_debtor == 1 & 
                                         ir_oth_16_debtor == 1, 1, 0),
      ir_oth_persistence_6yrs = ifelse(ir_oth_20_debtor == 1 & 
                                         ir_oth_19_debtor == 1 & 
                                         ir_oth_18_debtor == 1 & 
                                         ir_oth_17_debtor == 1 & 
                                         ir_oth_16_debtor == 1 & 
                                         ir_oth_15_debtor == 1, 1, 0),
      ir_oth_persistence_7yrs = ifelse(ir_oth_20_debtor == 1 & 
                                         ir_oth_19_debtor == 1 & 
                                         ir_oth_18_debtor == 1 & 
                                         ir_oth_17_debtor == 1 & 
                                         ir_oth_16_debtor == 1 & 
                                         ir_oth_15_debtor == 1 & 
                                         ir_oth_14_debtor == 1, 1, 0),
      ir_oth_persistence_8yrs = ifelse(ir_oth_20_debtor == 1 & 
                                         ir_oth_19_debtor == 1 & 
                                         ir_oth_18_debtor == 1 & 
                                         ir_oth_17_debtor == 1 & 
                                         ir_oth_16_debtor == 1 & 
                                         ir_oth_15_debtor == 1 & 
                                         ir_oth_14_debtor == 1 &
                                         ir_oth_13_debtor == 1, 1, 0),
      ir_oth_persistence_9yrs = ifelse(ir_oth_20_debtor == 1 & 
                                         ir_oth_19_debtor == 1 & 
                                         ir_oth_18_debtor == 1 & 
                                         ir_oth_17_debtor == 1 & 
                                         ir_oth_16_debtor == 1 & 
                                         ir_oth_15_debtor == 1 & 
                                         ir_oth_14_debtor == 1 &
                                         ir_oth_13_debtor == 1 &
                                         ir_oth_12_debtor == 1, 1, 0),
      ir_oth_persistence_tot_yrs = (ir_oth_20_debtor + 
                                      ir_oth_19_debtor + 
                                      ir_oth_18_debtor + 
                                      ir_oth_17_debtor + 
                                      ir_oth_16_debtor + 
                                      ir_oth_15_debtor + 
                                      ir_oth_14_debtor +
                                      ir_oth_13_debtor +
                                      ir_oth_12_debtor)
    ) %>%
## Income post repayments ----------------------------------------------------------
    mutate(
      income_post_all_repayment = total_income - (msd_repaid_2020 + moj_total_repayment_20 + ir_total_repayment_20),
      income_post_msd_repayment = total_income - msd_repaid_2020,
      income_post_moj_repayment = total_income - moj_total_repayment_20,
      income_post_ir_repayment = total_income - ir_total_repayment_20
    ) %>%
## Debt to Income ratio ----------------------------------------------------------
    mutate(
      debt_vs_income_moj = ifelse(moj_total_debt_20 > MIN_DEBT_BAL & total_income > 1,
                                  round(moj_total_debt_20 / total_income, 2), NA),
      debt_vs_income_msd = ifelse(msd_total_debt_20 > MIN_DEBT_BAL & total_income > 1,
                                  round(msd_total_debt_20 / total_income, 2), NA),
      debt_vs_income_ird = ifelse(ir_total_debt_20 > MIN_DEBT_BAL & total_income > 1,
                                  round(ir_total_debt_20 / total_income, 2), NA),
      debt_vs_income_all = ifelse(moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20 > MIN_DEBT_BAL & total_income > 1,
                                  round((moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20) / total_income, 2), NA)
    ) %>%

## Repayment to Income ratio ----------------------------------------------------------
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

## Repayment to debt ratio ----------------------------------------------------------
    mutate(
      repayment_vs_debt_moj = ifelse(moj_total_repayment_20 > 0 & moj_total_debt_20 > MIN_DEBT_BAL,
                                  round(moj_total_repayment_20 / moj_total_debt_20, 2), NA),
      
      repayment_vs_debt_msd = ifelse(msd_repaid_2020 > 0 & msd_total_debt_20 > MIN_DEBT_BAL,
                                  round(msd_repaid_2020 / msd_total_debt_20, 2), NA),
      
      repayment_vs_debt_ird = ifelse(ir_total_repayment_20 > 0 & ir_total_debt_20 > MIN_DEBT_BAL,
                                  round(ir_total_repayment_20 / ir_total_debt_20, 2), NA),
      
      repayment_vs_debt_all = ifelse(moj_total_repayment_20 + msd_repaid_2020 + ir_total_repayment_20 > 0 & moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20 > MIN_DEBT_BAL,
                                  round((moj_total_repayment_20 + msd_repaid_2020 + ir_total_repayment_20) / (moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20), 2), NA)
    ) %>%
## Debt to income post repayment ----------------------------------------------------------
    mutate(
      debt_vs_ipr_moj = ifelse(moj_total_debt_20 > MIN_DEBT_BAL & income_post_moj_repayment > 1,
                                  round(moj_total_debt_20 / income_post_moj_repayment, 2), NA),
  
      debt_vs_ipr_msd = ifelse(msd_total_debt_20 > MIN_DEBT_BAL & income_post_msd_repayment > 1,
                                  round(msd_total_debt_20 / income_post_msd_repayment, 2), NA),
  
      debt_vs_ipr_ird = ifelse(ir_total_debt_20 > MIN_DEBT_BAL & income_post_ir_repayment > 1,
                                  round(ir_total_debt_20 / income_post_ir_repayment, 2), NA),
  
      debt_vs_ipr_all = ifelse(moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20 > MIN_DEBT_BAL & income_post_all_repayment > 1,
                                  round((moj_total_debt_20 + msd_total_debt_20 + ir_total_debt_20) / income_post_all_repayment, 2), NA)
    ) %>%
## Repayment to income post repayment ----------------------------------------------------------
    mutate(
      repayment_vs_ipr_moj = ifelse(moj_total_repayment_20 > 0 & income_post_moj_repayment > 1,
                                  round(moj_total_repayment_20 / income_post_moj_repayment, 2), NA),
      
      repayment_vs_ipr_msd = ifelse(msd_repaid_2020 > 0 & income_post_msd_repayment > 1,
                                  round(msd_repaid_2020 / income_post_msd_repayment, 2), NA),
      
      repayment_vs_ipr_ird = ifelse(ir_total_repayment_20 > 0 & income_post_ir_repayment > 1,
                                  round(ir_total_repayment_20 / income_post_ir_repayment, 2), NA),
      
      repayment_vs_ipr_all = ifelse(moj_total_repayment_20 + msd_repaid_2020 + ir_total_repayment_20 > 0 & income_post_all_repayment > 1,
                                  round((moj_total_repayment_20 + msd_repaid_2020 + ir_total_repayment_20) / income_post_all_repayment, 2), NA)
    ) %>%
## Income post repayment to income ratio ----------------------------------------------------------
    mutate(
      ipr_vs_income_moj = ifelse(income_post_moj_repayment > 0 & total_income > 1,
                                  round(income_post_moj_repayment / total_income, 2), NA),
      
      ipr_vs_income_msd = ifelse(income_post_msd_repayment > 0 & total_income > 1,
                                  round(income_post_msd_repayment / total_income, 2), NA),
      
      ipr_vs_income_ird = ifelse(income_post_ir_repayment > 0 & total_income > 1,
                                  round(income_post_ir_repayment / total_income, 2), NA),
      
      ipr_vs_income_all = ifelse(income_post_all_repayment > 0 & total_income > 1,
                                  round(income_post_all_repayment / total_income, 2), NA)
    ) %>%
## Time until repaid - MSD ----------------------------------------------------------
  mutate(
        time_to_repay_msd_newborrow = ifelse(msd_principle_2020 > 0 & 
                                             msd_repaid_2020 > 0 & 
                                             msd_balance_2020 > MIN_DEBT_BAL & 
                                             msd_repaid_2020 > msd_principle_2020,
            round(msd_balance_2020 / (msd_repaid_2020 - msd_principle_2020), 0),
            NA),

        time_to_repay_msd_noborrow = ifelse(msd_repaid_2020 > 0 & 
                                            msd_balance_2020 > MIN_DEBT_BAL,
            round(msd_balance_2020 / msd_repaid_2020, 0),
            NA)
      ) %>%
## Time until repaid - MOJ (Fines) ----------------------------------------------------------
    mutate(
      time_to_repay_moj_fine_newborrow = ifelse(moj_fine_principle_2020 > 0 &
                                                moj_fine_repaid_2020 > 0 &
                                                moj_fine_balance_2020 > MIN_DEBT_BAL &
                                                moj_fine_repaid_2020 > moj_fine_principle_2020,
          round(moj_fine_balance_2020 / (moj_fine_repaid_2020 - moj_fine_principle_2020), 0),
          NA),

      time_to_repay_moj_fine_noborrow = ifelse(moj_fine_repaid_2020 > 0 & 
                                              moj_fine_balance_2020 > MIN_DEBT_BAL,
       round(moj_fine_balance_2020 / moj_fine_repaid_2020, 0),
       NA)
    ) %>%
# # Time until repaid - MOJ (FCCO) ----------------------------------------------------------
    mutate(
      time_to_repay_moj_fcco_newborrow = ifelse(moj_fcco_principle_2020 > 0 &
                                                moj_fcco_repaid_2020 > 0 &
                                                moj_fcco_balance_2020 > MIN_DEBT_BAL &
                                                moj_fcco_repaid_2020 > moj_fcco_principle_2020,
          round(moj_fcco_balance_2020 / (moj_fcco_repaid_2020 - moj_fcco_principle_2020), 0),
          NA),

      time_to_repay_moj_fcco_noborrow = ifelse(moj_fcco_repaid_2020 > 0 & 
                                               moj_fcco_balance_2020 > MIN_DEBT_BAL,
        round(moj_fcco_balance_2020 / moj_fcco_repaid_2020, 0),
        NA)
    ) %>%
# ## Time until repaid - IR (Child) ----------------------------------------------------------
    mutate(
      time_to_repay_ird_child_newborrow = ifelse(ird_ps18_principle_child_2020 > 0 &
                                                 ird_ps18_repayment_child_2020 > 0 &
                                                 ird_ps18_balance_child_2020 > MIN_DEBT_BAL &
                                                 ird_ps18_repayment_child_2020 > ird_ps18_principle_child_2020,
          round(ird_ps18_balance_child_2020 / (ird_ps18_repayment_child_2020 - ird_ps18_principle_child_2020), 0),
          NA),

      time_to_repay_ird_child_noborrow = ifelse(ird_ps18_repayment_child_2020 > 0 & 
                                                ird_ps18_balance_child_2020 > MIN_DEBT_BAL,
        round(ird_ps18_balance_child_2020 / ird_ps18_repayment_child_2020, 0),
        NA)
    ) %>%
# ## Time until repaid - IR (Income) ----------------------------------------------------------
    mutate(
      time_to_repay_ird_income_newborrow = ifelse(ird_ps18_principle_income_2020 > 0 &
                                                  ird_ps18_repayment_income_2020 > 0 &
                                                  ird_ps18_balance_income_2020 > MIN_DEBT_BAL &
                                                  ird_ps18_repayment_income_2020 > ird_ps18_principle_income_2020,
          round(ird_ps18_balance_income_2020 / (ird_ps18_repayment_income_2020 - ird_ps18_principle_income_2020), 0),
          NA),

      time_to_repay_ird_income_noborrow = ifelse(ird_ps18_repayment_income_2020 > 0 & 
                                                 ird_ps18_balance_income_2020 > MIN_DEBT_BAL,
        round(ird_ps18_balance_income_2020 / ird_ps18_repayment_income_2020, 0),
        NA)
    ) %>%
# ## Time until repaid - IR (Student) ----------------------------------------------------------
    mutate(
      time_to_repay_ird_student_newborrow = ifelse(ird_ps18_principle_student_2020 > 0 &
                                                   ird_ps18_repayment_student_2020 > 0 &
                                                   ird_ps18_balance_student_2020 > MIN_DEBT_BAL &
                                                   ird_ps18_repayment_student_2020 > ird_ps18_principle_student_2020,
          round(ird_ps18_balance_student_2020 / (ird_ps18_repayment_student_2020 - ird_ps18_principle_student_2020), 0),
          NA),

      time_to_repay_ird_student_noborrow = ifelse(ird_ps18_repayment_student_2020 > 0 & 
                                                  ird_ps18_balance_student_2020 > MIN_DEBT_BAL,
        round(ird_ps18_balance_student_2020 / ird_ps18_repayment_student_2020, 0),
        NA)
    ) %>%
# ## Time until repaid - IR (WFF) ----------------------------------------------------------
    mutate(
      time_to_repay_ird_wff_newborrow = ifelse(ird_ps18_principle_wff_2020 > 0 &
                                               ird_ps18_repayment_wff_2020 > 0 &
                                               ird_ps18_balance_wff_2020 > MIN_DEBT_BAL &
                                               ird_ps18_repayment_wff_2020 > ird_ps18_principle_wff_2020,
          round(ird_ps18_balance_wff_2020 / (ird_ps18_repayment_wff_2020 - ird_ps18_principle_wff_2020), 0),
          NA),

      time_to_repay_ird_wff_noborrow = ifelse(ird_ps18_repayment_wff_2020 > 0 & 
                                              ird_ps18_balance_wff_2020 > MIN_DEBT_BAL,
         round(ird_ps18_balance_wff_2020 / ird_ps18_repayment_wff_2020, 0),
         NA)
    ) %>%
# ## Time until repaid - IR (Others) ----------------------------------------------------------
    mutate(
      time_to_repay_ird_oth_newborrow = ifelse(ird_ps18_principle_oth_2020 > 0 &
                                               ird_ps18_repayment_oth_2020 > 0 &
                                               ird_ps18_balance_oth_2020 > MIN_DEBT_BAL &
                                               ird_ps18_repayment_oth_2020 > ird_ps18_principle_oth_2020,
          round(ird_ps18_balance_oth_2020 / (ird_ps18_repayment_oth_2020 - ird_ps18_principle_oth_2020), 0),
          NA),

      time_to_repay_ird_oth_noborrow = ifelse(ird_ps18_repayment_oth_2020 > 0 & 
                                              ird_ps18_balance_oth_2020 > MIN_DEBT_BAL,
        round(ird_ps18_balance_oth_2020 / ird_ps18_repayment_oth_2020, 0),
        NA)
    ) %>%
## Age when debt is repaid (Assuming there is new borrowing) ----------------------------------------------------------
    mutate(
      age_repaid_msd_newborrow = age + time_to_repay_msd_newborrow,

      age_repaid_moj_fine_newborrow = age + time_to_repay_moj_fine_newborrow,
      age_repaid_moj_fcco_newborrow = age + time_to_repay_moj_fcco_newborrow,

      age_repaid_ird_child_newborrow = age + time_to_repay_ird_child_newborrow,
      age_repaid_ird_income_newborrow = age + time_to_repay_ird_income_newborrow,
      age_repaid_ird_student_newborrow = age + time_to_repay_ird_student_newborrow,
      age_repaid_ird_wff_newborrow = age + time_to_repay_ird_wff_newborrow,
      age_repaid_ird_oth_newborrow = age + time_to_repay_ird_oth_newborrow
    ) %>%
## Age when debt is repaid (Assuming there is no new borrowing) ----------------------------------------------------------
    mutate(
      age_repaid_msd_noborrow = age + time_to_repay_msd_newborrow,
    
      age_repaid_moj_fine_noborrow = age + time_to_repay_moj_fine_noborrow,
      age_repaid_moj_fcco_noborrow = age + time_to_repay_moj_fcco_noborrow,
    
      age_repaid_ird_child_noborrow = age + time_to_repay_ird_child_noborrow,
      age_repaid_ird_income_noborrow = age + time_to_repay_ird_income_noborrow,
      age_repaid_ird_student_noborrow = age + time_to_repay_ird_student_noborrow,
      age_repaid_ird_wff_noborrow = age + time_to_repay_ird_wff_noborrow,
      age_repaid_ird_oth_noborrow = age + time_to_repay_ird_oth_noborrow
    )
## Time to repay smallest debt (if all debts are concentrated on a single debt type) ----------------------------------------------------------
# Future Work (if necessary)
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
