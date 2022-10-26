#####################################################################################################
#' Description: Output summarised results
#'
#' Input: Tidied table
#'
#' Output: Excel summary files
#' 
#' Author: 
#' 
#' Dependencies: dbplyr_helper_functions.R, utility_functions.R, table_consistency_checks.R,
#' overview_dataset.R, summary_confidential.R
#' 
#' Notes: 
#' Last run time = 3 hours
#' 
#' Issues:
#' 
#' History (reverse order):
#' 2022-02-18 SA v1 complete and executed
#' 2022-02-15 SA v1 modified exemplar
#####################################################################################################

## parameters -------------------------------------------------------------------------------------

# locations
ABSOLUTE_PATH_TO_TOOL <- "~/Network-Shares/DataLabNas/MAA/MAA2020-01/debt to government_Phase3/tools"
ABSOLUTE_PATH_TO_ANALYSIS <- "~/Network-Shares/DataLabNas/MAA/MAA2020-01/debt to government_Phase3/analysis"
SANDPIT = "[IDI_Sandpit]"
USERCODE = "[IDI_UserCode]"
OUR_SCHEMA = "[DL-MAA2020-01]"

# inputs
TIDY_TABLE = "[d2gP3_tidy_table_v2]"
# outputs
INTERIM_TABLE = "[tmp_interum_summary]"
OUTPUT_FOLDER = "../output/"
SUMMARY_FILE = "summarised_results.xlsx"
CONFIDENTIALISED_FILE = "confidentialised_results.xlsx"

# controls
DEVELOPMENT_MODE = FALSE
VERBOSE = "details" # {"all", "details", "heading", "none"}

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
working_table = create_access_point(db_con, SANDPIT, OUR_SCHEMA, TIDY_TABLE)

if(DEVELOPMENT_MODE)
  working_table = working_table %>% filter(identity_column %% 100 == 0)

## variables for summarising ----------------------------------------------------------------------

working_table = working_table %>%
  # whole population
  mutate(everybody = 1) %>%
  # handling missings
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
  # grouping indicators
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
    low_income = ifelse(res_indicator == 1, low_income, -1),
    is_beneficiary = ifelse(res_indicator == 1, is_beneficiary, 0)
  ) %>%
  # handling nulls
  mutate(
    is_dep_child_hhld = coalesce(is_dep_child_hhld,0),
    is_dep_child_indv = coalesce(is_dep_child_indv,0),
    dep_chld_indv = coalesce(dep_chld_indv,0)
  ) %>%
  # subgrouping indicators
  mutate(
    fine_debtor = ifelse(moj_fine_20_debtor == 1, 1, 0),
    fcco_debtor = ifelse(moj_fcco_20_debtor == 1, 1, 0),
    child_debtor = ifelse(ir_child_20_debtor == 1, 1, 0),
    income_debtor = ifelse(ir_income_20_debtor == 1, 1, 0),
    student_debtor = ifelse(ir_student_20_debtor == 1, 1, 0),
    wff_debtor = ifelse(ir_wff_20_debtor == 1, 1, 0),
    other_debtor = ifelse(ir_oth_20_debtor == 1, 1, 0),
    total_debt_group = case_when(msd_total_debt_20 + moj_total_debt_20 + ir_total_debt_20 > 10000 ~ ">10k",
                                 msd_total_debt_20 + moj_total_debt_20 + ir_total_debt_20 >  5000 ~ "5-10k",
                                 msd_total_debt_20 + moj_total_debt_20 + ir_total_debt_20 >  1000 ~ "1-5k",
                                 msd_total_debt_20 + moj_total_debt_20 + ir_total_debt_20 >    10 ~ "0-1k"),
    yngst_child__indv_age = case_when(2020 - yngst_dep_chld_indv >= 19 ~ NA,
                                      2020 - yngst_dep_chld_indv >= 13 ~ "13_to_18",
                                      2020 - yngst_dep_chld_indv >=  7 ~ "07_to_12",
                                      2020 - yngst_dep_chld_indv >=  4 ~ "04_to_06",
                                      2020 - yngst_dep_chld_indv >=  0 ~ "00_to_03")
  ) %>%
## age categories ----
mutate(
  age_repaid_msd_newborrow = case_when(
    00 <= age_repaid_msd_newborrow & age_repaid_msd_newborrow < 10 ~ "00_to_09",
    10 <= age_repaid_msd_newborrow & age_repaid_msd_newborrow < 20 ~ "10_to_19",
    20 <= age_repaid_msd_newborrow & age_repaid_msd_newborrow < 30 ~ "20_to_29",
    30 <= age_repaid_msd_newborrow & age_repaid_msd_newborrow < 40 ~ "30_to_39",
    40 <= age_repaid_msd_newborrow & age_repaid_msd_newborrow < 50 ~ "40_to_49",
    50 <= age_repaid_msd_newborrow & age_repaid_msd_newborrow < 60 ~ "50_to_59",
    60 <= age_repaid_msd_newborrow & age_repaid_msd_newborrow < 70 ~ "60_to_69",
    70 <= age_repaid_msd_newborrow & age_repaid_msd_newborrow < 80 ~ "70_to_79",
    80 <= age_repaid_msd_newborrow ~ "80_up"
  ),
  age_repaid_moj_fine_newborrow = case_when(
    00 <= age_repaid_moj_fine_newborrow & age_repaid_moj_fine_newborrow < 10 ~ "00_to_09",
    10 <= age_repaid_moj_fine_newborrow & age_repaid_moj_fine_newborrow < 20 ~ "10_to_19",
    20 <= age_repaid_moj_fine_newborrow & age_repaid_moj_fine_newborrow < 30 ~ "20_to_29",
    30 <= age_repaid_moj_fine_newborrow & age_repaid_moj_fine_newborrow < 40 ~ "30_to_39",
    40 <= age_repaid_moj_fine_newborrow & age_repaid_moj_fine_newborrow < 50 ~ "40_to_49",
    50 <= age_repaid_moj_fine_newborrow & age_repaid_moj_fine_newborrow < 60 ~ "50_to_59",
    60 <= age_repaid_moj_fine_newborrow & age_repaid_moj_fine_newborrow < 70 ~ "60_to_69",
    70 <= age_repaid_moj_fine_newborrow & age_repaid_moj_fine_newborrow < 80 ~ "70_to_79",
    80 <= age_repaid_moj_fine_newborrow ~ "80_up"
  ),
  age_repaid_moj_fcco_newborrow = case_when(
    00 <= age_repaid_moj_fcco_newborrow & age_repaid_moj_fcco_newborrow < 10 ~ "00_to_09",
    10 <= age_repaid_moj_fcco_newborrow & age_repaid_moj_fcco_newborrow < 20 ~ "10_to_19",
    20 <= age_repaid_moj_fcco_newborrow & age_repaid_moj_fcco_newborrow < 30 ~ "20_to_29",
    30 <= age_repaid_moj_fcco_newborrow & age_repaid_moj_fcco_newborrow < 40 ~ "30_to_39",
    40 <= age_repaid_moj_fcco_newborrow & age_repaid_moj_fcco_newborrow < 50 ~ "40_to_49",
    50 <= age_repaid_moj_fcco_newborrow & age_repaid_moj_fcco_newborrow < 60 ~ "50_to_59",
    60 <= age_repaid_moj_fcco_newborrow & age_repaid_moj_fcco_newborrow < 70 ~ "60_to_69",
    70 <= age_repaid_moj_fcco_newborrow & age_repaid_moj_fcco_newborrow < 80 ~ "70_to_79",
    80 <= age_repaid_moj_fcco_newborrow ~ "80_up"
  ),
  age_repaid_ird_child_newborrow = case_when(
    00 <= age_repaid_ird_child_newborrow & age_repaid_ird_child_newborrow < 10 ~ "00_to_09",
    10 <= age_repaid_ird_child_newborrow & age_repaid_ird_child_newborrow < 20 ~ "10_to_19",
    20 <= age_repaid_ird_child_newborrow & age_repaid_ird_child_newborrow < 30 ~ "20_to_29",
    30 <= age_repaid_ird_child_newborrow & age_repaid_ird_child_newborrow < 40 ~ "30_to_39",
    40 <= age_repaid_ird_child_newborrow & age_repaid_ird_child_newborrow < 50 ~ "40_to_49",
    50 <= age_repaid_ird_child_newborrow & age_repaid_ird_child_newborrow < 60 ~ "50_to_59",
    60 <= age_repaid_ird_child_newborrow & age_repaid_ird_child_newborrow < 70 ~ "60_to_69",
    70 <= age_repaid_ird_child_newborrow & age_repaid_ird_child_newborrow < 80 ~ "70_to_79",
    80 <= age_repaid_ird_child_newborrow ~ "80_up"
  ),
  age_repaid_ird_income_newborrow = case_when(
    00 <= age_repaid_ird_income_newborrow & age_repaid_ird_income_newborrow < 10 ~ "00_to_09",
    10 <= age_repaid_ird_income_newborrow & age_repaid_ird_income_newborrow < 20 ~ "10_to_19",
    20 <= age_repaid_ird_income_newborrow & age_repaid_ird_income_newborrow < 30 ~ "20_to_29",
    30 <= age_repaid_ird_income_newborrow & age_repaid_ird_income_newborrow < 40 ~ "30_to_39",
    40 <= age_repaid_ird_income_newborrow & age_repaid_ird_income_newborrow < 50 ~ "40_to_49",
    50 <= age_repaid_ird_income_newborrow & age_repaid_ird_income_newborrow < 60 ~ "50_to_59",
    60 <= age_repaid_ird_income_newborrow & age_repaid_ird_income_newborrow < 70 ~ "60_to_69",
    70 <= age_repaid_ird_income_newborrow & age_repaid_ird_income_newborrow < 80 ~ "70_to_79",
    80 <= age_repaid_ird_income_newborrow ~ "80_up"
  ),
  age_repaid_ird_student_newborrow = case_when(
    00 <= age_repaid_ird_student_newborrow & age_repaid_ird_student_newborrow < 10 ~ "00_to_09",
    10 <= age_repaid_ird_student_newborrow & age_repaid_ird_student_newborrow < 20 ~ "10_to_19",
    20 <= age_repaid_ird_student_newborrow & age_repaid_ird_student_newborrow < 30 ~ "20_to_29",
    30 <= age_repaid_ird_student_newborrow & age_repaid_ird_student_newborrow < 40 ~ "30_to_39",
    40 <= age_repaid_ird_student_newborrow & age_repaid_ird_student_newborrow < 50 ~ "40_to_49",
    50 <= age_repaid_ird_student_newborrow & age_repaid_ird_student_newborrow < 60 ~ "50_to_59",
    60 <= age_repaid_ird_student_newborrow & age_repaid_ird_student_newborrow < 70 ~ "60_to_69",
    70 <= age_repaid_ird_student_newborrow & age_repaid_ird_student_newborrow < 80 ~ "70_to_79",
    80 <= age_repaid_ird_student_newborrow ~ "80_up"
  ),
  age_repaid_ird_wff_newborrow = case_when(
    00 <= age_repaid_ird_wff_newborrow & age_repaid_ird_wff_newborrow < 10 ~ "00_to_09",
    10 <= age_repaid_ird_wff_newborrow & age_repaid_ird_wff_newborrow < 20 ~ "10_to_19",
    20 <= age_repaid_ird_wff_newborrow & age_repaid_ird_wff_newborrow < 30 ~ "20_to_29",
    30 <= age_repaid_ird_wff_newborrow & age_repaid_ird_wff_newborrow < 40 ~ "30_to_39",
    40 <= age_repaid_ird_wff_newborrow & age_repaid_ird_wff_newborrow < 50 ~ "40_to_49",
    50 <= age_repaid_ird_wff_newborrow & age_repaid_ird_wff_newborrow < 60 ~ "50_to_59",
    60 <= age_repaid_ird_wff_newborrow & age_repaid_ird_wff_newborrow < 70 ~ "60_to_69",
    70 <= age_repaid_ird_wff_newborrow & age_repaid_ird_wff_newborrow < 80 ~ "70_to_79",
    80 <= age_repaid_ird_wff_newborrow ~ "80_up"
  ),
  age_repaid_ird_oth_newborrow = case_when(
    00 <= age_repaid_ird_oth_newborrow & age_repaid_ird_oth_newborrow < 10 ~ "00_to_09",
    10 <= age_repaid_ird_oth_newborrow & age_repaid_ird_oth_newborrow < 20 ~ "10_to_19",
    20 <= age_repaid_ird_oth_newborrow & age_repaid_ird_oth_newborrow < 30 ~ "20_to_29",
    30 <= age_repaid_ird_oth_newborrow & age_repaid_ird_oth_newborrow < 40 ~ "30_to_39",
    40 <= age_repaid_ird_oth_newborrow & age_repaid_ird_oth_newborrow < 50 ~ "40_to_49",
    50 <= age_repaid_ird_oth_newborrow & age_repaid_ird_oth_newborrow < 60 ~ "50_to_59",
    60 <= age_repaid_ird_oth_newborrow & age_repaid_ird_oth_newborrow < 70 ~ "60_to_69",
    70 <= age_repaid_ird_oth_newborrow & age_repaid_ird_oth_newborrow < 80 ~ "70_to_79",
    80 <= age_repaid_ird_oth_newborrow ~ "80_up"
  ),
  age_repaid_msd_noborrow = case_when(
    00 <= age_repaid_msd_noborrow & age_repaid_msd_noborrow < 10 ~ "00_to_09",
    10 <= age_repaid_msd_noborrow & age_repaid_msd_noborrow < 20 ~ "10_to_19",
    20 <= age_repaid_msd_noborrow & age_repaid_msd_noborrow < 30 ~ "20_to_29",
    30 <= age_repaid_msd_noborrow & age_repaid_msd_noborrow < 40 ~ "30_to_39",
    40 <= age_repaid_msd_noborrow & age_repaid_msd_noborrow < 50 ~ "40_to_49",
    50 <= age_repaid_msd_noborrow & age_repaid_msd_noborrow < 60 ~ "50_to_59",
    60 <= age_repaid_msd_noborrow & age_repaid_msd_noborrow < 70 ~ "60_to_69",
    70 <= age_repaid_msd_noborrow & age_repaid_msd_noborrow < 80 ~ "70_to_79",
    80 <= age_repaid_msd_noborrow ~ "80_up"
  ),
  age_repaid_moj_fine_noborrow = case_when(
    00 <= age_repaid_moj_fine_noborrow & age_repaid_moj_fine_noborrow < 10 ~ "00_to_09",
    10 <= age_repaid_moj_fine_noborrow & age_repaid_moj_fine_noborrow < 20 ~ "10_to_19",
    20 <= age_repaid_moj_fine_noborrow & age_repaid_moj_fine_noborrow < 30 ~ "20_to_29",
    30 <= age_repaid_moj_fine_noborrow & age_repaid_moj_fine_noborrow < 40 ~ "30_to_39",
    40 <= age_repaid_moj_fine_noborrow & age_repaid_moj_fine_noborrow < 50 ~ "40_to_49",
    50 <= age_repaid_moj_fine_noborrow & age_repaid_moj_fine_noborrow < 60 ~ "50_to_59",
    60 <= age_repaid_moj_fine_noborrow & age_repaid_moj_fine_noborrow < 70 ~ "60_to_69",
    70 <= age_repaid_moj_fine_noborrow & age_repaid_moj_fine_noborrow < 80 ~ "70_to_79",
    80 <= age_repaid_moj_fine_noborrow ~ "80_up"
  ),
  age_repaid_moj_fcco_noborrow = case_when(
    00 <= age_repaid_moj_fcco_noborrow & age_repaid_moj_fcco_noborrow < 10 ~ "00_to_09",
    10 <= age_repaid_moj_fcco_noborrow & age_repaid_moj_fcco_noborrow < 20 ~ "10_to_19",
    20 <= age_repaid_moj_fcco_noborrow & age_repaid_moj_fcco_noborrow < 30 ~ "20_to_29",
    30 <= age_repaid_moj_fcco_noborrow & age_repaid_moj_fcco_noborrow < 40 ~ "30_to_39",
    40 <= age_repaid_moj_fcco_noborrow & age_repaid_moj_fcco_noborrow < 50 ~ "40_to_49",
    50 <= age_repaid_moj_fcco_noborrow & age_repaid_moj_fcco_noborrow < 60 ~ "50_to_59",
    60 <= age_repaid_moj_fcco_noborrow & age_repaid_moj_fcco_noborrow < 70 ~ "60_to_69",
    70 <= age_repaid_moj_fcco_noborrow & age_repaid_moj_fcco_noborrow < 80 ~ "70_to_79",
    80 <= age_repaid_moj_fcco_noborrow ~ "80_up"
  ),
  age_repaid_ird_child_noborrow = case_when(
    00 <= age_repaid_ird_child_noborrow & age_repaid_ird_child_noborrow < 10 ~ "00_to_09",
    10 <= age_repaid_ird_child_noborrow & age_repaid_ird_child_noborrow < 20 ~ "10_to_19",
    20 <= age_repaid_ird_child_noborrow & age_repaid_ird_child_noborrow < 30 ~ "20_to_29",
    30 <= age_repaid_ird_child_noborrow & age_repaid_ird_child_noborrow < 40 ~ "30_to_39",
    40 <= age_repaid_ird_child_noborrow & age_repaid_ird_child_noborrow < 50 ~ "40_to_49",
    50 <= age_repaid_ird_child_noborrow & age_repaid_ird_child_noborrow < 60 ~ "50_to_59",
    60 <= age_repaid_ird_child_noborrow & age_repaid_ird_child_noborrow < 70 ~ "60_to_69",
    70 <= age_repaid_ird_child_noborrow & age_repaid_ird_child_noborrow < 80 ~ "70_to_79",
    80 <= age_repaid_ird_child_noborrow ~ "80_up"
  ),
  age_repaid_ird_income_noborrow = case_when(
    00 <= age_repaid_ird_income_noborrow & age_repaid_ird_income_noborrow < 10 ~ "00_to_09",
    10 <= age_repaid_ird_income_noborrow & age_repaid_ird_income_noborrow < 20 ~ "10_to_19",
    20 <= age_repaid_ird_income_noborrow & age_repaid_ird_income_noborrow < 30 ~ "20_to_29",
    30 <= age_repaid_ird_income_noborrow & age_repaid_ird_income_noborrow < 40 ~ "30_to_39",
    40 <= age_repaid_ird_income_noborrow & age_repaid_ird_income_noborrow < 50 ~ "40_to_49",
    50 <= age_repaid_ird_income_noborrow & age_repaid_ird_income_noborrow < 60 ~ "50_to_59",
    60 <= age_repaid_ird_income_noborrow & age_repaid_ird_income_noborrow < 70 ~ "60_to_69",
    70 <= age_repaid_ird_income_noborrow & age_repaid_ird_income_noborrow < 80 ~ "70_to_79",
    80 <= age_repaid_ird_income_noborrow ~ "80_up"
  ),
  age_repaid_ird_student_noborrow = case_when(
    00 <= age_repaid_ird_student_noborrow & age_repaid_ird_student_noborrow < 10 ~ "00_to_09",
    10 <= age_repaid_ird_student_noborrow & age_repaid_ird_student_noborrow < 20 ~ "10_to_19",
    20 <= age_repaid_ird_student_noborrow & age_repaid_ird_student_noborrow < 30 ~ "20_to_29",
    30 <= age_repaid_ird_student_noborrow & age_repaid_ird_student_noborrow < 40 ~ "30_to_39",
    40 <= age_repaid_ird_student_noborrow & age_repaid_ird_student_noborrow < 50 ~ "40_to_49",
    50 <= age_repaid_ird_student_noborrow & age_repaid_ird_student_noborrow < 60 ~ "50_to_59",
    60 <= age_repaid_ird_student_noborrow & age_repaid_ird_student_noborrow < 70 ~ "60_to_69",
    70 <= age_repaid_ird_student_noborrow & age_repaid_ird_student_noborrow < 80 ~ "70_to_79",
    80 <= age_repaid_ird_student_noborrow ~ "80_up"
  ),
  age_repaid_ird_wff_noborrow = case_when(
    00 <= age_repaid_ird_wff_noborrow & age_repaid_ird_wff_noborrow < 10 ~ "00_to_09",
    10 <= age_repaid_ird_wff_noborrow & age_repaid_ird_wff_noborrow < 20 ~ "10_to_19",
    20 <= age_repaid_ird_wff_noborrow & age_repaid_ird_wff_noborrow < 30 ~ "20_to_29",
    30 <= age_repaid_ird_wff_noborrow & age_repaid_ird_wff_noborrow < 40 ~ "30_to_39",
    40 <= age_repaid_ird_wff_noborrow & age_repaid_ird_wff_noborrow < 50 ~ "40_to_49",
    50 <= age_repaid_ird_wff_noborrow & age_repaid_ird_wff_noborrow < 60 ~ "50_to_59",
    60 <= age_repaid_ird_wff_noborrow & age_repaid_ird_wff_noborrow < 70 ~ "60_to_69",
    70 <= age_repaid_ird_wff_noborrow & age_repaid_ird_wff_noborrow < 80 ~ "70_to_79",
    80 <= age_repaid_ird_wff_noborrow ~ "80_up"
  ),
  age_repaid_ird_oth_noborrow = case_when(
    00 <= age_repaid_ird_oth_noborrow & age_repaid_ird_oth_noborrow < 10 ~ "00_to_09",
    10 <= age_repaid_ird_oth_noborrow & age_repaid_ird_oth_noborrow < 20 ~ "10_to_19",
    20 <= age_repaid_ird_oth_noborrow & age_repaid_ird_oth_noborrow < 30 ~ "20_to_29",
    30 <= age_repaid_ird_oth_noborrow & age_repaid_ird_oth_noborrow < 40 ~ "30_to_39",
    40 <= age_repaid_ird_oth_noborrow & age_repaid_ird_oth_noborrow < 50 ~ "40_to_49",
    50 <= age_repaid_ird_oth_noborrow & age_repaid_ird_oth_noborrow < 60 ~ "50_to_59",
    60 <= age_repaid_ird_oth_noborrow & age_repaid_ird_oth_noborrow < 70 ~ "60_to_69",
    70 <= age_repaid_ird_oth_noborrow & age_repaid_ird_oth_noborrow < 80 ~ "70_to_79",
    80 <= age_repaid_ird_oth_noborrow ~ "80_up"
  )
  ) %>%
## time to repay categories ----------
mutate(
  time_to_repay_msd_newborrow = case_when(
    00 <= time_to_repay_msd_newborrow & time_to_repay_msd_newborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_msd_newborrow & time_to_repay_msd_newborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_msd_newborrow & time_to_repay_msd_newborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_msd_newborrow & time_to_repay_msd_newborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_msd_newborrow & time_to_repay_msd_newborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_msd_newborrow ~ "50_up"
  ),
  time_to_repay_moj_fine_newborrow = case_when(
    00 <= time_to_repay_moj_fine_newborrow & time_to_repay_moj_fine_newborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_moj_fine_newborrow & time_to_repay_moj_fine_newborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_moj_fine_newborrow & time_to_repay_moj_fine_newborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_moj_fine_newborrow & time_to_repay_moj_fine_newborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_moj_fine_newborrow & time_to_repay_moj_fine_newborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_moj_fine_newborrow ~ "50_up"
  ),
  time_to_repay_moj_fcco_newborrow = case_when(
    00 <= time_to_repay_moj_fcco_newborrow & time_to_repay_moj_fcco_newborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_moj_fcco_newborrow & time_to_repay_moj_fcco_newborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_moj_fcco_newborrow & time_to_repay_moj_fcco_newborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_moj_fcco_newborrow & time_to_repay_moj_fcco_newborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_moj_fcco_newborrow & time_to_repay_moj_fcco_newborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_moj_fcco_newborrow ~ "50_up"
  ),
  time_to_repay_ird_child_newborrow = case_when(
    00 <= time_to_repay_ird_child_newborrow & time_to_repay_ird_child_newborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_ird_child_newborrow & time_to_repay_ird_child_newborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_ird_child_newborrow & time_to_repay_ird_child_newborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_ird_child_newborrow & time_to_repay_ird_child_newborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_ird_child_newborrow & time_to_repay_ird_child_newborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_ird_child_newborrow ~ "50_up"
  ),
  time_to_repay_ird_income_newborrow = case_when(
    00 <= time_to_repay_ird_income_newborrow & time_to_repay_ird_income_newborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_ird_income_newborrow & time_to_repay_ird_income_newborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_ird_income_newborrow & time_to_repay_ird_income_newborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_ird_income_newborrow & time_to_repay_ird_income_newborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_ird_income_newborrow & time_to_repay_ird_income_newborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_ird_income_newborrow ~ "50_up"
  ),
  time_to_repay_ird_student_newborrow = case_when(
    00 <= time_to_repay_ird_student_newborrow & time_to_repay_ird_student_newborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_ird_student_newborrow & time_to_repay_ird_student_newborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_ird_student_newborrow & time_to_repay_ird_student_newborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_ird_student_newborrow & time_to_repay_ird_student_newborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_ird_student_newborrow & time_to_repay_ird_student_newborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_ird_student_newborrow ~ "50_up"
  ),
  time_to_repay_ird_wff_newborrow = case_when(
    00 <= time_to_repay_ird_wff_newborrow & time_to_repay_ird_wff_newborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_ird_wff_newborrow & time_to_repay_ird_wff_newborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_ird_wff_newborrow & time_to_repay_ird_wff_newborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_ird_wff_newborrow & time_to_repay_ird_wff_newborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_ird_wff_newborrow & time_to_repay_ird_wff_newborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_ird_wff_newborrow ~ "50_up"
  ),
  time_to_repay_ird_oth_newborrow = case_when(
    00 <= time_to_repay_ird_oth_newborrow & time_to_repay_ird_oth_newborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_ird_oth_newborrow & time_to_repay_ird_oth_newborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_ird_oth_newborrow & time_to_repay_ird_oth_newborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_ird_oth_newborrow & time_to_repay_ird_oth_newborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_ird_oth_newborrow & time_to_repay_ird_oth_newborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_ird_oth_newborrow ~ "50_up"
  ),
  time_to_repay_msd_noborrow = case_when(
    00 <= time_to_repay_msd_noborrow & time_to_repay_msd_noborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_msd_noborrow & time_to_repay_msd_noborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_msd_noborrow & time_to_repay_msd_noborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_msd_noborrow & time_to_repay_msd_noborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_msd_noborrow & time_to_repay_msd_noborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_msd_noborrow ~ "50_up"
  ),
  time_to_repay_moj_fine_noborrow = case_when(
    00 <= time_to_repay_moj_fine_noborrow & time_to_repay_moj_fine_noborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_moj_fine_noborrow & time_to_repay_moj_fine_noborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_moj_fine_noborrow & time_to_repay_moj_fine_noborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_moj_fine_noborrow & time_to_repay_moj_fine_noborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_moj_fine_noborrow & time_to_repay_moj_fine_noborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_moj_fine_noborrow ~ "50_up"
  ),
  time_to_repay_moj_fcco_noborrow = case_when(
    00 <= time_to_repay_moj_fcco_noborrow & time_to_repay_moj_fcco_noborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_moj_fcco_noborrow & time_to_repay_moj_fcco_noborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_moj_fcco_noborrow & time_to_repay_moj_fcco_noborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_moj_fcco_noborrow & time_to_repay_moj_fcco_noborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_moj_fcco_noborrow & time_to_repay_moj_fcco_noborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_moj_fcco_noborrow ~ "50_up"
  ),
  time_to_repay_ird_child_noborrow = case_when(
    00 <= time_to_repay_ird_child_noborrow & time_to_repay_ird_child_noborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_ird_child_noborrow & time_to_repay_ird_child_noborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_ird_child_noborrow & time_to_repay_ird_child_noborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_ird_child_noborrow & time_to_repay_ird_child_noborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_ird_child_noborrow & time_to_repay_ird_child_noborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_ird_child_noborrow ~ "50_up"
  ),
  time_to_repay_ird_income_noborrow = case_when(
    00 <= time_to_repay_ird_income_noborrow & time_to_repay_ird_income_noborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_ird_income_noborrow & time_to_repay_ird_income_noborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_ird_income_noborrow & time_to_repay_ird_income_noborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_ird_income_noborrow & time_to_repay_ird_income_noborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_ird_income_noborrow & time_to_repay_ird_income_noborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_ird_income_noborrow ~ "50_up"
  ),
  time_to_repay_ird_student_noborrow = case_when(
    00 <= time_to_repay_ird_student_noborrow & time_to_repay_ird_student_noborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_ird_student_noborrow & time_to_repay_ird_student_noborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_ird_student_noborrow & time_to_repay_ird_student_noborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_ird_student_noborrow & time_to_repay_ird_student_noborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_ird_student_noborrow & time_to_repay_ird_student_noborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_ird_student_noborrow ~ "50_up"
  ),
  time_to_repay_ird_wff_noborrow = case_when(
    00 <= time_to_repay_ird_wff_noborrow & time_to_repay_ird_wff_noborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_ird_wff_noborrow & time_to_repay_ird_wff_noborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_ird_wff_noborrow & time_to_repay_ird_wff_noborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_ird_wff_noborrow & time_to_repay_ird_wff_noborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_ird_wff_noborrow & time_to_repay_ird_wff_noborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_ird_wff_noborrow ~ "50_up"
  ),
  time_to_repay_ird_oth_noborrow = case_when(
    00 <= time_to_repay_ird_oth_noborrow & time_to_repay_ird_oth_noborrow < 10 ~ "00_to_09",
    10 <= time_to_repay_ird_oth_noborrow & time_to_repay_ird_oth_noborrow < 20 ~ "10_to_19",
    20 <= time_to_repay_ird_oth_noborrow & time_to_repay_ird_oth_noborrow < 30 ~ "20_to_29",
    30 <= time_to_repay_ird_oth_noborrow & time_to_repay_ird_oth_noborrow < 40 ~ "30_to_39",
    40 <= time_to_repay_ird_oth_noborrow & time_to_repay_ird_oth_noborrow < 50 ~ "40_to_49",
    50 <= time_to_repay_ird_oth_noborrow ~ "50_up"
  )
) %>%
## ratio categories ----
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
## income categories ----  
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

## keep only needed cols and save for reuse -------------------------------------------------------
  
# remove unneeded columns
unneeded_columns = c(
  "audit_personal_detail", 
  "birth_year", 
  "days_benefit", 
  "income_t2_benefits", 
  "income_t3_benefits", 
  "income_taxible", 
  "income_wff", 
  "ir_child_12_debtor", 
  "ir_child_13_debtor", 
  "ir_child_14_debtor", 
  "ir_child_15_debtor", 
  "ir_child_16_debtor", 
  "ir_child_17_debtor", 
  "ir_child_18_debtor", 
  "ir_child_19_debtor", 
  "ir_income_12_debtor", 
  "ir_income_13_debtor", 
  "ir_income_14_debtor", 
  "ir_income_15_debtor", 
  "ir_income_16_debtor", 
  "ir_income_17_debtor", 
  "ir_income_18_debtor", 
  "ir_income_19_debtor", 
  "ir_oth_12_debtor", 
  "ir_oth_13_debtor", 
  "ir_oth_14_debtor", 
  "ir_oth_15_debtor", 
  "ir_oth_16_debtor", 
  "ir_oth_17_debtor", 
  "ir_oth_18_debtor", 
  "ir_oth_19_debtor", 
  "ir_student_12_debtor", 
  "ir_student_13_debtor", 
  "ir_student_14_debtor", 
  "ir_student_15_debtor", 
  "ir_student_16_debtor", 
  "ir_student_17_debtor", 
  "ir_student_18_debtor", 
  "ir_student_19_debtor", 
  "ir_total_repayment_12", 
  "ir_total_repayment_13", 
  "ir_total_repayment_14", 
  "ir_total_repayment_15", 
  "ir_total_repayment_16", 
  "ir_total_repayment_17", 
  "ir_total_repayment_18", 
  "ir_total_repayment_19", 
  "ir_wff_12_debtor", 
  "ir_wff_13_debtor", 
  "ir_wff_14_debtor", 
  "ir_wff_15_debtor", 
  "ir_wff_16_debtor", 
  "ir_wff_17_debtor", 
  "ir_wff_18_debtor", 
  "ir_wff_19_debtor", 
  "ird_pr19_balance_child_2012", 
  "ird_pr19_balance_child_2013", 
  "ird_pr19_balance_child_2014", 
  "ird_pr19_balance_child_2015", 
  "ird_pr19_balance_child_2016", 
  "ird_pr19_balance_child_2017", 
  "ird_pr19_balance_child_2018", 
  "ird_pr19_balance_income_2012", 
  "ird_pr19_balance_income_2013", 
  "ird_pr19_balance_income_2014", 
  "ird_pr19_balance_income_2015", 
  "ird_pr19_balance_income_2016", 
  "ird_pr19_balance_income_2017", 
  "ird_pr19_balance_income_2018", 
  "ird_pr19_balance_oth_2012", 
  "ird_pr19_balance_oth_2013", 
  "ird_pr19_balance_oth_2014", 
  "ird_pr19_balance_oth_2015", 
  "ird_pr19_balance_oth_2016", 
  "ird_pr19_balance_oth_2017", 
  "ird_pr19_balance_oth_2018", 
  "ird_pr19_balance_student_2012", 
  "ird_pr19_balance_student_2013", 
  "ird_pr19_balance_student_2014", 
  "ird_pr19_balance_student_2015", 
  "ird_pr19_balance_student_2016", 
  "ird_pr19_balance_student_2017", 
  "ird_pr19_balance_student_2018", 
  "ird_pr19_balance_wff_2012", 
  "ird_pr19_balance_wff_2013", 
  "ird_pr19_balance_wff_2014", 
  "ird_pr19_balance_wff_2015", 
  "ird_pr19_balance_wff_2016", 
  "ird_pr19_balance_wff_2017", 
  "ird_pr19_balance_wff_2018", 
  "ird_pr19_principle_child_2012", 
  "ird_pr19_principle_child_2013", 
  "ird_pr19_principle_child_2014", 
  "ird_pr19_principle_child_2015", 
  "ird_pr19_principle_child_2016", 
  "ird_pr19_principle_child_2017", 
  "ird_pr19_principle_child_2018", 
  "ird_pr19_principle_income_2012", 
  "ird_pr19_principle_income_2013", 
  "ird_pr19_principle_income_2014", 
  "ird_pr19_principle_income_2015", 
  "ird_pr19_principle_income_2016", 
  "ird_pr19_principle_income_2017", 
  "ird_pr19_principle_income_2018", 
  "ird_pr19_principle_oth_2012", 
  "ird_pr19_principle_oth_2013", 
  "ird_pr19_principle_oth_2014", 
  "ird_pr19_principle_oth_2015", 
  "ird_pr19_principle_oth_2016", 
  "ird_pr19_principle_oth_2017", 
  "ird_pr19_principle_oth_2018", 
  "ird_pr19_principle_student_2012", 
  "ird_pr19_principle_student_2013", 
  "ird_pr19_principle_student_2014", 
  "ird_pr19_principle_student_2015", 
  "ird_pr19_principle_student_2016", 
  "ird_pr19_principle_student_2017", 
  "ird_pr19_principle_student_2018", 
  "ird_pr19_principle_wff_2012", 
  "ird_pr19_principle_wff_2013", 
  "ird_pr19_principle_wff_2014", 
  "ird_pr19_principle_wff_2015", 
  "ird_pr19_principle_wff_2016", 
  "ird_pr19_principle_wff_2017", 
  "ird_pr19_principle_wff_2018", 
  "ird_pr19_repaid_child_2012", 
  "ird_pr19_repaid_child_2013", 
  "ird_pr19_repaid_child_2014", 
  "ird_pr19_repaid_child_2015", 
  "ird_pr19_repaid_child_2016", 
  "ird_pr19_repaid_child_2017", 
  "ird_pr19_repaid_child_2018", 
  "ird_pr19_repaid_income_2012", 
  "ird_pr19_repaid_income_2013", 
  "ird_pr19_repaid_income_2014", 
  "ird_pr19_repaid_income_2015", 
  "ird_pr19_repaid_income_2016", 
  "ird_pr19_repaid_income_2017", 
  "ird_pr19_repaid_income_2018", 
  "ird_pr19_repaid_oth_2012", 
  "ird_pr19_repaid_oth_2013", 
  "ird_pr19_repaid_oth_2014", 
  "ird_pr19_repaid_oth_2015", 
  "ird_pr19_repaid_oth_2016", 
  "ird_pr19_repaid_oth_2017", 
  "ird_pr19_repaid_oth_2018", 
  "ird_pr19_repaid_student_2012", 
  "ird_pr19_repaid_student_2013", 
  "ird_pr19_repaid_student_2014", 
  "ird_pr19_repaid_student_2015", 
  "ird_pr19_repaid_student_2016", 
  "ird_pr19_repaid_student_2017", 
  "ird_pr19_repaid_student_2018", 
  "ird_pr19_repaid_wff_2012", 
  "ird_pr19_repaid_wff_2013", 
  "ird_pr19_repaid_wff_2014", 
  "ird_pr19_repaid_wff_2015", 
  "ird_pr19_repaid_wff_2016", 
  "ird_pr19_repaid_wff_2017", 
  "ird_pr19_repaid_wff_2018", 
  "ird_ps18_balance_child_2019", 
  "ird_ps18_balance_income_2019", 
  "ird_ps18_balance_oth_2019", 
  "ird_ps18_balance_wff_2019", 
  "ird_ps18_principle_child_2019", 
  "ird_ps18_principle_income_2019", 
  "ird_ps18_principle_oth_2019", 
  "ird_ps18_principle_wff_2019", 
  "ird_ps18_repayment_child_2019", 
  "ird_ps18_repayment_income_2019", 
  "ird_ps18_repayment_oth_2019", 
  "ird_ps18_repayment_wff_2019", 
  "moj_fcco_14_debtor", 
  "moj_fcco_15_debtor", 
  "moj_fcco_16_debtor", 
  "moj_fcco_17_debtor", 
  "moj_fcco_18_debtor", 
  "moj_fcco_19_debtor", 
  "moj_fcco_balance_2014", 
  "moj_fcco_balance_2015", 
  "moj_fcco_balance_2016", 
  "moj_fcco_balance_2017", 
  "moj_fcco_balance_2018", 
  "moj_fcco_balance_2019", 
  "moj_fcco_principle_2014", 
  "moj_fcco_principle_2015", 
  "moj_fcco_principle_2016", 
  "moj_fcco_principle_2017", 
  "moj_fcco_principle_2018", 
  "moj_fcco_principle_2019", 
  "moj_fcco_repaid_2014", 
  "moj_fcco_repaid_2015", 
  "moj_fcco_repaid_2016", 
  "moj_fcco_repaid_2017", 
  "moj_fcco_repaid_2018", 
  "moj_fcco_repaid_2019", 
  "moj_fine_12_debtor", 
  "moj_fine_13_debtor", 
  "moj_fine_14_debtor", 
  "moj_fine_15_debtor", 
  "moj_fine_16_debtor", 
  "moj_fine_17_debtor", 
  "moj_fine_18_debtor", 
  "moj_fine_19_debtor", 
  "moj_fine_balance_2012", 
  "moj_fine_balance_2013", 
  "moj_fine_balance_2014", 
  "moj_fine_balance_2015", 
  "moj_fine_balance_2016", 
  "moj_fine_balance_2017", 
  "moj_fine_balance_2018", 
  "moj_fine_balance_2019", 
  "moj_fine_principle_2012", 
  "moj_fine_principle_2013", 
  "moj_fine_principle_2014", 
  "moj_fine_principle_2015", 
  "moj_fine_principle_2016", 
  "moj_fine_principle_2017", 
  "moj_fine_principle_2018", 
  "moj_fine_principle_2019", 
  "moj_fine_repaid_2012", 
  "moj_fine_repaid_2013", 
  "moj_fine_repaid_2014", 
  "moj_fine_repaid_2015", 
  "moj_fine_repaid_2016", 
  "moj_fine_repaid_2017", 
  "moj_fine_repaid_2018", 
  "moj_fine_repaid_2019", 
  "moj_total_repayment_14", 
  "moj_total_repayment_15", 
  "moj_total_repayment_16", 
  "moj_total_repayment_17", 
  "moj_total_repayment_18", 
  "moj_total_repayment_19", 
  "msd_12_debtor", 
  "msd_13_debtor", 
  "msd_14_debtor", 
  "msd_15_debtor", 
  "msd_16_debtor", 
  "msd_17_debtor", 
  "msd_18_debtor", 
  "msd_19_debtor", 
  "msd_balance_2009", 
  "msd_balance_2010", 
  "msd_balance_2011", 
  "msd_balance_2012", 
  "msd_balance_2013", 
  "msd_balance_2014", 
  "msd_balance_2015", 
  "msd_balance_2016", 
  "msd_balance_2017", 
  "msd_balance_2018", 
  "msd_balance_2019", 
  "msd_principle_2009", 
  "msd_principle_2010", 
  "msd_principle_2011", 
  "msd_principle_2012", 
  "msd_principle_2013", 
  "msd_principle_2014", 
  "msd_principle_2015", 
  "msd_principle_2016", 
  "msd_principle_2017", 
  "msd_principle_2018", 
  "msd_principle_2019", 
  "msd_repaid_2009", 
  "msd_repaid_2010", 
  "msd_repaid_2011", 
  "msd_repaid_2012", 
  "msd_repaid_2013", 
  "msd_repaid_2014", 
  "msd_repaid_2015", 
  "msd_repaid_2016", 
  "msd_repaid_2017", 
  "msd_repaid_2018", 
  "msd_repaid_2019", 
  "SA2_code"
)

keep_cols = colnames(working_table)
keep_cols = keep_cols[! keep_cols %in% unneeded_columns]

working_table = working_table %>%
  select(all_of(keep_cols))
    
working_table = write_for_reuse(db_con, SANDPIT, OUR_SCHEMA, INTERIM_TABLE, working_table)

## list variables for summarising -----------------------------------------------------------------

run_time_inform_user("prep for summarising", context = "heading", print_level = VERBOSE)

# all columns containing the text "persistence"
persistence_columns = colnames(working_table)
persistence_columns = persistence_columns[grepl("persistence", persistence_columns)]

# columns to summarise
always_summarise_these_columns = c("msd_debtor", "moj_debtor", "ird_debtor", "is_dep_child_hhld",
                                   "is_dep_child_indv", "low_income", "res_indicator", "is_beneficiary")

summarise_each_of_these_columns = c("everybody", "fine_debtor", "fcco_debtor", "child_debtor",
                                    "income_debtor", "student_debtor", "wff_debtor", "other_debtor",
                                    "total_debt_group")

count_each_of_these_columns = c(persistence_columns, "eth_asian", "eth_european", "eth_maori",
                                "eth_MELAA", "eth_other", "eth_pacific", "sex_code", "region_code",
                                "area_type", "age_cat", "deprivation", "dep_chld_indv", 
                                "yngst_child__indv_age", "time_to_repay_msd_newborrow",
                                "time_to_repay_msd_noborrow", "time_to_repay_moj_fine_newborrow",
                                "time_to_repay_moj_fine_noborrow", "time_to_repay_moj_fcco_newborrow",
                                "time_to_repay_moj_fcco_noborrow", "time_to_repay_ird_child_newborrow",
                                "time_to_repay_ird_child_noborrow", "time_to_repay_ird_income_newborrow",
                                "time_to_repay_ird_income_noborrow", "time_to_repay_ird_student_newborrow",
                                "time_to_repay_ird_student_noborrow", "time_to_repay_ird_wff_newborrow",
                                "time_to_repay_ird_wff_noborrow", "time_to_repay_ird_oth_newborrow",
                                "time_to_repay_ird_oth_noborrow", "debt_vs_income_moj", "debt_vs_income_msd",
                                "debt_vs_income_ird", "debt_vs_income_all", "repayment_vs_income_moj",
                                "repayment_vs_income_msd", "repayment_vs_income_ird", "repayment_vs_income_all",
                                "repayment_vs_debt_moj", "repayment_vs_debt_msd", "repayment_vs_debt_ird",
                                "repayment_vs_debt_all", "total_income_cat", "income_post_all_repayment_cat",
                                "income_post_msd_repayment_cat", "income_post_moj_repayment_cat",
                                "income_post_ir_repayment_cat")

sum_each_of_these_columns = list("ird_ps18_balance_child_2020", "ird_ps18_balance_income_2020", 
                              "ird_ps18_balance_oth_2020", "ird_ps18_balance_student_2020", 
                              "ird_ps18_balance_wff_2020", "ird_ps18_principle_child_2020", 
                              "ird_ps18_principle_income_2020", "ird_ps18_principle_oth_2020", 
                              "ird_ps18_principle_student_2020", "ird_ps18_principle_wff_2020", 
                              "ird_ps18_repayment_child_2020", "ird_ps18_repayment_income_2020", 
                              "ird_ps18_repayment_oth_2020", "ird_ps18_repayment_student_2020", 
                              "ird_ps18_repayment_wff_2020", "moj_fcco_balance_2020", 
                              "moj_fcco_principle_2020", "moj_fcco_repaid_2020", 
                              "moj_fine_balance_2020", "moj_fine_principle_2020", 
                              "moj_fine_repaid_2020", "msd_balance_2020", 
                              "msd_principle_2020", "msd_repaid_2020", "total_income", 
                              "moj_total_debt_20", "ir_total_debt_20", "moj_total_repayment_20", 
                              "ir_total_repayment_20")

## summarise dataset ------------------------------------------------------------------------------

run_time_inform_user("summarising datasets", context = "heading", print_level = VERBOSE)

###### counts --- --- --- --- --- --- ---
run_time_inform_user("begun summarising count", context = "details", print_level = VERBOSE)

list_for_count = cross_product_column_names(summarise_each_of_these_columns
                                            , count_each_of_these_columns
                                            , always = always_summarise_these_columns
                                            , drop.dupes = TRUE)

count_summary = summarise_and_label_over_lists(
  df = working_table, 
  group_by_list = list_for_count,
  summarise_list = list("identity_column"),
  make_distinct = FALSE,
  make_count = TRUE,
  make_sum = FALSE,
  clean = "zero.as.na", # {"none", "na.as.zero", "zero.as.na"}
  remove.na.from.groups = TRUE
)

run_time_inform_user("complete summarising count", context = "details", print_level = VERBOSE)

###### sums --- --- --- --- --- --- --- ---
run_time_inform_user("begun summarising sum", context = "details", print_level = VERBOSE)

list_for_sum = cross_product_column_names(summarise_each_of_these_columns
                                          , always = always_summarise_these_columns
                                          , drop.dupes = TRUE)

sum_summary = summarise_and_label_over_lists(
  df = working_table, 
  group_by_list = list_for_sum,
  summarise_list = sum_each_of_these_columns,
  make_distinct = FALSE,
  make_count = TRUE,
  make_sum = TRUE,
  clean = "zero.as.na", # {"none", "na.as.zero", "zero.as.na"}
  remove.na.from.groups = TRUE
)

run_time_inform_user("complete summarising sum", context = "details", print_level = VERBOSE)

## confidentialise summaries ----------------------------------------------------------------------

run_time_inform_user("confidentialise summaries", context = "heading", print_level = VERBOSE)

count_summary_conf = confidentialise_results(count_summary)
sum_summary_conf = confidentialise_results(sum_summary)

run_time_inform_user("confidentialising complete", context = "details", print_level = VERBOSE)

## write for output -------------------------------------------------------------------------------

run_time_inform_user("writing csv output", context = "heading", print_level = VERBOSE)

run_time_inform_user("count summary", context = "details", print_level = VERBOSE)
write.csv(count_summary, file = paste0(OUTPUT_FOLDER, "count summary.csv"))

run_time_inform_user("sum summary", context = "details", print_level = VERBOSE)
write.csv(sum_summary, file = paste0(OUTPUT_FOLDER, "sum summary.csv"))

run_time_inform_user("count confidentialised", context = "details", print_level = VERBOSE)
write.csv(count_summary_conf, file = paste0(OUTPUT_FOLDER, "count confidentialised.csv"))

run_time_inform_user("sum confidentialised", context = "details", print_level = VERBOSE)
write.csv(sum_summary_conf, file = paste0(OUTPUT_FOLDER, "sum confidentialised.csv"))

run_time_inform_user("file write complete", context = "details", print_level = VERBOSE)

## conclude ---------------------------------------------------------------------------------------

# close connection
close_database_connection(db_con)
run_time_inform_user("grand completion", context = "heading", print_level = VERBOSE)
