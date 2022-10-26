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
TIDY_TABLE = "[d2gP3_tidy_table]"
# outputs
INTERIM_TABLE = "[tmp_interum_summary]"
OUTPUT_FOLDER = "../output/"
SUMMARY_FILE = "summarised_results.xlsx"
CONFIDENTIALISED_FILE = "confidentialised_results.xlsx"
SUMMARY_FILE2 = "summarised_results2.xlsx"
CONFIDENTIALISED_FILE2 = "confidentialised_results2.xlsx"

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

## Filter only resident population
# working_table = working_table %>% filter(res_indicator == 1)

if(DEVELOPMENT_MODE)
  working_table = working_table %>% filter(identity_column %% 100 == 0)
## keep only needed cols and save for reuse -------------------------------------------------------
unneeded_columns = c(
  "audit_personal_detail", 
  "birth_year", 
  "days_benefit", 
  "income_t2_benefits", 
  "income_t3_benefits", 
  "income_taxible", 
  "income_wff", 
  "everybody",
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
  "msd_balance_2012", 
  "msd_balance_2013", 
  "msd_balance_2014", 
  "msd_balance_2015", 
  "msd_balance_2016", 
  "msd_balance_2017", 
  "msd_balance_2018", 
  "msd_balance_2019", 
  "msd_principle_2012", 
  "msd_principle_2013", 
  "msd_principle_2014", 
  "msd_principle_2015", 
  "msd_principle_2016", 
  "msd_principle_2017", 
  "msd_principle_2018", 
  "msd_principle_2019", 
  "msd_repaid_2012", 
  "msd_repaid_2013", 
  "msd_repaid_2014", 
  "msd_repaid_2015", 
  "msd_repaid_2016", 
  "msd_repaid_2017", 
  "msd_repaid_2018", 
  "msd_repaid_2019", 
  "SA2_code",
  "oldst_dep_child_hhld",
  "oldst_dep_child_indv",
  "yngst_dep_chld_hhld",
  "yngst_dep_chld_indv",
  "resident"
)
keep_cols = colnames(working_table)
keep_cols = keep_cols[! keep_cols %in% unneeded_columns]

working_table = working_table %>%
  select(all_of(keep_cols))

working_table = write_for_reuse(db_con, SANDPIT, OUR_SCHEMA, INTERIM_TABLE, working_table)

## list variables for summarising -----------------------------------------------------------------

run_time_inform_user("prep for summarising", context = "heading", print_level = VERBOSE)

# all columns containing the text "persistence"
# persistence_columns = colnames(working_table)
# persistence_columns = persistence_columns[grepl("persistence", persistence_columns)]

# columns to summarise
always_summarise_these_columns = c("msd_debtor", 
                                   "moj_debtor", 
                                   "ird_debtor", 
                                   "any_persistence", 
                                   "low_income", 
                                   "res_indicator", 
                                   "is_beneficiary")

summarise_each_of_these_columns = c("current_debtor",
                                    "fine_debtor", 
                                    "fcco_debtor", 
                                    "child_debtor",
                                    "income_debtor", 
                                    "student_debtor", 
                                    "wff_debtor", 
                                    "other_debtor",
                                    "total_debt_group",
                                    "any_msd_persistence", 
                                    "any_moj_fine_persistence",
                                    "any_moj_fcco_persistence", 
                                    "any_ir_child_persistence", 
                                    "any_ir_income_persistence",
                                    "any_ir_student_persistence",
                                    "any_ir_wff_persistence", 
                                    "any_ir_oth_persistence")

count_each_of_these_columns = c("msd_persistence_2yrs",
                                "msd_persistence_3yrs",
                                "msd_persistence_4yrs",
                                "msd_persistence_5yrs",
                                "msd_persistence_6yrs",
                                "msd_persistence_7yrs",
                                "msd_persistence_8yrs",
                                "msd_persistence_9yrs",
                                "moj_fine_persistence_2yrs",
                                "moj_fine_persistence_3yrs",
                                "moj_fine_persistence_4yrs",
                                "moj_fine_persistence_5yrs",
                                "moj_fine_persistence_6yrs",
                                "moj_fine_persistence_7yrs",
                                "moj_fine_persistence_8yrs",
                                "moj_fine_persistence_9yrs",
                                "moj_fcco_persistence_2yrs",
                                "moj_fcco_persistence_3yrs",
                                "moj_fcco_persistence_4yrs",
                                "moj_fcco_persistence_5yrs",
                                "moj_fcco_persistence_6yrs",
                                "moj_fcco_persistence_7yrs",
                                "ir_child_persistence_2yrs",
                                "ir_child_persistence_3yrs",
                                "ir_child_persistence_4yrs",
                                "ir_child_persistence_5yrs",
                                "ir_child_persistence_6yrs",
                                "ir_child_persistence_7yrs",
                                "ir_child_persistence_8yrs",
                                "ir_child_persistence_9yrs",
                                "ir_income_persistence_2yrs",
                                "ir_income_persistence_3yrs",
                                "ir_income_persistence_4yrs",
                                "ir_income_persistence_5yrs",
                                "ir_income_persistence_6yrs",
                                "ir_income_persistence_7yrs",
                                "ir_income_persistence_8yrs",
                                "ir_income_persistence_9yrs",
                                "ir_student_persistence_2yrs",
                                "ir_student_persistence_3yrs",
                                "ir_student_persistence_4yrs",
                                "ir_student_persistence_5yrs",
                                "ir_student_persistence_6yrs",
                                "ir_student_persistence_7yrs",
                                "ir_student_persistence_8yrs",
                                "ir_student_persistence_9yrs",
                                "ir_wff_persistence_2yrs",
                                "ir_wff_persistence_3yrs",
                                "ir_wff_persistence_4yrs",
                                "ir_wff_persistence_5yrs",
                                "ir_wff_persistence_6yrs",
                                "ir_wff_persistence_7yrs",
                                "ir_wff_persistence_8yrs",
                                "ir_wff_persistence_9yrs",
                                "ir_oth_persistence_2yrs",
                                "ir_oth_persistence_3yrs",
                                "ir_oth_persistence_4yrs",
                                "ir_oth_persistence_5yrs",
                                "ir_oth_persistence_6yrs",
                                "ir_oth_persistence_7yrs",
                                "ir_oth_persistence_8yrs",
                                "ir_oth_persistence_9yrs",
                                "eth_asian", 
                                "eth_european", 
                                "eth_maori",
                                "eth_MELAA", 
                                "eth_other", 
                                "eth_pacific", 
                                "sex_code", 
                                "region_code",
                                "area_type", 
                                "age_cat", 
                                "deprivation", 
                                "dep_chld_indv",
                                "dep_chld_hhld", 
                                "yngst_child_indv_age", 
                                "yngst_child_hhld_age",
                                "debt_vs_income_moj", 
                                "debt_vs_income_msd",
                                "debt_vs_income_ird", 
                                "debt_vs_income_all", 
                                "repayment_vs_income_moj",
                                "repayment_vs_income_msd", 
                                "repayment_vs_income_ird", 
                                "repayment_vs_income_all",
                                "repayment_vs_debt_moj", 
                                "repayment_vs_debt_msd", 
                                "repayment_vs_debt_ird",
                                "repayment_vs_debt_all",
                                "ipr_vs_income_msd",
                                "ipr_vs_income_moj",
                                "ipr_vs_income_ird",
                                "ipr_vs_income_all",
                                "debt_vs_ipr_msd",
                                "debt_vs_ipr_moj",
                                "debt_vs_ipr_ird",
                                "debt_vs_ipr_all",
                                "repayment_vs_ipr_msd",
                                "repayment_vs_ipr_moj",
                                "repayment_vs_ipr_ird",
                                "repayment_vs_ipr_all",
                                "total_income_cat", 
                                "income_post_all_repayment_cat",
                                "income_post_msd_repayment_cat", 
                                "income_post_moj_repayment_cat",
                                "income_post_ir_repayment_cat")

sum_each_of_these_columns = list("ird_ps18_balance_child_2020", 
                                 "ird_ps18_balance_income_2020", 
                                 "ird_ps18_balance_oth_2020", 
                                 "ird_ps18_balance_student_2020", 
                                 "ird_ps18_balance_wff_2020", 
                                 "ird_ps18_principle_child_2020", 
                                 "ird_ps18_principle_income_2020", 
                                 "ird_ps18_principle_oth_2020", 
                                 "ird_ps18_principle_student_2020", 
                                 "ird_ps18_principle_wff_2020", 
                                 "ird_ps18_repayment_child_2020", 
                                 "ird_ps18_repayment_income_2020", 
                                 "ird_ps18_repayment_oth_2020", 
                                 "ird_ps18_repayment_student_2020", 
                                 "ird_ps18_repayment_wff_2020", 
                                 "moj_fcco_balance_2020", 
                                 "moj_fcco_principle_2020", 
                                 "moj_fcco_repaid_2020", 
                                 "moj_fine_balance_2020", 
                                 "moj_fine_principle_2020", 
                                 "moj_fine_repaid_2020", 
                                 "msd_balance_2020", 
                                 "msd_principle_2020", 
                                 "msd_repaid_2020", 
                                 "total_income", 
                                 "income_post_all_repayment",
                                 "income_post_msd_repayment",
                                 "income_post_moj_repayment",
                                 "income_post_ir_repayment",
                                 "moj_total_debt_20", 
                                 "ir_total_debt_20", 
                                 "moj_total_repayment_20", 
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

## follow up persistence check for specific populations -------------------------------------------

aux_table = working_table %>%
  # any debt persistence
  mutate(
    any_persistence_2yrs = ifelse(
      msd_persistence_2yrs == 1 |
        moj_fine_persistence_2yrs == 1 |
        moj_fcco_persistence_2yrs == 1 |
        ir_child_persistence_2yrs == 1 |
        ir_income_persistence_2yrs == 1 |
        ir_student_persistence_2yrs == 1 |
        ir_wff_persistence_2yrs == 1 |
        ir_oth_persistence_2yrs == 1, 1, 0),
    any_persistence_5yrs = ifelse(
      msd_persistence_5yrs == 1 |
        moj_fine_persistence_5yrs == 1 |
        moj_fcco_persistence_5yrs == 1 |
        ir_child_persistence_5yrs == 1 |
        ir_income_persistence_5yrs == 1 |
        ir_student_persistence_5yrs == 1 |
        ir_wff_persistence_5yrs == 1 |
        ir_oth_persistence_5yrs == 1, 1, 0),
    any_persistence_9yrs = ifelse(
      msd_persistence_9yrs == 1 |
        moj_fine_persistence_9yrs == 1 |
        # moj_fcco_persistence_9yrs == 1 | # fcco data does not go back 9 years
        ir_child_persistence_9yrs == 1 |
        ir_income_persistence_9yrs == 1 |
        ir_student_persistence_9yrs == 1 |
        ir_wff_persistence_9yrs == 1 |
        ir_oth_persistence_9yrs == 1, 1, 0)
  ) %>%
  mutate(total = 1) %>%
  select(
    identity_column,
    current_debtor,
    any_persistence_2yrs,
    any_persistence_5yrs,
    any_persistence_9yrs,
    total,
    low_income,
    is_beneficiary,
    res_indicator,
    msd_debtor,
    moj_debtor,
    ird_debtor
  )

each_of = c("total", "any_persistence_2yrs", "any_persistence_5yrs", "any_persistence_9yrs")
all_of = c("msd_debtor", "moj_debtor", "ird_debtor", "res_indicator", "low_income", "is_beneficiary", "current_debtor")
aux_summary_groups = cross_product_column_names(each_of, always = all_of, drop.dupes = TRUE)

aux_summary = summarise_and_label_over_lists(
  df = aux_table, 
  group_by_list = aux_summary_groups,
  summarise_list = list("identity_column"),
  make_distinct = FALSE,
  make_count = TRUE,
  make_sum = FALSE,
  clean = "zero.as.na", # {"none", "na.as.zero", "zero.as.na"}
  remove.na.from.groups = TRUE
)


aux_summary_conf = confidentialise_results(aux_summary)
write.csv(aux_summary_conf, file = paste0(OUTPUT_FOLDER, "count summary2.csv"))

## conclude ---------------------------------------------------------------------------------------
# close connection
close_database_connection(db_con)
run_time_inform_user("grand completion", context = "heading", print_level = VERBOSE)
