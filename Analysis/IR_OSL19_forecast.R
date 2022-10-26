#############################################################################################################
#' Description: Predicting 2019 data for IRD - Overdue Student Loan
#' - Debt to IR was loaded into IDI in two parts namely,
#'     - Pre 2019 (From 2010 to 2018)
#'     - Post 2018 (2019 and 2020)
#' - The key issue with overdue student loan in the later dataset is, it is available only from March 2020.
#'   Hence, the combined debtor table isused to impute the missing data (2019) and identify if the person is still in debt.
#'   
#' Input: 
#'
#' Output: Sandpit table with the predicted values
#' 
#' Author:  Manjusha Radhakrishnan
#' 
#' Dependencies: utility_functions.R, dbplyr_helper_functions.R, table_consistency_checks.R
#' 
#' Issues:
#' 
#' Notes:
#' 1. Debt balance is reported every quarterly; Hence, Q4 (September) is considered as the indicative balance for the year.
#' 2. >90% of the balance values are concentrated between $40000 and -$60000.
#' 3. snz_uid,snz_ird_uid and snz_case_number is considered as unique identifier.
#' 4. Debt balances are not reported every quarter. A row/record appears in the dataset if there were transactions that quarter. 
#'    Since absence of record means the person has no more debt, NA is replaced with 0.
#' 5. More than 80% of the values under $10 are exactly $0. Hence, $10 is set as a limit to determine if a person is in debt.
#' 
#' History (reverse order):
#' 2022-01-17 MR 
#############################################################################################################
## parameters -----------------------------------------------------------------------------------------------

# locations
ABSOLUTE_PATH_TO_TOOL <- "~/Network-Shares/DataLabNas/MAA/MAA2020-01/debt to government_Phase3/tools"
ABSOLUTE_PATH_TO_ANALYSIS <- "~/Network-Shares/DataLabNas/MAA/MAA2020-01/debt to government_Phase3/analysis"
SANDPIT = "[IDI_Sandpit]"
USERCODE = "[IDI_UserCode]"
OUR_SCHEMA = "[DL-MAA2020-01]"

# inputs
ASSEMBLED_TABLE = "[d2gp3_rectangular]"

# outputs
TIDY_TABLE = "[d2gp3_rectangualr_final]"

# controls
DEVELOPMENT_MODE = FALSE
VERBOSE = "details" # {"all", "details", "heading", "none"}

TRAIN_YEARS = c(2016,2017,2018,2020)

## setup ---------------------------------------------------------------------------------------------------

setwd(ABSOLUTE_PATH_TO_TOOL)
source("utility_functions.R")
source("dbplyr_helper_functions.R")
source("table_consistency_checks.R")
source("overview_dataset.R")
source("summary_confidential.R")
setwd(ABSOLUTE_PATH_TO_ANALYSIS)

library(tidyr)

## access dataset ------------------------------------------------------------------------------------------

run_time_inform_user("GRAND START", context = "heading", print_level = VERBOSE)

db_con = create_database_connection(database = "IDI_Sandpit")

working_table = create_access_point(db_con, SANDPIT, OUR_SCHEMA, ASSEMBLED_TABLE)

## prediction modelling ------------------------------------------------------------------------------------

if(DEVELOPMENT_MODE){
  working_table = working_table %>% 
    filter(identity_column %% 100 == 0)
}

## Data Preparation ------------------------------------------------------------------------------------
run_time_inform_user("data preparation", context = "details", print_level = VERBOSE)

train_table <- working_table %>%
  # filter(year(year_date) %in% TRAIN_YEARS)
  # pivot_table("year_date","eoy_balance")
  select(ird_pr19_balance_student_2016,
         ird_pr19_balance_student_2017,
         ird_pr19_balance_student_2018,
         ird_ps18_balance_student_2020) %>%
  collect()

# train_table <- data.frame(train_table)

train_table <- train_table %>%
  # Spread
  # spread(year_date,eoy_balance) %>%
  
  # Renaming the column names
  rename(bal_16 = ird_pr19_balance_student_2016, 
         bal_17 = ird_pr19_balance_student_2017, 
         bal_18 = ird_pr19_balance_student_2018,
         bal_20 = ird_ps18_balance_student_2020) %>%

  # Converting NA's to 0
  mutate(bal_16 = coalesce(bal_16,0),
         bal_17 = coalesce(bal_17,0),
         bal_18 = coalesce(bal_18,0),
         bal_20 = coalesce(bal_20,0)) %>%

  # Creating the output column based on the balance
  mutate(bal_16 = ifelse(bal_16 > 10, "debt", "nodebt"),
         bal_17 = ifelse(bal_17 > 10, "debt", "nodebt"),
         bal_18 = ifelse(bal_18 > 10, "debt", "nodebt"),
         bal_20 = ifelse(bal_20 > 10, "debt", "nodebt")) %>%

  # mutate(bal_17 = ifelse(bal_17 > 10, "debt", "nodebt")) %>%

  # Converting the output column into factors for prediction
  mutate(bal_16 = as.factor(bal_16),
         bal_17 = as.factor(bal_17),
         bal_18 = as.factor(bal_18),
         bal_20 = as.factor(bal_20))

  # mutate(bal_17 = as.factor(bal_17))

train_data <- train_table %>%
  
  # Train the model to use 2016 and 2018 and predict 2017
  select(bal_16,bal_17,bal_18) %>%
  
  # Renaming the column names
  rename(prev = bal_16, 
         curr = bal_17, 
         post = bal_18)

test_data <- train_table %>%
  
  # Train the model to use 2016 and 2018 and predict 2017
  select(bal_18,bal_20) %>%
  
  # Renaming the column names
  rename(prev = bal_18, 
         post = bal_20)

## Training the model ------------------------------------------------------------------------------------------
run_time_inform_user("test and train split", context = "details", print_level = VERBOSE)

# Test - train Split (Train:Test = 80:20)
smp_size <- floor(0.80 * nrow(train_data))
set.seed(123)
train_ind <- sample(seq_len(nrow(train_data)), size = smp_size)

train <- train_data[train_ind, ]
test <- train_data[-train_ind, ]

run_time_inform_user("fitting the model", context = "heading", print_level = VERBOSE)

# Fit the model
model <- glm(curr ~ prev + post, data = train, family = binomial)

run_time_inform_user("model summary", context = "details", print_level = VERBOSE)

# Summarize the model
summary(model)

run_time_inform_user("testing the fitted model", context = "details", print_level = VERBOSE)

# Prediction testing
probabilities <- model %>%
  predict(test, type = "response")

predicted.classes <- ifelse(probabilities < 0.5, "debt", "nodebt")

run_time_inform_user("model accuracy assessment", context = "details", print_level = VERBOSE)

# Model accuracy
table(predicted.classes,test$curr)
mean(predicted.classes == test$curr)

# close connection -------------------------------------------------------------------------------------------
close_database_connection(db_con)
run_time_inform_user("grand completion", context = "heading", print_level = VERBOSE)