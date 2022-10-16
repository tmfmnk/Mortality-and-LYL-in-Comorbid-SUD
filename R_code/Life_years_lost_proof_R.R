#Libraries

library(data.table)
library(tidyverse)
library(lillies)

#Load data

load(file = "/path/Data_models_recursive_revisions.RData")

#Import mortality tables for year 2008 (the middle year between 1999 and 2017)

mortality_table_males <- fread(file = "/path/LifeTables_Males_2008_CumSurv.csv",
                               select = c("age", 
                                          "mortality_rates",
                                          "survival"),
                               dec = ",")

mortality_table_females <- fread(file = "/path/LifeTables_Females_2008_CumSurv.csv",
                                 select = c("age", 
                                            "mortality_rates",
                                            "survival"),
                                 dec = ",")

#Prepare the data for the procedure

data_lyl_males_SUD <- map(.x = data_models,
                          ~ .x %>%
                             filter(sex == 1 & group == "exposed") %>%
                             select(mortality,
                                    age_death_or_censoring,
                                    age_first_hosp_comorbid_condition = ends_with("_first_comorbid_hosp_age")) %>%
                             mutate(across(everything(), as.numeric)) %>%
                             as.data.frame())

data_lyl_males_no_SUD <- map(.x = data_models,
                             ~ .x %>%
                                filter(sex == 1 & group == "unexposed") %>%
                                select(mortality,
                                       age_death_or_censoring,
                                       age_first_hosp_comorbid_condition = ends_with("_first_comorbid_hosp_age")) %>%
                                mutate(across(everything(), as.numeric)) %>%
                                as.data.frame())

data_lyl_females_SUD <- map(.x = data_models,
                            ~ .x %>%
                               filter(sex == 2 & group == "exposed") %>%
                               select(mortality,
                                      age_death_or_censoring,
                                      age_first_hosp_comorbid_condition = ends_with("_first_comorbid_hosp_age")) %>%
                               mutate(across(everything(), as.numeric)) %>%
                               as.data.frame())

data_lyl_females_no_SUD <- map(.x = data_models,
                               ~ .x %>%
                                  filter(sex == 2 & group == "unexposed") %>%
                                  select(mortality,
                                         age_death_or_censoring,
                                         age_first_hosp_comorbid_condition = ends_with("_first_comorbid_hosp_age")) %>%
                                  mutate(across(everything(), as.numeric)) %>%
                                  as.data.frame())

#Estimate LYL 
#Disease onset at age 30
#Indices of datasets with at least 10 at-risk individuals

ind_males_30_SUD <- which(map_lgl(.x = data_lyl_males_SUD,
                                  ~ .x %>%
                                     summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 30) >= 10) %>%
                                     pull(n_at_risk)))

lyl_males_30_SUD <- map2_dfr(.x = data_lyl_males_SUD[ind_males_30_SUD],
                             .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_males_30_SUD],
                             ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                       t0 = age_first_hosp_comorbid_condition,
                                                       t = age_death_or_censoring,
                                                       status = mortality,
                                                       age_specific = 30,
                                                       tau = 81),
                                                   niter = 10000),
                                            data_ref = mortality_table_males, 
                                            age = age,
                                            surv = survival) %>%
                                as.data.frame() %>%
                                mutate(group = "exposed",
                                       onset_age = "30",
                                       exposure = .y))

#Indices of datasets with at least 10 at-risk individuals

ind_males_30_no_SUD <- which(map_lgl(.x = data_lyl_males_no_SUD,
                                     ~ .x %>%
                                        summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 30) >= 10) %>%
                                        pull(n_at_risk)))

lyl_males_30_no_SUD <- map2_dfr(.x = data_lyl_males_no_SUD[ind_males_30_no_SUD],
                                .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_males_30_no_SUD],
                                ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                          t0 = age_first_hosp_comorbid_condition,
                                                          t = age_death_or_censoring,
                                                          status = mortality,
                                                          age_specific = 30,
                                                          tau = 81),
                                                      niter = 10000),
                                               data_ref = mortality_table_males, 
                                               age = age,
                                               surv = survival) %>%
                                   as.data.frame() %>%
                                   mutate(group = "unexposed",
                                          onset_age = "30",
                                          exposure = .y))

#Indices of datasets with at least 10 at-risk individuals

ind_females_30_SUD <- which(map_lgl(.x = data_lyl_females_SUD,
                                    ~ .x %>%
                                       summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 30) >= 10) %>%
                                       pull(n_at_risk)))

lyl_females_30_SUD <- map2_dfr(.x = data_lyl_females_SUD[ind_females_30_SUD],
                               .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_females_30_SUD],
                               ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                         t0 = age_first_hosp_comorbid_condition,
                                                         t = age_death_or_censoring,
                                                         status = mortality,
                                                         age_specific = 30,
                                                         tau = 81),
                                                     niter = 10000),
                                              data_ref = mortality_table_females, 
                                              age = age,
                                              surv = survival) %>%
                                  as.data.frame() %>%
                                  mutate(group = "exposed",
                                         onset_age = "30",
                                         exposure = .y))

#Indices of datasets with at least 10 at-risk individuals

ind_females_30_no_SUD <- which(map_lgl(.x = data_lyl_females_no_SUD,
                                       ~ .x %>%
                                          summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 30) >= 10) %>%
                                          pull(n_at_risk)))

lyl_females_30_no_SUD <- map2_dfr(.x = data_lyl_females_no_SUD[ind_females_30_no_SUD],
                                  .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_females_30_no_SUD],
                                  ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                            t0 = age_first_hosp_comorbid_condition,
                                                            t = age_death_or_censoring,
                                                            status = mortality,
                                                            age_specific = 30,
                                                            tau = 81),
                                                        niter = 10000),
                                                 data_ref = mortality_table_females, 
                                                 age = age,
                                                 surv = survival) %>%
                                     as.data.frame() %>%
                                     mutate(group = "unexposed",
                                            onset_age = "30",
                                            exposure = .y))

#Disease onset at age 45
#Indices of datasets with at least 10 at-risk individuals

ind_males_45_SUD <- which(map_lgl(.x = data_lyl_males_SUD,
                                  ~ .x %>%
                                     summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 45) >= 10) %>%
                                     pull(n_at_risk)))

lyl_males_45_SUD <- map2_dfr(.x = data_lyl_males_SUD[ind_males_45_SUD],
                             .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_males_45_SUD],
                             ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                       t0 = age_first_hosp_comorbid_condition,
                                                       t = age_death_or_censoring,
                                                       status = mortality,
                                                       age_specific = 45,
                                                       tau = 81),
                                                   niter = 10000),
                                            data_ref = mortality_table_males, 
                                            age = age,
                                            surv = survival) %>%
                                as.data.frame() %>%
                                mutate(group = "exposed",
                                       onset_age = "45",
                                       exposure = .y))

#Indices of datasets with at least 10 at-risk individuals

ind_males_45_no_SUD <- which(map_lgl(.x = data_lyl_males_no_SUD,
                                     ~ .x %>%
                                        summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 45) >= 10) %>%
                                        pull(n_at_risk)))

lyl_males_45_no_SUD <- map2_dfr(.x = data_lyl_males_no_SUD[ind_males_45_no_SUD],
                                .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_males_45_no_SUD],
                                ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                          t0 = age_first_hosp_comorbid_condition,
                                                          t = age_death_or_censoring,
                                                          status = mortality,
                                                          age_specific = 45,
                                                          tau = 81),
                                                      niter = 10000),
                                               data_ref = mortality_table_males, 
                                               age = age,
                                               surv = survival) %>%
                                   as.data.frame() %>%
                                   mutate(group = "unexposed",
                                          onset_age = "45",
                                          exposure = .y))

#Indices of datasets with at least 10 at-risk individuals

ind_females_45_SUD <- which(map_lgl(.x = data_lyl_females_SUD,
                                    ~ .x %>%
                                       summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 45) >= 10) %>%
                                       pull(n_at_risk)))

lyl_females_45_SUD <- map2_dfr(.x = data_lyl_females_SUD[ind_females_45_SUD],
                               .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_females_45_SUD],
                               ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                         t0 = age_first_hosp_comorbid_condition,
                                                         t = age_death_or_censoring,
                                                         status = mortality,
                                                         age_specific = 45,
                                                         tau = 81),
                                                     niter = 10000),
                                              data_ref = mortality_table_females, 
                                              age = age,
                                              surv = survival) %>%
                                  as.data.frame() %>%
                                  mutate(group = "exposed",
                                         onset_age = "45",
                                         exposure = .y))

#Indices of datasets with at least 10 at-risk individuals

ind_females_45_no_SUD <- which(map_lgl(.x = data_lyl_females_no_SUD,
                                       ~ .x %>%
                                          summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 45) >= 10) %>%
                                          pull(n_at_risk)))

lyl_females_45_no_SUD <- map2_dfr(.x = data_lyl_females_no_SUD[ind_females_45_no_SUD],
                                  .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_females_45_no_SUD],
                                  ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                            t0 = age_first_hosp_comorbid_condition,
                                                            t = age_death_or_censoring,
                                                            status = mortality,
                                                            age_specific = 45,
                                                            tau = 81),
                                                        niter = 10000),
                                                 data_ref = mortality_table_females, 
                                                 age = age,
                                                 surv = survival) %>%
                                     as.data.frame() %>%
                                     mutate(group = "unexposed",
                                            onset_age = "45",
                                            exposure = .y))

#Disease onset at age 60
#Indices of datasets with at least 10 at-risk individuals

ind_males_60_SUD <- which(map_lgl(.x = data_lyl_males_SUD,
                                  ~ .x %>%
                                     summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 60) >= 10) %>%
                                     pull(n_at_risk)))

lyl_males_60_SUD <- map2_dfr(.x = data_lyl_males_SUD[ind_males_60_SUD],
                             .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_males_60_SUD],
                             ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                       t0 = age_first_hosp_comorbid_condition,
                                                       t = age_death_or_censoring,
                                                       status = mortality,
                                                       age_specific = 60,
                                                       tau = 81),
                                                   niter = 10000),
                                            data_ref = mortality_table_males, 
                                            age = age,
                                            surv = survival) %>%
                                as.data.frame() %>%
                                mutate(group = "exposed",
                                       onset_age = "60",
                                       exposure = .y))

#Indices of datasets with at least 10 at-risk individuals

ind_males_60_no_SUD <- which(map_lgl(.x = data_lyl_males_no_SUD,
                                     ~ .x %>%
                                        summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 60) >= 10) %>%
                                        pull(n_at_risk)))

lyl_males_60_no_SUD <- map2_dfr(.x = data_lyl_males_no_SUD[ind_males_60_no_SUD],
                                .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_males_60_no_SUD],
                                ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                          t0 = age_first_hosp_comorbid_condition,
                                                          t = age_death_or_censoring,
                                                          status = mortality,
                                                          age_specific = 60,
                                                          tau = 81),
                                                      niter = 10000),
                                               data_ref = mortality_table_males, 
                                               age = age,
                                               surv = survival) %>%
                                   as.data.frame() %>%
                                   mutate(group = "unexposed",
                                          onset_age = "60",
                                          exposure = .y))

#Indices of datasets with at least 10 at-risk individuals

ind_females_60_SUD <- which(map_lgl(.x = data_lyl_females_SUD,
                                    ~ .x %>%
                                       summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 60) >= 10) %>%
                                       pull(n_at_risk)))

lyl_females_60_SUD <- map2_dfr(.x = data_lyl_females_SUD[ind_females_60_SUD],
                               .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_females_60_SUD],
                               ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                         t0 = age_first_hosp_comorbid_condition,
                                                         t = age_death_or_censoring,
                                                         status = mortality,
                                                         age_specific = 60,
                                                         tau = 81),
                                                     niter = 10000),
                                              data_ref = mortality_table_females, 
                                              age = age,
                                              surv = survival) %>%
                                  as.data.frame() %>%
                                  mutate(group = "exposed",
                                         onset_age = "60",
                                         exposure = .y))

#Indices of datasets with at least 10 at-risk individuals

ind_females_no_60 <- which(map_lgl(.x = data_lyl_females_no_SUD,
                                   ~ .x %>%
                                      summarise(n_at_risk = sum(age_first_hosp_comorbid_condition >= 60) >= 10) %>%
                                      pull(n_at_risk)))

lyl_females_60_no_SUD <- map2_dfr(.x = data_lyl_females_no_SUD[ind_females_no_60],
                                  .y = map(.x = data_models, ~ sub("_first_comorbid_hosp_end_date_year", "", names(select(.x, ends_with("_first_comorbid_hosp_end_date_year")))))[ind_females_no_60],
                                  ~ lyl_diff_ref(lyl_ci(lyl(data = .x,
                                                            t0 = age_first_hosp_comorbid_condition,
                                                            t = age_death_or_censoring,
                                                            status = mortality,
                                                            age_specific = 60,
                                                            tau = 81),
                                                        niter = 10000),
                                                 data_ref = mortality_table_females, 
                                                 age = age,
                                                 surv = survival) %>%
                                     as.data.frame() %>%
                                     mutate(group = "unexposed",
                                            onset_age = "60",
                                            exposure = .y))

#Combine results 
#Males

lyl_males <- mget(ls(pattern = "^lyl_males"), envir = .GlobalEnv) %>%
   bind_rows() %>%
   arrange(exposure, onset_age, group) %>%
   transmute(lyl_estimate_with_ci = paste(formatC(round(lyl_estimate.TotalLYL, 2), format = "f", digits = 2),
                                          paste0("(", 
                                                 formatC(round(lyl_ci_left.TotalLYL, 2), format = "f", digits = 2),
                                                 "; ",
                                                 formatC(round(lyl_ci_right.TotalLYL, 2), format = "f", digits = 2),
                                                 ")")),
             exposure,
             group,
             onset_age,
             sex = "males") %>%
   pivot_wider(names_from = c(onset_age, sex, group),
               names_glue = "{group}_{onset_age}_{sex}_{.value}",
               values_from = lyl_estimate_with_ci) 

#Females

lyl_females <- mget(ls(pattern = "^lyl_females"), envir = .GlobalEnv) %>%
   bind_rows() %>%
   arrange(exposure, onset_age, group) %>%
   transmute(lyl_estimate_with_ci = paste(formatC(round(lyl_estimate.TotalLYL, 2), format = "f", digits = 2),
                                          paste0("(", 
                                                 formatC(round(lyl_ci_left.TotalLYL, 2), format = "f", digits = 2),
                                                 "; ",
                                                 formatC(round(lyl_ci_right.TotalLYL, 2), format = "f", digits = 2),
                                                 ")")),
             exposure,
             group,
             onset_age,
             sex = "females") %>%
   pivot_wider(names_from = c(onset_age, sex, group),
               names_glue = "{group}_{onset_age}_{sex}_{.value}",
               values_from = lyl_estimate_with_ci) 

#Join and save tables

lyl_males %>%
   left_join(lyl_females,
             by = c("exposure" = "exposure")) %>%
   mutate(exposure = str_to_sentence(gsub("_", " ", sub("_binary", "", exposure))),
          exposure = case_when(exposure == "Parkinsons disease" ~ "Parkinson's disease",
                               TRUE ~ exposure)) %>%
   write.csv(file = "/path/LYL_both_sexes_revisions.csv",
             row.names = FALSE)

lyl_females_60_SUD %>%
 select(exposure, lyl_estimate.TotalLYL, lyl_ci_left.TotalLYL, lyl_ci_right.TotalLYL) %>%
 arrange(lyl_estimate.TotalLYL)
