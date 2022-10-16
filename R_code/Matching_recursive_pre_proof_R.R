#Libraries

library(data.table)
library(tidyverse)
library(purrr)
library(lubridate)

#Function for recursive random sampling

recursive_sample <- function(data, n) {
  
  groups <- unique(data[["exposed_ID"]])
  out <- data.frame(exposed_ID = character(), unexposed_ID = character())
  
  for (group in groups) {
    
    chosen <- data %>%
      filter(exposed_ID == group,
             !unexposed_ID %in% out$unexposed_ID) %>%
      sample_n(size = min(n, nrow(.))) 
    
    out <- rbind(out, chosen)
    
  }
  
  out
  
}

#Import index hospitalization with info on comorbidity

index_hospitalization_and_comorbidity <- fread(file = "/path/Index_hospitalization_and_comorbidity_revisions.csv",
                                               header = TRUE,
                                               sep = ",",
                                               dec = ".",
                                               fill = TRUE,
                                               encoding = "Latin-1",
                                               nThread = 8)

#Import data on mortality

deaths_1994_2013 <- fread(file = "/path/zem_1994_2013.csv")
deaths_2014 <- fread(file = "/path/zem_2014.csv")
deaths_2015 <- fread(file = "/path/zem_2015.csv")
deaths_2016 <- fread(file = "/path/zem_2016.csv")
deaths_2017 <- fread(file = "/path/zem_2017.csv")

#Unifying the format of mortality data

deaths_1994_2017 <- deaths_1994_2013 %>%
 transmute(RODCIS2 = RC,
           DAUMR = dmy(DAUMR),
           cause_of_death = trimws(DGP),
           external_cause_of_death = trimws(DGE),
           age_death = as.numeric(trimws(VEKZE))) %>%
 bind_rows(deaths_2014 %>%
            transmute(RODCIS2 = RC,
                      DAUMR = dmy(DAUMR),
                      cause_of_death = trimws(DGP),
                      external_cause_of_death = trimws(DGE),
                      age_death = VEKZE),
           deaths_2015 %>%
            transmute(RODCIS2 = RODCIS2,
                      DAUMR = ymd(DAUMR),
                      cause_of_death = trimws(DGP),
                      external_cause_of_death = trimws(DGE),
                      age_death = VEKZE),
           deaths_2016 %>%
            transmute(RODCIS2 = RCZEMAN2,
                      DAUMR = ymd(paste0(UMROK, str_pad(UMRMM, 2, pad = "0"), UMRDD)),
                      cause_of_death = trimws(DGUMR),
                      external_cause_of_death = trimws(DGUMR2),
                      age_death = VEK_ZEM),
           deaths_2017 %>%
            transmute(RODCIS2 = RCZEMAN2,
                      DAUMR = ymd(paste0(UMROK, str_pad(UMRMM, 2, pad = "0"), UMRDD)),
                      cause_of_death = trimws(DGUMR),
                      external_cause_of_death = trimws(DGUMR2),
                      age_death = VEK_ZEM))

rm(deaths_1994_2013)
rm(deaths_2014)
rm(deaths_2015)
rm(deaths_2016)
rm(deaths_2017)

#Recoding work status on the first occurrence of a subsequent health condition
#Binary coding (unemployed or a child vs employed)

index_hospitalization_and_comorbidity <- index_hospitalization_and_comorbidity %>%
 mutate(across(ends_with("_first_comorbid_hosp_work_status"), ~ as.numeric(. != 0))) 
        
#Matching on sex, age (+- 3 years) and work status at the first occurrence of subsequent health condition and year of 
#first occurrence of subsequent health condition 
#Ordering the datasets by the number of matched unexposed individuals per exposed individuals
#to ensure that individuals with less matches have a larger likelihood of obtaining enough matches (at least one) in the recursive sampling

all_matched_pairs <- map(names(select(index_hospitalization_and_comorbidity, ends_with("_binary"))),
                         function(x)
                           map_dfr(index_hospitalization_and_comorbidity %>%
                                     filter(group == "exposed") %>%
                                     group_split(split_ID = frank(RODCIS2, ties.method = "dense") %/% 10000),
                                   function(y)
                                     y %>%
                                     filter(across(all_of(x)) == TRUE & get(sub("_binary", "_historic", x)) == FALSE) %>%
                                     mutate(!!sub("_binary", "_first_comorbid_hosp_work_status", x) := get(sub("_binary", "_first_comorbid_hosp_work_status", x)),
                                            !!sub("_binary", "_first_comorbid_hosp_end_date_year", x) := year(get(sub("_binary", "_first_comorbid_hosp_end_date", x))),
                                            !!sub("_binary", "_first_comorbid_hosp_age_exposed", x) := get(sub("_binary", "_first_comorbid_hosp_age", x))) %>%
                                     select(exposed_ID = RODCIS2,
                                            sex,
                                            all_of(sub("_binary", "_first_comorbid_hosp_work_status", x)),
                                            all_of(sub("_binary", "_first_comorbid_hosp_end_date_year", x)),
                                            all_of(sub("_binary", "_first_comorbid_hosp_age_exposed", x))) %>%
                                     inner_join(index_hospitalization_and_comorbidity %>%
                                                  filter(group == "unexposed") %>%
                                                  filter(across(all_of(x)) == TRUE & get(sub("_binary", "_historic", x)) == FALSE) %>%
                                                  mutate(!!sub("_binary", "_first_comorbid_hosp_work_status", x) := get(sub("_binary", "_first_comorbid_hosp_work_status", x)),
                                                         !!sub("_binary", "_first_comorbid_hosp_end_date_year", x) := year(get(sub("_binary", "_first_comorbid_hosp_end_date", x))),
                                                         !!sub("_binary", "_first_comorbid_hosp_age_unexposed", x) := get(sub("_binary", "_first_comorbid_hosp_age", x))) %>%
                                                  select(unexposed_ID = RODCIS2,
                                                         sex,
                                                         all_of(sub("_binary", "_first_comorbid_hosp_work_status", x)),
                                                         all_of(sub("_binary", "_first_comorbid_hosp_end_date", x)),
                                                         all_of(sub("_binary", "_first_comorbid_hosp_end_date_year", x)),
                                                         all_of(sub("_binary", "_first_comorbid_hosp_age_unexposed", x)))) %>%
                                     filter(data.table::between(get(sub("_binary", "_first_comorbid_hosp_age_unexposed", x)), 
                                                                get(sub("_binary", "_first_comorbid_hosp_age_exposed", x)) - 3, 
                                                                get(sub("_binary", "_first_comorbid_hosp_age_exposed", x)) + 3))) %>%
                           rename_with(~ sub("_unexposed", "", .), all_of(sub("_binary", "_first_comorbid_hosp_age_unexposed", x))) %>%
                           select(-ends_with("_first_comorbid_hosp_age_exposed")) %>%
                           add_count(exposed_ID) %>%
                           arrange(n, exposed_ID) %>%
                           select(-n))

#Randomly choosing up to 3 unexposed individuals for each exposed individual

set.seed(123)
all_matched_sampled_pairs <- map(.x = all_matched_pairs,
                                 ~ recursive_sample(.x, 3) %>%
                                   mutate(group = "unexposed"))

#Combine with exposed individuals

all_matched_sampled_pairs_combined <- map2(.x = all_matched_sampled_pairs,
                                           .y = names(select(index_hospitalization_and_comorbidity, ends_with("_binary"))),
                                           ~ .x %>%
                                             rename(RODCIS2 = unexposed_ID) %>%
                                             bind_rows(index_hospitalization_and_comorbidity %>%
                                                         filter(RODCIS2 %in% .x$exposed_ID) %>%
                                                         filter(across(all_of(.y)) == TRUE & get(sub("_binary", "_historic", .y)) == FALSE) %>%
                                                         transmute(exposed_ID = RODCIS2,
                                                                   RODCIS2,
                                                                   sex,
                                                                   !!sub("_binary", "_first_comorbid_hosp_work_status", .y) := get(sub("_binary", "_first_comorbid_hosp_work_status", .y)),
                                                                   !!sub("_binary", "_first_comorbid_hosp_age", .y) := get(sub("_binary", "_first_comorbid_hosp_age", .y)),
                                                                   !!sub("_binary", "_first_comorbid_hosp_end_date", .y) := get(sub("_binary", "_first_comorbid_hosp_end_date", .y)),
                                                                   !!sub("_binary", "_first_comorbid_hosp_end_date_year", .y) := year(get(sub("_binary", "_first_comorbid_hosp_end_date", .y))),
                                                                   group)))
            
#Merge with data on mortality
#Adding a constant of one day to age at death to avoid t = 0

data_models <- map2(.x = all_matched_sampled_pairs_combined,
                    .y = names(select(index_hospitalization_and_comorbidity, ends_with("_binary"))),
                    ~ .x %>%
                     left_join(deaths_1994_2017,
                               by = c("RODCIS2" = "RODCIS2")) %>%
                     mutate(mortality = as.numeric(int_overlaps(interval(get(sub("_binary", "_first_comorbid_hosp_end_date", .y)), ymd("2017-12-31")),
                                                                interval(DAUMR, DAUMR))),
                            mortality = replace(mortality, is.na(mortality), 0),
                            years_diff_mortality_followup = as.duration(get(sub("_binary", "_first_comorbid_hosp_end_date", .y)) %--% ymd("2017-12-31"))/dyears(1),
                            years_until_death = as.duration(get(sub("_binary", "_first_comorbid_hosp_end_date", .y)) %--% DAUMR)/dyears(1),
                            years_until_death_or_censoring = ifelse(mortality == 1, years_until_death, years_diff_mortality_followup),
                            age_death_or_censoring = ifelse(mortality == 0, 
                                                            get(sub("_binary", "_first_comorbid_hosp_age", .y)) + years_diff_mortality_followup + 1/365.25,
                                                            get(sub("_binary", "_first_comorbid_hosp_age", .y)) + years_until_death + 1/365.25)) %>%
                     mutate(group = factor(group, levels = c("unexposed", "exposed"))))

#Save data

save(data_models, 
     file = "/path/Data_models_recursive_revisions.RData")

#Table with counts of matched individuals and individuals matching with <3 unexposed individuals

map_dfr(.x = names(select(index_hospitalization_and_comorbidity, ends_with("_binary"))),
        ~ index_hospitalization_and_comorbidity %>%
          filter(group == "exposed") %>%
          filter(across(all_of(.x)) == TRUE & get(sub("_binary", "_historic", .x)) == FALSE) %>%
          count() %>%
          mutate(exposure = .x)) %>%
  bind_cols(map_dfr(.x = all_matched_sampled_pairs, 
                    ~ .x %>%
                      summarise(n_matched = n_distinct(exposed_ID)))) %>%
  transmute(exposure = str_to_sentence(gsub("_", " ", sub("_binary", "", exposure))),
            exposure = case_when(exposure == "Parkinsons disease" ~ "Parkinson's disease",
                                 TRUE ~ exposure),
            n_prop_matched = paste(formatC(n_matched, big.mark = " "),
                                   paste0("(",
                                          formatC(round(n_matched/n * 100, 2), format = "f", digits = 2),
                                          ")"))) %>%
  bind_cols(map_dfr(all_matched_sampled_pairs,
                    ~ .x %>%
                      count(exposed_ID) %>%
                      summarise(n_prop_matched_incomplete = paste(formatC(sum(n != 3), big.mark = " "),
                                                                  paste0("(",
                                                                         formatC(round(sum(n != 3)/n() * 100, 2), format = "f", digits = 2),
                                                                         ")"))))) %>%
  arrange(exposure) %>%
  write.csv(file = "/path/Matching_procedure_counts_revisions.csv",
           row.names = FALSE)

#Table with the distribution on matching variables per groups and per exposures

distr_exposed <- map_dfr(.x = data_models,
                         ~ .x %>%
                          filter(group == "exposed") %>%
                          summarise(across(ends_with("_first_comorbid_hosp_age"),
                                           ~ sub("_first_comorbid_hosp_age", "", cur_column()),
                                           .names = "cohort"),
                                    overall_n = formatC(n(), big.mark = " "),
                                    males = paste(formatC(sum(sex == 1), big.mark = " "),
                                                  paste0("(",
                                                         formatC(round(sum(sex == 1)/n() * 100, 2), format = "f", digits = 2),
                                                         ")")),
                                    females = paste(formatC(sum(sex == 2), big.mark = " "),
                                                    paste0("(",
                                                           formatC(round(sum(sex == 2)/n() * 100, 2), format = "f", digits = 2),
                                                           ")")),
                                    across(ends_with("_first_comorbid_hosp_work_status"),
                                           ~ paste(formatC(sum(. == 1), big.mark = " "),
                                                   paste0("(",
                                                          formatC(round(sum(. == 1)/n() * 100, 2), format = "f", digits = 2),
                                                          ")")),
                                           .names = "work"),
                                    across(ends_with("_first_comorbid_hosp_work_status"),
                                           ~ paste(formatC(sum(. == 0), big.mark = " "),
                                                   paste0("(",
                                                          formatC(round(sum(. == 0)/n() * 100, 2), format = "f", digits = 2),
                                                          ")")),
                                           .names = "not_employed"),
                                    across(ends_with("_first_comorbid_hosp_age"),
                                           ~ paste(formatC(round(mean(.), 2), format = "f", digits = 2),
                                                   paste0("(",
                                                          formatC(round(sd(.), 2), format = "f", digits = 2),
                                                          ")")),
                                           .names = "age"),
                                    across(ends_with("_first_comorbid_hosp_end_date_year"),
                                           ~ paste(median(.),
                                                   paste0("(",
                                                          paste0(quantile(., 0.25, na.rm = TRUE),
                                                                 "-",
                                                                 quantile(., 0.75, na.rm = TRUE)),
                                                          ")")),
                                           .names = "year")) %>%
                          mutate(cohort = str_to_sentence(gsub("_", " ", cohort)),
                                 cohort = case_when(cohort == "Parkinsons disease" ~ "Parkinson's disease",
                                                    TRUE ~ cohort)) %>%
                          rename_with(~ paste0(., "_exposed"), -1))

distr_unexposed <- map_dfr(.x = data_models,
                           ~ .x %>%
                            filter(group == "unexposed") %>%
                            summarise(across(ends_with("_first_comorbid_hosp_age"),
                                             ~ sub("_first_comorbid_hosp_age", "", cur_column()),
                                             .names = "cohort"),
                                      overall_n = formatC(n(), big.mark = " "),
                                      males = paste(formatC(sum(sex == 1), big.mark = " "),
                                                    paste0("(",
                                                           formatC(round(sum(sex == 1)/n() * 100, 2), format = "f", digits = 2),
                                                           ")")),
                                      females = paste(formatC(sum(sex == 2), big.mark = " "),
                                                      paste0("(",
                                                             formatC(round(sum(sex == 2)/n() * 100, 2), format = "f", digits = 2),
                                                             ")")),
                                      across(ends_with("_first_comorbid_hosp_work_status"),
                                             ~ paste(formatC(sum(. == 1), big.mark = " "),
                                                     paste0("(",
                                                            formatC(round(sum(. == 1)/n() * 100, 2), format = "f", digits = 2),
                                                            ")")),
                                             .names = "work"),
                                      across(ends_with("_first_comorbid_hosp_work_status"),
                                             ~ paste(formatC(sum(. == 0), big.mark = " "),
                                                     paste0("(",
                                                            formatC(round(sum(. == 0)/n() * 100, 2), format = "f", digits = 2),
                                                            ")")),
                                             .names = "not_employed"),
                                      across(ends_with("_first_comorbid_hosp_age"),
                                             ~ paste(formatC(round(mean(.), 2), format = "f", digits = 2),
                                                     paste0("(",
                                                            formatC(round(sd(.), 2), format = "f", digits = 2),
                                                            ")")),
                                             .names = "age"),
                                      across(ends_with("_first_comorbid_hosp_end_date_year"),
                                             ~ paste(median(.),
                                                     paste0("(",
                                                            paste0(quantile(., 0.25, na.rm = TRUE),
                                                                   "-",
                                                                   quantile(., 0.75, na.rm = TRUE)),
                                                            ")")),
                                             .names = "year")) %>%
                            mutate(cohort = str_to_sentence(gsub("_", " ", cohort)),
                                   cohort = case_when(cohort == "Parkinsons disease" ~ "Parkinson's disease",
                                                      TRUE ~ cohort)) %>%
                            rename_with(~ paste0(., "_unexposed"), -1))

distr_exposed %>%
 arrange(cohort) %>%
 left_join(distr_unexposed,
           by = "cohort") %>%
 write.csv(file = "/path/Matching_procedure_distribution_per_groups_pre_proof.csv",
           row.names = FALSE)

#Median number of individuals per cohorts

map_dfr(.x = data_models, 
        ~ .x %>%
         tally()) %>%
 summarise(median_n = median(n),
           iqr = paste0(quantile(n, 0.25, na.rm = TRUE),
                        "-",
                        quantile(n, 0.75, na.rm = TRUE)),
           range = paste(min(n), max(n), sep = "-"))

