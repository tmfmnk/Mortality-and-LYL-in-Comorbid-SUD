#libraries

library(tidyverse)

#Load data

load(file = "/path/Data_models_recursive_revisions.RData")

#Outcomes per groups 

map_dfr(.x = data_models,
        ~ .x %>%
         group_by(group) %>%
         summarise(across(ends_with("_first_comorbid_hosp_work_status"), 
                          ~ cur_column(),
                          .names = "exposure"),
                   outcome_n_prop = paste(formatC(sum(mortality == 1), big.mark = " "),
                                          paste0("(",
                                                 formatC(round(sum(mortality == 1)/n() * 100, 2), format = "f", digits = 2),
                                                 ")"))) %>%
         ungroup() %>%
         mutate(exposure = str_to_sentence(gsub("_", " ", sub("_first_comorbid_hosp_work_status", "", exposure))),
                exposure = case_when(exposure == "Parkinsons disease" ~ "Parkinson's disease",
                                     TRUE ~ exposure))) %>%
 pivot_wider(names_from = "group",
             values_from = "outcome_n_prop") %>%
 arrange(exposure)  %>%
 write.csv(file = "/path/Outcomes_per_groups_revisions.csv",
           row.names = FALSE)
