#libraries

library(tidyverse)

#Load data

load(file = "/path")

#Outcomes per groups 

map_dfr(.x = data_models,
        ~ .x %>%
         group_by(group) %>%
         summarise(across(3, 
                          ~ cur_column(),
                          .names = "exposure"),
                   outcome_n_prop = paste(formatC(sum(mortality == 1), big.mark = " "),
                                          paste0("(",
                                                 formatC(round(sum(mortality == 1)/n() * 100, 2), format = "f", digits = 2),
                                                 ")"))) %>%
         ungroup() %>%
         mutate(exposure = str_to_sentence(gsub("_", " ", sub("_first_comorbid_hosp_end_date_year", "", exposure))),
                exposure = case_when(exposure == "Hiv or aids" ~ "HIV or AIDS",
                                     exposure == "Parkinsons disease" ~ "Parkinson's disease",
                                     TRUE ~ exposure))) %>%
 pivot_wider(names_from = "group",
             values_from = "outcome_n_prop") %>%
 arrange(exposure)  %>%
 write.csv(file = "/path",
           row.names = FALSE)
