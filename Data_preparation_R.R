#Libraries

library(data.table)
library(tidyverse)
library(purrr)
library(lubridate)

#Import all hospitalizations from 1994 to 2012

hospitalizations_1994_2012 <- list.files(path = "/Users/tomasformanek/Documents/Comorbidity_SUD/Data/Hospitalizations_1994_2017", 
                                         full.names = TRUE) %>%
 .[str_detect(., "1[3-7].csv$", negate = TRUE)] %>%
 map_dfr(~ fread(.,
                 select = c("RODCIS2" = "character", 
                            "DATPRI" = "integer", 
                            "DATUKO" = "integer", 
                            "VEK"= "integer", 
                            "POHL" = "integer",
                            "NBYDL" = "character",
                            "STAV" = "integer",
                            "ZAM" = "integer",
                            "ZDG" = "character",
                            "DDG1" = "character",
                            "DDG2" = "character",
                            "DDG3" = "character",
                            "DDG4" = "character",
                            "DDG5" = "character"),
                 header = TRUE,
                 sep = ",",
                 dec = ".",
                 encoding = "Latin-1",
                 nThread = 8))

#Selecting the last hospitalization if multiple hospitalizations on the same day

hospitalizations_1994_2012 <- hospitalizations_1994_2012 %>%
 group_by(RODCIS2, DATPRI) %>%
 slice(which.max(DATUKO)) %>%
 ungroup() 

#Export pre-filtered data

fwrite(hospitalizations_1994_2012,
       file = "/Users/tomasformanek/Documents/Comorbidity_SUD/Data/Hospitalizations_1994_2012_pre_filtered/Hospitalizations_1994_2012_pre_filtered.csv")

#Import pre-filtered data

hospitalizations_1994_2012 <- fread(file = "/Users/tomasformanek/Documents/Comorbidity_SUD/Data/Hospitalizations_1994_2012_pre_filtered/Hospitalizations_1994_2012_pre_filtered.csv",
                                    nThread = 8)

#Import data on mortality

deaths_1994_2013 <- fread(file = "/Users/tomasformanek/Documents/Comorbidity_SUD/Data/Deaths_1994_2017/zem_1994_2013.csv")
deaths_2014 <- fread(file = "/Users/tomasformanek/Documents/Comorbidity_SUD/Data/Deaths_1994_2017/zem_2014.csv")
deaths_2015 <- fread(file = "/Users/tomasformanek/Documents/Comorbidity_SUD/Data/Deaths_1994_2017/zem_2015.csv")
deaths_2016 <- fread(file = "/Users/tomasformanek/Documents/Comorbidity_SUD/Data/Deaths_1994_2017/zem_2016.csv")
deaths_2017 <- fread(file = "/Users/tomasformanek/Documents/Comorbidity_SUD/Data/Deaths_1994_2017/zem_2017.csv")

#Unifying the format of mortality data

deaths_1994_2017 <- deaths_1994_2013 %>%
 transmute(RODCIS2 = RC,
           DAUMR = dmy(DAUMR),
           cause_of_death = trimws(DGP),
           external_cause_of_death = trimws(DGE)) %>%
 bind_rows(deaths_2014 %>%
            transmute(RODCIS2 = RC,
                      DAUMR = dmy(DAUMR),
                      cause_of_death = trimws(DGP),
                      external_cause_of_death = trimws(DGE)),
           deaths_2015 %>%
            transmute(RODCIS2 = RODCIS2,
                      DAUMR = ymd(DAUMR),
                      cause_of_death = trimws(DGP),
                      external_cause_of_death = trimws(DGE)),
           deaths_2016 %>%
            transmute(RODCIS2 = RCZEMAN2,
                      DAUMR = ymd(paste0(UMROK, str_pad(UMRMM, 2, pad = "0"), UMRDD)),
                      cause_of_death = trimws(DGUMR),
                      external_cause_of_death = trimws(DGUMR2)),
           deaths_2017 %>%
            transmute(RODCIS2 = RCZEMAN2,
                      DAUMR = ymd(paste0(UMROK, str_pad(UMRMM, 2, pad = "0"), UMRDD)),
                      cause_of_death = trimws(DGUMR),
                      external_cause_of_death = trimws(DGUMR2)))

#Excluding individuals with more than one date of death
#Excluding individuals with hospitalizations after death

hospitalizations_1994_2012 <- hospitalizations_1994_2012 %>%
 anti_join(hospitalizations_1994_2012 %>%
            inner_join(deaths_1994_2017, 
                       by = c("RODCIS2" = "RODCIS2")) %>%
            mutate(DATUKO = ymd(DATUKO)) %>%
            group_by(RODCIS2) %>%
            filter(DAUMR < max(DATUKO) | n_distinct(DAUMR) > 1) %>%
            ungroup(),
           by = c("RODCIS2" = "RODCIS2"))

#First hospitalization with SDUs as main diagnosis between 1995 and 2002
#Excluding younger than 15 and older than 70 years
#SDUs as main diagnosis of hospitalization between 1994 and 2002
#No diagnosis except of SUDs on any position
#Acute intoxication as the main diagnosis only if other SUD is present

hospitalizations_SDU_1995_2003 <- hospitalizations_1994_2012 %>%
 filter(year(ymd(DATPRI)) >= 1995 & year(ymd(DATUKO)) <= 2003) %>%
 filter(VEK >= 15 & VEK <= 70) %>%
 mutate(across(ZDG:DDG5, trimws)) %>%
 mutate(across(ZDG:DDG5, ~ replace(., . == "", NA_character_))) %>%
 filter(grepl("^F1", ZDG)) %>%
 filter(reduce(.x = across(DDG1:DDG5, ~ grepl("^F1", .) | is.na(.)), .f = `&`)) %>%
 filter(reduce(.x = across(ZDG:DDG5, ~ !(. %in% paste0("F", seq(100, 190, 10)) | is.na(.))), .f = `|`)) 

#Selecting the first hospitalization in the examined time period
#Excluding individuals with invalid information on region of residence

first_hospitalizations_SDU_1995_2003 <- hospitalizations_SDU_1995_2003 %>%
 group_by(RODCIS2) %>%
 filter(DATPRI == min(DATPRI)) %>%
 ungroup()

#Excluding individuals hospitalized in the previous year

first_hospitalizations_SDU_1995_2003 <- first_hospitalizations_SDU_1995_2003 %>%
 anti_join(first_hospitalizations_SDU_1995_2003 %>%
            inner_join(hospitalizations_1994_2012 %>%
                        select(RODCIS2,
                               DATPRI_non_index = DATPRI,
                               DATUKO_non_index = DATUKO), 
                       by = c("RODCIS2" = "RODCIS2")) %>%
            filter(!(DATPRI == DATPRI_non_index & DATUKO == DATUKO_non_index)) %>%
            mutate(DATPRI_minus_one_year = ymd(DATPRI) %m-% years(1),
                   hosp_last_year = int_overlaps(interval(ymd(DATPRI_non_index), ymd(DATUKO_non_index)),
                                                 interval(DATPRI_minus_one_year, ymd(DATUKO)))) %>%
            group_by(RODCIS2) %>%
            filter(any(hosp_last_year)) %>%
            ungroup() %>%
            select(RODCIS2),
           by = c("RODCIS2" = "RODCIS2")) 

#Matching controls
#Excluding younger than 15 and older than 70 years
#Excluding  individuals with any diagnosis of SUDs
#Excluding individuals with any comorbidity

hospitalizations_no_SDU_1995_2012 <- hospitalizations_1994_2012 %>%
        filter(year(ymd(DATPRI)) >= 1995 & year(ymd(DATUKO)) <= 2003) %>%
        filter(VEK >= 15 & VEK <= 70) %>%
        mutate(across(ZDG:DDG5, trimws)) %>%
        mutate(across(ZDG:DDG5, ~ replace(., . == "", NA_character_))) %>%
        group_by(RODCIS2) %>%
        filter(!grepl("F1", paste(ZDG, DDG1, DDG2, DDG3, DDG4, DDG5, collapse = ""))) %>% 
        ungroup() %>%
        filter(reduce(.x = across(DDG1:DDG5, ~ substr(ZDG, 1, 2) == substr(., 1, 2) | is.na(.)), .f = `&`)) 

#Creating the variables to match on 

to_match_first_hospitalizations_SDU_1995_2003 <- first_hospitalizations_SDU_1995_2003 %>%
 mutate(month_DATPRI = month(ymd(DATPRI)),
        year_DATPRI = year(ymd(DATPRI))) %>%
 select(ID_case = RODCIS2,
        VEK = VEK, 
        POHL = POHL,
        month_DATPRI,
        year_DATPRI)

hospitalizations_no_SDU_1995_2012 <- hospitalizations_no_SDU_1995_2012 %>%
 mutate(month_DATPRI = month(ymd(DATPRI)),
        year_DATPRI = year(ymd(DATPRI)))

#Matching

matched_hospitalizations_no_SDU_1995_2003 <- setDT(to_match_first_hospitalizations_SDU_1995_2003)[setDT(hospitalizations_no_SDU_1995_2012), 
                                                                                                  nomatch = NULL, 
                                                                                                  on = c("POHL" = "POHL", 
                                                                                                         "VEK" = "VEK", 
                                                                                                         "month_DATPRI" = "month_DATPRI",
                                                                                                         "year_DATPRI" = "year_DATPRI"),
                                                                                                  allow.cartesian = TRUE]



set.seed(123)
matched_and_sampled_hospitalizations_no_SDU_1995_2003 <- matched_hospitalizations_no_SDU_1995_2003 %>%
 group_by(ID_case) %>%
 filter(row_number() %in% sample(1:n(), 50)) %>%
 ungroup()

matched_and_sampled_hospitalizations_no_SDU_1995_2003 <- matched_and_sampled_hospitalizations_no_SDU_1995_2003 %>%
 anti_join(matched_and_sampled_hospitalizations_no_SDU_1995_2003 %>%
            inner_join(hospitalizations_1994_2012 %>%
                        select(RODCIS2,
                               DATPRI_non_index = DATPRI,
                               DATUKO_non_index = DATUKO), 
                       by = c("RODCIS2" = "RODCIS2")) %>%
            filter(!(DATPRI == DATPRI_non_index & DATUKO == DATUKO_non_index)) %>%
            mutate(DATPRI_minus_one_year = ymd(DATPRI) %m-% years(1),
                   hosp_last_year = int_overlaps(interval(ymd(DATPRI_non_index), ymd(DATUKO_non_index)),
                                                 interval(DATPRI_minus_one_year, ymd(DATUKO)))) %>%
            group_by(ID_case, RODCIS2) %>%
            filter(any(hosp_last_year)) %>%
            ungroup() %>%
            select(RODCIS2,
                   ID_case),
           by = c("RODCIS2" = "RODCIS2",
                  "ID_case" = "ID_case"))

set.seed(123)
matched_and_sampled_hospitalizations_no_SDU_1995_2003 <- matched_and_sampled_hospitalizations_no_SDU_1995_2003 %>%
 group_by(RODCIS2) %>%
 filter(row_number() %in% sample(1:n(), 1)) %>%
 group_by(ID_case) %>%
 filter(row_number() %in% sample(1:n(), 3)) %>%
 ungroup()

#Match individuals with SUDs as main diagnosis with their non-SUD hospitalizations

comorbid_hospitalizations_1995_2012 <- first_hospitalizations_SDU_1995_2003 %>%
 transmute(RODCIS2,
           primary_diagnosis = ZDG,
           comorbidity_followup_start = ymd(DATUKO),
           comorbidity_followup_end = comorbidity_followup_start %m+% years(9),
           days_diff_comorbidity_followup = as.numeric(comorbidity_followup_end - comorbidity_followup_start)) %>%
 inner_join(hospitalizations_1994_2012,
            by = c("RODCIS2" = "RODCIS2")) %>%
 mutate(comorbid_hospitalization_start = ymd(DATPRI),
        comorbid_hospitalization_end = ymd(DATUKO)) %>%
 filter(int_overlaps(interval(comorbid_hospitalization_start, comorbid_hospitalization_end),
                     interval(comorbidity_followup_start, comorbidity_followup_end))) %>%
 mutate(across(ZDG:DDG5, trimws)) %>%
 mutate(across(ZDG:DDG5, ~ replace(., . == "", NA_character_))) %>%
 mutate(across(ZDG:DDG5, list(not_as_primary_diagnosis = ~ ifelse(substr(primary_diagnosis, 1, 2) != substr(., 1, 2),
                                                                  .,
                                                                  NA_character_)))) %>%
 mutate(psychiatric_comorbidity_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^F", .)), .f = `|`),
        infection_and_parasitic_diseases_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^A|^B", .)), .f = `|`),
        neoplasms_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^C|^D0[0-9]|^D1[0-9]|^D2[0-9]|^D3[0-9]|^D4[0-9]", .)), .f = `|`),
        blood_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^D5[0-9]|^D6[0-9]|^D7[0-9]|^D8[0-9]", .)), .f = `|`),
        endocrine_diseases_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^E", .)), .f = `|`),
        nervous_system_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^G", .)), .f = `|`),
        eye_and_ear_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^H", .)), .f = `|`),
        circulatory_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^I", .)), .f = `|`),
        respiratory_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^J", .)), .f = `|`),
        digestive_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^K", .)), .f = `|`),
        skin_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^L", .)), .f = `|`),
        musculoskeletal_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^M", .)), .f = `|`),
        genitourinary_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^N", .)), .f = `|`),
        symptoms_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^R", .)), .f = `|`),
        injury_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^S|^T", .)), .f = `|`),
        external_causes_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^V|^W|^X|^Y", .)), .f = `|`)) %>%
 group_by(RODCIS2) %>%
 summarise(across(ends_with("_binary"), list(days_until_comorbidity = ~ first(if(all(. == FALSE)) days_diff_comorbidity_followup else as.numeric(min(comorbid_hospitalization_start[. == 1]) - comorbidity_followup_start)))),
           across(ends_with("_binary"), ~ as.numeric(any(.)))) %>%
 ungroup() %>%
 mutate(somatic_comorbidity_binary = as.numeric(rowSums(select(., c(ends_with("_binary"), -psychiatric_comorbidity_binary))) >= 1)) %>%
 mutate(somatic_comorbidity_binary_days_until_comorbidity = pmap_dbl(across(c(ends_with("days_until_comorbidity"), -psychiatric_comorbidity_binary_days_until_comorbidity)), min)) 

#Match controls with their other hospitalizations

comorbid_hospitalizations_controls_1995_2012 <- matched_and_sampled_hospitalizations_no_SDU_1995_2003 %>%
 transmute(RODCIS2,
           ID_case,
           primary_diagnosis = ZDG,
           comorbidity_followup_start = ymd(DATUKO),
           comorbidity_followup_end = comorbidity_followup_start %m+% years(9),
           days_diff_comorbidity_followup = as.numeric(comorbidity_followup_end - comorbidity_followup_start)) %>%
 inner_join(hospitalizations_1994_2012,
            by = c("RODCIS2" = "RODCIS2")) %>%
 mutate(comorbid_hospitalization_start = ymd(DATPRI),
        comorbid_hospitalization_end = ymd(DATUKO)) %>%
 filter(int_overlaps(interval(comorbid_hospitalization_start, comorbid_hospitalization_end),
                     interval(comorbidity_followup_start, comorbidity_followup_end))) %>%
 mutate(across(ZDG:DDG5, trimws)) %>%
 mutate(across(ZDG:DDG5, ~ replace(., . == "", NA_character_))) %>%
 mutate(across(ZDG:DDG5, list(not_as_primary_diagnosis = ~ ifelse(substr(primary_diagnosis, 1, 2) != substr(., 1, 2),
                                                                  .,
                                                                  NA_character_)))) %>%
 mutate(psychiatric_comorbidity_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^F", .)), .f = `|`),
        infection_and_parasitic_diseases_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^A|^B", .)), .f = `|`),
        neoplasms_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^C|^D0[0-9]|^D1[0-9]|^D2[0-9]|^D3[0-9]|^D4[0-9]", .)), .f = `|`),
        blood_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^D5[0-9]|^D6[0-9]|^D7[0-9]|^D8[0-9]", .)), .f = `|`),
        endocrine_diseases_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^E", .)), .f = `|`),
        nervous_system_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^G", .)), .f = `|`),
        eye_and_ear_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^H", .)), .f = `|`),
        circulatory_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^I", .)), .f = `|`),
        respiratory_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^J", .)), .f = `|`),
        digestive_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^K", .)), .f = `|`),
        skin_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^L", .)), .f = `|`),
        musculoskeletal_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^M", .)), .f = `|`),
        genitourinary_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^N", .)), .f = `|`),
        symptoms_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^R", .)), .f = `|`),
        injury_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^S|^T", .)), .f = `|`),
        external_causes_binary = reduce(.x = across(ends_with("not_as_primary_diagnosis"), ~ grepl("^V|^W|^X|^Y", .)), .f = `|`)) %>%
 group_by(RODCIS2) %>%
 summarise(ID_case = first(ID_case),
           across(ends_with("_binary"), list(days_until_comorbidity = ~ first(if(all(. == FALSE)) days_diff_comorbidity_followup else as.numeric(min(comorbid_hospitalization_start[. == 1]) - comorbidity_followup_start)))),
           across(ends_with("_binary"), ~ as.numeric(any(.)))) %>%
 ungroup() %>%
 mutate(somatic_comorbidity_binary = as.numeric(rowSums(select(., c(ends_with("_binary"), -psychiatric_comorbidity_binary))) >= 1)) %>%
 mutate(somatic_comorbidity_binary_days_until_comorbidity = pmap_dbl(across(c(ends_with("days_until_comorbidity"), -psychiatric_comorbidity_binary_days_until_comorbidity)), min)) 

#Merging index hospitalizations with comorbidity

index_and_comorbid_hospitalizations_1995_2012 <- comorbid_hospitalizations_controls_1995_2012 %>%
 left_join(matched_and_sampled_hospitalizations_no_SDU_1995_2003 %>%
            select(RODCIS2,
                   VEK,
                   POHL,
                   NBYDL,
                   STAV,
                   ZAM,
                   ZDG:DDG5,
                   DATPRI,
                   DATUKO),
           by = c("RODCIS2" = "RODCIS2")) %>%
 mutate(group = "controls") %>%
 bind_rows(comorbid_hospitalizations_1995_2012 %>%
            left_join(first_hospitalizations_SDU_1995_2003 %>%
                       select(RODCIS2,
                              VEK,
                              POHL,
                              NBYDL,
                              STAV,
                              ZAM,
                              ZDG:DDG5,
                              DATPRI,
                              DATUKO),
                      by = c("RODCIS2" = "RODCIS2")) %>%
            mutate(group = "cases",
                   ID_case = RODCIS2))

#Merging hospitalizations with mortality
#Days until comorbidity as date of death or date of hospitalization, whichever comes first

hospitalizations_and_mortality_1995_2017 <- index_and_comorbid_hospitalizations_1995_2012 %>%
        left_join(deaths_1994_2017,
                  by = c("RODCIS2" = "RODCIS2")) %>%
        mutate(mortality = as.numeric(int_overlaps(interval(ymd(DATUKO), ymd(DATUKO) %m+% years(14)),
                                                   interval(DAUMR, DAUMR))),
               mortality = replace(mortality, is.na(mortality), 0),
               days_diff_mortality_followup = as.numeric((ymd(DATUKO) %m+% years(14)) - ymd(DATUKO)),
               days_until_death = as.numeric(DAUMR - ymd(DATUKO)),
               days_until_death = ifelse(mortality == 1, days_until_death, days_diff_mortality_followup),
               age_death = ifelse(mortality == 1, VEK + (days_until_death + 1)/365, VEK + 14)) %>%
        mutate(across(ends_with("days_until_comorbidity"), ~ ifelse(mortality == 1, pmin(., days_until_death), .)))

#Export data

fwrite(hospitalizations_and_mortality_1995_2017,
       file = "/Users/tomasformanek/Dropbox/EpiCentre/Comorbidity_SUD/Data/Data_models/Hospitalizations_and_mortality_1995_2017.csv")
