#Libraries

library(data.table)
library(tidyverse)
library(purrr)
library(lubridate)
library(furrr)

#Set up the parallelization

future::plan(multisession)

#Variable definition

#RODCIS2 = unique personal identifier
#DATPRI = date of admission
#DATUKO = date of discharge
#VEK = age
#POHL = sex
#ZAM = work status
#NBYDL = region of residence
#ZDG = primary diagnosis
#DAUMR = date of death

#Import all hospitalizations from 1994 to 2017

hospitalizations_1994_2015 <- list.files(path = "/path/Hospitalizations_1994_2017", 
                                         full.names = TRUE) %>%
        .[str_detect(., "1[6-7].csv$", negate = TRUE)] %>%
        map_dfr(~ fread(.,
                        select = c("RODCIS2" = "character", 
                                   "DATPRI" = "integer", 
                                   "DATUKO" = "integer", 
                                   "VEK"= "integer", 
                                   "POHL" = "integer",
                                   "NBYDL"= "character",
                                   "ZDG" = "character",
                                   "ZAM" = "integer"),
                        header = TRUE,
                        sep = ",",
                        dec = ".",
                        fill = TRUE,
                        encoding = "Latin-1",
                        nThread = 8))

hospitalizations_2016_2017 <- list.files(path = "/path/Hospitalizations_1994_2017", 
                                         full.names = TRUE) %>%
        .[str_detect(., "1[6-7].csv$", negate = FALSE)] %>%
        map_dfr(~ fread(.,
                        select = c("RODCIS" = "character", 
                                   "DATPRI" = "integer", 
                                   "DATUKO" = "integer", 
                                   "VEK"= "integer", 
                                   "POHL" = "integer",
                                   "NBYDL"= "character",
                                   "ZDG" = "character",
                                   "ZAM" = "integer"),
                        header = TRUE,
                        sep = ";",
                        dec = ".",
                        fill = TRUE,
                        encoding = "Latin-1",
                        nThread = 8)) %>%
        rename(RODCIS2 = RODCIS)

#Combine all data 

hospitalizations_1994_2017 <- hospitalizations_1994_2015 %>%
        bind_rows(hospitalizations_2016_2017)

#Remove partial data

rm(hospitalizations_1994_2015)
rm(hospitalizations_2016_2017)

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

#Remove partial data

rm(deaths_1994_2013)
rm(deaths_2014)
rm(deaths_2015)
rm(deaths_2016)
rm(deaths_2017)

#Excluding records with missing values on key variables
#Excluding records with invalid dates

hospitalizations_1994_2017 <- future_map_dfr(.x = hospitalizations_1994_2017 %>%
                                               group_split(split_ID = row_number() %/% 100000),
                                             ~ .x %>%
                                               filter(rowSums(is.na(across(c(RODCIS2, DATPRI, DATUKO, VEK, POHL, NBYDL, ZDG, ZAM)))) == 0) %>%
                                               filter(!is.na(ymd(DATPRI)) & !is.na(ymd(DATUKO))))

#Excluding individuals with more than one date of death
#Excluding individuals with hospitalizations after death

hospitalizations_1994_2017 <- hospitalizations_1994_2017 %>%
        anti_join(hospitalizations_1994_2017 %>%
                          inner_join(deaths_1994_2017, 
                                     by = c("RODCIS2" = "RODCIS2")) %>%
                          mutate(DATUKO = ymd(DATUKO)) %>%
                          group_by(RODCIS2) %>%
                          filter(DAUMR < max(DATUKO) | n_distinct(DAUMR) > 1) %>%
                          ungroup(),
                  by = c("RODCIS2" = "RODCIS2"))

#Excluding records with invalid date overlaps (discharge date after the admission date of another record)

hospitalizations_1994_2017 <- hospitalizations_1994_2017 %>%
  anti_join(map_dfr(.x = hospitalizations_1994_2017 %>%
                      group_split(split_ID = frank(RODCIS2, ties.method = "dense") %/% 10000),
                    ~ .x %>% 
                      select(RODCIS2,
                             DATPRI_index = DATPRI,
                             DATUKO_index = DATUKO) %>%
                      inner_join(.x %>%
                                   select(RODCIS2,
                                          DATPRI_non_index = DATPRI,
                                          DATUKO_non_index = DATUKO), 
                                 by = c("RODCIS2" = "RODCIS2")) %>%
                      filter(DATPRI_non_index < DATPRI_index & DATUKO_non_index > DATPRI_index) %>%
                      pivot_longer(-RODCIS2,
                                   names_to = c(".value", "type"), 
                                   names_pattern = "([^_]+)_(.*)")),
            by = c("RODCIS2" = "RODCIS2",
                   "DATPRI" = "DATPRI",
                   "DATUKO" = "DATUKO"))

#Establishing the exposed cohort
#Keeping hospitalizations between 1999 and 2017
#Keeping individuals aged 15-70
#Excluding individuals with residence outside of Czechia
#Keeping individuals with primary diagnosis of F1x
#Excluding acute intoxication as the primary diagnosis

hospitalizations_SUD_1999_2017 <- future_map_dfr(.x = hospitalizations_1994_2017 %>%
                                                   group_split(split_ID = row_number() %/% 100000),
                                                 ~ .x %>%
                                                   filter(year(ymd(DATPRI)) >= 1999 & year(ymd(DATUKO)) <= 2017) %>%
                                                   filter(VEK >= 15 & VEK <= 70) %>%
                                                   filter(!grepl("^99", NBYDL)) %>%
                                                   mutate(ZDG = trimws(ZDG)) %>%
                                                   filter(grepl("^F1", ZDG)) %>%
                                                   filter(!(ZDG %in% paste0("F", seq(100, 190, 10)))))

#Selecting the first hospitalization in the examined time period

set.seed(123)
first_hospitalizations_SUD_1999_2017 <- hospitalizations_SUD_1999_2017 %>%
  group_by(RODCIS2, DATPRI, DATUKO) %>%
  filter(row_number() == sample(1:n(), 1)) %>%
  group_by(RODCIS2) %>%
  filter(DATPRI == min(DATPRI)) %>%
  filter(DATUKO == min(DATUKO)) %>%
  ungroup()

rm(hospitalizations_SUD_1999_2017)

#Establishing the unexposed cohort
#Keeping hospitalizations between 1999 and 2017
#Keeping individuals aged 15-70
#Excluding individuals with residence outside of Czechia
#Excluding individuals with any diagnosis of F1x

hospitalizations_no_SUD_1999_2017 <- future_map_dfr(.x = hospitalizations_1994_2017 %>%
                                                      group_split(split_ID = frank(RODCIS2, ties.method = "dense") %/% 10000),
                                                    ~ .x %>%
                                                      filter(year(ymd(DATPRI)) >= 1999 & year(ymd(DATUKO)) <= 2017) %>%
                                                      filter(VEK >= 15 & VEK <= 70) %>%
                                                      filter(!grepl("^99", NBYDL)) %>%
                                                      mutate(ZDG = trimws(ZDG)) %>%
                                                      group_by(RODCIS2) %>%
                                                      filter(!any(grepl("^F1", ZDG))) %>%
                                                      ungroup())

#Selecting the first hospitalization in the examined time period

set.seed(123)
first_hospitalizations_no_SUD_1999_2017 <- future_map_dfr(.x = hospitalizations_no_SUD_1999_2017 %>%
                                               group_split(split_ID = frank(RODCIS2, ties.method = "dense") %/% 10000),
                                             ~ .x %>%
                                               group_by(RODCIS2, DATPRI, DATUKO) %>%
                                               filter(row_number() == sample(1:n(), 1)) %>%
                                               group_by(RODCIS2) %>%
                                               filter(DATPRI == min(DATPRI)) %>%
                                               filter(DATUKO == min(DATUKO)) %>%
                                               ungroup(),
                                             .options = furrr_options(seed = TRUE))

rm(hospitalizations_no_SUD_1999_2017)

#Establish the presence of comorbidity from the index hospitalizations - exposed individuals
#Merging index hospitalization with all records per ID

comorbid_hospitalizations_SUD_1999_2017 <- first_hospitalizations_SUD_1999_2017 %>%
  transmute(RODCIS2,
            comorbidity_followup_start = ymd(DATUKO),
            comorbidity_followup_end = ymd("2017-12-31")) %>%
  inner_join(hospitalizations_1994_2017,
             by = c("RODCIS2" = "RODCIS2")) %>%
  mutate(comorbid_hospitalization_start = ymd(DATPRI),
         comorbid_hospitalization_end = ymd(DATUKO),
         age_comorbid_hospitalization = VEK) %>%
  filter(int_overlaps(interval(comorbid_hospitalization_start, comorbid_hospitalization_end),
                      interval(comorbidity_followup_start, comorbidity_followup_end))) 

#Checking the presence of ICD-10 diagnosis codes

comorbid_hospitalizations_SUD_1999_2017 <- comorbid_hospitalizations_SUD_1999_2017 %>%
  mutate(ZDG = trimws(ZDG)) %>%
  mutate(circulatory_system = grepl("^I1[0-3]|^I15|^I2[0-5]|^I48|^I50|^I7[0-4]|^I6[0-4]|^I69", ZDG),
         endocrine_system = grepl("^E1[0-4]|^E0[0-5]|^E06[1-9]|^E07", ZDG),
         gastrointestinal_system = grepl("^K221|^K2[5-8]|^K29[3-5]|^B1[6-9]|^K70|^K74|^K766|^I85|^K5[0-1]|^K57", ZDG),
         urogenital_system = grepl("^N03|^N11|^N1[8-9]|^N40", ZDG),
         connective_tissue_disorders = grepl("^M0[5-6]|^M0[8-9]|^M3[0-6]|^D86", ZDG),
         cancers = grepl("^C0|^C1|^C2|^C3|^C4[0-3]|^C4[5-9]|^C5|^C6|^C7|^C8|^C9[0-7]", ZDG),
         neurological_system = grepl("^G4[0-1]|^G2[0-2]|^G35", ZDG),
         infectious_diseases = grepl("^A15|^A16|^A17|^B18|^B2[0-4]", ZDG),
         chronic_pulmonary_diseases = grepl("^J4[0-7]", ZDG),
         hypertension = grepl("^I1[0-3]|^I15", ZDG),
         ischemic_heart_disease = grepl("^I2[0-5]", ZDG),
         atrial_fibrillation = grepl("^I48", ZDG),
         heart_failure = grepl("^I50", ZDG),
         peripheral_artery_occlusive_disease = grepl("^I7[0-4]", ZDG),
         stroke = grepl("^I6[0-4]|^I69", ZDG),
         diabetes_mellitus = grepl("^E1[0-4]", ZDG),
         thyroid_disorder = grepl("^E0[0-5]|^E06[1-9]|^E07", ZDG),
         ulcer_or_chronic_gastritis = grepl("^K221|^K2[5-8]|^K29[3-5]", ZDG),
         chronic_liver_disease = grepl("^B1[6-9]|^K70|^K74|^K766|^I85", ZDG),
         inflammatory_bowel_disease = grepl("^K5[0-1]", ZDG),
         diverticular_disease_of_intestine = grepl("^K57", ZDG),
         chronic_kidney_disease = grepl("^N03|^N11|^N1[8-9]", ZDG),
         prostate_disorders = grepl("^N40", ZDG),
         epilepsy = grepl("^G4[0-1]", ZDG),
         parkinsons_disease = grepl("^G2[0-2]", ZDG),
         multiple_sclerosis = grepl("^G35", ZDG),
         tuberculosis = grepl("^A15|^A16|^A17", ZDG),
         chronic_viral_hepatitis = grepl("^B18", ZDG)) %>%
  mutate(across(circulatory_system:chronic_viral_hepatitis, ~ replace(., . == FALSE, NA))) %>%
  pivot_longer(circulatory_system:chronic_viral_hepatitis,
               names_to = "comorbidity",
               values_to = "binary",
               values_drop_na = TRUE) %>%
  group_by(RODCIS2,
           comorbidity) %>%
  summarise(sex = first(POHL),
            first_comorbid_hosp_work_status = ZAM[which.min(comorbid_hospitalization_end)],
            first_comorbid_hosp_age = min(age_comorbid_hospitalization),
            first_comorbid_hosp_end_date = min(comorbid_hospitalization_end),
            binary = first(binary)) %>%
  ungroup() %>%
  pivot_wider(names_from = comorbidity,
              names_glue = "{comorbidity}_{.value}",
              values_from = c("first_comorbid_hosp_work_status",
                              "first_comorbid_hosp_age", 
                              "first_comorbid_hosp_end_date",
                              "binary"))

#Checking the history of comorbid health conditions 5-years before the index hospitalization

historic_comorbid_hospitalizations_SUD <- first_hospitalizations_SUD_1999_2017 %>%
  inner_join(comorbid_hospitalizations_SUD_1999_2017 %>%
               select(RODCIS2),
             by = c("RODCIS2" = "RODCIS2")) %>%
  transmute(RODCIS2,
            historic_comorbidity_followup_start = ymd(DATPRI) %m-% years(5),
            historic_comorbidity_followup_end = ymd(DATPRI)) %>%
  inner_join(hospitalizations_1994_2017,
             by = c("RODCIS2" = "RODCIS2")) %>%
  mutate(comorbid_hospitalization_start = ymd(DATPRI),
         comorbid_hospitalization_end = ymd(DATUKO)) %>%
  filter(int_overlaps(interval(comorbid_hospitalization_start, comorbid_hospitalization_end),
                      interval(historic_comorbidity_followup_start, historic_comorbidity_followup_end))) %>%
  mutate(ZDG = trimws(ZDG)) %>%
  mutate(circulatory_system_historic = grepl("^I1[0-3]|^I15|^I2[0-5]|^I48|^I50|^I7[0-4]|^I6[0-4]|^I69", ZDG),
         endocrine_system_historic = grepl("^E1[0-4]|^E0[0-5]|^E06[1-9]|^E07", ZDG),
         gastrointestinal_system_historic = grepl("^K221|^K2[5-8]|^K29[3-5]|^B1[6-9]|^K70|^K74|^K766|^I85|^K5[0-1]|^K57", ZDG),
         urogenital_system_historic = grepl("^N03|^N11|^N1[8-9]|^N40", ZDG),
         connective_tissue_disorders_historic = grepl("^M0[5-6]|^M0[8-9]|^M3[0-6]|^D86", ZDG),
         cancers_historic = grepl("^C0|^C1|^C2|^C3|^C4[0-3]|^C4[5-9]|^C5|^C6|^C7|^C8|^C9[0-7]", ZDG),
         neurological_system_historic = grepl("^G4[0-1]|^G2[0-2]|^G35", ZDG),
         infectious_diseases_historic = grepl("^A15|^A16|^A17|^B18|^B2[0-4]", ZDG),
         chronic_pulmonary_diseases_historic = grepl("^J4[0-7]", ZDG),
         hypertension_historic = grepl("^I1[0-3]|^I15", ZDG),
         ischemic_heart_disease_historic = grepl("^I2[0-5]", ZDG),
         atrial_fibrillation_historic = grepl("^I48", ZDG),
         heart_failure_historic = grepl("^I50", ZDG),
         peripheral_artery_occlusive_disease_historic = grepl("^I7[0-4]", ZDG),
         stroke_historic = grepl("^I6[0-4]|^I69", ZDG),
         diabetes_mellitus_historic = grepl("^E1[0-4]", ZDG),
         thyroid_disorder_historic = grepl("^E0[0-5]|^E06[1-9]|^E07", ZDG),
         ulcer_or_chronic_gastritis_historic = grepl("^K221|^K2[5-8]|^K29[3-5]", ZDG),
         chronic_liver_disease_historic = grepl("^B1[6-9]|^K70|^K74|^K766|^I85", ZDG),
         inflammatory_bowel_disease_historic = grepl("^K5[0-1]", ZDG),
         diverticular_disease_of_intestine_historic = grepl("^K57", ZDG),
         chronic_kidney_disease_historic = grepl("^N03|^N11|^N1[8-9]", ZDG),
         prostate_disorders_historic = grepl("^N40", ZDG),
         epilepsy_historic = grepl("^G4[0-1]", ZDG),
         parkinsons_disease_historic = grepl("^G2[0-2]", ZDG),
         multiple_sclerosis_historic = grepl("^G35", ZDG),
         tuberculosis_historic = grepl("^A15|^A16|^A17", ZDG),
         chronic_viral_hepatitis_historic = grepl("^B18", ZDG)) %>%
  group_by(RODCIS2) %>%
  summarise(across(circulatory_system_historic:chronic_viral_hepatitis_historic, ~ any(. == TRUE))) %>%
  ungroup()

#Establish the presence of comorbidity from the index hospitalizations - unexposed individuals
#Merging index hospitalization with all records per ID

comorbid_hospitalizations_no_SUD_1999_2017 <- first_hospitalizations_no_SUD_1999_2017 %>%
  transmute(RODCIS2,
            baseline_primary_diagnosis = ZDG,
            comorbidity_followup_start = ymd(DATUKO),
            comorbidity_followup_end = ymd("2017-12-31")) %>%
  inner_join(hospitalizations_1994_2017,
             by = c("RODCIS2" = "RODCIS2")) %>%
  mutate(comorbid_hospitalization_start = ymd(DATPRI),
         comorbid_hospitalization_end = ymd(DATUKO),
         age_comorbid_hospitalization = VEK) %>%
  filter(int_overlaps(interval(comorbid_hospitalization_start, comorbid_hospitalization_end),
                      interval(comorbidity_followup_start, comorbidity_followup_end))) 

#Checking the presence of ICD-10 diagnosis codes

comorbid_hospitalizations_no_SUD_1999_2017 <- future_map_dfr(.x = comorbid_hospitalizations_no_SUD_1999_2017 %>%
                                                               group_split(split_ID = frank(RODCIS2, ties.method = "dense") %/% 10000),
                                                             ~ .x %>%
                                                               mutate(ZDG = trimws(ZDG)) %>%
                                                               mutate(circulatory_system = !grepl("^I1[0-3]|^I15|^I2[0-5]|^I48|^I50|^I7[0-4]|^I6[0-4]|^I69", baseline_primary_diagnosis) & grepl("^I1[0-3]|^I15|^I2[0-5]|^I48|^I50|^I7[0-4]|^I6[0-4]|^I69", ZDG),
                                                                      endocrine_system = !grepl("^E1[0-4]|^E0[0-5]|^E06[1-9]|^E07", baseline_primary_diagnosis) & grepl("^E1[0-4]|^E0[0-5]|^E06[1-9]|^E07", ZDG),
                                                                      gastrointestinal_system = !grepl("^K221|^K2[5-8]|^K29[3-5]|^B1[6-9]|^K70|^K74|^K766|^I85|^K5[0-1]|^K57", baseline_primary_diagnosis) & grepl("^K221|^K2[5-8]|^K29[3-5]|^B1[6-9]|^K70|^K74|^K766|^I85|^K5[0-1]|^K57", ZDG),
                                                                      urogenital_system = !grepl("^N03|^N11|^N1[8-9]|^N40", baseline_primary_diagnosis) & grepl("^N03|^N11|^N1[8-9]|^N40", ZDG),
                                                                      connective_tissue_disorders = !grepl("^M0[5-6]|^M0[8-9]|^M3[0-6]|^D86", baseline_primary_diagnosis) & grepl("^M0[5-6]|^M0[8-9]|^M3[0-6]|^D86", ZDG),
                                                                      cancers = !grepl("^C0|^C1|^C2|^C3|^C4[0-3]|^C4[5-9]|^C5|^C6|^C7|^C8|^C9[0-7]", baseline_primary_diagnosis) & grepl("^C0|^C1|^C2|^C3|^C4[0-3]|^C4[5-9]|^C5|^C6|^C7|^C8|^C9[0-7]", ZDG),
                                                                      neurological_system = !grepl("^G4[0-1]|^G2[0-2]|^G35", baseline_primary_diagnosis) & grepl("^G4[0-1]|^G2[0-2]|^G35", ZDG),
                                                                      infectious_diseases = !grepl("^A15|^A16|^A17|^B18|^B2[0-4]", baseline_primary_diagnosis) & grepl("^A15|^A16|^A17|^B18|^B2[0-4]", ZDG),
                                                                      chronic_pulmonary_diseases = !grepl("^J4[0-7]", baseline_primary_diagnosis) & grepl("^J4[0-7]", ZDG),
                                                                      hypertension = !grepl("^I1[0-3]|^I15", baseline_primary_diagnosis) & grepl("^I1[0-3]|^I15", ZDG),
                                                                      ischemic_heart_disease = !grepl("^I2[0-5]", baseline_primary_diagnosis) & grepl("^I2[0-5]", ZDG),
                                                                      atrial_fibrillation = !grepl("^I48", baseline_primary_diagnosis) & grepl("^I48", ZDG),
                                                                      heart_failure = !grepl("^I50", baseline_primary_diagnosis) & grepl("^I50", ZDG),
                                                                      peripheral_artery_occlusive_disease = !grepl("^I7[0-4]", baseline_primary_diagnosis) & grepl("^I7[0-4]", ZDG),
                                                                      stroke = !grepl("^I6[0-4]|^I69", baseline_primary_diagnosis) & grepl("^I6[0-4]|^I69", ZDG),
                                                                      diabetes_mellitus = !grepl("^E1[0-4]", baseline_primary_diagnosis) & grepl("^E1[0-4]", ZDG),
                                                                      thyroid_disorder = !grepl("^E0[0-5]|^E06[1-9]|^E07", baseline_primary_diagnosis) & grepl("^E0[0-5]|^E06[1-9]|^E07", ZDG),
                                                                      ulcer_or_chronic_gastritis = !grepl("^K221|^K2[5-8]|^K29[3-5]", baseline_primary_diagnosis) & grepl("^K221|^K2[5-8]|^K29[3-5]", ZDG),
                                                                      chronic_liver_disease = !grepl("^B1[6-9]|^K70|^K74|^K766|^I85", baseline_primary_diagnosis) & grepl("^B1[6-9]|^K70|^K74|^K766|^I85", ZDG),
                                                                      inflammatory_bowel_disease = !grepl("^K5[0-1]", baseline_primary_diagnosis) & grepl("^K5[0-1]", ZDG),
                                                                      diverticular_disease_of_intestine = !grepl("^K57", baseline_primary_diagnosis) & grepl("^K57", ZDG),
                                                                      chronic_kidney_disease = !grepl("^N03|^N11|^N1[8-9]", baseline_primary_diagnosis) & grepl("^N03|^N11|^N1[8-9]", ZDG),
                                                                      prostate_disorders = !grepl("^N40", baseline_primary_diagnosis) & grepl("^N40", ZDG),
                                                                      epilepsy = !grepl("^G4[0-1]", baseline_primary_diagnosis) & grepl("^G4[0-1]", ZDG),
                                                                      parkinsons_disease = !grepl("^G2[0-2]", baseline_primary_diagnosis) & grepl("^G2[0-2]", ZDG),
                                                                      multiple_sclerosis = !grepl("^G35", baseline_primary_diagnosis) & grepl("^G35", ZDG),
                                                                      tuberculosis = !grepl("^A15|^A16|^A17", baseline_primary_diagnosis) & grepl("^A15|^A16|^A17", ZDG),
                                                                      chronic_viral_hepatitis = !grepl("^B18", baseline_primary_diagnosis) & grepl("^B18", ZDG)) %>%
                                                               mutate(across(circulatory_system:chronic_viral_hepatitis, ~ replace(., . == FALSE, NA))) %>%
                                                               pivot_longer(circulatory_system:chronic_viral_hepatitis,
                                                                            names_to = "comorbidity",
                                                                            values_to = "binary",
                                                                            values_drop_na = TRUE)) %>%
  group_by(RODCIS2,
           comorbidity) %>%
  summarise(sex = first(POHL),
            first_comorbid_hosp_work_status = ZAM[which.min(comorbid_hospitalization_end)],
            first_comorbid_hosp_age = min(age_comorbid_hospitalization),
            first_comorbid_hosp_end_date = min(comorbid_hospitalization_end),
            binary = first(binary)) %>%
  ungroup() %>%
  pivot_wider(names_from = comorbidity,
              names_glue = "{comorbidity}_{.value}",
              values_from = c("first_comorbid_hosp_work_status",
                              "first_comorbid_hosp_age", 
                              "first_comorbid_hosp_end_date",
                              "binary"))

#Checking the history of comorbid health conditions 5-years before the index hospitalization

historic_comorbid_hospitalizations_no_SUD <- first_hospitalizations_no_SUD_1999_2017 %>%
  inner_join(comorbid_hospitalizations_no_SUD_1999_2017 %>%
               select(RODCIS2),
             by = c("RODCIS2" = "RODCIS2")) %>%
  transmute(RODCIS2,
            baseline_primary_diagnosis = ZDG,
            historic_comorbidity_followup_start = ymd(DATPRI) %m-% years(5),
            historic_comorbidity_followup_end = ymd(DATPRI)) %>%
  inner_join(hospitalizations_1994_2017,
             by = c("RODCIS2" = "RODCIS2")) %>%
  mutate(comorbid_hospitalization_start = ymd(DATPRI),
         comorbid_hospitalization_end = ymd(DATUKO)) %>%
  filter(int_overlaps(interval(comorbid_hospitalization_start, comorbid_hospitalization_end),
                      interval(historic_comorbidity_followup_start, historic_comorbidity_followup_end))) %>%
  mutate(ZDG = trimws(ZDG)) %>%
  mutate(circulatory_system_historic = !grepl("^I1[0-3]|^I15|^I2[0-5]|^I48|^I50|^I7[0-4]|^I6[0-4]|^I69", baseline_primary_diagnosis) & grepl("^I1[0-3]|^I15|^I2[0-5]|^I48|^I50|^I7[0-4]|^I6[0-4]|^I69", ZDG),
         endocrine_system_historic = !grepl("^E1[0-4]|^E0[0-5]|^E06[1-9]|^E07", baseline_primary_diagnosis) & grepl("^E1[0-4]|^E0[0-5]|^E06[1-9]|^E07", ZDG),
         gastrointestinal_system_historic = !grepl("^K221|^K2[5-8]|^K29[3-5]|^B1[6-9]|^K70|^K74|^K766|^I85|^K5[0-1]|^K57", baseline_primary_diagnosis) & grepl("^K221|^K2[5-8]|^K29[3-5]|^B1[6-9]|^K70|^K74|^K766|^I85|^K5[0-1]|^K57", ZDG),
         urogenital_system_historic = !grepl("^N03|^N11|^N1[8-9]|^N40", baseline_primary_diagnosis) & grepl("^N03|^N11|^N1[8-9]|^N40", ZDG),
         connective_tissue_disorders_historic = !grepl("^M0[5-6]|^M0[8-9]|^M3[0-6]|^D86", baseline_primary_diagnosis) & grepl("^M0[5-6]|^M0[8-9]|^M3[0-6]|^D86", ZDG),
         cancers_historic = !grepl("^C0|^C1|^C2|^C3|^C4[0-3]|^C4[5-9]|^C5|^C6|^C7|^C8|^C9[0-7]", baseline_primary_diagnosis) & grepl("^C0|^C1|^C2|^C3|^C4[0-3]|^C4[5-9]|^C5|^C6|^C7|^C8|^C9[0-7]", ZDG),
         neurological_system_historic = !grepl("^G4[0-1]|^G2[0-2]|^G35", baseline_primary_diagnosis) & grepl("^G4[0-1]|^G2[0-2]|^G35", ZDG),
         infectious_diseases_historic = !grepl("^A15|^A16|^A17|^B18|^B2[0-4]", baseline_primary_diagnosis) & grepl("^A15|^A16|^A17|^B18|^B2[0-4]", ZDG),
         chronic_pulmonary_diseases_historic = !grepl("^J4[0-7]", baseline_primary_diagnosis) & grepl("^J4[0-7]", ZDG),
         hypertension_historic = !grepl("^I1[0-3]|^I15", baseline_primary_diagnosis) & grepl("^I1[0-3]|^I15", ZDG),
         ischemic_heart_disease_historic = !grepl("^I2[0-5]", baseline_primary_diagnosis) & grepl("^I2[0-5]", ZDG),
         atrial_fibrillation_historic = !grepl("^I48", baseline_primary_diagnosis) & grepl("^I48", ZDG),
         heart_failure_historic = !grepl("^I50", baseline_primary_diagnosis) & grepl("^I50", ZDG),
         peripheral_artery_occlusive_disease_historic = !grepl("^I7[0-4]", baseline_primary_diagnosis) & grepl("^I7[0-4]", ZDG),
         stroke_historic = !grepl("^I6[0-4]|^I69", baseline_primary_diagnosis) & grepl("^I6[0-4]|^I69", ZDG),
         diabetes_mellitus_historic = !grepl("^E1[0-4]", baseline_primary_diagnosis) & grepl("^E1[0-4]", ZDG),
         thyroid_disorder_historic = !grepl("^E0[0-5]|^E06[1-9]|^E07", baseline_primary_diagnosis) & grepl("^E0[0-5]|^E06[1-9]|^E07", ZDG),
         ulcer_or_chronic_gastritis_historic = !grepl("^K221|^K2[5-8]|^K29[3-5]", baseline_primary_diagnosis) & grepl("^K221|^K2[5-8]|^K29[3-5]", ZDG),
         chronic_liver_disease_historic = !grepl("^B1[6-9]|^K70|^K74|^K766|^I85", baseline_primary_diagnosis) & grepl("^B1[6-9]|^K70|^K74|^K766|^I85", ZDG),
         inflammatory_bowel_disease_historic = !grepl("^K5[0-1]", baseline_primary_diagnosis) & grepl("^K5[0-1]", ZDG),
         diverticular_disease_of_intestine_historic = !grepl("^K57", baseline_primary_diagnosis) & grepl("^K57", ZDG),
         chronic_kidney_disease_historic = !grepl("^N03|^N11|^N1[8-9]", baseline_primary_diagnosis) & grepl("^N03|^N11|^N1[8-9]", ZDG),
         prostate_disorders_historic = !grepl("^N40", baseline_primary_diagnosis) & grepl("^N40", ZDG),
         epilepsy_historic = !grepl("^G4[0-1]", baseline_primary_diagnosis) & grepl("^G4[0-1]", ZDG),
         parkinsons_disease_historic = !grepl("^G2[0-2]", baseline_primary_diagnosis) & grepl("^G2[0-2]", ZDG),
         multiple_sclerosis_historic = !grepl("^G35", baseline_primary_diagnosis) & grepl("^G35", ZDG),
         tuberculosis_historic = !grepl("^A15|^A16|^A17", baseline_primary_diagnosis) & grepl("^A15|^A16|^A17", ZDG),
         chronic_viral_hepatitis_historic = !grepl("^B18", baseline_primary_diagnosis) & grepl("^B18", ZDG)) %>%
  group_by(RODCIS2) %>%
  summarise(across(circulatory_system_historic:chronic_viral_hepatitis_historic, ~ any(. == TRUE))) %>%
  ungroup()
  
#Merge index hospitalization with subsequent and historic hospitalizations
#Export data

comorbid_hospitalizations_SUD_1999_2017 %>%
  left_join(historic_comorbid_hospitalizations_SUD,
            by = c("RODCIS2" = "RODCIS2")) %>%
  left_join(first_hospitalizations_SUD_1999_2017,
            by = c("RODCIS2" = "RODCIS2")) %>%
  mutate(group = "exposed") %>%
  bind_rows(comorbid_hospitalizations_no_SUD_1999_2017 %>%
              left_join(historic_comorbid_hospitalizations_no_SUD,
                        by = c("RODCIS2" = "RODCIS2")) %>%
              left_join(first_hospitalizations_no_SUD_1999_2017,
                        by = c("RODCIS2" = "RODCIS2")) %>%
              mutate(group = "unexposed")) %>%
  select(-split_ID) %>%
  fwrite(file = "/path/Index_hospitalization_and_comorbidity_revisions.csv")

#Baseline characteristics of individuduals with and without subsequent health conditions,
#stratified by SUD status
#Prepare data

index_hospitalization_and_comorbidity <- first_hospitalizations_SUD_1999_2017 %>%
  left_join(comorbid_hospitalizations_SUD_1999_2017,
            by = c("RODCIS2" = "RODCIS2")) %>%
  left_join(historic_comorbid_hospitalizations_SUD,
            by = c("RODCIS2" = "RODCIS2")) %>%
  mutate(group = "exposed") %>%
  bind_rows(first_hospitalizations_no_SUD_1999_2017 %>%
              left_join(comorbid_hospitalizations_no_SUD_1999_2017,
                        by = c("RODCIS2" = "RODCIS2")) %>%
              left_join(historic_comorbid_hospitalizations_no_SUD,
                        by = c("RODCIS2" = "RODCIS2")) %>%
              mutate(group = "unexposed")) 

#Prepare the table

map_dfr(.x = names(select(index_hospitalization_and_comorbidity, ends_with("_binary"))),
        ~ index_hospitalization_and_comorbidity %>%
         mutate(work_status = as.numeric(ZAM != 0)) %>%
         mutate(across(all_of(.x), ~ as.numeric(. == TRUE & get(sub("_binary", "_historic", cur_column())) == FALSE))) %>%
         group_by(across(c(group, all_of(.x)))) %>%
         summarise(exposure = .x,
                   overall_n = formatC(n(), big.mark = " "),
                   sex = paste(formatC(sum(POHL == 1), big.mark = " "),
                               paste0("(",
                                      formatC(round(sum(POHL == 1)/n() * 100, 2), format = "f", digits = 2),
                                      ")")),
                   work_status = paste(formatC(sum(work_status == 1), big.mark = " "),
                                       paste0("(",
                                              formatC(round(sum(work_status == 1)/n() * 100, 2), format = "f", digits = 2),
                                              ")")),
                   age = paste(formatC(round(mean(VEK), 2), format = "f", digits = 2),
                               paste0("(",
                                      formatC(round(sd(VEK), 2), format = "f", digits = 2),
                                      ")")),
                   index_hosp_end_date_year =  paste(median(year(ymd(DATUKO))),
                                                     paste0("(",
                                                            IQR(year(ymd(DATUKO))),
                                                            ")"))) %>%
         ungroup() %>%
         unite("group_exposure", c(group, 2)) %>%
         pivot_wider(names_from = group_exposure,
                     values_from = c(overall_n,
                                     sex, 
                                     work_status,
                                     age,
                                     index_hosp_end_date_year))) %>%
 mutate(exposure = sub("_binary", "", exposure),
        exposure = str_to_sentence(gsub("_", " ", exposure)),
        exposure = case_when(exposure == "Parkinsons disease" ~ "Parkinson's disease",
                             TRUE ~ exposure)) %>%
 arrange(exposure) %>%
 write.csv(file = "/path/Baseline_characteristics_revisions.csv",
           row.names = FALSE)

#Number of individuals developing at least one subsequent physical health condition

map(.x = names(select(index_hospitalization_and_comorbidity, ends_with("_binary"))),
    ~ index_hospitalization_and_comorbidity %>%
     select(RODCIS2, group, starts_with(sub("_binary", "", .x))) %>%
     filter(across(all_of(.x), ~ . == TRUE) & get(sub("_binary", "_historic", .x)) == FALSE)) %>%
 reduce(full_join, 
        by = c("RODCIS2", "group")) %>%
 count(group)
