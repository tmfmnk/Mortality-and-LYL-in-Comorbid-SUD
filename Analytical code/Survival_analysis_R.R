#libraries

library(tidyverse)
library(survival)
library(broom)
library(survminer)
library(cowplot)

#Load data

load(file = "/path")

#Survival analysis

mortality_coefs <- map2_dfr(.x = data_models,
                            .y = map(.x = data_models,
                                     ~ .x %>%
                                       select(ends_with("_first_comorbid_hosp_end_date_year")) %>%
                                       names()),
                            ~ tidy(coxph(as.formula(paste("Surv(years_until_death_or_censoring, mortality)", "~ group")),
                                         data = .x),
                                   conf.int = TRUE,
                                   exponentiate = TRUE) %>%
                              mutate(model = "Unadjusted",
                                     exposure = sub("_first_comorbid_hosp_end_date_year", "", .y))) %>%
  bind_rows(map2_dfr(.x = data_models,
                     .y = map(.x = data_models,
                              ~ .x %>%
                                select(ends_with("_first_comorbid_hosp_end_date_year")) %>%
                                names()),
                     ~ tidy(coxph(as.formula(paste("Surv(years_until_death_or_censoring, mortality)", "~ sex + group +", sub("_first_comorbid_hosp_end_date_year", "_first_comorbid_hosp_age", .y), "+", .y)),
                                  data = .x),
                            conf.int = TRUE,
                            exponentiate = TRUE) %>%
                       mutate(model = "Adjusted",
                              exposure = sub("_first_comorbid_hosp_end_date_year", "", .y)) %>%
                       filter(term == "groupexposed"))) 

#Table with the results of survival analyis

mortality_coefs %>%
  transmute(exposure = str_to_sentence(gsub("_", " ", exposure)),
            exposure = case_when(exposure == "Hiv or aids" ~ "HIV or AIDS",
                                        exposure == "Parkinsons disease" ~ "Parkinson's disease",
                                        TRUE ~ exposure),
            model,
            estimate_with_ci = paste(formatC(round(estimate, 2), format = "f", digits = 2),
                                     paste0("(", 
                                            formatC(round(conf.low, 2), format = "f", digits = 2),
                                            "; ",
                                            formatC(round(conf.high, 2), format = "f", digits = 2),
                                            ")"))) %>%
  pivot_wider(names_from = "model",
              values_from = "estimate_with_ci") %>%
  arrange(exposure) %>%
  write.csv(file = "/path",
            row.names = FALSE)

#Plotting the results from survival analysis

mortality_coefs %>%
  mutate(exposure = str_to_sentence(gsub("_", " ", exposure)),
         exposure = case_when(exposure == "Hiv or aids" ~ "HIV or AIDS",
                              exposure == "Parkinsons disease" ~ "Parkinson's disease",
                              TRUE ~ exposure),
         exposure = reorder(exposure, desc(exposure))) %>%
  ggplot(aes(x = exposure, 
             y = estimate, 
             ymin = conf.low, 
             ymax = conf.high,
             col = model,
             fill = model)) + 
  geom_point(size = 3, 
             stroke = 0.5,
             position = position_dodge(width = 0.5)) +
  geom_errorbar(width = 0.5,
                cex = 1,
                position = position_dodge(width = 0.5)) +
  geom_hline(yintercept = 1, 
             linetype = 2) +
  coord_flip() +
  ylab("HR (95% CI) for all-cause mortality") +
  labs(fill = "Model",
       colour = "Model") +
  scale_y_log10(limits = c(0.4, 9), breaks = c(0.4, 0.7, 1, 1.25, 1.5, 2, 3, 5, 7, 9)) +
  scale_color_manual(values = c("blue", "red"),
                     limits = c("Unadjusted", "Adjusted")) +
  scale_fill_manual(values = c("blue", "red"),
                    limits = c("Unadjusted", "Adjusted")) +
  theme(plot.title = element_text(face = "bold", hjust = 0.5),
        axis.title.x = element_text(face = "bold", size = 12),
        axis.title.y = element_blank(),
        axis.text.x = element_text(face = "bold", size = 10),
        axis.text.y = element_text(face = "bold", hjust = 0, size = 10),
        legend.key = element_rect(colour = "transparent", fill = "white"),
        legend.title = element_text(face = "bold", size = 12),
        legend.text = element_text(face = "bold", size = 10),
        axis.ticks.y = element_blank(),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        panel.border = element_blank(),
        panel.background = element_blank(),
        axis.line.x = element_line("black", size = 0.5))

ggsave(file = "/path",
       width = 30,
       height = 20,
       units = "cm",
       dpi = 300)

#Survival probability plots
#Define names of exposures

exposure_names <- map(.x = data_models, 
                      ~ names(select(.x, ends_with("_first_comorbid_hosp_end_date_year"))) %>%
                         sub("_first_comorbid_hosp_end_date_year", "", .) %>%
                         gsub("_", " ", .) %>%
                         str_to_sentence()  %>%
                         {case_when(. == "Hiv or aids" ~ "HIV or AIDS",
                                   . == "Parkinsons disease" ~ "Parkinson's disease",
                                   TRUE ~ .)})
                   
#Define custom plotting function

custom_theme <- function() {
   theme_survminer() %+replace%
      theme(plot.title = element_text(face = "bold", hjust = 0.5),
            axis.title.x = element_text(face = "bold", size = 10),
            axis.title.y = element_text(face = "bold", size = 10, angle = 90),
            axis.text.x = element_text(face = "bold", size = 10),
            axis.text.y = element_text(face = "bold", size = 10),
            axis.ticks.x = element_blank(),    
            axis.ticks.y = element_blank(),    
            legend.text = element_text(face = "bold", size = 10),
            legend.title = element_blank())
}

#Programmatic ploting

map2(surv_fit(Surv(years_until_death_or_censoring, mortality) ~ group, 
              data = data_models,
              match.fd = FALSE),
     exposure_names,
    
     function(x, y) {
       
       p <- ggsurvplot(x, 
                 conf.int = TRUE, 
                 conf.int.alpha = 0.3,
                 risk.table = TRUE,
                 cumcensor = TRUE,
                 legend.labs = c("People without SUD", "People with SUD"),
                 title = paste0(y, " survival by SUD status"),
                 xlab = "Time (years) since the onset of the health condition",
                 ylab = "Survival probability (95% CI)",
                 palette = c("blue", "red"),
                 legend.title = "",
                 ggtheme = custom_theme())
       
       p1 = p$plot
       p2 = p$table
       p3 = p$ncensor.plot
       plots = cowplot::plot_grid(p1, p2, p3, align = "v", ncol = 1, rel_heights = c(4, 1, 1))
       
       ggsave(plot = plots,
              filename = paste0("Survival_probability_plot_", y, ".png"),
              path = "/path",
              device = "png",
              width = 10, 
              height = 7, 
              dpi = 300)
       }
    )
