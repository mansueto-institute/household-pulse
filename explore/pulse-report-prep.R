
library(knitr)
library(tidyverse)
library(ggrepel)
library(Cairo)

#' @data data frame, survey results in predefined format
#' @question_variable string, contains question variable code in the q_var field
#' @return data
viz_prep <- function(data, question_variable) {
  # Subset on a question 
  df <- data %>%
    filter(q_var_group %in% c(question_variable))
  
  # Balance and fit loess model
  df <- df %>%
    select(cbsa_title_short, fielding_date, collection_dates, q_var_group, q_var, q_val, proportions, question, order, question_type, q_var_count, topic_area, level_p_or_h) %>%
    arrange(q_var, q_val, cbsa_title_short, fielding_date) %>%
    complete(cbsa_title_short, q_var_group, q_var, q_val, question, order, question_type, q_var_count, topic_area, level_p_or_h,  
             nesting(fielding_date, collection_dates), fill = list(proportions = NA)) %>%
    group_by(q_var, cbsa_title_short, fielding_date) %>%
    mutate(proportions_sum = sum(proportions)) %>%
    ungroup() %>%
    mutate(proportions = ifelse(is.na(proportions) & proportions_sum == 1, 0, proportions)) %>%
    group_by(q_var, q_var_group, q_val, cbsa_title_short) %>% 
    fill(proportions, .direction = "downup") %>%
    mutate(y = loess(proportions ~ as.numeric(fielding_date), span = 0.75)$fitted) %>% 
    ungroup() %>%
    mutate(y = ifelse(y < 0, 0, y),
           y = ifelse(y > 1, 1, y)) %>%
    group_by(q_var, q_var_group, fielding_date, cbsa_title_short) %>% 
    mutate(y_sum = sum(y)) %>%
    ungroup() %>%
    mutate(proportions_loess = y/y_sum) 
  
  # Prep labels
  df <- df %>%
    mutate(most_recent = case_when(fielding_date == max(fielding_date) ~ 1, TRUE ~ as.numeric(0) )) %>%
    arrange(q_var, fielding_date, cbsa_title_short, q_val) %>%
    group_by(q_var, fielding_date, cbsa_title_short) %>%
    mutate(pos_id_share = (cumsum(proportions_loess) - 0.5*proportions_loess)) %>%
    ungroup() %>% 
    arrange(proportions_loess) %>%
    modify_if(is.factor, as.character) %>% 
    separate(col = q_val, sep = ' - ', into= c('response_order','response_label'), extra = "merge") 
  
  return(df)
}

#' Visualizes only question responses starting with "1 - " 
#' Produces plots for each metro and national over time
#' @param data data frame, after applying viz_prep function
#' @return plot
viz_trendline <- function(data) {
  df <- data %>% filter(response_order == 1)
  area_order <- c("National", "Atlanta, GA", "Boston, MA-NH", "Chicago, IL-IN-WI", "Dallas, TX", "Detroit, MI", "Houston, TX", "Los Angeles, CA", "Miami, FL", "New York, NY-NJ-PA", "Philadelphia, PA-NJ-DE-MD", "Phoenix, AZ", "Riverside, CA", "San Francisco, CA", "Seattle, WA", "Washington, DC-VA-MD-WV")
  df$cbsa_title_short <- factor(x = df$cbsa_title_short, levels = area_order)
  
  (plot <- ggplot(df) +
      facet_wrap(~ cbsa_title_short) +
      geom_smooth(aes(x = fielding_date, y = proportions, color = cbsa_title_short),
                  size = .5,  method = 'loess', formula = 'y ~ x') +
      geom_point(aes(x = fielding_date, y = proportions, color = cbsa_title_short)) +
      geom_text_repel(mapping = aes(x = fielding_date, y = proportions, 
                                    label=paste0(round(proportions*100,0),"%")), 
                      force = 4, point.padding = 1.5, nudge_y = .01, size = 3.5, 
                      direction = "y", segment.color = '#4b5b66')+ #family='Roboto', 
      scale_color_manual(values = colorRampPalette(c('#F77552',"#0194D3","#49DEA4","#ffc425"))(16)) +
      scale_y_continuous(labels = scales::percent, breaks = c(.5)) + 
      scale_x_date(date_labels = "%b", date_breaks = "2 month") +
      labs(subtitle = '', #unique(df$response_label)
           y="", x="", color = "Metro", 
           caption = paste0()) + #'Source: U.S. Census Household Pulse Survey, ',
      # paste0(format(min(df$fielding_date), format='%B %Y'),' to ',format(max(df$fielding_date), format='%B %Y')))) + #,
      # '\nQuestion text: ',str_wrap(unique(df$question),200) )) + #str_extract(, '.*?[a-z0-9][.?!](?= )')
      theme_bw() +
      theme(strip.background = element_blank(),
            legend.position  = 'none',
            legend.title = element_blank(),
            strip.text.x = element_text(face = "bold", size = 14),
            plot.subtitle = element_blank(),
            axis.text.y=element_text(size = 10),
            #axis.ticks.y=element_blank(),
            plot.caption = element_blank(), # element_text(size=9, hjust = 0, vjust = 0, face = 'italic'),
            text = element_text(size = 13, color = "#333333"), # family = "Roboto", 
            panel.spacing.y = unit(-0.1, "lines"),
            panel.spacing.x = unit(0.2, "lines"),
            plot.margin = ggplot2::margin(t = 0, r = 2, b = -4, l = -2, unit = "mm")))
  
  return(plot)
}

#' Visualizes all question responses for each metro and U.S. in most recent time period  
#' @param data data frame, survey results processed by viz_prep function
#' @return plot
viz_stackedbar <- function(data) {
  df <- data %>%
    modify_if(is.factor, as.character) %>%
    filter(most_recent == 1) %>%
    arrange(q_var, fielding_date, cbsa_title_short, desc(response_order))
  
  yaxis_order <- df %>%
    filter(response_order == 1 & most_recent == 1) %>% 
    arrange(proportions_loess) %>% 
    select(cbsa_title_short) %>% 
    distinct() %>%
    pull(cbsa_title_short)
  
  df$cbsa_title_short <- factor(x = df$cbsa_title_short, levels = yaxis_order)
  df$response_label <- factor(x = df$response_label, labels = str_wrap(unique(df$response_label),150), levels = (unique(df$response_label)))
  response_count = length(unique(df$response_label))
  
  (plot <- ggplot(df ) +
      geom_bar(aes(x = proportions_loess, y = cbsa_title_short, fill= response_label ), 
               color = 'white', stat="identity") +
      scale_fill_manual(values = c('#F77552',"#0194D3","#49DEA4","#ffc425",'#93328E','#FF925A','#00A9A7')) + 
      scale_x_continuous(labels = scales::percent,expand = c(0, 0)) +
      geom_text(aes(label=ifelse(proportions_loess >= 0.075, paste0(round(proportions_loess*100,0),"%"),""), 
                    y = cbsa_title_short, x=pos_id_share), fontface = "bold", size = 3) +
      labs(y= "", x = '', subtitle = paste0(''), #family = 'Roboto',  str_wrap(unique(df$question),130)
           caption = paste0("") ) + 
      #Source: U.S. Census Bureau Household Pulse Survey.\n
      #unique(df$collection_dates) Estimates based on locally weighted smoothing model.
      theme_bw() +
      guides(fill=guide_legend(nrow=ifelse(response_count>3,round(response_count/2,0),response_count),byrow=TRUE)) +
      theme(legend.position = 'bottom',
            legend.title = element_blank(),
            plot.subtitle=element_text(size=9, hjust=0, face = 'bold'),
            legend.margin = margin(l = -40, t=-5, unit = "mm"),
            legend.spacing.y = unit(.02, 'mm'),
            plot.caption = element_text(size=8, hjust = 0, vjust = 0, face = 'italic'),
            text = element_text( size = 11, color = "#333333"), # family = "Roboto",
            plot.title.position = "plot",
            plot.caption.position =  "plot",
            axis.ticks.y = element_blank(),
            panel.grid = ggplot2::element_blank(),
            panel.border = ggplot2::element_blank(),
            panel.background = ggplot2::element_blank(),
            plot.margin = ggplot2::margin(t = 0, r = 5, b = -4, l = 0, unit = "mm")))
  
  return(plot)
}

# panel.margin = unit(c(-2, -2), "lines")

# Read in data and stack -
df_pulse_cbsa <- read_csv('/Users/nm/Desktop/projects/work/mansueto/pulse/analytics/crosstabs.csv') %>% rename_all(tolower)
df_pulse_us <- read_csv('/Users/nm/Desktop/projects/work/mansueto/pulse/analytics/crosstabs_national.csv') %>% rename_all(tolower) %>%
  mutate(est_msa = 'National', cbsa_title = 'National')
df_xwalk <- read_csv('/Users/nm/Desktop/projects/work/mansueto/pulse/analytics/household_pulse_data_dictionary - question_mapping.csv') %>%
  select(variable, question, order, question_type, topic_area, level_p_or_h) 

df_pulse <- rbind(df_pulse_us,df_pulse_cbsa) %>%
  filter(!is.na(pweight_full)) %>%
  left_join(., df_xwalk, by = c('q_var'='variable')) %>%
  mutate(q_var_group = gsub('[[:digit:]]+', '', q_var)) %>%
  group_by(q_var_group) %>%
  mutate(q_var_count = sum(n())) %>%
  ungroup()

# Run some data cleanup and relabel
df_pulse <- df_pulse %>%
  filter(ct_var == 'TOPLINE', q_val != '0 - not selected', !(q_val %in% c('-99','-88'))) %>%
  mutate(cbsa_title_short = case_when(cbsa_title == "Atlanta-Sandy Springs-Alpharetta, GA" ~ "Atlanta, GA",
                                      cbsa_title == "Boston-Cambridge-Newton, MA-NH" ~ "Boston, MA-NH",
                                      cbsa_title == "Chicago-Naperville-Elgin, IL-IN-WI" ~ "Chicago, IL-IN-WI",
                                      cbsa_title == "Dallas-Fort Worth-Arlington, TX" ~ "Dallas, TX",
                                      cbsa_title == "Detroit-Warren-Dearborn, MI" ~ "Detroit, MI",
                                      cbsa_title == "Houston-The Woodlands-Sugar Land, TX" ~ "Houston, TX",
                                      cbsa_title == "Los Angeles-Long Beach-Anaheim, CA" ~ "Los Angeles, CA",
                                      cbsa_title == "Miami-Fort Lauderdale-Pompano Beach, FL" ~ "Miami, FL",
                                      cbsa_title == "New York-Newark-Jersey City, NY-NJ-PA" ~ "New York, NY-NJ-PA",
                                      cbsa_title == "Philadelphia-Camden-Wilmington, PA-NJ-DE-MD" ~ "Philadelphia, PA-NJ-DE-MD",
                                      cbsa_title == "Phoenix-Mesa-Chandler, AZ" ~ "Phoenix, AZ",
                                      cbsa_title == "Riverside-San Bernardino-Ontario, CA" ~ "Riverside, CA",
                                      cbsa_title == "San Francisco-Oakland-Berkeley, CA" ~ "San Francisco, CA",
                                      cbsa_title == "Seattle-Tacoma-Bellevue, WA" ~ "Seattle, WA",
                                      cbsa_title == "Washington-Arlington-Alexandria, DC-VA-MD-WV" ~ "Washington, DC-VA-MD-WV",
                                      TRUE ~ as.character(cbsa_title)),
         fielding_date = case_when(collection_dates == 'August 19 – August 31 2020' ~ '08-31-2020',
                                   collection_dates == 'September 2 – September 14 2020' ~ '09-14-2020',
                                   collection_dates == 'September 16 – September 28 2020' ~ '09-28-2020',
                                   collection_dates == 'September 30 – October 12 2020' ~ '10-12-2020',
                                   collection_dates == 'October 14 – October 26 2020' ~ '10-26-2020',
                                   collection_dates == 'October 28 – November 9 2020' ~ '11-09-2020',
                                   collection_dates == 'November 11 – November 23 2020' ~ '11-23-2020',
                                   collection_dates == 'November 25 – December 7 2020' ~ '12-07-2020',
                                   collection_dates == 'December 9 – December 21 2020' ~ '12-21-2020',
                                   collection_dates == 'January 6 – January 18 2021' ~ '01-18-2021',
                                   collection_dates == 'January 20 – February 1 2021' ~ '02-01-2021',
                                   collection_dates == 'February 3 – February 15 2021' ~ '02-15-2021',
                                   collection_dates == 'February 17 – March 1 2021' ~ '03-01-2021',
                                   collection_dates == 'March 3 – March 15 2021' ~ '03-15-2021',
                                   collection_dates == 'March 17 – March 29 2021' ~ '03-29-2021',
                                   collection_dates == 'April 14 – April 26 2021' ~ '04-26-2021',
                                   TRUE ~ as.character('')),
         fielding_date = as.Date(fielding_date,format="%m-%d-%Y"))

df_dict <- df_pulse %>% 
  select(order, question, q_var, q_var_group, q_var_count, question_type, topic_area) %>%
  filter(question_type %in% c('Binary Outcome','Multiple Choice','Select All')) %>%
  distinct() %>%
  arrange(order)


#' Generate a sub-chunk to be interpreted by knitr.  The enclosing chunk must have "results='asis'"
#' @param g The output to chunkify (only tested with figures to date)
#' @param ... Additional named arguments to the chunk
#' @details The chunk is automatically output to the console. There is no need to print/cat its result.
#' Based on code from http://michaeljw.com/blog/post/subchunkify/
subchunkify <- local({
  chunk_count <- 0
  function(g, ...) {
    chunk_count <<- chunk_count + 1
    g_deparsed <- paste0(deparse(function() {g} ), collapse = '')
    args <- list(...)
    args <- lapply(names(args), FUN=function(nm, arglist) {
      current <- arglist[[nm]]
      if (length(current) > 1) {
        stop("Only scalars are supported by subchunkify")
      } else if (is.character(current) | is.factor(current)) {
        current <- as.character(current)
        ret <- paste0('"', gsub('"', '\"', current, fixed=TRUE), '"')
      } else if (is.numeric(current) | is.logical(current)) {
        ret <- as.character(current)
      } else {stop("Unhandled class in subchunkify argument handling")}
      paste0(nm, "=", ret)}, arglist=args)
    args <- paste0(unlist(args), collapse=", ")
    chunk_header <- paste(paste0("{r sub_chunk_", chunk_count),
                          if (nchar(args) > 0) {
                            paste(",", args) 
                          } else { NULL }, ", echo=FALSE}")
    sub_chunk <- paste0("\n```",chunk_header, "\n","(",g_deparsed,")()\n","```\n")
    cat(knitr::knit(text = knitr::knit_expand(text = sub_chunk), quiet = TRUE))
  }
})
