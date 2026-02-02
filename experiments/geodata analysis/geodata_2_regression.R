# load packages
library(lme4)
library(dplyr)
library(glmmTMB)

# LOAD DATA----
df <- read.csv("data/pks_fem_news_for_R.csv")

df <- df[-which(df$NUTS == ""), ]
df <- df %>% replace(is.na(.), 0)

# make sure data types are correct
df$NUTS <- factor(df$NUTS)
df$year <- factor(df$year)


# EXPLORE DATA (PLOTS)----

# plot dependent variable (news_count)

plot(table(df$fem))

# plot relation between dependent + independent variables

plot(news_count ~ fem, data = df)

clog <- function(x) log(x+0.5)

cfac <- function(x, breaks = NULL) {
  if(is.null(breaks)) breaks <- unique(quantile(x, 0:10/10)) 
  x <- cut(x, breaks, include.lowest = TRUE, right = FALSE) 
  levels(x) <- paste(breaks[-length(breaks)], ifelse(diff(breaks) > 1, 
                                                     c(paste("-", breaks[-c(1, length(breaks))] - 1, sep = ""), "+"), ""), 
                     sep = "") 
  return(x) 
}

plot(clog(news_count) ~ cfac(fem), data = df)
plot(clog(news_count) ~ fem, data = df)
plot(clog(news_count) ~ pop22_tot, data = df)
plot(news_count ~ pop22_tot, data = df)
plot(clog(fem) ~ pop22_tot, data = df)
plot(clog(news_count) ~ year, data = df)


# NEG. BIN: REGRESSION MODEL----

# offset
df$log_pop <- log(df$pop22_tot)

# numeric year
df$year_num <- as.numeric(as.character(df$year))
df$year_c <- df$year_num - mean(df$year_num)

# lagged effects

df <- df %>%
  arrange(NUTS, year_num) %>%
  group_by(NUTS) %>%
  mutate(
    fem_lag1 = lag(fem, 1),
    fem_lag2 = lag(fem, 2)
  ) %>%
  ungroup()

model_nb_lag <- glmmTMB(
  news_count ~ fem + fem_lag1 + fem_lag2 + year_c +
    offset(log_pop) + (1 | NUTS),
  data = df,
  family = nbinom2()
)

summary(model_nb_lag)

sink("output_regression_model.txt")
print(summary(model_nb_lag))
sink() 

# CALCULATE RESIDUALS----

df_model <- df %>% 
  subset(!(is.na(fem_lag2) | is.na(fem_lag1)))

df_out <- df_out %>%
  mutate(
    pred_news2 = predict(
      mod2,
      type = "response",
      allow.new.levels = TRUE
    ),
    
    resid_pearson2 = residuals(
      mod2,
      type = "pearson"
    ),
    
    resid_deviance2 = residuals(
      mod2,
      type = "deviance"
    )
  )

# save residuals

write.csv(
  df_out,
  "data/news_fem_residuals.csv",
  row.names = FALSE
)
