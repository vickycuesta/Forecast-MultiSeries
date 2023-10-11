library(lubridate)
library(tidyverse)
library(tidymodels)
library(modeltime)
library(modeltime.resample)
library(tibbletime)
library(tseries)
library(rsample)
library(timetk)
library(gt)
library(future)
library(sknifedatar)
library(tictoc)
library(furrr)
library(parsnip)
library(xgboost)


serie_limpia <- read_csv("files/serie_limpia.csv") %>%
  unite('nodo_tipo',nodo,tipo,sep = '-') %>%  
  select(-utilizacion,-anomalia,-kbps) %>% 
  rename(value = kbps_corregido,
         date = fecha) %>% 
  arrange(nodo_tipo, date) %>% 
  mutate(date = as.Date(date)) %>% 
  unique() 

ult_obs <- max(serie_limpia$date)

set.seed(42)
plan(multisession, workers = 18)
future.seed = TRUE

# Division de nodos en grupos para su proxima paralelizacion

nodos <- serie_limpia %>%
  select(nodo_tipo) %>%
  unique()

lista_nodos <- nodos %>%
  cbind(grupo = sample(1:50,nrow(nodos),replace = T))

table(lista_nodos$grupo)

# Arma funcion para la seleccion del modelo y pronostico

funcion_forecast <- function(x) {
  h <- 24
  
  muestra <- serie_limpia %>% 
    filter(nodo_tipo %in% lista_nodos[lista_nodos$grupo == x,]$nodo_tipo)
  
  nested_nodos <- muestra %>% 
    nest(nested_column=-nodo_tipo)
  
  receta_nodos <- recipe(value ~ ., data = muestra %>% select(-nodo_tipo)) %>%
    step_date(date, features = c("month", "quarter", "year"), ordinal = TRUE)
  
  # Modelo ARIMA Boost
  
  m_arima_boosted_nodos <- workflow() %>%
    add_recipe(receta_nodos) %>%
    add_model(arima_boost() %>% 
                set_engine(engine = "auto_arima_xgboost"))
  
  
  # Modelo ETS
  
  m_ETS_nodos <- seasonal_reg() %>%
    set_engine("stlm_ets")
  
  # Modelo prophet boosted
  
  m_prophet_boost_nodos <- workflow() %>%
    add_recipe(receta_nodos) %>%
    add_model(prophet_boost(mode='regression') %>% set_engine("prophet_xgboost"))
  
  # Modelo NNar
  # Red Neuronal
  
  m_nnetar_nodos <- workflow() %>%
    add_recipe(receta_nodos) %>%
    add_model(nnetar_reg(mode='regression') %>% set_engine("nnetar"))
  
  
  model_table_nodos <- suppressMessages(modeltime_multifit(
    serie = nested_nodos,
    .prop = 0.85,
    m_arima_boosted_nodos,
    m_ETS_nodos,
    m_prophet_boost_nodos,
    m_nnetar_nodos)) 
  
  forecast_nodos <- suppressMessages(modeltime_multiforecast(
    model_table_nodos$table_time,
    .prop = 0.85))
  
  best_model_mape <-suppressMessages(modeltime_multibestmodel(
    .table = forecast_nodos,
    .metric = "mape",
    .minimize = TRUE))
  
  best_model_mase <- suppressMessages(modeltime_multibestmodel(
    .table = forecast_nodos,
    .metric = "mase",
    .minimize = TRUE))
  
  best_model_rmse <- suppressMessages(modeltime_multibestmodel(
    .table = forecast_nodos,
    .metric = "rmse",
    .minimize = TRUE))
  
  best_model_mae <- suppressMessages(modeltime_multibestmodel(
    .table = forecast_nodos,
    .metric = "mae",
    .minimize = TRUE))
  
  
  metricas <- rbind(best_model_mape,best_model_mase,best_model_rmse,best_model_mae) %>%
    mutate(modelo = as.numeric(best_model))
  
  freq_model <-  metricas %>%
    group_by(nodo_tipo, modelo) %>%
    summarise(freq = n()) %>%
    ungroup() %>% 
    group_by(nodo_tipo) %>%
    filter(freq == max(freq)) %>%
    select(-freq)
  
  best_model <- metricas %>%
    merge(freq_model, by = c('nodo_tipo', 'modelo')) %>%
    unique() %>%
    select(-modelo)
  rm(metricas,model_table_nodos,forecast_nodos,best_model_mape,best_model_mase,best_model_rmse,best_model_mae)
  
  model_refit<-suppressMessages(modeltime_multirefit(best_model))
  
  forecast_final <-suppressMessages(modeltime_multiforecast(
    model_refit, .h=24
  ))
  
  fct <- forecast_final %>%
    select(nodo_tipo, nested_forecast) %>%
    unnest(nested_forecast)
  
  return(fct)
  
}

# Corre la funcion para todos los grupos de nodos

tic()

x = c(1:50)
forecast_total <- future_map_dfr(.x = x,
                                 .f = ~funcion_forecast(x = .x),
                                 .options = furrr_options(seed = 42))

toc()

# Modifica tabla de resultados y crea ensambles

forecast <- forecast_total %>% 
  select(nodo_tipo, fecha = .index, kbps = .value, modelo = .model_details) %>%
  filter(modelo != 'ACTUAL') %>% 
  mutate(kbps = ifelse(kbps < 0, 0, kbps)) %>%  
  group_by(nodo_tipo,fecha) %>% 
  mutate(  freq = length(modelo),
           mbps = mean(kbps)/1000,
           modelo = ifelse(freq == 1, modelo, 'Ensamble')
  ) %>% 
  select(nodo_tipo,fecha,mbps,modelo) %>% 
  unique() %>% 
  separate(nodo_tipo, c('nodo','tipo'), sep = '-') %>% 
  ungroup()


capacidad <- read_csv('files/capacidad_nodos.csv') %>% 
  select(- fecha) %>%
  rename(capacidad = capacidad_kbps) %>% 
  mutate(capacidad = capacidad/1000)

forecast_final <- merge(forecast,capacidad, by = c('nodo','tipo'), all.x = T) %>%
  mutate(utilizacion = round((mbps/capacidad)*100)) %>% 
  select(-capacidad)

write_csv(forecast_final,"files/forecast.csv")
