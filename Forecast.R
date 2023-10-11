library(DBI)
library(RSQLite)
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
library(parsnip)
library(tictoc)

setwd('C:/Users/U631719/OneDrive - Telecom Argentina SA/Desktop/Tesis')

sqlite <- dbDriver("SQLite")
con_tesis <- DBI::dbConnect(sqlite, "BD_TESIS")

serie_limpia <- dbReadTable(con_tesis,"serie_limpia") %>%
  unite('nodo_tipo',nodo,tipo,sep = '-') %>% 
  select(-utilizacion) %>% 
  rename(value = kbps_corregido,
         date = fecha ) %>% 
  arrange(nodo_tipo, date) %>% 
  mutate(date = as.Date(date))

muestra <- serie_limpia %>% filter(nodo_tipo == '0006-upstream' |nodo_tipo == '0006-downstream')

ult_obs <- max(serie_limpia$date)

set.seed(42)
plan(multisession, workers = 10)
future.seed = TRUE

# Division de nodos en grupos para su proxima paralelización

nodos <- serie_limpia %>%
  select(nodo_tipo) %>%
  unique()

lista_nodos <- nodos %>%
  cbind(grupo = sample(1:50,nrow(nodos),replace = T))

table(lista_nodos$grupo)

# Arma función para la selección y estimación del modelo

funcion_forecast <- function(x) {
  
  h <- 2  
  
  muestra <- serie_limpia %>% 
    filter(nodo_tipo %in% lista_nodos[lista_nodos$grupo == x,]$nodo_tipo)
  
  nested_nodos <- muestra %>% 
    nest(nested_column=-nodo_tipo)
  
  receta_nodos <- recipe(value ~ ., data = muestra %>% select(-nodo_tipo)) %>%
    step_date(date, features = c("month", "quarter", "year"), ordinal = TRUE)
  
  
  # Modelo ARIMA Boost
  
  m_arima_boosted_nodos <- workflow() %>%
    add_recipe(receta_nodos) %>%
    add_model(arima_boost() %>% set_engine(engine = "auto_arima_xgboost"))
  
  
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
    .prop = 0.80,
    m_arima_boosted_nodos,
    m_ETS_nodos,
    m_prophet_boost_nodos,
    m_nnetar_nodos
  )) 
  
  forecast_nodos <- suppressMessages(modeltime_multiforecast(
    model_table_nodos$table_time,
    .prop = 0.80
  ))
  
  
  best_model_mape <-suppressMessages(modeltime_multibestmodel(
    .table = forecast_nodos,
    .metric = "mape",
    .minimize = TRUE
  ))
  
  
  best_model_mase <- suppressMessages(modeltime_multibestmodel(
    .table = forecast_nodos,
    .metric = "mase",
    .minimize = TRUE
  ))
  
  best_model_rmse <- suppressMessages(modeltime_multibestmodel(
    .table = forecast_nodos,
    .metric = "rmse",
    .minimize = TRUE
  ))
  
  
  metricas <- rbind(best_model_mape,best_model_mase,best_model_rmse) %>%
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
  
  rm(metricas,model_table_nodos,forecast_nodos,best_model_mape,best_model_mase,best_model_rmse)
  
  model_refit<-suppressMessages(modeltime_multirefit(best_model))
  
  forecast_final <-suppressMessages(modeltime_multiforecast(
    model_refit, .h=24
  ))
  
  fct <- forecast_final %>%
    select(nodo_tipo, nested_forecast) %>%
    unnest(nested_forecast)
  
  return(fct)
  
}

# Corre la función para todos los grupos de nodos

tic()

x = c(1:50)
forecast_total <- future_map_dfr(.x = x,
                                 .f = ~funcion_forecast(x = .x),
                                 .options = furrr_options(seed = 42))

toc()

# Modifica tabla de resultados y crea ensambles

forecast <- forecast_total %>% 
  select(nodo_tipo, fecha = .index, kbps = value, modelo = .model_details) %>%
  filter(modelo != 'ACTUAL') %>% 
  mutate(kbps = ifelse(kbps < 0, 0, kbps)) %>%  
  group_by(nodo_tipo,fecha) %>% 
  mutate(  freq = length(modelo),
           kbps = mean(kbps),
           modelo = ifelse(freq == 1, modelo, 'Ensamble')
  ) %>% 
  select(nodo_tipo,fecha,kbps,modelo) %>% 
  unique() %>% 
  separate(nodo_tipo, c('nodo','tipo'), sep = '-') %>% 
  ungroup()

# Forecast nodos poca historia

poca_historia <- dbReadTable(con_tesis,'poca_historia_limpia')


h = 12
# Calcula porcentajes de prorrateo

inicial <-(2^(1/h))-1
fic <- c(1,rep(0,h))
for (i in 1:(h+1)) {
  fic[i+1] = fic[i]*(1+inicial)
}
fic = round(fic,2)-1
fic = fic[2:(h+1)]

serie_hub <- serie_limpia %>%
  separate(nodo_tipo, c('nodo', 'tipo'), sep = '-') %>%
  select(nodo, tipo, fecha = date , kbps = value) %>% 
  rbind(forecast %>% select(nodo, tipo, fecha, kbps)) %>% 
  mutate(hub = substr(nodo,1,3)) %>%  
  select(-nodo) %>% 
  filter(fecha >= as.Date(paste0(ult_obs %>% year()-1,'/04/01'))) %>% 
  group_by(fecha, hub, tipo) %>% 
  mutate(kbps = sum(kbps)) %>%
  unique()

serie_max_hub <- serie_hub %>%
  mutate(grupo = case_when(fecha <= as.Date(paste0(ult_obs %>% year(),'/03/01'))~ 0, 
                           fecha >= as.Date(paste0(ult_obs %>% year(),'/04/01')) &&
                             fecha <= as.Date(paste0(ult_obs %>% year()+1,'/03/01')) ~ 1, 
                           fecha >= as.Date(paste0(ult_obs %>% year()+1,'/04/01')) ~ 2)) %>% 
  group_by(hub, tipo, grupo) %>% 
  filter(kbps == max(kbps)) %>% 
  select(-fecha) %>% 
  arrange(hub, tipo, grupo) 

serie_max_hub <- serie_max_hub %>% 
  group_by(hub,tipo) %>% 
  mutate(cagr = round((lead(kbps,n=1)/kbps)-1,2)) %>% 
  filter(!is.na(cagr)) %>% 
  ungroup() %>% 
  select(-kbps)

forecast_pocahistoria <- poca_historia %>% 
  filter(fecha > max(fecha) %m-% months(3)) %>% 
  group_by(nodo,tipo) %>% 
  mutate(mbps = mean(kbps/1000, na.rm = T)) %>% 
  filter(utilizacion == max(utilizacion, na.rm = T)) %>% 
  select(-fecha,-kbps) %>% 
  unique()

forecast_PH_g1 <- bind_rows(replicate(12,forecast_pocahistoria , simplify = FALSE)) %>% 
  arrange(tipo,nodo) %>% 
  group_by(nodo,tipo) %>% 
  mutate(fecha = seq(as.Date(paste0(ult_obs %>% year(),'/01/01')), by = "month", length.out = 12),
         hub = substr(nodo, 1,3)) %>% 
  merge(serie_max_hub %>% filter(grupo == 0), by=c('hub','tipo'), all.x = T) %>% 
  unite('nodo_tipo',nodo,tipo,sep = '-') %>% 
  select(-hub) %>% 
  arrange(nodo_tipo,grupo,fecha)

FIC <- as.data.frame(rep(fic, times = length(unique(forecast_PH_g1$nodo_tipo))*2)) %>%  
  rename(FIC = "rep(fic, times = length(unique(forecast_PH_g1$nodo_tipo)) * 2)")

forecast_g1 <- cbind(forecast_PH_g1,FIC) %>% 
  mutate(mbps = round((1+FIC*cagr)*mbps,2)) %>% 
  select(nodo_tipo,fecha,mbps) %>% 
  separate(nodo_tipo,c('nodo','tipo'),sep = '-')

forecast_PH_g2 <- forecast_g1 %>% 
  filter(fecha == max(fecha)) %>% 
  select(nodo,tipo,mbps) %>%  
  unique()

forecast_PH_g2_1 <- bind_rows(replicate(12,forecast_PH_g2 , simplify = FALSE)) %>% 
  arrange(tipo,nodo) %>% 
  group_by(nodo,tipo) %>% 
  mutate(fecha = seq(as.Date(paste0(ult_obs %>% year()+1,'/01/01')), by = "month", length.out = 12),
         hub = substr(nodo, 1,3)) %>% 
  merge(serie_max_hub %>% filter(grupo == 1), by=c('hub','tipo'), all.x = T) %>% 
  unite('nodo_tipo',nodo,tipo,sep = '-') %>% 
  select(-hub) %>% 
  arrange(nodo_tipo,grupo,fecha)

FIC <- as.data.frame(rep(fic, times = length(unique(forecast_PH_g2_1$nodo_tipo))*2)) %>%  
  rename(FIC = "rep(fic, times = length(unique(forecast_PH_g2_1$nodo_tipo)) * 2)")

forecast_g2 <- cbind(forecast_PH_g2_1,FIC) %>% 
  mutate(mbps = round((1+FIC*cagr)*mbps,2)) %>% 
  select(nodo_tipo,fecha,mbps) %>% 
  separate(nodo_tipo,c('nodo','tipo'),sep = '-')

forecast_PH_g3 <- forecast_g2 %>% 
  filter(fecha == max(fecha)) %>% 
  select(nodo,tipo,mbps) %>%  
  unique()

forecast_PH_g3_1 <- bind_rows(replicate(12,forecast_PH_g3 , simplify = FALSE)) %>% 
  arrange(tipo,nodo) %>% 
  group_by(nodo,tipo) %>% 
  mutate(fecha = seq(as.Date(paste0(ult_obs %>% year()+2,'/01/01')), by = "month", length.out = 12),
         hub = substr(nodo, 1,3)) %>% 
  merge(serie_max_hub %>% filter(grupo == 1), by=c('hub','tipo'), all.x = T) %>% 
  unite('nodo_tipo',nodo,tipo,sep = '-') %>% 
  select(-hub) %>% 
  arrange(nodo_tipo,grupo,fecha)

FIC <- as.data.frame(rep(fic, times = length(unique(forecast_PH_g3_1$nodo_tipo))*2)) %>%  
  rename(FIC = "rep(fic, times = length(unique(forecast_PH_g3_1$nodo_tipo)) * 2)")

forecast_g3 <- cbind(forecast_PH_g3_1,FIC) %>% 
  mutate(mbps = round((1+FIC*cagr)*mbps,2)) %>% 
  select(nodo_tipo,fecha,mbps) %>% 
  separate(nodo_tipo,c('nodo','tipo'),sep = '-')

fin <- as.Date(paste0(ult_obs %>% year()+2,'-03-01'))

forecast_final_PH <- rbind(forecast_g1,forecast_g2,forecast_g3) %>%
  filter(fecha > actual & fecha <= fin ) %>%
  mutate(modelo = 'Tendencia HUB') %>% 
  unique()

fct_total <- forecast %>% 
  mutate(mbps = kbps/1000) %>% 
  select(-kbps) %>% 
  rbind(forecast_final_PH) %>% 
  unique()

