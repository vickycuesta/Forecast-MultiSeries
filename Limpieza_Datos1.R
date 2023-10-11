library(DBI)
library(tidyverse)
library(lubridate)
library(timetk)
library(furrr)
library(future)

serie_completa <- read_csv("files/serie_completa.csv")

set.seed(42)
plan(multisession, workers = 18)
future.seed = TRUE

# Ano
yr = today() %m-% months(1) %>% year()

# Mes
m = today() %m-% months(1) %>% month()

# Periodo
if (m <  10) periodo = paste(yr, m, sep = '_0')
if (m >= 10) periodo = paste(yr, m, sep = '_')

# Ultima fecha en esta tabla
fecha_fin <- paste(yr, m, '01', sep = '-') %>% as.Date()

series_trf <- serie_completa %>% 
  select(nodo, periodo, kbps_us, kbps_ds) %>%
  gather(key="tipo", value = kbps, kbps_us:kbps_ds) %>% 
  mutate(tipo = ifelse(tipo == "kbps_ds", "downstream", "upstream"))

# Doy formato a la fecha
series_trf = series_trf %>%
  mutate(fecha = as.Date(paste0(as.character(periodo), '01'), 
                         format = '%Y%m%d')) %>%
  filter(kbps >= 1 & kbps <= 5e7) %>% 
  select(nodo, tipo, fecha, kbps)

# Completo con observaciones faltantes (padding) 
pad_series <- series_trf %>% 
  group_by(nodo, tipo) %>% 
  pad_by_time(
    .date_var = fecha,
    .by       = 'month',
    .end_date = fecha_fin) %>% 
  mutate(nodo_tipo = paste(nodo, tipo))

# Nodos apagados ----
ult_3_meses = fecha_fin %m-% months(3)
apagados = pad_series %>%
  filter(fecha > ult_3_meses & (kbps == 0 | is.na(kbps))) %>%
     group_by(nodo_tipo) %>%
     summarise(apagado = n())%>% 
     filter(apagado == 3) %>%
     select(nodo_tipo) %>%
     unlist() %>%
     unique()
   
 if (length(apagados) > 0) { 
   nodos_apagados = series_trf %>%
     mutate(nodo_tipo = paste(nodo, tipo)) %>% 
     filter(nodo_tipo %in% apagados) %>% 
     select(-nodo_tipo)
   
   pad_series <- pad_series %>% 
     filter(nodo_tipo %in% apagados == FALSE)  
 }
 
 min_obs <- pad_series %>%
   filter(!is.na(kbps)) %>%
   group_by(nodo_tipo) %>%
   summarise(obs = n()) %>%
   filter(obs > 2) %>%
   select(nodo_tipo) %>%
   unique() %>%
   unlist()
 
 # Tabla final sin missings 
 series_pad <- pad_series %>% 
   filter(nodo_tipo %in% min_obs) %>% 
   select(-nodo_tipo) %>% 
   group_by(nodo, tipo) %>% 
   mutate(kbps = ts_impute_vec(kbps))
 
 serie_pocasobs <- pad_series %>% 
   filter(!(nodo_tipo %in% min_obs))
 
 series_complete <- rbind(series_pad,serie_pocasobs) %>% select(-nodo_tipo)
 
 # Deteccion y correccion de outliers
 
 # Paso 1: funcion para detectar anomalias 
 detectar_anomalias <- function(df, freq) {  
   suppressMessages(
     anomaly_df_prueba <- df %>%
       group_by(nodo, tipo) %>% 
       tk_anomaly_diagnostics(
         .date_var = fecha, 
         .value = kbps,
         .frequency = freq) %>% 
       mutate(fit = season + trend,
              kbps_corregido = ifelse(anomaly == 'Yes', fit, observed)) %>% 
       select(nodo, tipo, fecha, kbps = observed, anomalia = anomaly,
              kbps_corregido))
   
   return(anomaly_df_prueba) 
   
 }
 
 # Paso 2: aplicar con furrr 
 
 # nodos con 25 y + observaciones
 nobs_25 <- series_complete %>%
   mutate(nodo_tipo = paste(nodo, tipo)) %>% 
   group_by(nodo_tipo) %>%
   summarise(n = n()) %>% 
   filter(n >= 25) %>% 
   select(nodo_tipo) %>% 
   unique() %>% 
   unlist()
 
 if (length(nobs_25) > 0) {
   
   series_nobs_25 <- series_complete %>% 
     mutate(nodo_tipo = paste(nodo, tipo)) %>% 
     filter(nodo_tipo %in% nobs_25)
   
   series_nobs_25_split <- series_nobs_25 %>% 
     group_by(nodo, tipo) %>% 
     group_split()
   
   series_nobs_25_corr <- series_nobs_25_split %>% 
     future_map_dfr(
       .f       = ~detectar_anomalias(df = .x, freq = 12), 
       .options = furrr_options(seed = 42)
     )
   
   rm(series_nobs_25, series_nobs_25_split)
   
 } else {
   
   series_nobs_25_corr <- data.frame(
     nodo              = c(),
     tipo              = c(),
     fecha             = c(),
     kbps              = c(),
     anomalia          = c(),
     kbps_corregido = c()
   )
   
 }
 
 # nodos con 13+ observaciones (menos de 25)
 nobs_13 <- series_complete %>%
   mutate(nodo_tipo = paste(nodo, tipo)) %>% 
   group_by(nodo_tipo) %>%
   summarise(n = n()) %>% 
   filter(n >= 13 & n < 25) %>% 
   select(nodo_tipo) %>% 
   unique() %>% 
   unlist()
 
 if (length(nobs_13) > 0) {
   
   series_nobs_13 <- series_complete %>% 
     mutate(nodo_tipo = paste(nodo, tipo)) %>% 
     filter(nodo_tipo %in% nobs_13)
   
   series_nobs_13_split <- series_nobs_13 %>% 
     group_by(nodo, tipo) %>% 
     group_split()
   
   series_nobs_13_corr <- series_nobs_13_split %>% 
     future_map_dfr(
       .f       = ~detectar_anomalias(df = .x, freq = 6), 
       .options = furrr_options(seed = 42)
     )
   
   rm(series_nobs_13, series_nobs_13_split)
   
 } else {
   
   series_ds_nobs_13_corr <- data.frame(
     nodo              = c(),
     tipo              = c(),
     fecha             = c(),
     kbps              = c(),
     anomalia          = c(),
     kbps_corregido    = c()
   )
   
 }
 # Serie con poca historia 
 # nodos menos de 13 obs
 nobs_pocahist <- series_complete %>%
   mutate(nodo_tipo = paste(nodo, tipo)) %>% 
   group_by(nodo_tipo) %>%
   summarise(n = n()) %>% 
   filter(n < 13) %>% 
   select(nodo_tipo) %>% 
   unique() %>% 
   unlist()
 
 series_nobs_pocahist <- series_complete %>% 
   mutate(nodo_tipo = paste(nodo, tipo)) %>% 
   filter(nodo_tipo %in% nobs_pocahist) %>% 
   select(-nodo_tipo)
 
 # Esto es lo que se exporta 
 series_clean <- rbind(series_nobs_25_corr, series_nobs_13_corr)
 
 series_clean <- series_clean %>% 
   mutate(kbps_corregido = ifelse(kbps_corregido < 0, kbps, kbps_corregido))
 
 # Agrega utilizacion a las series
 serie_utilizacion <- serie_completa %>% 
   select(nodo, fecha = periodo, upstream = utilizacion_us, downstream = utilizacion_ds) %>% 
   gather('tipo','utilizacion',3:4) %>% 
   mutate(fecha = as.Date(paste0(substr(fecha,1,4),'-',substr(fecha,5,6),'-01')))
 
 serie_limpia_final <- merge(series_clean,
                             serie_utilizacion, 
                             by = c("nodo", "tipo", "fecha"), all.x = T) %>% 
   mutate(fecha = as.character(fecha), utilizacion = round(utilizacion))
 
 serie_PH_final <- merge(series_nobs_pocahist,
                         serie_utilizacion, 
                         by = c("nodo", "tipo", "fecha"), all.x = T)%>% 
   mutate(fecha = as.character(fecha), utilizacion = round(utilizacion))
 
 
 write_csv(serie_limpia_final,'files/serie_limpia.csv') 
 write_csv(serie_PH_final,'files/serie_poca_historia.csv')
       