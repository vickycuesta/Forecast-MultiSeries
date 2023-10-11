library(scales)
library(shiny)
library(tidyverse)
library(DBI)
library(shinythemes)
library(DT)
library(leaflet)
library(xlsx)
library(data.table)
library(lubridate)
library(shinyWidgets)
library(plotly)
library(sp)
library(bslib)
library(showtext) 
library(showtextdb)
library(sysfonts)


# Importar de tablas

coord <- read_csv('files/coordenadas.csv',show_col_types = FALSE) %>% rename(latitud = lat) %>% rename(longitud = lon)
serie_limpia <- read_csv('files/serie_limpia.csv',show_col_types = FALSE) %>% 
  mutate(mbps = kbps_corregido/1000,
         modelo = 'Historico') %>% 
  select(fecha,nodo,tipo,mbps,utilizacion,modelo)

forecast <- read_csv('files/forecast.csv', show_col_types = FALSE) %>% 
  arrange(nodo) %>% 
  mutate(modelo = case_when(grepl('ARIMA',modelo) ~ 'ARIMA con XGBOOST',
                            grepl('ETS',modelo) ~ 'ETS',
                            grepl('PROPHET',modelo) ~ 'PROPHET con XGBOOST',
                            grepl('NNAR',modelo) ~ 'RED NEURONAL',
                            TRUE ~ modelo))

nodos_fct <- forecast %>% select(nodo) %>% unique()

serie_forecast <- rbind(serie_limpia, forecast) %>% 
  mutate(mbps = as.numeric(mbps)) %>% 
  merge(coord, by = 'nodo')%>% 
  mutate(utilizacion = ifelse(utilizacion > 100, 100, utilizacion)) %>% 
  mutate(latitud = as.numeric(latitud)) %>% 
  mutate(longitud = as.numeric(longitud)) %>% 
  filter(nodo %in% nodos_fct$nodo)

fct_ant<- read_csv('files/forecast_anterior.csv',show_col_types = FALSE) %>% 
  select(fecha,tipo,nodo,mbps) %>% 
  mutate(modelo = 'Pronostico Julio 2022') %>% 
  filter(nodo %in% nodos_fct$nodo)

serie_grafico <- rbind(serie_forecast %>% 
                         select(fecha,tipo,nodo,mbps,modelo) %>% 
                         mutate(modelo = ifelse(modelo == 'Historico', 'Historico', 'Pronostico Actual')),
                       fct_ant)


total <- serie_forecast %>% 
  select(fecha,tipo,mbps,modelo) %>%
  group_by(fecha,tipo) %>%
  mutate(gbps = sum(mbps,na.rm = TRUE)/1000,
         modelo = ifelse(modelo == 'Historico','Historico','Pronostico')) %>% 
  select(-mbps) %>% 
  unique() %>% 
  rbind(fct_ant %>%
          select(-nodo) %>% 
          group_by(fecha,tipo) %>%
          mutate(gbps = sum(mbps,na.rm = TRUE)/1000) %>% 
          select(-mbps) %>% 
          unique())  

lista_nodos <- serie_forecast$nodo %>% unique() %>% unlist()

mi_tema <- bs_theme(bootswatch = "cerulean",
                    base_font = font_google("Baloo 2"))

source('modulos/Mapa.R')
source('modulos/Monitoreo.R')

ui <- fluidPage(
  theme = mi_tema,
  tabsetPanel(
    mapaUI("1.",lista_nodos),
    monitoreoUI("2.",lista_nodos)))

server <- function(input, output) {
  mapaServer("1.",serie_forecast)
  monitoreoServer("2.",total,serie_grafico)
  
}

shinyApp(ui = ui, server = server)
