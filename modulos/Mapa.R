# Rango de fechas
fecha_min <- ymd('2023-04-01')
fecha_max <- ymd('2025-04-01')

# Obtener lista de fechas mensuales
fechas <- seq(fecha_min, fecha_max, by = '1 month')

mapaUI <- function(id,lista_nodos){
  ns <- NS(id)
  tabPanel(ns("Mapa de nodos"),
           titlePanel("Red HFC"),
           sidebarLayout(
             sidebarPanel(strong('Filtros:'),
                          radioGroupButtons(
                            inputId = ns("tipo"),
                            label = "",
                            choices =  c('Downstream'="downstream",'Upstream'="upstream") ,
                            justified = TRUE,
                            size = "sm",
                            width = "250px",
                            checkIcon = list(
                              yes = icon("ok", 
                                         lib = "glyphicon"),
                              no = icon("remove",
                                        lib = "glyphicon"))
                          ),
                          dateInput(ns('fecha'), 
                                    "Fecha:",
                                    min    = "2023-04-01",
                                    max    = "2025-04-01",
                                    format = "mm/yyyy",
                                    value = "2023-04-01"),
                          pickerInput(
                            inputId =ns("nodo"),
                            label = "Nodo: ", 
                            choices = c('-',lista_nodos),
                            width = "250px",
                            options = list(
                              `live-search` = TRUE)
                          ),
                          sliderInput(ns("util"),
                                      "% de Utilización:",
                                      0,
                                      100,
                                      value = c(0,100),
                                      width = "250px"),
                          
                          p(strong('Descargas:')),
                          p(' '),
                          downloadButton(ns("descarga"), 
                                         "Pronostico", 
                                         class="btn btn-primary"),
                          
                          width = 3,
                          fluid = T),
             
             mainPanel( 
               leafletOutput(ns("mapa"),height = 450),
               p(' '),
               plotOutput(ns('graf')))))
}


mapaServer <- function(id,serie_forecast) {
  moduleServer(
    id,
    function(input,output,session){
      output$mapa = renderLeaflet({
        req(input$nodo)
        
        fecha_filtro <-input$fecha + days(1 - day(input$fecha))
        
        if (input$nodo == '-') {
          datos <- serie_forecast %>% 
            filter(tipo == input$tipo) %>% 
            filter(fecha == fecha_filtro) %>% 
            filter(utilizacion >= input$util[1] & utilizacion <= input$util [2]) 
        } else {
          datos <- serie_forecast %>% 
            filter(nodo == input$nodo) %>% 
            filter(tipo == input$tipo) %>% 
            filter(fecha == fecha_filtro) %>% 
            filter(utilizacion >= input$util[1] & utilizacion <= input$util[2]) 
        }
        
        pal <- colorNumeric(c("green4","#FFD700", "#CD3333"), c(0,100), reverse = F)
        
        leaflet() %>%
          addTiles() %>%  
          addCircleMarkers(data = datos %>% arrange(utilizacion),
                           lat = ~latitud,
                           lng = ~longitud,
                           color = ~pal(utilizacion),
                           stroke = FALSE, 
                           fillOpacity = 0.75,
                           label=~paste0(round(utilizacion,2)," % de utilización en ",tipo,", Nodo: ", nodo),
                           layerId = ~ nodo
          ) %>%
          addLegend(pal = pal, values = c(0,100), opacity = 1.0, title = "% de Utilización",
                    position = "bottomright") 
        
      })
      
      # ------- Gráfico --------
      
      observeEvent(c(input$mapa_marker_click, 
                     input$tipo1), {
                       
                       p <- input$mapa_marker_click
                       click_nodo <- p$id
                       
                       if (is.null(click_nodo)){
                         return()}
                       
                       nodo = serie_forecast %>% 
                         filter(tipo == input$tipo) %>%
                         select(nodo, fecha, mbps , modelo) %>%
                         filter(nodo == click_nodo)
                       
                       title = paste0('Nodo: ', click_nodo,' - ',input$tipo)
                       output$graf <- renderPlot(
                         ggplot(nodo, aes(x=fecha,y=mbps, color = modelo)) +
                           geom_line() +
                           geom_point(size = 0.7) +
                           scale_x_date(labels = date_format("%m/%Y"), breaks = '1 month') +
                           ggtitle(title)+
                           xlab('Fecha') +
                           ylab('Mbps')  +
                           guides(color = guide_legend(title = " ")) +
                           scale_color_manual(values = c("#479EAD",
                                                         'red'))+
                           theme(axis.text.x = element_text(angle = 45, vjust = 0.5),
                                 legend.position="bottom")
                       )})})}

