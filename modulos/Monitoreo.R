monitoreoUI <- function(id, lista_nodos){
  ns<-NS(id)
  
  tabPanel(ns('Monitoreo'),
           sidebarLayout(
             sidebarPanel(
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
               pickerInput(
                 inputId =ns("nodo"),
                 label = "Nodo: ", 
                 choices = c('-',lista_nodos),
                 width = "250px",
                 options = list(
                   `live-search` = TRUE)
               ),
               width = 3  
             ),
             mainPanel(tags$h3(strong("Monitoreo de pronóstico de tráfico de internet")),
                       plotOutput(ns('graf')))))
  
}


monitoreoServer <- function(id,total,serie_grafico){
  moduleServer(
    id,
    function(input,output,session){
      observeEvent(c(input$tipo,input$nodo),{
        
        if (input$nodo == '-') {
          data <- total %>% filter(tipo == input$tipo) %>% rename(trafico = gbps) 
          titulo <- 'Tráfico total de la red HFC'
          ylab <- 'Gbps'
        } 
        
        else {
          data <- serie_grafico %>% filter(nodo == input$nodo) %>% filter(tipo == input$tipo) %>% rename(trafico = mbps)
          titulo <- paste0('Tráfico del nodo: ',input$nodo)
          ylab <- 'Mpbs'
        }
        
        output$graf <- renderPlot(
          ggplot(data, aes(x=fecha,y=trafico, color = modelo)) +
            geom_line() +
            geom_point(size = 0.7) +
            scale_x_date(labels = date_format("%m/%Y"), breaks = '1 month') +
            ggtitle(titulo)+
            xlab('Fecha') +
            ylab(ylab)  +
            guides(color = guide_legend(title = " ")) +
            scale_color_manual(values = c("#479EAD",
                                          'red',
                                          'darkolivegreen'))+
            theme(axis.text.x = element_text(angle = 45, vjust = 0.5),
                  legend.position="bottom")
        )})})}

