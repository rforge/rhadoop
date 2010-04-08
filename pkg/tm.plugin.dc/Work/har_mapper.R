library("hive")

hadoop_home <- sprintf( "/home/%s/tmp/hadoop/hadoop_%s", system("whoami", intern = TRUE), "8414" )
hive( hive_create(hadoop_home) )
hive_start()

library("tm.plugin.dc")

mapper <- function() {
  require("tm")
  split_line <- function (line) 
    {
      val <- unlist(strsplit(line, "\t"))
      list(key = val[1], value = val[2])
    }
  
  mapred_write_output <- function(key, value) cat(sprintf("%s\t%s", 
                                                          key, tm.plugin.dc:::dc_serialize_object(value)), 
                                                  sep = "\n")
  first <- TRUE
  con <- file("stdin", open = "r")
  while(length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
    input <- split_line(line)
    mapred_write_output(nchar(input$key), input$key)
  }
}
         
hive::hive_stream(mapper, NULL, input = "/tmp/minimal.har", output = "/user/stheussl/out2")
