tm_map.DistributedCorpus <- function(x, FUN, ..., cmdenv_arg = NULL, useMeta = FALSE, lazy = FALSE) {
  tm_map_reduce(x, FUN, cmdenv_arg = cmdenv_arg, useMeta = useMeta, ...)
}

.generate_tm_mapper <- function() {
  function(){
    require("tm")
    fun <- match.fun(Sys.getenv("_HIVE_FUNCTION_TO_APPLY_"))    

    split_line <- function(line) {
      val <- unlist(strsplit(line, "\t"))
      list(key = val[1], value = unserialize(charToRaw(gsub("\\n", "\n", val[2], fixed = TRUE))))
    }

    mapred_write_output <- function(key, value)
      cat(paste(key, gsub("\n", "\\n", rawToChar(serialize(value, NULL, TRUE)), fixed = TRUE), sep = "\t"), sep = "\n")
    
    ## very important: hash table for this chunk (necessary to create mapping between part-x and original texts)
    mapping <- new.env()
    chunkname <- paste(paste(sample(c(letters, 0:9), 10, replace = TRUE), collapse = ""), system("hostname", intern = TRUE), sep = "-")
    position <- 1L

    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
      input <- split_line(line)
      
      result <- fun(input$value)
      if(length(result)){
        mapping[[input$key]] <- c(chunk = chunkname, position = position)
        position <- position + 1L
        mapred_write_output(input$key, result)
      }
    }
    mapred_write_output(chunkname, mapping)
    close(con)
    
  }
}
