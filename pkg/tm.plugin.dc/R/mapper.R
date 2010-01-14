
tm_map.DistributedCorpus <- function(x, FUN, ..., cmdenv_arg = NULL, useMeta = FALSE, lazy = FALSE) {
  ## FUN is a function e.g., provided by R/tm or any user defined function.
  ## It is supplied to the Rscript via an object file written to disk and exported as environment variable

  ## TODO: shouldn't we have a function to check provided function for sanity?
  ## FIXME: this file MUST be on a network file system mounted on every node
  foo_file <- "~/tmp/_hive_map_function.Rda"
  save(FUN, file = foo_file)
  
  cmdenv_arg <- c(cmdenv_arg, sprintf("_HIVE_FUNCTION_TO_APPLY_=%s", foo_file))
  rev <- .tm_map_reduce(x, .generate_tm_mapper(), cmdenv_arg = cmdenv_arg, useMeta = useMeta, ...)

  unlink(foo_file)
  ## add new revision to corpus meta info
  attr(x, "Revisions") <- c(attr(x, "Revisions"), rev)
  ## update ActiveRevision in dc
  x <- updateRevision(x, rev)
  x
}

.generate_tm_mapper <- function() {
  function(){
    require("tm")
    load(Sys.getenv("_HIVE_FUNCTION_TO_APPLY_"))

    split_line <- function(line) {
      val <- unlist(strsplit(line, "\t"))
      list(key = as.integer(val[1]), value = unserialize(charToRaw(gsub("\\n", "\n", val[2], fixed = TRUE))))
    }

    mapred_write_output <- function(key, value)
      cat(paste(key, gsub("\n", "\\n", rawToChar(serialize(value, NULL, TRUE)), fixed = TRUE), sep = "\t"), sep = "\n")

    first <- TRUE
    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
      input <- split_line(line)
      result <- FUN(input$value)
      if(first){
        firstkey <- input$key
        first <- FALSE
      }
        
      if(is.character(result)){
        mapred_write_output(input$key, result)
      } else
        mapred_write_output(input$key, PlainTextDocument(id = ID(result)))
    }
    
    ## In the last step we need to add a stamp to this chunk
    ## <key:randomstring, value_serialized:c(firstdocumentkey,lastdocumentkey)>
    mapred_write_output(paste(paste(sample(c(letters, 0:9), 10, replace = TRUE),
                             collapse = ""), system("hostname", intern = TRUE), sep = "-"),
                        c(First_key = firstkey,
                    Last_key = input$key))
    close(con)
  }
}
