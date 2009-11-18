
TermDocumentMatrix.DistributedCorpus <- function( x, control = list() ){
  ## control contains preprocessing function, see help page of termFreq()

  ## if empty then reduce only (e.g., when preprocessing has already be done with tm_map)
  ## otherwise call tm_map_reduce where the mapper does the proprocessing and the reducer
  ## makes the TDMs
  
}


## TODO: we should support several 'map functions' e.g. stripWhitespace, stemming, etc.
.tm_map_reduce <- function(x, MAP, REDUCE = NULL, ..., cmdenv_arg = NULL, useMeta = FALSE, lazy = FALSE) {
    stopifnot(inherits(x, "DistributedCorpus"))
    rev <- tempfile()
    cmdenv_arg <- c(cmdenv_arg, sprintf("_HIVE_FUNCTION_TO_APPLY_=%s", as.character(substitute(FUN))))
    ## start the streaming job
    hive_stream(.generate_tm_mapper(), #hive:::hadoop_generate_mapper("tm", deparse(substitute(FUN))),
                input = attr(x, "ActiveRevision"), output = rev,
                cmdenv_arg = cmdenv_arg)
    ## in case the streaming job failed to create output directory return an error
    stopifnot(DFS_dir_exists(rev))
    ## add new revision to corpus meta info
    attr(x, "Revisions") <- c(attr(x, "Revisions"), rev)
    ## update ActiveRevision in dc
    x <- updateRevision(x, rev)
    x
}

## currently the tm reducer only makes TDMs out of the input (documents)
## NOTE: Works only for term-document matrices (and NOT document-term matrices)
.generate_tm_reducer <- function() {
  function(){
    require("tm")

    split_line <- function(line) {
      val <- unlist(strsplit(line, "\t"))
      list(key = val[1], value = unserialize(charToRaw(gsub("\\n", "\n", val[2], fixed = TRUE))))
    }

    mapred_write_output <- function(key, value)
      cat(paste(key, gsub("\n", "\\n", rawToChar(serialize(value, NULL, TRUE)), fixed = TRUE), sep = "\t"), sep = "\n")

    c_TermDocumentMatrix <-function(m, doc, control = list()) {
      tf <- termFreq(doc, control)
      
      m$dimnames <- list(Terms = c(Terms(m), setdiff(names(tf), Terms(m))),
                         Docs = c(Docs(m), ID(doc)))
      m$nrow <- length(Terms(m))
      m$ncol <- length(Docs(m))
      
      m$i <- c(m$i, which(Terms(m) %in% names(tf)))
      m$j <- c(m$j, rep(nDocs(m), length(tf)))
      m$v <- c(m$v, tf)
      
      m
    }

    ## initialize TDM
    out <- tm:::.TermDocumentMatrix()
    
    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
      input <- split_line(line)
      
      out <- c_TermDocumentMatrix(out, input$value)
    }
    mapred_write_output(chunkname, mapping)
    close(con)

    ## key temporarily the name of the last active document
    ## FIXME: should be replaced by sort of a checksum of the matrix
    mapred_write_output(input$key, out)
  }
}
