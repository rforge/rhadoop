
TermDocumentMatrix.DistributedCorpus <- function( x, control = list() ){
  ## control contains preprocessing function, see help page of termFreq()

  ## if empty then termFreq is called with default options (e.g., when preprocessing has already be done using tm_map())
  ## otherwise call tm_map_reduce where the mapper does the preprocessing (supplied with the control argument) and the reducer
  ## makes the TDMs
  cmdenv_arg <- NULL
  if( length(control) ){
    ## TODO: shouldn't we have a function to check control for sanity?
    ## NOTE: the control file MUST be on a network file system mounted on every node
    control_file <- "~/tmp/_hive_termfreq_control.Rda"
    save(control, file = control_file)
    cmdenv_arg <- sprintf("_HIVE_TERMFREQ_CONTROL_=%s", control_file)
  }
  ## MAP is basically a call to termFreq
  ## REDUCE builds then the termDoc matrix
  revision <- .tm_map_reduce(x, .generate_TDM_mapper(), .generate_TDM_reducer(), cmdenv_arg = cmdenv_arg)
  if(!is.null(cmdenv_arg))
    unlink(control_file)
  tdm <- tm:::.TermDocumentMatrix()
  chunks <- grep("part-", DFS_list(revision), value = TRUE)
  for(chunk in chunks){
    object <- hive:::DFS_read_lines3( file.path(revision, chunk), henv = hive() )
    value <- strsplit( object, "\t" )[[ 1 ]][2]
    new <- unserialize(charToRaw(gsub("\\n", "\n", value, fixed = TRUE)))
    tdm <- c( tdm, new )
  }
  tdm
}


## TODO: we should support several 'map functions' e.g. stripWhitespace, stemming, etc.
.tm_map_reduce <- function(x, MAP, REDUCE = NULL, ..., cmdenv_arg = NULL, useMeta = FALSE, lazy = FALSE) {
  stopifnot(inherits(x, "DistributedCorpus"))
  rev <- tempfile()
  ## start the streaming job
  hive_stream(MAP, REDUCE, input = attr(x, "ActiveRevision"), output = rev,
              cmdenv_arg = cmdenv_arg)
  ## in case the streaming job failed to create output directory return an error
  stopifnot(DFS_dir_exists(rev))
  rev
}

.generate_TDM_mapper <- function() {
  function(){
    require("tm")

    ## We need to get the control list from the environment variables
    control_file <- Sys.getenv("_HIVE_TERMFREQ_CONTROL_")
               
    if(file.exists(control_file))
      load(control_file)
    else
      control <- list()
    
    split_line <- function(line) {
      val <- unlist(strsplit(line, "\t"))
      list(key = val[1], value = unserialize(charToRaw(gsub("\\n", "\n", val[2], fixed = TRUE))))
    }

    mapred_write_output <- function(key, value)
      cat(paste(key, gsub("\n", "\\n", rawToChar(serialize(value, NULL, TRUE)), fixed = TRUE), sep = "\t"), sep = "\n")
    
    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
      input <- split_line(line)

      ## in the TDM mapper we apply termFreq on the documents, doing also all the
      ## preprocessing tasks. Note: the last entry is the DocMapping (=environment)
      if(!is.environment(input$value)){
        result <- termFreq(input$value, control)
        if(length(result)){
          mapred_write_output(ID(input$value), result)
        }
      }
    }
    close(con)
    
  }
}


## currently the tm reducer only makes TDMs out of the input (documents)
## NOTE: Works only for term-document matrices (and NOT document-term matrices)
.generate_TDM_reducer <- function() {
  function(){
    require("tm")
    
    split_line <- function(line) {
      val <- unlist(strsplit(line, "\t"))
      list(key = val[1], value = unserialize(charToRaw(gsub("\\n", "\n", val[2], fixed = TRUE))))
    }
    
    mapred_write_output <- function(key, value)
      cat(paste(key, gsub("\n", "\\n", rawToChar(serialize(value, NULL, TRUE)), fixed = TRUE), sep = "\t"), sep = "\n")

    c_TermDocumentMatrix <-function(m, docID, tf) {
  
      m$dimnames <- list(Terms = c(Terms(m), setdiff(names(tf), Terms(m))),
                         Docs = c(Docs(m), docID))
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
      
      out <- c_TermDocumentMatrix(out, input$key, input$value)
    }
    close(con)
    
    ## key temporarily the name of the last active document
    ## FIXME: should be replaced by sort of a checksum of the matrix
    mapred_write_output(input$key, out)
  }
}
