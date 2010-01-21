
TermDocumentMatrix.DistributedCorpus <- function( x, control = list() ){
  ## control contains preprocessing function, see help page of termFreq()

  ## if empty then termFreq is called with default options (e.g., when
  ## preprocessing has already be done using tm_map())
  ## otherwise call tm_map_reduce where the mapper does the preprocessing
  ## (supplied with the control argument) and the reducer
  ## makes the TDMs
  cmdenv_arg <- NULL
  if( length(control) ){
    ## TODO: shouldn't we have a function to check control for sanity?
    ## NOTE: the control file MUST be on a network file system mounted on
    ## every node
    control_file <- "~/tmp/_hive_termfreq_control.Rda"
    save(control, file = control_file)
    cmdenv_arg <- sprintf("_HIVE_TERMFREQ_CONTROL_=%s", control_file)
  }
  ## MAP is basically a call to termFreq
  ## REDUCE builds then the termDoc matrix
  ## TODO: implement check nreducers <= nDocs 
  revision <- .tm_map_reduce(x,
                             .generate_TDM_mapper(),
                             .generate_TDM_reducer(),
                             cmdenv_arg = cmdenv_arg)
  if(!is.null(cmdenv_arg))
    unlink(control_file)
  chunks <- file.path(revision, grep("part-", DFS_list(revision), value = TRUE))
  tdms <- lapply( chunks, function(x) {
      object <- hive:::DFS_read_lines3(x, henv = hive())
      value <- strsplit( object, "\t" )[[ 1 ]][2]
      unserialize(charToRaw(gsub("\\n", "\n", value, fixed = TRUE))) }
                 )
  do.call(c, tdms)
}


## TODO: we should support several 'map functions' e.g. stripWhitespace,
## stemming, etc.
.tm_map_reduce <- function(x, MAP, REDUCE = NULL, ..., cmdenv_arg = NULL,
                           useMeta = FALSE, lazy = FALSE) {
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
      list( key   = as.integer(val[1]),
            value = unserialize(charToRaw(gsub("\\n", "\n", val[2], fixed = TRUE))))
    }

    mapred_write_output <- function(key, value)
      cat(paste(key, gsub("\n", "\\n", rawToChar(serialize(value, NULL, TRUE)), fixed = TRUE), sep = "\t"), sep = "\n")
    
    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
      input <- split_line(line)

      ## in the TDM mapper we apply termFreq on the documents, doing also all the
      ## preprocessing tasks. Note: the last entry is the DocMapping (=environment)
      if(!is.integer(input$value)){
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
        
    ## initialize TDM
    #out <- tm:::.TermDocumentMatrix()
    out <- list(tm:::.TermDocumentMatrix())
    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
      input <- split_line(line)
      out <- c(out,
               list(tm:::.TermDocumentMatrix(i = seq_along(input$value),
                                             j = rep(1, length(input$value)),
                                             v = as.numeric(input$value),
                                             nrow = length(input$value),
                                             ncol = 1,
                                             dimnames = list(names(input$value),
                                                             input$key))))
#      new <- tm:::.TermDocumentMatrix(i = seq_along(input$value),
#                                      j = rep(1, length(input$value)),
#                                      v = as.numeric(input$value),
#                                      nrow = length(input$value),
#                                      ncol = 1,
#                                      dimnames = list(names(input$value), input$key))
#      out <- tm:::c.TermDocumentMatrix1(out, new)
    }
    close(con)
    
    ## key temporarily the name of the last active document
    ## FIXME: should be replaced by sort of a checksum of the matrix
   mapred_write_output(input$key, do.call(c, out))
  }
}
