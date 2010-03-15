TermDocumentMatrix.DistributedCorpus <- function( x, control = list() ){
    ## control contains preprocessing function, see help page of termFreq()

    ## if empty then termFreq is called with default options (e.g., when
    ## preprocessing has already be done using tm_map())
    ## otherwise call tm_map_reduce where the mapper does the preprocessing
    ## (supplied with the control argument) and the reducer
    ## makes the TDMs

    storage <- dc_get_corpus_storage(x)
    rev <- .dc_TermDocumentMatrix( storage, x, control )

    chunks <- file.path( rev,
                         grep("part-", dc_list_directory(storage, rev),
                              value =TRUE) )
    .fix_TDM( do.call( c, lapply(chunks, function(x) {
      object <- dc_read_lines(storage, x)
      dc_unserialize_object( strsplit(object, "\t")[[ 1 ]][2] ) }) ),
             attr(x, "Keys") )
}

.dc_TermDocumentMatrix <- function(storage, x, control)
    UseMethod(".dc_TermDocumentMatrix")

.dc_TermDocumentMatrix.local_disk <- function(storage, x, control){
    active <- attr(x, "ActiveRevision")
    chunks <- attr(x, "Chunks")[[active]]
    rev <- .generate_random_revision()
    dc_dir_create(storage, rev)
    for(chunk in chunks)
        .local_disk_TDM_mapreducer( control,
                      input  = file.path(storage$base_directory, active, chunk),
                      output = file.path(storage$base_directory, rev, chunk) )
    rev
}

.local_disk_TDM_mapreducer <- function( control, input, output){
    con  <- file( input, open = "r" )
    con2 <- file( output, open = "w" )
    out  <- list( tm:::.TermDocumentMatrix() )
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
        input <- dc_split_line(line)
        ## in the TDM mapper we apply termFreq on the documents,
        ## doing also all the preprocessing tasks.
        ## Note: the last entry is the DocMapping matrix
        if(!is.integer(input$value)){
            result <- termFreq(input$value, control)
            if(length(result)){
                out <- c(out, list(tm:::.TermDocumentMatrix(i=seq_along(result),
                                                  j = rep(1, length(result)),
                                                  v = as.numeric(result),
                                                  nrow = length(result),
                                                  ncol = 1,
                                                  dimnames = list(names(result),
                                                  input$key))))
            }
        }
    }
    close(con)
    writeLines( sprintf("%s\t%s",
                        input$key,
                        dc_serialize_object(do.call(c, out))), con2 )
    close(con2)
}

.dc_TermDocumentMatrix.HDFS <- function(storage, x, control){
    cmdenv_arg <- NULL
    if( length(control) ){
        ## TODO: shouldn't we have a function to check control for sanity?
        ## NOTE: the control file MUST be on a network file system mounted on
        ## every node; TODO: should go to the HDFS eventually
        control_file <- "~/tmp/_hive_termfreq_control.Rda"
        save(control, file = control_file)
        cmdenv_arg <- sprintf("_HIVE_TERMFREQ_CONTROL_=%s", control_file)
    }
    ## MAP is basically a call to termFreq
    ## REDUCE builds then the termDoc matrix
    ## TODO: implement check nreducers <= nDocs (or chunks?)
    rev <- .tm_map_reduce(x,
                          .generate_TDM_mapper(),
                          .generate_TDM_reducer(),
                          cmdenv_arg = cmdenv_arg)
    if( !is.null(cmdenv_arg) )
        unlink(control_file)
    rev
}

## TODO: we should support several 'map functions' e.g. stripWhitespace,
## stemming, etc.
.tm_map_reduce <- function( x, MAP, REDUCE = NULL, ..., cmdenv_arg = NULL,
                            useMeta = FALSE, lazy = FALSE ) {
    stopifnot( inherits(x, "DistributedCorpus") )
    rev <- .generate_random_revision()

    ## MAP/REDUCE are functions e.g., provided by R/tm or any user defined
    ## function. It is supplied to the Rscript via an object file written to
    ## disk and exported as environment variable

    ## start the streaming job
    hive::hive_stream( MAP, REDUCE,
                      input = file.path(dc_get_corpus_storage(x)$base_directory,
                                        attr(x, "ActiveRevision")),
                      output = file.path(
                                   dc_get_corpus_storage(x)$base_directory,rev),
                      cmdenv_arg = cmdenv_arg )

    ## in case the streaming job failed to create output directory return error
    stopifnot( hive::DFS_dir_exists(file.path(
                                        dc_get_corpus_storage(x)$base_directory,
                                        rev)) )
    rev
}

.generate_TDM_mapper <- function() {
    function(){
        require("tm")

        ## We need to get the control list from the environment variables
        control_file <- Sys.getenv("_HIVE_TERMFREQ_CONTROL_")

        if( file.exists(control_file) )
            load( control_file )
        else
            control <- list()

        split_line <- tm.plugin.dc:::dc_split_line
        mapred_write_output <- function(key, value)
            cat( sprintf("%s\t%s", key,
                         tm.plugin.dc:::dc_serialize_object(value)), sep ="\n" )

        con <- file("stdin", open = "r")
        while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
            input <- split_line(line)
            ## in the TDM mapper we apply termFreq on the documents,
            ## doing also all the preprocessing tasks.
            ## Note: the last entry is the DocMapping matrix
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

        split_line <- tm.plugin.dc:::dc_split_line
        mapred_write_output <- function(key, value)
            cat( sprintf("%s\t%s", key,
                         tm.plugin.dc:::dc_serialize_object(value)), sep ="\n" )

        ## initialize TDM
        ##out <- tm:::.TermDocumentMatrix()
        out <- list(tm:::.TermDocumentMatrix())
        con <- file("stdin", open = "r")
        while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
            input <- split_line(line)
            out <- c(out,
                     list(tm:::.TermDocumentMatrix(i = seq_along(input$value),
                                                   j = rep(1L, length(input$value)),
                                                   v = as.numeric(input$value),
                                                   nrow = length(input$value),
                                                   ncol = 1L,
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

## FIXME: we can do this more efficiently
.fix_TDM <- function(x, ids){
  not_included <- ids[ ! (ids %in% Docs(x)) ]
  x$ncol <- x$ncol + length( not_included )  
  x$dimnames$Docs <- c(x$dimnames$Docs, as.character(not_included))
  x <- x[, as.character(ids)]
  x <- x[sort(Terms(x)), ]
  names(x$dimnames) <- c("Terms", "Docs")
  ## column major order
  cmo <- order(x$j)
  x$i <- x$i[cmo]
  x$j <- x$j[cmo]
  x$v <- x$v[cmo]
  x
}

