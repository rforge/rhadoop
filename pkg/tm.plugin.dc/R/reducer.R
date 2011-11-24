TermDocumentMatrix.DCorpus <- function( x, control = list() ){
    ## control contains preprocessing function, see help page of termFreq()

    ## if empty then termFreq is called with default options (e.g., when
    ## preprocessing has already be done using tm_map())
    ## otherwise call tm_map_reduce where the mapper does the preprocessing
    ## (supplied with the control argument) and the reducer
    ## makes the TDMs

    ## Start MapReduce job based on storage class
    dl <- .dc_TermDocumentMatrix( dc_storage(x), x, control )

    ## Retrieve partial results from file system (term / {key / term frequency})
    ## {} indicates serialized object
    results <- DGather( dl )



    ## first extract the terms. NOTE: they are not necessarily unique as there may be
    ## some terms duplicated among different chunks. Terms derived from the same chunk are unique.
    terms <- factor(dc_decode_term(unlist(lapply(results, function(x) x[[1]][1])) ))
    uniq_terms <- sort(unique(as.character(terms)))
    levels(terms) <- seq_along(levels(terms))

    ## unserialize count and doc data, i.e., list( term = list( c(key/doc), c(tf) ) )
    results <- lapply( results,
                       function(x) dc_unserialize_object(x[[ 1 ]][2]) )

    ## first we need to sort indices based on the row index (row major order)
    ## then we put the sparse matrix into column major order (via .fix_TDM())
    i <- rep(as.integer(terms), unlist(lapply(results, function(x) length(x[[ 1 ]]))))
    rmo <- order(i)
    .fix_TDM( tm:::.TermDocumentMatrix(
                simple_triplet_matrix(i = as.integer(i)[rmo],
                                      j = unlist(lapply(results, function(x) x[[ 1 ]]))[rmo],
                                      v = as.numeric(unlist(lapply(results, function(x) x[[ 2 ]])))[rmo],
                                      nrow = length(uniq_terms),
                                      ncol = length(x),
                                      dimnames = list(Terms = uniq_terms,
                                                      Docs = rownames(dc_get_text_mapping_from_revision(x)))),
                weighting = tm::weightTf) )
}



REDUCE <- function( keypair ){
    tryCatch( assign(keypair$value$word,
                     tm.plugin.dc:::.collector2(
                                       if(tryCatch(exists(keypair$value$word, envir = env, inherits = FALSE), error = function(x) FALSE))
                                       get(keypair$value$word, envir = env, inherits = FALSE)
                                       else
                                       NULL,
                                       list(keypair$key, keypair$value$count)
                                       ),
                           envir = env
                           ), error = function(x) FALSE )

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
    out  <- list( tm:::.TermDocumentMatrix(simple_triplet_zero_matrix(nrow = 0), weighting=tm::weightTf) )
    env <- new.env( hash = TRUE, size = 10240 )
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
        input <- dc_split_line(line)
        ## in the TDM mapper we apply termFreq on the documents,
        ## doing also all the preprocessing tasks.
        ## Note: the last entry is the DocMapping matrix

        if(!is.integer(input$value)){
            result <- termFreq(input$value, control)
            if(length(result)){
                lapply( names(result), function(x) tryCatch( assign(x, tm.plugin.dc:::.collector2(
                                                                                                  if(tryCatch(exists(x, envir = env, inherits = FALSE), error = function(x) FALSE))
                                                                                                  get(x, envir = env, inherits = FALSE)
                                                                                                  else
                                                                                                  NULL,
                                                                                                  list(as.integer(input$key), result[x])
                                                                                                  ),
                                                                    envir = env
                                                                    ), error = function(x) FALSE ) )
            }
        }
    }
    close(con)

    env <- as.list(env)
    env <- lapply(env, tm.plugin.dc:::.collector2, NULL)
    for( term in names(env) )
        writeLines( sprintf("%s\t%s", dc_encode_term(term),
                     dc_serialize_object(env[[ term ]])), con = con2 )
    close(con2)
}

dc_encode_term <- function(x)
    gsub("\n", "\\\\n", x)

dc_decode_term <- function(x)
    gsub("\\\\n", "\n", x)

.dc_TermDocumentMatrix.HDFS <- function(storage, x, control){
    cmdenv_arg <- NULL
    if( length(control) ){
        ## TODO: shouldn't we have a function to check control for sanity?
        cmdenv_arg <- sprintf("_HIVE_TERMFREQ_CONTROL_=%s", dc_serialize_object(control))
    }
    ## MAP is basically a call to termFreq
    ## REDUCE builds then the termDoc matrix
    ## TODO: implement check nreducers <= nDocs (or chunks?)
    ## FIXME: old streaming job. can be removed once new version works

    .tm_map_reduce(x,
                   .generate_TDM_mapper(),
                   .generate_TDM_reducer(),
                   cmdenv_arg = cmdenv_arg)
}

.tm_map_reduce <- function( x, MAP, REDUCE = NULL, ..., cmdenv_arg = NULL,
                            useMeta = FALSE, lazy = FALSE ) {
    stopifnot( inherits(x, "DistributedCorpus") )
    rev <- .generate_random_revision()

    ## MAP/REDUCE are functions e.g., provided by R/tm or any user defined
    ## function. It is supplied to the Rscript via an object file written to
    ## disk and exported as environment variable

    ## start the streaming job
    hive::hive_stream( MAP, REDUCE,
                      input = file.path(dc_storage(x)$base_directory,
                                        attr(x, "ActiveRevision")),
                      output = file.path(
                                   dc_storage(x)$base_directory,rev),
                      cmdenv_arg = cmdenv_arg )

    ## in case the streaming job failed to create output directory return error
    stopifnot( hive::DFS_dir_exists(file.path(
                                        dc_storage(x)$base_directory,
                                        rev)) )
    rev
}


## instead of serializing termFreq vectors we write plain text files (version 2)
.generate_TDM_mapper <- function() {
    function(){
        require("tm")
        hive:::redirect_java_output(NULL)
        ## We need to get the control argument list from the
        ## environment variables
        serialized <- Sys.getenv("_HIVE_TERMFREQ_CONTROL_")
        control <- if( serialized == "" ){
            list()
        } else {
            unserialize(charToRaw(gsub("\\n", "\n", serialized, fixed = TRUE)))
        }
        ##split_line <- tm.plugin.dc:::dc_split_line
        split_line <- function(line) {
          val <- unlist(strsplit(line, "\t"))
          list( key = val[1], value = unserialize(charToRaw(gsub("\\n", "\n", val[2], fixed = TRUE))))
        }

        con <- file("stdin", open = "r")
        while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
            input <- split_line(line)
            ## in the TDM mapper we apply termFreq on the documents,
            ## doing also all the preprocessing tasks.
            ## Note: the last entry is the DocMapping matrix
            if(!is.integer(input$value)){
                result <- termFreq(input$value, control)
                if(length(result)){
                  cat(paste(names(result), input$key, result, sep = "\t"), sep = "\n")
                }
            }
        }
        close(con)
    }
}

## Second mapper reduces term vectors per document to TDM per chunk
## can be seen as a "combiner" in Hadoop Terminology
## FIXME: obsolete?
## .generate_TDM_combiner <- function() {
##     function(){
##         require("tm")

##         split_line <- tm.plugin.dc:::dc_split_line
##         mapred_write_output <- function(key, value)
##             cat( sprintf("%s\t%s", key,
##                          tm.plugin.dc:::dc_serialize_object(value)), sep ="\n" )

##         ## initialize TDM
##         out <- list( tm:::.TermDocumentMatrix(simple_triplet_zero_matrix(nrow = 0), weighting=tm::weightTf) )
##         con <- file("stdin", open = "r")
##         while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
##             input <- split_line(line)
##             out <- c(out,
##                      list(tm:::.TermDocumentMatrix(i = seq_along(input$value),
##                                                    j = rep(1L, length(input$value)),
##                                                    v = as.numeric(input$value),
##                                                    nrow = length(input$value),
##                                                    ncol = 1L,
##                                                    dimnames = list(names(input$value),
##                                                    input$key))))
##         }
##         close(con)

##     ## key temporarily the name of the last active document
##     ## FIXME: should be replaced by sort of a checksum of the matrix
##         mapred_write_output(input$key, do.call(c, out))
##   }
## }

## using C function for collecting termFreq results (version 4)
.generate_TDM_reducer <- function() {
    function(){

        split_line <- function(line) {
          val <- unlist(strsplit(line, "\t"))
          list( word = as.character(val[1]), id = as.integer(val[2]), count = as.integer(val[3]) )
        }

        mapred_write_output <- function(key, value)
            cat( sprintf("%s\t%s", key,
                         tm.plugin.dc:::dc_serialize_object(value)), sep ="\n" )

        ## initialize environment holding words
        env <- new.env( hash = TRUE, size = 10240 )

        con <- file("stdin", open = "r")
        while( length(line <- readLines(con, n = 1L, warn = FALSE)) > 0 ) {
          input <- split_line( line )
          tryCatch( assign(input$word,
                           tm.plugin.dc:::.collector2(
                                       if(tryCatch(exists(input$word, envir = env, inherits = FALSE), error = function(x) FALSE))
                                       get(input$word, envir = env, inherits = FALSE)
                                       else
                                       NULL,
                                       list(input$id, input$count)
                                       ),
                           envir = env
                           ), error = function(x) FALSE )
        }
        close(con)

        env <- as.list(env)
        env <- lapply(env, tm.plugin.dc:::.collector2, NULL)
        for( term in names(env) )
            mapred_write_output( term, env[[ term ]] )
    }
}

## FIXME: we can do this more efficiently
.fix_TDM <- function(x){
  #x$ncol <- x$ncol + length( not_included )
  #x$dimnames$Docs <- c(x$dimnames$Docs, as.character(not_included))
  ## column major order
  cmo <- order(x$j)
  x$i <- x$i[cmo]
  x$j <- x$j[cmo]
  x$v <- x$v[cmo]
  x
}

## thanks to ceeboo: improved collector version to make reduce step run more efficiently
.collector2 <- function(x = NULL, y, ...)
      .Call("_collector2", x, y, package = "tm.plugin.dc")
