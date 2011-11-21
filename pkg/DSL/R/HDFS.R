################################################################################
## DStorage: HDFS
## The functions below implement DList operations for the
## "Hadoop Distributed File System"
################################################################################

.DMap.HDFS <- function( storage, x, MAP, parallel, ... ){

    ## SET a user specific command environment variable here
    cmdenv_arg <- NULL
    cmdenv_arg <- c( cmdenv_arg,
                     sprintf("_HIVE_FUNCTION_TO_APPLY_=%s",
                             DSL_serialize_object(MAP)) )

    rev <- .MapReduce( x, .HDFS_mapper(), cmdenv_arg = cmdenv_arg, ... )

    ## now handle part-xxxx stuff
    ## read all chunk signatures
    .fix_chunknames( DStorage(x), rev )
    rev
}


## Hadoop Streaming mapper
.HDFS_mapper <- function() {
    function(){
        ## we need this in order to let only the actual output be
        ## written to stdout, does not work with current stemDocument
        ## so not using until needed
        #sink(stderr(), type = "output")
        hive:::redirect_java_output( NULL )

        serialized <- Sys.getenv( "_HIVE_FUNCTION_TO_APPLY_" )
        MAP <- if( serialized == "" ){
            identity
        } else {
            unserialize( charToRaw(gsub("\\n", "\n", serialized, fixed = TRUE)) )
        }

        con <- file("stdin", open = "r")

        first <- TRUE
        firstkey <- NA
        lastkey <- NA
        chunk <- NA
        while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
            input <- DSL:::DSL_split_line( line )
            if( length(grep("^<<EOF-", input$key)) ){
                chunk <- as.character(input$value["Chunk"])
                break
            }
            result <- MAP( input )
            if( first ){
                firstkey <- result$key
                first <- FALSE
            }
            lastkey <- result$key
            writeLines( sprintf("%s\t%s", result$key, DSL:::DSL_serialize_object(result$value)) )
        }

        ## In the last step we need to add a stamp to this chunk
        ## <key:randomstring, value_serialized:c(firstdocumentkey,lastdocumentkey)>
        if( !is.na(lastkey) )
            writeLines( DSL:::.make_chunk_signature(firstkey, lastkey, chunk) )

        close(con)

        invisible( TRUE )
    }
}


.MapReduce <- function( x, MAP, REDUCE = NULL, ..., cmdenv_arg = NULL ) {

    x <- as.DList( x )
    rev <- .make_DSL_revision()

    ## MAP/REDUCE are functions e.g., provided by R/packages or any user defined
    ## function. It is supplied to the Rscript via an object file written to
    ## disk and exported as environment variable

    ## start the streaming job
    hive::hive_stream( MAP, REDUCE,
                       input  = file.path(DStorage(x)$base_directory, .revisions(x)[1]),
                       output = file.path(DStorage(x)$base_directory, rev),
                       cmdenv_arg = cmdenv_arg )

    ## in case the streaming job failed to create output directory return error
    stopifnot( hive::DFS_dir_exists(file.path(DStorage(x)$base_directory, rev)) )

    rev
}

.get_chunks_after_MapReduce <- function(storage, rev)
    file.path( rev, grep("part-", DSL:::DS_list_directory(storage, rev),
                              value =TRUE) )


.fix_chunknames <- function( storage, rev ){
    parts <- .get_chunks_after_MapReduce( storage, rev )
    chunks <- lapply( parts, function(part)
                     .read_chunk_signature( storage, part)$value["Chunk"] )
    names(chunks) <- parts
    ind <- !unlist( lapply(chunks, is.null) )
    ## FIXME: need to provide a DS_<> method here?
    for( part in parts[ind] )
        hive:::DFS_rename( from = file.path(DS_base_dir(storage), part),
                           to   = file.path(DS_base_dir(storage), rev, basename(chunks[[part]])) )
    invisible( TRUE )
}


