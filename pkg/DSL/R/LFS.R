################################################################################
## DStorage: LFS
## The functions below implement DList operations for the "Local File System"
################################################################################

.DMap.LFS <- function( storage, x, MAP, parallel, ... ){
    chunks <- basename( .get_chunks(x) )
    new_rev <- .make_DSL_revision()
    DS_dir_create(storage, new_rev)
    ## the active revision is always located at the beginning
    active <- .revisions( x )[1]
    LAPPLY <- if( parallel ){
        parallel::mclapply
    } else {
        lapply
    }

    LAPPLY( chunks, function( chunk )
         .LFS_mapper( MAP = MAP,
                             input  = file.path(storage$base_directory, active, chunk),
                             output = file.path(storage$base_directory, new_rev, chunk))
         )
    new_rev
}

.LFS_mapper <- function( MAP, input, output ){
    first <- TRUE
    con <- file( input, open = "r" )
    con2 <- file( output, open = "w" )
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
        input <- DSL_split_line( line )
        if( length(grep("^<<EOF-", input$key)))
            break
        result <- MAP( input )
        if(first){
            firstkey <- result$key
            first <- FALSE
        }
        lastkey <- result$key
        writeLines( sprintf("%s\t%s", result$key, DSL_serialize_object(result$value)),
                       con2 )
    }
    ## In the last step we need to add a stamp to this chunk
    ## <key:randomstring, value_serialized:c(firstdocumentkey,lastdocumentkey)>
    writeLines( .make_chunk_signature(firstkey, lastkey),
                con2 )
    close(con)
    close(con2)
    invisible( TRUE )
}
