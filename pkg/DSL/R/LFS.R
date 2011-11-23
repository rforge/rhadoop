################################################################################
## DStorage: LFS
## The functions below implement DList operations for the "Local File System"
################################################################################

.DMap.LFS <- function( storage, x, MAP, parallel, ... ){

    new_rev <- .make_DSL_revision()
    DS_dir_create(storage, new_rev)

    LAPPLY <- if( parallel ){
        parallel::mclapply
    } else {
        lapply
    }

    LAPPLY( basename(.get_chunks(x)), function( chunk )
            .LFS_mapper( MAP = MAP,
                         input  = file.path(storage$base_directory, .revisions( x )[1], chunk),
                         output = file.path(storage$base_directory, new_rev, chunk)),
            ...
         )

    new_rev
}

.LFS_mapper <- function( MAP, input, output ){

    con <- file( input, open = "r" )
    con2 <- file( output, open = "w" )

    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
        input <- DSL_split_line( line )
        if( length(grep("^<<EOF-", input$key)) ){
            chunk <- as.character(input$value["Chunk"])
            break
        }

        result <- MAP( input )

        ## FIXME: should be an object oriented approach here
        if( length(result) > 2 )
            for( i in seq_along(result) )
                writeLines( sprintf("%s\t%s", result[[i]]$key, DSL_serialize_object(result[[i]]$value)),
                           con2 )
        else
            writeLines( sprintf("%s\t%s", result$key, DSL_serialize_object(result$value)),
                           con2 )
    }
    ## In the last step we need to add a stamp to this chunk
    ## <key:randomstring, value_serialized:c(firstdocumentkey,lastdocumentkey)>
    writeLines( .make_chunk_signature( chunk ),
                con2 )
    close(con)
    close(con2)

    invisible( TRUE )
}
