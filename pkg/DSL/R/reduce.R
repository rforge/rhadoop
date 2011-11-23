################################################################################
## This file defines the reduce function on DList objects
## DReduce() is conceptually based on Google's MapReduce paradigm
################################################################################

################################################################################
## DReduce function
################################################################################

DReduce <- function( x, REDUCE, parallel, ..., keep = FALSE ){
    x <- as.DList( x )
    if( missing(parallel) )
        parallel <- FALSE
    ## HDFS is always parallel since we cannot easily control parallel
    ## execution on Hadoop clusters
    if( inherits(DStorage(x), "HDFS") )
        parallel <- TRUE

    stop( "Not implemented yet" )
    new_rev <- .DReduce( DStorage(x), x = x, REDUCE = REDUCE, parallel = parallel, ... )

    if( keep )
        #### FIXME: o currently this has some possibly unforeseen side effects
        ####        o garbage collection
        out <- .DList_add_revision( x, new_rev )
    else
        out <- .DList( list(),
                       .make_chunk_handler(file.path(new_rev, basename(.get_chunks( x ))),
                                           new_rev,
                                           DStorage(x)),
                       attr( x, "Keys" ),
                       attr( x, "Mapping" ),
                       DStorage( x )
                      )
    attr( out, "Keys") <- .get_keys_from_current_revision( out )
    out
}

################################################################################
## .DReduce methods (depend on storage type)
################################################################################

.DReduce <- function( storage, x, FUN, parallel, ... )
    UseMethod( ".DReduce" )

