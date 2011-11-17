################################################################################
## This file defines the map/lapply function on DList objects
## DMap is conceptually based on Google's MapReduce paradigm
## DLapply behaves like R's lapply
################################################################################

################################################################################
## DLapply function
################################################################################

DLapply <- function( x, FUN, parallel, ..., keep = FALSE ){

    MAP <- function( keypair )
        list( key = keypair$key, value = FUN(keypair$value) )
    DMap( x = x, MAP = MAP, parallel = parallel, ..., keep = keep )

}

## (
## idea: DSL constructor might include a modifyable plain text
## reader to read a bunch of documents
## )

################################################################################
## DMap function
################################################################################

DMap <- function( x, MAP, parallel, ..., keep = FALSE ){
    x <- as.DList( x )
    if( missing(parallel) )
        parallel <- FALSE
    ## HDFS is always parallel since we cannot easily control parallel
    ## execution on Hadoop clusters
    if( inherits(DStorage(x), "HDFS") )
        parallel <- TRUE
    new_rev <- .DMap( DStorage(x), x = x, MAP = MAP, parallel = parallel, ... )
    if( keep )
        #### FIX HERE ####
        out <- .update_DSL( x, new_rev )
    else
        out <- .DList( list(),
                       .make_chunk_handler(file.path(new_rev, basename(.get_chunks( x ))),
                                                     new_rev,
                                                     DStorage(x)),
                                 attr( x, "Keys" ),
                                 attr( x, "Mapping" ),
                                 DStorage( x )
                                 )
    out
}

################################################################################
## .DMap methods (depend on storage type)
################################################################################

.DMap <- function( storage, x, FUN, parallel, ... )
    UseMethod( ".DMap" )
