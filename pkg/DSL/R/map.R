################################################################################
## This file defines the map/lapply function on DList objects
## DMap is conceptually based on Google's MapReduce paradigm
## DLapply behaves like R's lapply
################################################################################

################################################################################
## DLapply function
################################################################################

DLapply <- function( x, FUN, parallel, ..., keep = FALSE ){
    foo <- match.fun(FUN)
    MAP <- function( keypair )
        list( key = keypair$key, value = foo(keypair$value) )
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
## .DMap methods (depend on storage type)
################################################################################

.DMap <- function( storage, x, FUN, parallel, ... )
    UseMethod( ".DMap" )

## not yet used:
DPair <- function( key, value )
    list( key = make.names(key), value = value )

.get_keys_from_current_revision <- function( x ){
    structure( unlist(DGather( x, keys = TRUE)), names = NULL )
}

## updates given list with new revision
.DList_add_revision <- function( x, rev ){
    ## add new revision
    chunks <- .get_chunks_from_current_revision( x )
    assign(rev, file.path(rev, basename(chunks)), envir = attr(x, "Chunks"))
    .revisions( x ) <- c( rev, .revisions(x) )
    ## update to active revision
    x
}
