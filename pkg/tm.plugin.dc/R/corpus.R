## Authors: Ingo Feinerer, Stefan Theussl

## "DCorpus" class
.DCorpus <- function( x, active_revision, cmeta, dmeta ) {
  attr( x, "CMetaData" )      <- cmeta
  attr( x, "DMetaData" )      <- dmeta
  class( x )                  <- c( "DCorpus", "DList", "Corpus", "list" )
  x
}

DistributedCorpus <-
DCorpus <- function( x,
                    readerControl = list(reader   = x$DefaultReader,
                    language = "eng"),
                    storage = NULL, keys = NULL, ... ) {
    ## For the moment we
    ##   - only support a directory as source (DirSource)
    ## TODO: add DList source
    if( !inherits(x, "DirSource") )
            stop("unsupported source type (use DirSource instead)")

    readerControl <- tm:::prepareReader(readerControl, x$DefaultReader, ...)

    if (is.function(readerControl$init))
        readerControl$init()

    if (is.function(readerControl$exit))
        on.exit(readerControl$exit())

    if (x$Vectorized){
        elem <- pGetElem(x)
        if (is.null(x$Names))
            names(elem) <- x$Names
        tdl <- DMap(as.DList(elem, DStorage = storage), function(keypair) list(key = keypair$key, value = readerControl$reader(keypair$value, readerControl$language, keypair$key)) )
    }
    else
        stop( "Non-vectorized operation not yet implemented.")

    names(tdl) <- x$Names
    df <- data.frame(MetaID = rep(0, length(tdl)), stringsAsFactors = FALSE)
    .DCorpus( tdl, DSL:::.revisions(tdl)[1], tm:::.MetaDataNode(), df )
}


print.DCorpus <- function(x, ...) {
    cat("DCorpus. ")
    tm:::print.Corpus( x, ... )
}


as.DistributedCorpus <-
as.DCorpus <- function(x, storage = NULL, ...){
  UseMethod("as.DCorpus")
}

as.DCorpus.DCorpus <- function(x, storage = NULL, ...){
    identity(x)
}

as.DCorpus.Corpus <- function(x, storage = NULL, ...){
    dl <- as.DList(x, DStorage = storage, ...)
    .DCorpus( dl,
              active_revision = DSL:::.revisions(dl)[1],
              cmeta = CMetaData(x),
              dmeta = DMetaData(x) )
}


as.Corpus <- function( x ){
    UseMethod("as.Corpus")
}

as.Corpus.Corpus <- identity

as.Corpus.DCorpus <- function( x ){
    tm:::.VCorpus( as.list(x), CMetaData(x), DMetaData(x) )
}

DMetaData.DCorpus <- function( x )
    attr(x, "DMetaData")

summary.DCorpus <- function( object, ... ) {
    tm:::summary.Corpus( object )
    cat( "\nDCorpus revisions:\n" )
    cat( strwrap(paste(unlist(getRevisions(object)), collapse = " "), indent = 2, exdent = 2), "\n" )
    cat( sprintf("DCorpus active revision: %s\n\n", DSL:::.revisions(object)[1]) )
    print( DStorage(object) )
}

## Get all available revisions from the DC
getRevisions <- function( corpus ){
    DSL:::.revisions( corpus )
}

## Set active revision in the DC to the specified revision
setRevision <- function( corpus, revision ){
    pos <- as.character(revision) == getRevisions(corpus)
    if( !any(pos) )
        warning( "invalid revision" )
    DSL:::.revisions( corpus ) <- c( revision, getRevisions(corpus)[!pos] )
    invisible(corpus)
}

## remove a given revision
removeRevision <- function( corpus, revision ){
    pos <- revision == getRevisions(corpus)
    if( !any(pos) )
        stop( "Revision to remove does not exist." )
    DSL:::.revisions(corpus) <- getRevisions(corpus)[!pos]
    invisible( corpus )
}

## as.DList.DirSource <- function( x, DStorage = NULL, ... ){
##     if( is.null(DStorage) )
##         DStorage <- DS_default()
##     ## we like to have one file in one chunk so that
##     DStorage$chunksize = 1L
##     as.DList( as.list(x$FileList), DStorage = DStorage, ... )
## }

