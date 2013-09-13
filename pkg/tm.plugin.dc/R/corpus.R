## Authors: Ingo Feinerer, Stefan Theussl

## "DCorpus" class
.DCorpus <- function( x, keep, cmeta, dmeta ) {
  attr( x, "CMetaData" )      <- cmeta
  attr( x, "DMetaData" )      <- dmeta
  ## use revisions? Default: TRUE. This can be turned off using
  ## setRevision() replacement function.
  if( missing(keep) )
      keep <- TRUE
  attr( x, "keep" )           <- keep
  class( x )                  <- c( "DCorpus", "DList", "Corpus", "list" )
  x
}

DistributedCorpus <-
DCorpus <- function( x,
                    readerControl = list(reader = x$DefaultReader,
                                         language = "en"),
                    storage = NULL, keep = TRUE, ... ) {
    ## For the moment we
    ##   - only support a directory as source (DirSource)
    ## TODO: add DList source
    ## FIXME: in earlier versions DCorpus had a keys argument for supplying user chosen keys
    if( !inherits(x, "DirSource") )
            stop("unsupported source type (use DirSource instead)")

    if (is.null(readerControl$reader))
        readerControl$reader <- x$DefaultReader
    if (inherits(readerControl$reader, "FunctionGenerator"))
        readerControl$reader <- readerControl$reader(...)
    if (is.null(readerControl$language))
        readerControl$language <- "en"

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
    mdn <- structure(list(NodeID = 0,
                          MetaData = list(),
                          Children = NULL),
                     class = "MetaDataNode")
    .DCorpus( tdl, keep, mdn, df )
}


print.DCorpus <- function(x, ...) {
    cat("DCorpus. ")
    getS3method("print", "Corpus")(x)
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
              keep = TRUE,
              cmeta = CMetaData(x),
              dmeta = DMetaData(x) )
}


as.Corpus <- function( x ){
    UseMethod("as.Corpus")
}

as.Corpus.Corpus <- identity

as.Corpus.DCorpus <- function( x )
    structure(as.list(x),
              CMetaData = CMetaData(x),
              DMetaData = DMetaData(x),
              class = c("VCorpus", "Corpus", "list"))

DMetaData.DCorpus <- function( x )
    attr(x, "DMetaData")

summary.DCorpus <- function( object, ... ) {
    getS3method("summary", "Corpus")(object)
    cat( "\nDCorpus revisions:\n" )
    cat( strwrap(paste(unlist(getRevisions(object)), collapse = " "), indent = 2, exdent = 2), "\n" )
    cat( sprintf("DCorpus active revision: %s\n\n", DSL:::.revisions(object)[1]) )
    print( DL_storage(object) )
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

## the setRevision replacement function is used to turn revisions on and off
keepRevisions <- function( corpus )
    attr( corpus, "keep" )

`keepRevisions<-` <- function( corpus, value ){
    stopifnot( length(value) == 1L )
    stopifnot( is.logical(value) )
    attr(corpus, "keep") <- value
    corpus
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
