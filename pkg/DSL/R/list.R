## This file defines the DList class and
## includes all methods operating on such classes
################################################################################

################################################################################
## DList (DL) high level constructor
################################################################################

## FIXME: handle special case where DStorage is not a storage type object
DList <- function( ... ){
    as.DList( list(...), DStorage = NULL )
}

as.DList <- function(x, DStorage = NULL, ...){
    UseMethod("as.DList")
}

as.DList.DList <- function(x, DStorage = NULL, ...)
    x

as.DList.list <- function(x, DStorage = NULL, ...){
    if( is.null(DStorage) )
        storage <- DS_default()

    ## dont think we need active revision here, we write directly into base_dir
    ## activeRev <- .generate_random_revision()
    ## DS_dir_create(storage, activeRev)

    ## we could do this much more efficiently as we know apriori the number of
    ## documents etc.
    ##  n <- 1
    ##  if(chunksize <= object.size(x))
    ##    n <- ceiling( object.size(x)/chunksize )

    ## Initialization (see above)
    chunk_iterator <- 1L
    chunks <- character(0)
    position <- 1L
    size <- 0L
    mapping <- DSL_hash( length(x), ids = names(x) )
    rev <- .make_DSL_revision()
    DS_dir_create( storage, rev ) ## comparable to revision
    outlines <- character( 0L )
    firstkey <- 1L
    ## Loop over list elements and write element per element into tempfile() in DFS
    for(i in 1L:length(x) ){
        ## construct key/value pairs

        mapping[i, ] <- c( chunk_iterator, position )
        position <- position + 1L

        ## add key/value pair to outlines, which will be written to disk after reaching max chunk size
        outlines <- c( outlines, sprintf("%s\t%s", as.character(i), DSL_serialize_object(x[[i]])) )

        ## write chunk if size greater than pre-defined chunksize5B
        if(object.size(outlines) >= DS_chunksize(storage)){
            outlines <- c( outlines, .make_chunk_signature(firstkey, i) )
            chunk <- .make_chunk_filename( rev )
            chunks <- c( chunks, chunk )
            DS_write_lines(storage, outlines, chunk )
            outlines <- character(0L)
            firstkey <- i + 1
            position <- 1L
            chunk_iterator <- chunk_iterator + 1
        }
    }
    ## write remaining elements to final chunk
    if(length(outlines)){
        outlines <- c( outlines, .make_chunk_signature(firstkey, i) )
        chunk <- .make_chunk_filename( rev )
        chunks <- c( chunks, chunk )
        DS_write_lines(storage, outlines, chunk )
        chunk_iterator <- chunk_iterator + 1
    }

    .DList( x = list(),
                      chunks = .make_chunk_handler(chunks, rev, storage),
                      keys = seq_len(length(x)),
                      mapping = mapping,
                      storage = storage )
}

.DList <- function( x,  chunks, keys,
                                mapping, storage ) {
  attr( x, "Chunks" )             <- as.environment(chunks)
  attr( x, "Keys" )               <- keys
  attr( x, "Mapping" )            <- mapping
  attr( x, "DStorage" ) <- storage
  class( x )                      <- c( "DList", class(x) )
  x
}

.make_chunk_handler <- function(chunks, rev, storage){
    e <- new.env()
    assign( "base_dir", DS_base_dir(storage), envir = e )
    assign( "FUN", storage$unlink, envir = e )
    assign( "Revisions", rev, envir = e )
    assign(rev, chunks, envir = e)
    ## Finalizer
    reg.finalizer( e, function(x){
        revisions <- get("Revisions", envir = x)
        base_dir <- get("base_dir", envir = x)
        for(rev in revisions){
            lapply(get(rev, envir = x), function(f) get("FUN", envir = x)(file.path(base_dir, f)))
            get("FUN", envir = x)(file.path(base_dir, rev))
        }
    } )
    e
}

## S3 Methods
print.DList <- function(x, ...) {
    cat(sprintf(ngettext(length(x),
                         "A DList with %d element\n",
                         "A DList with %d elements\n"),
                length(x)))
    invisible(x)
}

length.DList <- function(x)
  length(Keys(x))

names.DList <- function(x)
  rownames(attr(x, "Mapping"))

`names<-.DList` <- function(x, value){
    rownames(attr(x, "Mapping")) <- value
    x
}

`[[.DList` <- function( x, i ) {
    ## TODO: what if there is more than 1 chunk
    mapping <- attr(x, "Mapping")[ i, ]
    ## when using accessor we always use first
    line <- DS_read_lines( DStorage(x),
                           get( get("Revisions", envir = attr(x, "Chunks"))[1],
                                envir = attr(x, "Chunks"))[ mapping["Chunk"] ]
                           ) [ mapping["Position"] ]
    DSL_unserialize_object( strsplit( line, "\t" )[[ 1 ]][ 2 ] )
}


Keys <- function( x )
    attr(x, "Keys")

## hash table constructor
DSL_hash <- function( n, ids = NULL )
    matrix(0L, nrow = n, ncol = 2L, dimnames = list(if(is.null(ids)){
        character(n) } else { ids }, c("Chunk", "Position")))

## Serialization

DSL_serialize_object <- function( x )
  gsub("\n", "\\\\n", gsub("\\n", "\\\n", rawToChar(serialize(x, NULL, TRUE)), fixed = TRUE))

DSL_unserialize_object <- function( x )
    unserialize( charToRaw(gsub("\\\\n", "\n", x)) )

## Revision names
.make_DSL_revision <- function(){
    sprintf("DSL-%s-%s%s", format(Sys.time(), "%Y%m%d-%H%M%S"), sample(0:9, 1), sample(letters, 1))
}

## chunk file names
.make_chunk_filename <- function( dir )
    tempfile(pattern="chunk-", tmpdir = dir)

## reads line (e.g. taken from standard input) and returns
## the key and the deserialized object
DSL_split_line <- function( line ) {
    val <- unlist(strsplit(line, "\t"))
    list( key = val[1], value = DSL_unserialize_object(val[2]) )
}
