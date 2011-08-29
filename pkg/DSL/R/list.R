## This file defines the DistributedList class and
## includes all methods operating on such classes
################################################################################

################################################################################
## DistributedList (DL) high level constructor
################################################################################

## FIXME: handle special case where DS is not a storage type object
DistributedList <- function( ... ){
    as.DistributedList( list(...), DS = NULL )
}

as.DistributedList <- function(x, DS = NULL, ...){
  UseMethod("as.DistributedList")
}

as.DistributedList.DistributedList <- function(x, DS = NULL, ...){
  identity(x)
}

as.DistributedList.list <- function(x, DS = NULL, ...){
    if( is.null(DS) )
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
    outlines <- character( 0L )

    ## Loop over list elements and write element per element into tempfile() in DFS
    for(i in 1L:length(x) ){
        ## construct key/value pairs

        mapping[i, ] <- c( chunk_iterator, position )
        position <- position + 1L

        ## add key/value pair to outlines, which will be written to disk after reaching max chunk size
        outlines <- c( outlines, sprintf("%s\t%s", as.character(i), DSL_serialize_object(x[[i]])) )

        ## write chunk if size greater than pre-defined chunksize5B
        if(object.size(outlines) >= DS_chunksize(storage)){
            DS_write_lines(storage, outlines, file.path(activeRev, sprintf("part-%d", chunk_iterator)) )
            outlines <- character(0L)
            position <- 1L
            chunk_iterator <- chunk_iterator + 1
        }
    }

    if(length(outlines)){
        DS_dir_create(storage, "DSL")
        chunk <- tempfile(pattern="DSL-", tmpdir = "DSL")
        chunks <- c( chunks, chunk )
        DS_write_lines(storage, outlines, chunk )
        chunk_iterator <- chunk_iterator + 1
    }

    .DistributedList( x = list(),
                      chunks = chunks,
                      keys = seq_len(length(x)),
                      mapping = mapping,
                      storage = storage )
}

.DistributedList <- function( x,  chunks, keys,
                                mapping, storage ) {
  attr( x, "Chunks" )             <- chunks
  attr( x, "Keys" )               <- keys
  attr( x, "Mapping" )            <- mapping
  attr( x, "DistributedStorage" ) <- storage
  class( x )                      <- c( "DistributedList", class(x) )
  x
}

## S3 Methods
print.DistributedList <- function(x, ...) {
    cat(sprintf(ngettext(length(x),
                         "A DistributedStorageList with %d element\n",
                         "A DistributedStorageList with %d elements\n"),
                length(x)))
    invisible(x)
}

length.DistributedList <- function(x)
  length(Keys(x))

names.DistributedList <- function(x)
  rownames(attr(x, "Mapping"))

`[[.DistributedList` <- function( x, i ) {
    ## TODO: what if there are more than 1 chunk
    mapping <- attr(x, "Mapping")[ i, ]
    line <- DS_read_lines( DistributedStorage(x),
                           attr(x, "Chunks")[ mapping["Chunk"] ]
                           ) [ mapping["Position"] ]
    DSL_unserialize_object( strsplit( line, "\t" )[[ 1 ]][ 2 ] )
}


Keys <- function( x )
    attr(x, "Keys")

## hash table constructor
DSL_hash <- function( n, ids = NULL )
    matrix(0L, nrow = n, ncol = 2L, dimnames = list(if(is.null(ids)){
        character(n) } else { ids }, c("Chunk", "Position")))

DSL_serialize_object <- function( x )
  gsub("\n", "\\\\n", gsub("\\n", "\\\n", rawToChar(serialize(x, NULL, TRUE)), fixed = TRUE))

DSL_unserialize_object <- function( x )
    unserialize( charToRaw(gsub("\\\\n", "\n", x)) )


