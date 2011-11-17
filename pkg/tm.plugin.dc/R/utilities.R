## dc helper functions (extractors etc.)

## updates given corpus with new revision
.finalize_and_update_corpus <- function( x, rev ){
    ## add new revision to corpus meta info
    attr( x, "Revisions" ) <- c( attr(x, "Revisions"), rev )
    ## update ActiveRevision in dc
    x <- updateRevision( x, rev )
    x
}

## generates random revision strings
.generate_random_revision <- function()
  sprintf("%s-%s-%s", format(Sys.time(), "%Y%m%d%H%M%S"), sample(0:9, 1), sample(letters, 1))

dc_get_file_path_for_chunk <- function( x, chunk,
                                        revision = attr(x, "ActiveRevision") )
  file.path( revision, attr(x, "Chunks")[[ revision ]] [ chunk ] )

dc_get_text_mapping_from_revision <- function( x,
                                          revision = attr(x, "ActiveRevision") )
  attr( x, "Mapping" )[[ revision ]]

## hash table constructor
dc_hash <- function( n, ids = NULL )
  matrix(0L, nrow = n, ncol = 2L, dimnames = list(if(is.null(ids)){ character(n) } else { ids }, c("Chunk", "Position")))

## serializes a given object to a character string
## FIXME: we need to add a second gsub before we replace \n with \\n as otherwise
##        \\n possibly contained in texts leads to not unserializable results
dc_serialize_object <- function( x )
  gsub("\n", "\\\\n", gsub("\\n", "\\\n", rawToChar(serialize(x, NULL, TRUE)), fixed = TRUE))

## reads line (e.g. taken from standard input) and returns
## the key and the deserialized object
dc_split_line <- function( line ) {
    val <- unlist(strsplit(line, "\t"))
    list( key = val[1], value = dc_unserialize_object(val[2]) )
}

## deserializes an object from a given character string
dc_unserialize_object <- function( x )
    unserialize( charToRaw(gsub("\\\\n", "\n", x)) )

## takes the key and the corresponding value and creates a single
## <key, value> pair. The R object is serialized to a character string
dc_write_output <- function( key, value )
  cat( paste(key, dc_serialize_object(value), sep = "\t"), sep = "\n")

Keys <- function( x )
    attr(x, "Keys")

## Operations on DSL objects (getters)

.get_chunks_from_current_revision <- function(x){
    .get_chunks( x )
}

.get_chunks <- function( x, rev ){
    if( missing(rev) )
        rev <- .get_revisions( x )[1]
    rev <- as.character( rev )
    get(rev, attr(x, "Chunks"))
}

.get_revisions <- function( x )
    get("Revisions", envir = attr( as.DistributedList(x), "Chunks"))

