
## collects all elements in a chunk file and returns a named list of those elements
DGather <- function( x ){
    chunks <- .get_chunks_from_current_revision(x) ## utils.R
    out <- lapply(chunks,
                  function(f){
                      lines <- DS_read_lines( DStorage(x),
                                     f )
                      ## note, the last line just contains information about the keys
                      len <- length( lines )
                      lapply(lines[ -len ], function(line) DSL_unserialize_object( strsplit( line, "\t" )[[ 1 ]][ 2 ] ))
                  })
    names(out) <- chunks
    out
}

as.list.DList <- function(x, ...)
    structure( do.call(c, DGather(x)), names = names(x) )
