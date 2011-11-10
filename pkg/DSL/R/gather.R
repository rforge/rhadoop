
## collects all elements in a chunk file and returns a named list of those elements
DSL_gather <- function( x ){
    x <- as.DistributedList(x)
    revs <- .get_chunks_from_current_revision(x)
    out <- lapply(revs,
                  function(f){
                      lines <- DS_read_lines( DistributedStorage(x),
                                     f )
                      lapply(lines, function(line) DSL_unserialize_object( strsplit( line, "\t" )[[ 1 ]][ 2 ] ))
                  })
    names(out) <- revs
    out
}

.get_chunks_from_current_revision <- function(x){
    rev <- get("Revisions", envir = attr(x, "Chunks"))[1]
    get(rev, attr(x, "Chunks"))
}

as.list.DistributedList <- function(x, ...)
    structure( do.call(c, DSL_gather(x)), names = names(x) )


