################################################################################
## Utility functions
################################################################################

## key value pairs
## DKey_value_pair<- function( key, value )
##     structure( list(key = key, value = value), class = c("DKey_value_pair", "list") )

## print.DKey_value_pair <- function( x, ... )
##     writeLines( make_string_from_Dkey_value_pair(x) )

## make_string_from_Dkey_value_pair <- function( x )
##     sprintf( "< %s , %s >", as.character(x$key), make_printable(x$value) )

## make_printable <- function(x)
##     UseMethod("make_printable")
## make_printable.character <- identity
## make_printable.numeric <- identity
## make_printable.DKey_value_pair <- function(x)
##     make_string_from_Dkey_value_pair( x )
## make_printable.default <- function( x )
##     sprintf( "( %s )", class(x)[1] )


## updates given list with new revision
.update_DSL <- function( x, rev ){
    ## add new revision
    .revisions( x ) <- c( rev, .revisions(x) )
    ## update to active revision
    x <- .update_to_revision( x, rev )
    x
}

.update_to_revision <- function( x, rev ){
    chunks <- grep("part-",
                   DS_list_directory( DStorage(x), rev),
                   value = TRUE)

    ## we need to read a certain number of bytes with DFS_tail.
    chunk_stamps <- lapply( chunks,
                            function(chunk) DS_fetch_last_line(DStorage(x),
                                                      file.path(rev, chunk)) )
    ## chunk order is equal to order of first keys
    firstkeys <- as.integer(unlist(lapply(chunk_stamps,
                              function(x) DSL_split_line(x)$value["First_key"])))
    lastkeys <- as.integer(unlist(lapply(chunk_stamps,
                              function(x) DSL_split_line(x)$value["Last_key"])))
    ## remove duplicated entries
    if( any(duplicated(firstkeys)) ){
        chunks <- chunks[ !duplicated(firstkeys) ]
        firstkeys <- firstkeys[ !duplicated(firstkeys) ]
        lastkeys <- lastkeys[ !duplicated(firstkeys) ]
    }

    keyorder <- order( firstkeys )
    ## now populate the hash table
    hash_table <- DSL_hash(length(x), ids = rownames(DSL_get_text_mapping_from_revision(x)))
    ##hash_table[, "Position"] <- seq_len(length(x))

    for(i in seq_along(chunks)){
        hash_table[ firstkeys[i]:lastkeys[i], 2L ] <- seq_len( lastkeys[i] -
                                                              firstkeys[i] + 1 )
        hash_table[ firstkeys[i]:lastkeys[i], 1L ] <- keyorder[i]
    }

    attr(x, "Chunks") <- c( attr(x, "Chunks"),
                                structure(list(chunks[keyorder]),
                                          names = rev))
    attr(x, "Mapping")[[rev]] <- hash_table

    ## Finally, update revision number
    .revisions(x) <- c(rev, .revisions(x))
}


## Operations on DList objects (getters)

.get_chunks_from_current_revision <- function(x){
    .get_chunks( x )
}

.get_chunks <- function( x, rev ){
    if( missing(rev) )
        rev <- .revisions( x )[1]
    rev <- as.character( rev )
    get(rev, attr(x, "Chunks"))
}

DSL_get_text_mapping_from_revision <- function( x, rev = .revisions(x)[1] )
  attr( x, "Mapping" )[[ rev ]]

.revisions <- function( x )
    get("Revisions", envir = attr( as.DList(x), "Chunks"))

`.revisions<-` <- function( x, value )
    attr( x, "Revisions" ) <- value


## chunk signature (each chunk contains a signature determining the final line)

.make_chunk_signature <- function(first, last)
    sprintf("%s\t%s",
            .stamp(),
            DSL_serialize_object(c(First_key = as.integer(first),
                                   Last_key  = as.integer(last))) )

## make key for final line in MapReduce operations
.stamp <- function(){
    paste("<<EOF", paste(sample(c(letters, 0:9), 10, replace = TRUE),
                collapse = ""),
          Sys.info()["nodename"], sep = "-")
}


