# Authors: Ingo Feinerer, Stefan Theussl

.DistributedCorpus <- function( x, active_revision, chunks, cmeta, dmeta, keys,
                                mapping, revisions, storage ) {
  attr( x, "ActiveRevision" ) <- active_revision
  attr( x, "Chunks" )         <- chunks
  attr( x, "CMetaData" )      <- cmeta
  attr( x, "DMetaData" )      <- dmeta
  attr( x, "Keys" )           <- keys
  attr( x, "Mapping" )        <- mapping
  attr( x, "Revisions" )      <- revisions
  attr( x, "Storage" )        <- storage
  class( x )                  <- c( "DistributedCorpus", "Corpus", "list" )
  x
}

DistributedCorpus <-
    # For the moment we
    #   - only support a directory as source (DirSource)
    function( source,
              readerControl = list(reader   = source$DefaultReader,
                                   language = "eng"),
              storage = NULL, ... ) {

    if( is.null(storage) )
        storage <- dc_default_storage()
    storage <- as.dc_storage(storage)

    if( !inherits(source, "DirSource") )
        stop("unsupported source type (use DirSource instead)")

    readerControl <- tm:::prepareReader(readerControl, source$DefaultReader,...)
    activeRev <- .generate_random_revision()
    dc_dir_create(storage, activeRev)

    ## Initialization
    ## - keys           -> uniquely identifies document in corpus
    ## - chunk_iterator -> specifies the file chunk for each document
    ## - position       -> specifies the position of the document, i.e.,
    ##                     the row in the chunk
    ## - size           -> specifies the current size of chunks in bytes
    ## - mapping        -> defines the hash table to efficiently retrieve
    ##                     chunk and position of the given document
    ## - outlines       -> contains the current serialized documents to be
    ##                     written to the DFS
    keys <- seq_len( source$Length )
    ind <- 0L
    chunk_iterator <- 1L
    position <- 1L
    size <- 0L
    mapping <- dc_hash( source$Length )
    outlines <- character( 0L )
    ## FIXME: for debugging purposes
    timer_start <- proc.time()["elapsed"]
    sumdoc <- 0L
    ## Loop over sources and write to activeRev in DFS
    while (!eoi(source)) {
        source <- stepNext(source)
        elem <- getElem(source)

        ## construct key/value pairs and save mapping
        ind <- ind + 1
        mapping[ind, ] <- c(chunk_iterator, position)
        position <- position + 1L
        doc <- readerControl$reader(elem,
                                                    readerControl$language,
                                                    source$FileList[ind])
        rownames(mapping)[ind] <- ID(doc)
        ## create vector containing serialized documents as <key, value> pairs
        outlines <- c( outlines,
                      sprintf("%s\t%s",
                              as.character(keys[ind]),
                              dc_serialize_object(doc)) )

        ## write chunk if size greater than pre-defined chunksize
        if( object.size(outlines) >= dc_chunksize(storage) ) {
            dc_write_lines( storage,
                            outlines,
                            file.path(activeRev,
                                      sprintf("part-%d", chunk_iterator)) )
            sumdoc <- sum( sumdoc, length(outlines) )
            outlines <- character( 0L )
            position <- 1L
            ## NOTE: temporary be more verbose if(verbose)
            writeLines( sprintf("Finished streaming part-%d to DFS. Time since begin of streaming: %s. Files processed: %d", chunk_iterator, as.character(proc.time()["elapsed"] - timer_start), sumdoc))
            chunk_iterator <- chunk_iterator + 1L
        }
    }

    ## write remaining documents to disk
    if( length(outlines) ){
        dc_write_lines( storage,
                        outlines,
                        file.path(activeRev,
                                  sprintf("part-%d", chunk_iterator)) )
        chunk_iterator <- chunk_iterator + 1
    }

    .DistributedCorpus( x = list(),
                        storage = storage,
                        active_revision = activeRev,
                        chunks = structure(list(paste("part-",
                                             1:(chunk_iterator - 1), sep = "")),
                                           names = activeRev),
                        cmeta = tm:::.MetaDataNode(),
                        dmeta = data.frame(MetaID = rep(0, source$Length),
                                           stringsAsFactors = FALSE),
                        keys = keys,
                        mapping = structure(list(mapping), names = activeRev),
                        revisions = list(activeRev))
}

print.DistributedCorpus <- function(x, ...) {
    cat(sprintf(ngettext(length(Keys(x)),
                         "A corpus with %d text document\n",
                         "A corpus with %d text documents\n"),
                length(Keys(x))))
    invisible(x)
}

length.DistributedCorpus <- function(x)
  length(Keys(x))

as.DistributedCorpus <- function(x, storage = NULL, ...){
  UseMethod("as.DistributedCorpus")
}

as.DistributedCorpus.DistributedCorpus <- function(x, storage = NULL, ...){
  identity(x)
}

as.DistributedCorpus.Corpus <- function(x, storage = NULL, ...){
    if( is.null(storage) )
        storage <- dc_default_storage()

    activeRev <- .generate_random_revision()
    dc_dir_create(storage, activeRev)

    ## we could do this much more efficiently as we know apriori the number of
    ## documents etc.
    ##  n <- 1
    ##  if(chunksize <= object.size(x))
    ##    n <- ceiling( object.size(x)/chunksize )

    ## Initialization (see above)
    chunk_iterator <- 1L
    position <- 1L
    size <- 0L
    mapping <- dc_hash( length(x) )
    outlines <- character( 0L )

    ## Loop over documents and write document per document to activeRev in DFS
    for(i in 1L:length(x) ){
        ## construct key/value pairs
        value <- dc_serialize_object( x[[i]] )

        mapping[i, ] <- c(chunk_iterator, position)
        rownames(mapping)[i] <- ID(x[[i]])
        position <- position + 1L

        outlines <- c(outlines, sprintf("%s\t%s", as.character(i), value))

        ## write chunk if size greater than pre-defined chunksize5B
        if(object.size(outlines) >= dc_chunksize(storage)){
            dc_write_lines(storage, outlines, file.path(activeRev, sprintf("part-%d", chunk_iterator)) )
            outlines <- character(0L)
            position <- 1L
            chunk_iterator <- chunk_iterator + 1
        }
    }

    if(length(outlines)){
        dc_write_lines(storage, outlines, file.path(activeRev, sprintf("part-%d", chunk_iterator)) )
        chunk_iterator <- chunk_iterator + 1
    }

    .DistributedCorpus(x = list(),
                       active_revision = activeRev,
                       chunks = structure(list(paste("part-", 1:(chunk_iterator - 1), sep = "")), names = activeRev),
                       cmeta = tm:::.MetaDataNode(),
                       dmeta = meta(x),
                       keys = seq_len(length(x)),
                       mapping = structure(list(mapping), names = activeRev),
                       revisions = list(activeRev),
                       storage = storage)
}

as.Corpus <- function(x, ...){
  UseMethod("as.Corpus")
}

as.Corpus.Corpus <- function(x, ...){
  identity(x)
}

as.Corpus.DistributedCorpus <- function(x, ...){
    chunks <- lapply( unique(dc_get_text_mapping_from_revision(x)[, "Chunk"]), function(chunk) dc_get_file_path_for_chunk(x, chunk) )
    contents <- lapply( unlist(lapply( chunks, function(chunk) {lines <- dc_read_lines( dc_storage(x), chunk )
                                      lines[-length(lines)]} )),
                       function( line ) dc_unserialize_object(strsplit(line, "\t")[[1]][2]) )

    tm:::.VCorpus(contents, CMetaData(x), DMetaData(x))
}

`[[.DistributedCorpus` <- function( x, i ) {
    ## TODO: what if there are more than 1 chunk
    mapping <- dc_get_text_mapping_from_revision( x )[ i, ]
    line <- dc_read_lines( dc_storage(x),
                           dc_get_file_path_for_chunk(x, mapping["Chunk"])
                           ) [ mapping["Position"] ]
    dc_unserialize_object( strsplit( line, "\t" )[[ 1 ]][ 2 ] )
}

DMetaData.DistributedCorpus <- function( x )
    attr(x, "DMetaData")

summary.DistributedCorpus <- function( object, ... ) {
    tm:::summary.Corpus(object)
    cat("\n--- Distributed Corpus ---\n")
    cat("Available revisions:\n")
    cat(strwrap(paste(unlist(attr(object, "Revisions")), collapse = " "), indent = 2, exdent = 2), "\n")
    cat(sprintf("Active revision: %s\n", attr(object, "ActiveRevision")))
    print(dc_storage(object))
}

## Set active revision in the DC to the specified revision
setRevision <- function( corpus, revision ){
    if( !(revision %in% attr(corpus, "Revisions")) )
        warning( "invalid revision" )
    attr( corpus, "ActiveRevision" ) <- revision
    corpus
}

## Get all available revisions from the DC
getRevisions <- function(corpus){
    attr(corpus, "Revisions")
}

updateRevision <- function( corpus, revision ){
    chunks <- grep("part-",
                   dc_list_directory(dc_storage(corpus), revision),
                   value = TRUE)

    ## we need to read a certain number of bytes with DFS_tail.
    chunk_stamps <- lapply( chunks,
                            function(x) dc_fetch_last_line(
                                          dc_storage(corpus),
                                          file.path(revision, x)) )
    ## chunk order is equal to order of first keys
    firstkeys <- as.integer(unlist(lapply(chunk_stamps,
                              function(x) dc_split_line(x)$value["First_key"])))
    lastkeys <- as.integer(unlist(lapply(chunk_stamps,
                              function(x) dc_split_line(x)$value["Last_key"])))
    ## remove duplicated entries
    if( any(duplicated(firstkeys)) ){
        chunks <- chunks[ !duplicated(firstkeys) ]
        firstkeys <- firstkeys[ !duplicated(firstkeys) ]
        lastkeys <- lastkeys[ !duplicated(firstkeys) ]
    }

    keyorder <- order( firstkeys )
    ## now populate the hash table
    hash_table <- dc_hash(length(corpus), ids = rownames(dc_get_text_mapping_from_revision(corpus)))
    ##hash_table[, "Position"] <- seq_len(length(corpus))

    for(i in seq_along(chunks)){
        hash_table[ firstkeys[i]:lastkeys[i], 2L ] <- seq_len( lastkeys[i] -
                                                              firstkeys[i] + 1 )
        hash_table[ firstkeys[i]:lastkeys[i], 1L ] <- keyorder[i]
    }

    attr(corpus, "Chunks") <- c( attr(corpus, "Chunks"),
                                structure(list(chunks[keyorder]),
                                          names = revision))
    attr(corpus, "Mapping")[[revision]] <- hash_table

    ## Finally, update revision number
    setRevision(corpus, revision)
}
