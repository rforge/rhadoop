# Authors: Ingo Feinerer, Stefan Theussl

.DistributedCorpus <-
function(x, active_revision, chunks, cmeta, dmeta, keys, mapping, revisions) {
  attr(x, "ActiveRevision") <- active_revision
  attr(x, "Chunks") <- chunks
  attr(x, "CMetaData") <- cmeta
  attr(x, "DMetaData") <- dmeta
  attr(x, "Keys") <- keys
  attr(x, "Mapping") <- mapping
  attr(x, "Revisions") <- revisions
  class(x) <- c("DistributedCorpus", "Corpus", "list")
  x
}

DistributedCorpus <-
    # For the moment we
    #   - only support a directory as source (DirSource)
    function(source,
             readerControl = list(reader = source$DefaultReader, language = "eng"),
             chunksize = 8 * 1024^2, ...) {

        if (!inherits(source, "DirSource"))
            stop("unsupported source type (use DirSource instead)")

        readerControl <- tm:::prepareReader(readerControl, source$DefaultReader, ...)
        
        activeRev <- tmpdir <- tempfile()
        DFS_dir_create(tmpdir)

        ## Initialization
        ## - key            -> uniquely identifies document in corpus
        ## - chunk_iterator -> specifies the file chunk in which the document resides
        ## - position       -> specifies the position of the document (the row) in the chunk
        ## - size           -> specifies the current size of the active chunk in bytes
        ## - mapping        -> defines the hash table to efficiently retrieve chunk and position of the given document
        ## - outlines       -> contains the current serialized documents to be written to the DFS
        key <- 0L
        chunk_iterator <- 1L
        position <- 1L
        size <- 0L
        mapping <- dc_hash(length(source$FileList))
        outlines <- character(0L)
        
        ## FIXME: for debugging purposes
        timer_start <- proc.time()["elapsed"]
        sumdoc <- 0L
        
        ## Loop over sources and write to activeRev in DFS
        while (!eoi(source)) {
          source <- stepNext(source)
          elem <- getElem(source)

          ## construct key/value pairs
          key <- key + 1
          doc <- readerControl$reader(elem, readerControl$language, source$FileList[key])
          value <- gsub("\n", "\\\\n", rawToChar(serialize(doc, NULL, TRUE)))
                                        #gsub("\n", "\\\\n", paste(Content(doc), collapse = ""))

          mapping[key, ] <- c(chunk_iterator, position)
          position <- position + 1L

          outlines <- c(outlines, sprintf("%s\t%s", as.character(key), value))

          ## write chunk if size greater than pre-defined chunksize
          if(object.size(outlines) >= chunksize){
            DFS_write_lines(outlines, file.path(activeRev, sprintf("part-%d", chunk_iterator)), henv = hive() )
            sumdoc <- sum(sumdoc, length(outlines))
            outlines <- character(0L)
            position <- 1L
            ## NOTE: temporary be more verbose if(verbose)
            writeLines(sprintf("Finished streaming part-%d to DFS. Time since begin of streaming: %s. Files processed: %d", chunk_iterator, as.character(proc.time()["elapsed"] - timer_start), sumdoc))
            chunk_iterator <- chunk_iterator + 1L
          }
        }

        if(length(outlines)){
          DFS_write_lines(outlines, file.path(activeRev, sprintf("part-%d", chunk_iterator)), henv = hive() )
          chunk_iterator <- chunk_iterator + 1
        }

        .DistributedCorpus(x = list(),
                           active_revision = activeRev,
                           chunks = structure(list(paste("part-", 1:(chunk_iterator - 1), sep = "")), names = activeRev),
                           cmeta = tm:::.MetaDataNode(),
                           dmeta = data.frame(MetaID = rep(0, source$Length), stringsAsFactors = FALSE),
                           keys = source$FileList,
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

as.DistributedCorpus <- function(x, chunksize = 8 * 1024^2, ...){
  UseMethod("as.DistributedCorpus")
}

as.DistributedCorpus.DistributedCorpus <- function(x, chunksize = 8 * 1024^2, ...){
  identity(x)
}

as.DistributedCorpus.Corpus <- function(x, chunksize = 8 * 1024^2, ...){
  
  activeRev <- tmpdir <- tempfile()
  DFS_dir_create(tmpdir)

  ## we could do this much more efficiently as we know apriori the number of documents etc.
  ##  n <- 1
  ##  if(chunksize <= object.size(x))
  ##    n <- ceiling( object.size(x)/chunksize )

  ## Initialization
  ## - key            -> uniquely identifies document in corpus (NOTE: here the key is the iterator 'i'
  ## - chunk_iterator -> specifies the file chunk in which the document resides
  ## - position       -> specifies the position of the document (the row) in the chunk
  ## - size           -> specifies the current size of the active chunk in bytes
  ## - mapping        -> defines the hash table to efficiently retrieve chunk and position of the given document
  ## - outlines       -> contains the current serialized documents to be written to the DFS
  chunk_iterator <- 1L
  position <- 1L
  size <- 0L
  mapping <- dc_hash(length(x))
  outlines <- character(0L)

  ## Loop over documents and write document per document to activeRev in DFS
  for(i in 1L:length(x) ){
    ## construct key/value pairs
    value <- gsub("\n", "\\\\n", rawToChar(serialize(x[[i]], NULL, TRUE)))
    
    mapping[i, ] <- c(chunk_iterator, position)
    position <- position + 1L
    
    outlines <- c(outlines, sprintf("%s\t%s", as.character(i), value))
    
    ## write chunk if size greater than pre-defined chunksize
    if(object.size(outlines) >= chunksize){
      DFS_write_lines(outlines, file.path(activeRev, sprintf("part-%d", chunk_iterator)), henv = hive() )
      outlines <- character(0L)
      position <- 1L
      chunk_iterator <- chunk_iterator + 1
    }
  }
  
  if(length(outlines)){
    DFS_write_lines(outlines, file.path(activeRev, sprintf("part-%d", chunk_iterator)), henv = hive() )
    chunk_iterator <- chunk_iterator + 1
  }
  
  .DistributedCorpus(x = list(),
                     active_revision = activeRev,
                     chunks = structure(list(paste("part-", 1:(chunk_iterator - 1), sep = "")), names = activeRev),
                     cmeta = tm:::.MetaDataNode(),
                     dmeta = meta(x),
                     keys = seq_len(length(x)),
                     mapping = structure(list(mapping), names = activeRev),
                     revisions = list(activeRev)) 
}


`[[.DistributedCorpus` <- function(x, i) {
    ## TODO: what if there are more than 1 chunk
    current_map <- attr(x, "Mapping")[[attr(x, "ActiveRevision")]][i, ]
    object <- hive:::DFS_read_lines3( file.path(attr(x, "ActiveRevision"), attr(x, "Chunks")[[ attr(x, "ActiveRevision") ]] [ current_map["Chunk"] ]),
                             henv = hive() )[ current_map["Position"] ]
    value <- strsplit(object, "\t")[[1]][2]

    unserialize(charToRaw(gsub("\\n", "\n", value, fixed = TRUE)))
}

summary.DistributedCorpus <- function(object, ...) {
    show(object)
    cat("\nAvailable revisions:\n")
    cat(strwrap(paste(unlist(attr(object, "Revisions")), collapse = " "), indent = 2, exdent = 2), "\n")
    cat(sprintf("Active revision: %s\n", attr(object, "ActiveRevision")))
}

Keys <- function(x) attr(x, "Keys")

setRevision <- function(corpus, revision){
    if(!(revision %in% attr(corpus, "Revisions")))
        warning("invalid revision")
    attr(corpus, "ActiveRevision") <- revision
    corpus
}

getRevisions <- function(corpus){
    attr(corpus, "Revisions")
}

updateRevision <- function(corpus, revision){
  split_line <- function(line) {
    val <- unlist(strsplit(line, "\t"))
    ## four/three backslashes necessary as we have to write this code to disk.
    ## Otherwise backslash n would be interpreted as newline.
    list(key = val[1], value = unserialize(charToRaw(gsub("\\n", "\n", val[2], fixed = TRUE))))
  }

  chunks <- grep("part-", DFS_list(revision), value = TRUE)

  ## we need to read a certain number of bytes with DFS_tail.
  ## we estimate the size using the following formula:
  ## (base char vec size + ( char size all keys + (chars generated by serialize +
  ## "position" size + "chunk" size + chunk name + 2 * integer size + selfdefined constant) * #documents) / (#chunks - 1 )) * correction factor
  #ndocs <- length(Keys(corpus))
  #keysize <- sum(nchar(Keys(corpus)))
  #nchunks <- length(attr(corpus, "Chunks")[[length(attr(corpus, "Chunks"))]])
  #maxbytes <- ceiling( (88 + (keysize + (70 + 8 + 5 + 27 + 2*8 + 30)*ndocs ) / (nchunks - 1))*1.3 )
  #minbytes <- 8192
  #readbytes <- ifelse(minbytes > maxbytes, minbytes, maxbytes)
  # TODO: Do not use sapply
  #mapping <- sapply(sapply(chunks,
  #                         function(x) DFS_tail(n = 1, file.path(revision, x), size = readbytes, henv = hive())), split_line)
  chunk_stamps <- lapply(chunks,
                           function(x) DFS_tail(n = 1, file.path(revision, x), henv = hive()))
  ## chunk order is equal to order of first keys
  firstkeys <- unlist(lapply(chunk_stamps, function(x) split_line(x)$value["First_key"]))
  lastkeys <- unlist(lapply(chunk_stamps, function(x) split_line(x)$value["Last_key"]))
  ## remove duplicated entries
  chunks <- chunks[ !duplicated(firstkeys) ]
  firstkeys <- firstkeys[ !duplicated(firstkeys) ]
  lastkeys <- lastkeys[ !duplicated(firstkeys) ]
  keyorder <- order( firstkeys )

  ## now populate the hash table
  hash_table <- dc_hash(length(corpus))
  ##hash_table[, "Position"] <- seq_len(length(corpus))  

  for(i in seq_along(chunks)){
    hash_table[ firstkeys[i]:lastkeys[i], 2L ] <- seq_len( lastkeys[i] - firstkeys[i] + 1 )
    hash_table[ firstkeys[i]:lastkeys[i], 1L ] <- keyorder[i]
  }

  attr(corpus, "Chunks") <- c(attr(corpus, "Chunks"), structure(list(chunks[keyorder]), names = revision))
  attr(corpus, "Mapping")[[revision]] <- hash_table

  ## Finally, update revision number
  setRevision(corpus, revision)
}

dc_hash <- function(n){
  matrix(0L, nrow = n, ncol = 2L, dimnames = list(NULL, c("Chunk", "Position")))
}
