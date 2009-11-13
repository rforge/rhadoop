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

        counter <- 1

        ## TODO: chunksize + temporary directory
        size <- 0L
        chunk_iterator <- 1L
        mapping <- new.env()

        outlines <- character(0L)
        position <- 1L

        ## Loop over sources and write to activeRev in DFS
        while (!eoi(source)) {
          source <- stepNext(source)
          elem <- getElem(source)
          doc <- readerControl$reader(elem, readerControl$language, as.character(counter))

          ## construct key/value pairs
          key <- source$FileList[counter]
          ##value <- paste(serialize(paste(Content(doc), collapse = "\\n"), NULL), collapse = " ")
          value <- gsub("\n", "\\\\n", rawToChar(serialize(doc, NULL, TRUE)))
                                        #gsub("\n", "\\\\n", paste(Content(doc), collapse = ""))

          mapping[[key]] <- c(chunk = chunk_iterator, position = position)
          position <- position + 1L

          outlines <- c(outlines, sprintf("%s\t%s", key, value))

          ## write chunk if size greater than pre-defined chunksize
          if(object.size(outlines) >= chunksize){
            DFS_write_lines(outlines, file.path(activeRev, sprintf("part-%d", chunk_iterator)), henv = hive() )
            outlines <- character(0L)
            position <- 1L
            chunk_iterator <- chunk_iterator + 1
          }

          counter <- counter + 1
        }

        if(length(outlines)){
          DFS_write_lines(outlines, file.path(activeRev, sprintf("part-%d", chunk_iterator)), henv = hive() )
          outlines <- character(0L)
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
    cat(sprintf(ngettext(length(attr(x, "Keys")),
                         "A corpus with %d text document\n",
                         "A corpus with %d text documents\n"),
                length(attr(x, "Keys"))))
    invisible(x)
}

`[[.DistributedCorpus` <- function(x, i) {
    ## TODO: what if there are more than 1 chunk
    current_map <- attr(x, "Mapping")[[attr(x, "ActiveRevision")]][[ Keys(x)[i] ]]
    object <- hive:::DFS_read_lines3( file.path(attr(x, "ActiveRevision"), attr(x, "Chunks")[[ attr(x, "ActiveRevision") ]] [ current_map["chunk"] ]),
                             henv = hive() )[ current_map["position"] ]
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

tm_map.DistributedCorpus <- function(x, FUN, ..., cmdenv_arg = NULL, useMeta = FALSE, lazy = FALSE) {
    rev <- tempfile()
    cmdenv_arg <- c(cmdenv_arg, sprintf("_HIVE_FUNCTION_TO_APPLY_=%s", as.character(substitute(FUN))))
    ## start the streaming job
    hive_stream(.generate_tm_mapper(), #hive:::hadoop_generate_mapper("tm", deparse(substitute(FUN))),
                input = attr(x, "ActiveRevision"), output = rev,
                cmdenv_arg = cmdenv_arg)
    ## in case the streaming job failed to create output directory return an error
    stopifnot(DFS_dir_exists(rev))
    ## add new revision to corpus meta info
    attr(x, "Revisions") <- c(attr(x, "Revisions"), rev)
    ## update ActiveRevision in dc
    x <- updateRevision(x, rev)
    x
}

setRevision <- function(corpus, revision){
    if(!(revision %in% attr(corpus, "Revisions")))
        warning("invalid revision")
    attr(corpus, "ActiveRevision") <- revision
    corpus
}

updateRevision <- function(corpus, revision){
  split_line <- function(line) {
    val <- unlist(strsplit(line, "\t"))
    ## four/three backslashes necessary as we have to write this code to disk. Otherwise backslash n would be interpreted as newline.
    list(key = val[1], value = unserialize(charToRaw(gsub("\\n", "\n", val[2], fixed = TRUE))))
  }

  chunks <- grep("part-", DFS_list(revision), value = TRUE)

  attr(corpus, "Chunks") <- c(attr(corpus, "Chunks"), structure(list(chunks), names = revision))

  # TODO: Do not use sapply
  mapping <- sapply(sapply(chunks, function(x) DFS_tail(n = 1, file.path(revision, x), size = 8196, henv = hive())), split_line)

  hash_table <- new.env()

  for(part in colnames(mapping)){
    env <- mapping[, part]$value
    for(key in ls(env))
      hash_table[[key]] <- c(chunk = match(part, chunks), position = as.integer(env[[key]]["position"]))
  }

  attr(corpus, "Mapping")[[revision]] <- hash_table
   # corpus@Chunks <- if(identical(revision, corpus@Revisions[[1]]))
   #     as.list(corpus@Keys)
   # else {
   #     parts <- sprintf("part-%05d", seq_along(corpus@Chunks) - 1)
   #     as.list( structure(parts, names = gsub("\t", "", sapply(file.path(revision, parts), function(x) {
   #       DFS_read_lines(x, n = 1L, henv = hive() )} )))[corpus@Keys] )
   # }
  setRevision(corpus, revision)
}

.generate_tm_mapper <- function() {
  function(){
    require("tm")
    fun <- match.fun(Sys.getenv("_HIVE_FUNCTION_TO_APPLY_"))    

    split_line <- function(line) {
      val <- unlist(strsplit(line, "\t"))
      ## four/three backslashes necessary as we have to write this code to disk. Otherwise backslash n would be interpreted as newline.
      list(key = val[1], value = unserialize(charToRaw(gsub("\\n", "\n", val[2], fixed = TRUE))))
    }

    mapred_write_output <- function(key, value)
      cat(paste(key, gsub("\n", "\\n", rawToChar(serialize(value, NULL, TRUE)), fixed = TRUE), sep = "\t"), sep = "\n")
    
    ## very important: hash table for this chunk (necessary to create mapping between part-x and original texts)
    mapping <- new.env()
    chunkname <- paste(paste(sample(c(letters, 0:9), 10, replace = TRUE), collapse = ""), system("hostname", intern = TRUE), sep = "-")
    position <- 1L

    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
      input <- split_line(line)
      
      result <- fun(input$value)
      if(length(result)){
        mapping[[input$key]] <- c(chunk = chunkname, position = position)
        position <- position + 1L
        mapred_write_output(input$key, result)
      }
    }
    mapred_write_output(chunkname, mapping)
    close(con)
    
  }
}
