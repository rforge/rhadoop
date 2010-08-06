
tm_map.DistributedCorpus <-
function(x, FUN, ..., useMeta = FALSE, lazy = FALSE) {
    ## TODO: shouldn't we check provided function for sanity?

    ## call the appropriate tm_map method based on storage technology
    rev <- .dc_tm_map(dc_storage(x), x, FUN, useMeta = useMeta, ...)

    ## add new revision to corpus meta info, returns corpus
    .finalize_and_update_corpus(x, rev)
}

.dc_tm_map <- function(storage, x, FUN, useMeta = useMeta, ...)
    UseMethod(".dc_tm_map")

## Local Disk Storage mapper S3 method
.dc_tm_map.local_disk <- function(storage, x, FUN, useMeta, ... ){
    active <- attr(x, "ActiveRevision")
    chunks <- attr(x, "Chunks")[[active]]
    rev <- .generate_random_revision()
    dc_dir_create(storage, rev)
    for(chunk in chunks)
        .local_disk_preprocess_mapper( FUN,
                      input  = file.path(storage$base_directory, active, chunk),
                      output = file.path(storage$base_directory, rev, chunk) )
    rev
}

.dc_tm_map.HDFS <- function( storage, x, FUN, useMeta, ... ){

    ## FIXME: this file MUST be on a network file system mounted on every node
    foo_file <- "~/tmp/_hive_map_function.Rda"
    save(FUN, file = foo_file)

    ## SET a user specific command environment variable here
    cmdenv_arg = NULL

    cmdenv_arg <- c( cmdenv_arg,
                     sprintf("_HIVE_FUNCTION_TO_APPLY_=%s",
                             dc_serialize_object(FUN)) )
    rev <- .tm_map_reduce( x, .generate_preprocess_mapper(),
                           cmdenv_arg = cmdenv_arg, useMeta = useMeta, ... )

    unlink(foo_file)
    rev
}

.local_disk_preprocess_mapper <- function( FUN, input, output ){
    first <- TRUE
    con <- file( input, open = "r" )
    con2 <- file( output, open = "w" )
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
        input <- dc_split_line( line )
        result <- FUN( input$value )
        if(first){
            firstkey <- input$key
            first <- FALSE
        }

        if(is.character(result)){
            writeLines( sprintf("%s\t%s", input$key, dc_serialize_object(result)),
                       con2 )
        } else
        writeLines( sprintf( "%s\t%s", input$key,
                             dc_serialize_object(PlainTextDocument(id=ID(result)))),
                    con2 )
    }
    ## In the last step we need to add a stamp to this chunk
    ## <key:randomstring, value_serialized:c(firstdocumentkey,lastdocumentkey)>
    writeLines( sprintf( "%s\t%s",
                        paste(paste(sample(c(letters, 0:9), 10, replace = TRUE),
                                    collapse = ""),
                              system("hostname", intern = TRUE), sep = "-"),
                        dc_serialize_object(c(First_key = as.integer(firstkey),
                                           Last_key  = as.integer(input$key)))),
               con2 )
    close(con)
    close(con2)

}

## Hadoop Streaming mapper for preprocessing tasks
.generate_preprocess_mapper <- function() {
  function(){
    require("tm")
    FUN <- NA
    serialized <- Sys.getenv("_HIVE_FUNCTION_TO_APPLY_")
    control <- if( serialized == "" ){
        list()
    } else {
        unserialize(charToRaw(gsub("\\n", "\n", serialized, fixed = TRUE)))
    }

    split_line <- tm.plugin.dc:::dc_split_line
    ##split_line <- function(line) {
    ##  val <- unlist(strsplit(line, "\t"))
    ##  list( key = val[1], value = gsub("\n", "\\n", rawToChar(serialize(val[2], NULL, TRUE)), fixed = TRUE) )
    ##}

    mapred_write_output <- function(key, value)
      cat( sprintf("%s\t%s",
                   key,
                   tm.plugin.dc:::dc_serialize_object(value)), sep = "\n" )

    first <- TRUE
    con <- file("stdin", open = "r")
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
      input <- split_line(line)
      result <- FUN(input$value)
      if(first){
        firstkey <- input$key
        first <- FALSE
      }

      if(length(result)){
        mapred_write_output(input$key, result)
      } else
        mapred_write_output(input$key, PlainTextDocument(id = ID(result)))
    }

    ## In the last step we need to add a stamp to this chunk
    ## <key:randomstring, value_serialized:c(firstdocumentkey,lastdocumentkey)>
    mapred_write_output(paste(paste(sample(c(letters, 0:9), 10, replace = TRUE),
                             collapse = ""), system("hostname", intern = TRUE), sep = "-"),
                        c(First_key = as.integer(firstkey),
                          Last_key  = as.integer(input$key)))
    close(con)
  }
}
