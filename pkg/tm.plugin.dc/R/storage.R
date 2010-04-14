## dc_storage accessor and replacement functions for class "DistributedCorpus"

dc_storage <- function(x, ...)
  UseMethod("dc_storage")

dc_storage.dc_storage <- function(x, ...)
  identity(x)

dc_storage.DistributedCorpus <- function( x, ... )
  attr(x, "Storage")

`dc_storage<-` <- function(x, value)
  UseMethod("dc_storage<-")

`dc_storage<-.DistributedCorpus` <- function(x, value){
  old_stor <- dc_storage(x)
  if( inherits(old_stor, "local_disk") && inherits(value, "HDFS") ){
    DFS_dir_create( value$base_directory )
    DFS_put( file.path(old_stor$base_directory, attr(x, "ActiveRevision")),
             file.path(   value$base_directory, attr(x, "ActiveRevision")) )
  } else {
    stop("not implemented!")
  }
  attr(x, "Storage") <- value
  x
}

  
## .storage_init() initializes the storage to be used for the distributed corpus.
.dc_storage_init <- function( x ) {
    if( missing(x) )
        x <- dc_default_storage()
    ## Create storage base directory
    if( is.dc_storage(x) )
        x$dir_create( x$base_directory )

    checked <- tryCatch( check_storage_storage_for_sanity(x), error = identity )
    out <- if( inherits(checked, "error") )
        .dc_empty_storage()
    else
        x

    out
}

.dc_empty_storage <- function(){
    NA
}

## Storage constructor
.dc_storage <- function(description,
                        base_directory,
                        chunksize,
                        dir_create,
                        fetch_last_line,
                        list_directory,
                        read_lines,
                        unlink,
                        write_lines){
    structure(list(description = description,
                   chunksize = chunksize,
                   base_directory = base_directory,
                   dir_create = dir_create,
                   list_directory = list_directory,
                   fetch_last_line = fetch_last_line,
                   read_lines = read_lines,
                   unlink = unlink,
                   write_lines = write_lines),
              class = "dc_storage")
}

## Default storage is the local disk
dc_default_storage <- function(){
    dcs <- .dc_storage(description     = "Local Disk Storage",
                       base_directory  = tempfile(),
                       chunksize       = 10 * 1024^2,
                       dir_create      = base::dir.create,
                       fetch_last_line = function(x) utils::tail(base::readLines(as.character(x)), n = 1L),
                       list_directory  = base::dir,
                       read_lines      = function(x) base::readLines(as.character(x)),
                       unlink          = function(x) base::file.remove(x),
                       write_lines     = function(text, fil) base::writeLines(text, con = as.character(fil))
                       )
    class(dcs) <- c( "local_disk", class(dcs) )
    dcs
}

dc_HDFS_storage <- function(henv = hive::hive()){
    dcs <- .dc_storage(description     = "Hadoop Distributed File System (HDFS)",
                       base_directory  = tempfile(),
                       chunksize       = 10 * 1024^2,
                       dir_create      = function(x) hive::DFS_dir_create(x, henv = hive()),
                       fetch_last_line = function(x) hive::DFS_tail(n = 1L, as.character(x), henv = hive()),
                       list_directory  = function(x) hive::DFS_list(x, henv = hive()),
                       read_lines      = function(x) hive::DFS_read_lines(as.character(x), henv = hive()),
                       unlink          = function(x) hive::DFS_dir_remove(x, henv = hive()),
                       write_lines     = function(text, fil) hive::DFS_write_lines(text, as.character(fil), henv = hive())
                       )
    class(dcs) <- c( "HDFS", class(dcs) )
    dcs
}

is.dc_storage <- function( x )
    inherits( x, "dc_storage" )

print.dc_storage <- function( x, ... ){
    writeLines( "DistributedCorpus: Storage" )
    writeLines( sprintf("- Description: %s", x$description) )
    writeLines( sprintf("- Base directory on storage: %s", x$base_directory) )
    writeLines( sprintf("- Current chunk size [bytes]: %s", x$chunksize) )
}

summary.dc_storage <- function( object, ... ){
    print( object )
    writeLines( sprintf("- Registered methods: %s",
                        paste( names(object)[!(names(object)
                                                %in% c("description", "chunksize", "base_directory"))],
                              collapse = ", ")))
}

dc_chunksize <-function( storage )
    storage$chunksize

dc_dir_create <- function( storage, dir ){
    stopifnot( is.dc_storage(storage) )
    dir <- sub("/./", "", file.path(storage$base_directory, dir ))
    storage$dir_create( dir )
}

dc_list_directory <- function( storage, dir = "." ){
    stopifnot( is.dc_storage(storage) )
    if( dir == "." )
        dir <- ""
    storage$list_directory( file.path(storage$base_directory, dir ) )
}

dc_fetch_last_line <- function( storage, file ){
    stopifnot( is.dc_storage(storage) )
    file <- sub("/./", "", file.path(storage$base_directory, file ))
    storage$fetch_last_line( file )
}

dc_read_lines <- function( storage, file ){
    stopifnot( is.dc_storage(storage) )
    #file <- sub("/./", "", file.path(storage$base_directory, file ))
    file <- file.path(storage$base_directory, file )
    storage$read_lines( file )
}

## NOTE: due to security reasons no recursive unlinking is permitted!
## deletes a link on the corresponding storage (file, directory)
dc_unlink <- function( storage, link ){
    stopifnot( is.dc_storage(storage) )
    link <- sub("/./", "", file.path(storage$base_directory, link ))
    storage$unlink( link )
}

dc_write_lines <- function( storage, text, file ){
    stopifnot( is.dc_storage(storage) )
    file <- sub("/./", "", file.path(storage$base_directory, file ))
    storage$write_lines( text, file )
}

check_storage_storage_for_sanity <- function( storage ){
    stopifnot( is.dc_storage(storage) )

    dir <- .generate_random_revision()
    dc_dir_create( storage, dir )

    contents <- dc_list_directory( storage )
    stopifnot(contents == dir)

    stopifnot(dc_unlink( storage, dir ))

    text <- c("some text", "other text", "more text")
    file <- "some_file"
    dc_write_lines( storage, text, file )
    text_read <- dc_read_lines( storage, file )
    all(text == text_read)

    line <- dc_fetch_last_line( storage, file )
    stopifnot( line == text[length(text)] )

    stopifnot(dc_unlink( storage, file ))
    invisible(TRUE)
}
