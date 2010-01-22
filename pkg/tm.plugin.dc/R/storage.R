## .storage_init() initializes the storage to be used for the distributed corpus.
.dc_storage_init <- function( storage ) {
  if( missing(storage) )
    storage <- dc_default_storage()
  tmp <- tryCatch( check_storage_storage_for_sanity )
  storage <- if( inherits(tmp, "error") )
    storage <- .dc_empty_storage()
  else
    tmp
  ## Initialize storage directory
  storage$dir_create(storage$base_directory)
  storage
}

.dc_empty_storage <- function(){
  NA
}

## Storage constructor
dc_storage <- function(description,
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
  dc_storage(description     = "Local Disk Storage",
             base_directory  = tempfile(),
             chunksize       = 1024^3,
             dir_create      = base::dir.create,
             fetch_last_line = function(x) utils::tail(base::readLines(file(x, "r")), n = 1L),
             list_directory  = base::dir,
             read_lines      = function(x) base::readLines(file(x, "r")),
             unlink          = function(x) base::file.remove(x),
             write_lines     = function(text, fil) base::writeLines(text, con = as.character(fil))
             )
}

dc_HDFS_storage <- function(henv = hive()){
  dc_storage(description     = "Hadoop Distributed File System (HDFS)",
             base_directory  = tempfile(),
             chunksize       = 1024^3,
             dir_create      = function(x) hive::DFS_dir_create(x, henv = henv),
             fetch_last_line = function(x) hive::DFS_tail(n = 1L, as.character(x), henv = henv),
             list_directory  = function(x) hive:::DFS_list(x, henv = henv),
             read_lines      = function(x) hive:::DFS_read_lines3(as.character(x), henv = henv),
             unlink          = function(x) hive::DFS_dir_remove(x, henv = henv),
             write_lines     = function(text, fil) hive::DFS_write_lines(text, as.character(fil), henv = henv)
             )
}

is.dc_storage <- function( x )
  inherits( x, "dc_storage" )

print.dc_storage <- function( x, ... ){
  writeLines( "DistributedCorpus: Storage" )
  writeLines( sprintf("- Description: %s", x$description) )
  writeLines( sprintf("- Base directory on storage: %s", x$base_directory) )
  writeLines( sprintf("- Current chunk size [bytes]: %s", x$chunksize) )
}

summary.dc_storage <- function( x, ... ){
  print( x )
  writeLines( sprintf("- Registered methods: %s",
                      paste( names(storage)[!(names(storage)
                                              %in% c("description", "chunksize", "base_directory"))],
                            collapse = ", ")))
}

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
  file <- sub("/./", "", file.path(storage$base_directory, file ))
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

check_storage_storage_for_sanity <- function(storage){
  stopifnot( is.dc_storage(storage) )

  dir <- sub( "/", "", tempfile("", "") )
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
