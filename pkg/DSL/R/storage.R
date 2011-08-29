## All functions dealing with the storage implementation
## of the DistributedCorpus are provided here.
## Each storage is of general class 'dc_storage' and of a subclass defining
## the actual storage, e.g. 'local_disk', 'HDFS'
################################################################################



################################################################################
## DistributedStorage (DS) high level constructor
################################################################################

NewDistributedStorage <- function( type = c("local_disk", "HDFS"),
                                   base_dir,
                                   chunksize = 1024^2 ){
    type <- match.arg(type)
    .DS_init( switch(type,
            "local_disk" = .make_local_disk_storage(base_dir, chunksize),
            "HDFS"       = .make_HDFS_storage      (base_dir, chunksize)) )
}

################################################################################
## Default storage (local disk)
################################################################################

DS_default <- function(){
    .DS_init( .make_local_disk_storage(base_dir  = tempdir(),
                                       chunksize = 10 * 1024^2) )
}

################################################################################
## DS_storage low level constructor
################################################################################

.DistributedStorage <- function( type,
                                 base_directory,
                                 chunksize,
                                 dir_create,
                                 fetch_last_line,
                                 list_directory,
                                 read_lines,
                                 unlink,
                                 write_lines){
    structure( list(base_directory = base_directory,
                    chunksize = chunksize,
                    dir_create = dir_create,
                    fetch_last_line = fetch_last_line,
                    list_directory = list_directory,
                    read_lines = read_lines,
                    unlink = unlink,
                    write_lines = write_lines),
              class = c(type, "DistributedStorage"))
}

.make_local_disk_storage <- function( base_dir, chunksize, ... ){
    .DistributedStorage( type            = "local_disk",
                         base_directory  = base_dir,
                         chunksize       = chunksize,
                         dir_create      = base::dir.create,
                         fetch_last_line = function(x) utils::tail(base::readLines(as.character(x)), n = 1L),
                         list_directory  = base::dir,
                         read_lines      = function(x) base::readLines(as.character(x)),
                         unlink          = function(x) DSL:::lfs_remove(x),
                         write_lines     = function(text, fil) base::writeLines(text, con = as.character(fil)), ...
                        )
}

.make_HDFS_storage <- function( base_dir, chunksize, ... ){
    .DistributedStorage( type = "HDFS",
                         base_directory  = base_dir,
                         chunksize       = chunksize,
                         dir_create      = function(x) hive::DFS_dir_create(x, henv = hive()),
                         fetch_last_line = function(x) hive::DFS_tail(n = 1L, as.character(x), henv = hive()),
                         list_directory  = function(x) hive::DFS_list(x, henv = hive()),
                         read_lines      = function(x) hive::DFS_read_lines(as.character(x), henv = hive()),
                         unlink          = function(x) hive::DFS_delete(x, recursive = TRUE, henv = hive()),
                         write_lines     = function(text, fil) hive::DFS_write_lines(text, as.character(fil), henv = hive()), ...
                        )
}

################################################################################
## DistributedStorage S3 methods
################################################################################

is.DistributedStorage <- function( x )
    inherits( x, "DistributedStorage" )

as.DistributedStorage <- function( x )
    UseMethod("as.DistributedStorage")

as.DistributedStorage.DistributedStorage <- identity

print.DistributedStorage <- function( x, ... ){
    writeLines( "Distributed storage." )
    writeLines( sprintf("- Type: %s", class(x)[1] ) )
    writeLines( sprintf("- Base directory on storage: %s", DS_base_dir(x)) )
    writeLines( sprintf("- Current chunk size [bytes]: %s", DS_chunksize(x)) )
}

summary.DistributedStorage <- function( object, ... ){
    print( object )
    writeLines( sprintf("- Registered methods: %s",
                        paste( names(object)[!(names(object)
                                                %in% c("description", "chunksize", "base_directory"))],
                              collapse = ", ")))
}

################################################################################
## DistributedStorage accessor and replacement functions for class "DistributedCorpus"
################################################################################

DistributedStorage <- function( x )
  UseMethod("DistributedStorage")

DistributedStorage.DistributedStorage <- identity

DistributedStorage.DistributedList <- function( x )
  attr(x, "DistributedStorage")

`DistributedStorage<-` <- function( x, value )
  UseMethod("DistributedStorage<-")

`DistributedStorage<-.DistributedList` <- function(x, value){
  old_stor <- DistributedStorage(x)
  stopifnot( DS_list_directory(value, DS_base_dir(value)) )
  if( inherits(old_stor, "local_disk") && inherits(value, "HDFS") ){
    if( !length(hive::DFS_list(DS_base_dir(value))) ){
      hive::DFS_put( file.path(DS_base_dir(old_stor), attr(x, "ActiveRevision")),
               file.path(DS_base_dir(value), attr(x, "ActiveRevision")) )
    }
    else {
      if( !.check_contents_of_storage(x, value) )
        stop("HDFS storage already contains this directory with different data for the active revision")
    }
  } else {
    stop("not implemented!")
  }
  attr(x, "DistributedStorage") <- value
  x
}

################################################################################
## DS_storage helper functions
################################################################################

## helper function for DistributedCorpus -> Corpus coercion
## checks if all chunks are already in place
## FIXME: we need to provide some checksums here
.check_contents_of_storage <- function(x, value){
    all( attr(x, "Chunks")[[ attr(x, "ActiveRevision") ]] %in%
        DS_list_directory( value, attr(x, "ActiveRevision")) )
}

.DS_empty <- function(){
    NA
}

## .storage_init() initializes the storage to be used for the distributed corpus.
.DS_init <- function( x ) {
    if( missing(x) )
        x <- DS_default()
    ## Create storage base directory
    if( is.DistributedStorage(x) )
       x$dir_create( DS_base_dir(x) )

    out <- if( inherits(tryCatch(check_storage_for_sanity(x),
                                 error = identity), "error") )
        .DS_empty()
    else
        x
    out
}

DS_base_dir <- function( storage )
    storage$base_directory

DS_chunksize <-function( storage )
    storage$chunksize

DS_dir_create <- function( storage, dir ){
    stopifnot( is.DistributedStorage(storage) )
    dir <- sub("/./", "", file.path(DS_base_dir(storage), dir ))
    storage$dir_create( dir )
}

DS_list_directory <- function( storage, dir = "." ){
    stopifnot( is.DistributedStorage(storage) )
    if( dir == "." )
        dir <- ""
    storage$list_directory( file.path(DS_base_dir(storage), dir ) )
}

DS_fetch_last_line <- function( storage, file ){
    stopifnot( is.DistributedStorage(storage) )
    file <- sub("/./", "", file.path(DS_base_dir(storage), file ))
    storage$fetch_last_line( file )
}

DS_read_lines <- function( storage, file ){
    stopifnot( is.DistributedStorage(storage) )
    #file <- sub("/./", "", file.path(storage$base_directory, file ))
    file <- file.path(DS_base_dir(storage), file )
    storage$read_lines( file )
}

## NOTE: due to security reasons no recursive unlinking is permitted!
## deletes a link on the corresponding storage (file, directory)
DS_unlink <- function( storage, link ){
    stopifnot( is.DistributedStorage(storage) )
    link <- sub("/./", "", file.path(DS_base_dir(storage), link ))
    storage$unlink( link )
}

DS_write_lines <- function( storage, text, file ){
    stopifnot( is.DistributedStorage(storage) )
    file <- sub("/./", "", file.path(DS_base_dir(storage), file ))
    storage$write_lines( text, file )
}

check_storage_for_sanity <- function( storage ){
    stopifnot( is.DistributedStorage(storage) )

    dir <- basename( tempfile() )
    DS_dir_create( storage, dir )

    contents <- DS_list_directory( storage )
    stopifnot(any(contents == dir))

    stopifnot(DS_unlink( storage, dir ))

    text <- c("some text", "other text", "more text")
    file <- "some_file"
    DS_write_lines( storage, text, file )
    text_read <- DS_read_lines( storage, file )
    stopifnot( all(text == text_read) )

    line <- DS_fetch_last_line( storage, file )
    stopifnot( line == text[length(text)] )

    stopifnot( DS_unlink( storage, file ) )
    invisible( TRUE )
}

################################################################################
## local file system manipulators
################################################################################

lfs_remove <- function( x ){
    foo <- file.remove
    ## on Windows file.remove does not remove empty directories
    ## so we need to check for it
    if( .Platform$OS.type == "windows" )
        if( length(dir(x, recursive = TRUE, all.files = TRUE)) )
            warning( sprintf("Directory %s not empty, so not removed.", x) )
        else
            foo <- function( x ) {
                unlink( x, recursive = TRUE )
                TRUE
            }
    foo( x )
}
