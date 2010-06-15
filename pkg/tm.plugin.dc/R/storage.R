## All functions dealing with the storage implementation
## of the DistributedCorpus are provided here.
## Each storage is of general class 'dc_storage' and of a subclass defining
## the actual storage, e.g. 'local_disk', 'HDFS'
################################################################################



################################################################################
## dc_storage high level constructor
################################################################################

dc_storage_create <- function( type = c("local_disk", "HDFS"),
                               base_dir,
                               chunksize = 1024^2 ){
    type <- match.arg(type)
    .dc_storage_init( switch(type,
            "local_disk" = .make_local_disk_storage(base_dir, chunksize),
            "HDFS"       = .make_HDFS_storage      (base_dir, chunksize)) )
}

################################################################################
## Default storage (local disk)
################################################################################

dc_default_storage <- function(){
    .dc_storage_init( .make_local_disk_storage(base_dir  = tempfile(),
                                               chunksize = 10 * 1024^2) )
}

################################################################################
## dc_storage low level constructor
################################################################################

.dc_storage <- function(type,
                        base_directory,
                        chunksize,
                        dir_create,
                        fetch_last_line,
                        list_directory,
                        read_lines,
                        unlink,
                        write_lines){
    structure(list(base_directory = base_directory,
                   chunksize = chunksize,
                   dir_create = dir_create,
                   fetch_last_line = fetch_last_line,
                   list_directory = list_directory,
                   read_lines = read_lines,
                   unlink = unlink,
                   write_lines = write_lines),
              class = c(type, "dc_storage"))
}

.make_local_disk_storage <- function( base_dir, chunksize, ... ){
    .dc_storage( type            = "local_disk",
                 base_directory  = base_dir,
                 chunksize       = chunksize,
                 dir_create      = base::dir.create,
                 fetch_last_line = function(x) utils::tail(base::readLines(as.character(x)), n = 1L),
                 list_directory  = base::dir,
                 read_lines      = function(x) base::readLines(as.character(x)),
                 unlink          = function(x) base::file.remove(x),
                 write_lines     = function(text, fil) base::writeLines(text, con = as.character(fil)), ...
                )
}

.make_HDFS_storage <- function( base_dir, chunksize, ... ){
    .dc_storage( type = "HDFS",
                 base_directory  = base_dir,
                 chunksize       = chunksize,
                 dir_create      = function(x) hive::DFS_dir_create(x, henv = hive()),
                 fetch_last_line = function(x) hive::DFS_tail(n = 1L, as.character(x), henv = hive()),
                 list_directory  = function(x) hive::DFS_list(x, henv = hive()),
                 read_lines      = function(x) hive::DFS_read_lines(as.character(x), henv = hive()),
                 unlink          = function(x) hive::DFS_dir_remove(x, henv = hive()),
                 write_lines     = function(text, fil) hive::DFS_write_lines(text, as.character(fil), henv = hive()), ...
                )
}

################################################################################
## dc_storage S3 methods
################################################################################

is.dc_storage <- function( ds )
    inherits( ds, "dc_storage" )

as.dc_storage <- function( ds, ... )
    UseMethod("as.dc_storage")

as.dc_storage.dc_storage <- function(ds, ...)
  identity(ds)

print.dc_storage <- function( x, ... ){
    writeLines( "DistributedCorpus: Storage" )
    writeLines( sprintf("- Type: %s", class(x)[1] ) )
    writeLines( sprintf("- Base directory on storage: %s", dc_base_dir(x)) )
    writeLines( sprintf("- Current chunk size [bytes]: %s", dc_chunksize(x)) )
}

summary.dc_storage <- function( object, ... ){
    print( object )
    writeLines( sprintf("- Registered methods: %s",
                        paste( names(object)[!(names(object)
                                                %in% c("description", "chunksize", "base_directory"))],
                              collapse = ", ")))
}

################################################################################
## dc_storage accessor and replacement functions for class "DistributedCorpus"
################################################################################

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
    if( !DFS_dir_exists(dc_base_dir(value)) ){
      DFS_dir_create( dc_base_dir(value) )
      DFS_put( file.path( dc_base_dir(old_stor), attr(x, "ActiveRevision")),
               file.path( dc_base_dir(value), attr(x, "ActiveRevision")) )
    }
    else {
      if( !.check_contents_of_storage(x, value) )
        stop("HDFS storage already contains this directory with different data for the active revision")
    }
  } else {
    stop("not implemented!")
  }
  attr(x, "Storage") <- value
  x
}

################################################################################
## dc_storage helper functions
################################################################################

## helper function for DistributedCorpus -> Corpus coercion
## checks if all chunks are already in place
## FIXME: we need to provide some checksums here
.check_contents_of_storage <- function(x, value){
    all( attr(x, "Chunks")[[ attr(x, "ActiveRevision") ]] %in%
        tm.plugin.dc:::dc_list_directory( value, attr(x, "ActiveRevision")) )
}

.dc_empty_storage <- function(){
    NA
}

## .storage_init() initializes the storage to be used for the distributed corpus.
.dc_storage_init <- function( x ) {
    if( missing(x) )
        x <- dc_default_storage()
    ## Create storage base directory
    if( is.dc_storage(x) )
        x$dir_create( dc_base_dir(x) )

    out <- if( inherits(tryCatch(check_storage_for_sanity(x),
                                 error = identity), "error") )
        .dc_empty_storage()
    else
        x
    out
}

dc_base_dir <- function( storage )
    storage$base_directory

dc_chunksize <-function( storage )
    storage$chunksize

dc_dir_create <- function( storage, dir ){
    stopifnot( is.dc_storage(storage) )
    dir <- sub("/./", "", file.path(dc_base_dir(storage), dir ))
    storage$dir_create( dir )
}

dc_list_directory <- function( storage, dir = "." ){
    stopifnot( is.dc_storage(storage) )
    if( dir == "." )
        dir <- ""
    storage$list_directory( file.path(dc_base_dir(storage), dir ) )
}

dc_fetch_last_line <- function( storage, file ){
    stopifnot( is.dc_storage(storage) )
    file <- sub("/./", "", file.path(dc_base_dir(storage), file ))
    storage$fetch_last_line( file )
}

dc_read_lines <- function( storage, file ){
    stopifnot( is.dc_storage(storage) )
    #file <- sub("/./", "", file.path(storage$base_directory, file ))
    file <- file.path(dc_base_dir(storage), file )
    storage$read_lines( file )
}

## NOTE: due to security reasons no recursive unlinking is permitted!
## deletes a link on the corresponding storage (file, directory)
dc_unlink <- function( storage, link ){
    stopifnot( is.dc_storage(storage) )
    link <- sub("/./", "", file.path(dc_base_dir(storage), link ))
    storage$unlink( link )
}

dc_write_lines <- function( storage, text, file ){
    stopifnot( is.dc_storage(storage) )
    file <- sub("/./", "", file.path(dc_base_dir(storage), file ))
    storage$write_lines( text, file )
}

check_storage_for_sanity <- function( storage ){
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
