################################################################################
## DStorage: the distributed storage class
## All functions dealing with the storage implementation
## of the DList class are provided here.
## Each storage is of general class 'DStorage' and of a subclass defining
## the actual storage, e.g. 'LFS', 'HDFS'
################################################################################


################################################################################
## DStorage (DS) high level constructor
################################################################################

DStorage_create <- function( type = c("LFS", "HDFS"),
                             base_dir,
                             chunksize = 1024^2 ){
    type <- match.arg( type )
    .DS_init( switch(type,
                     "LFS"  = .make_LFS_storage(base_dir, chunksize),
                     "HDFS" = .make_HDFS_storage(base_dir, chunksize)) )
}

################################################################################
## Default storage (local disk)
################################################################################

DS_default <- function(){
    .DS_init( .make_LFS_storage(base_dir  = tempdir(),
                                chunksize = 10 * 1024^2) )
}

################################################################################
## DS_storage low level constructor
################################################################################

.DStorage <- function( type,
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
              class = c(type, "DStorage"))
}

.make_LFS_storage <- function( base_dir, chunksize, ... ){
    .DStorage( type = "LFS",
               base_directory  = base_dir,
               chunksize       = chunksize,
               dir_create      = function(x) base::dir.create(x, showWarnings = FALSE),
               fetch_last_line = function(x) utils::tail(base::readLines(as.character(x)), n = 1L),
               list_directory  = base::dir,
               read_lines      = function(x) base::readLines(as.character(x)),
               unlink          = function(x) DSL:::LFS_remove(x),
               write_lines     = function(text, fil) base::writeLines(text, con = as.character(fil)), ...
              )
}

.make_HDFS_storage <- function( base_dir, chunksize, ... ){
    .DStorage( type = "HDFS",
               base_directory  = base_dir,
               chunksize       = chunksize,
               dir_create      = function(x) hive::DFS_dir_create(x, henv = hive::hive()),
               fetch_last_line = function(x) hive::DFS_tail(n = 1L, as.character(x), henv = hive::hive()),
               list_directory  = function(x) hive::DFS_list(x, henv = hive::hive()),
               read_lines      = function(x) hive::DFS_read_lines(as.character(x), henv = hive::hive()),
               unlink          = function(x) hive::DFS_delete(x, recursive = TRUE, henv = hive::hive()),
               write_lines     = function(text, fil) hive::DFS_write_lines(text, as.character(fil), henv = hive::hive()), ...
              )
}

################################################################################
## DStorage S3 methods
################################################################################

is.DStorage <- function( ds )
    inherits( ds, "DStorage" )

as.DStorage <- function( x )
    UseMethod("as.DStorage")

as.DStorage.DStorage <- identity

print.DStorage <- function( x, ... ){
    writeLines( "DStorage." )
    writeLines( sprintf("- Type: %s", class(x)[1] ) )
    writeLines( sprintf("- Base directory on storage: %s", DS_base_dir(x)) )
    writeLines( sprintf("- Current chunk size [bytes]: %s", DS_chunksize(x)) )
}

summary.DStorage <- function( object, ... ){
    print( object )
    writeLines( sprintf("- Registered methods: %s",
                        paste( names(object)[!(names(object)
                                                %in% c("description", "chunksize", "base_directory"))],
                              collapse = ", ")))
}

################################################################################
## DStorage accessor and replacement functions for class "DCorpus"
################################################################################

DStorage <- function( x )
  UseMethod("DStorage")

DStorage.DStorage <- identity

DStorage.DList <- function( x )
  attr(x, "DStorage")

`DStorage<-` <- function( x, value )
  UseMethod("DStorage<-")

## FIXME
`DStorage<-.DList` <- function(x, value){
    old_stor <- DStorage(x)
    if( inherits(old_stor, "LFS") && inherits(value, "HDFS") ){
        if( !length(hive::DFS_list(DS_base_dir(value))) ){
            for( rev in .revisions(x) )
                hive::DFS_put( file.path(DS_base_dir(old_stor), rev),
                              file.path(DS_base_dir(value), rev) )
        }
        else {
            if( !.check_contents_of_storage(x, value) )
                stop("HDFS storage already contains this directory with different data for the active revision")
        }
    } else {
        stop("not implemented!")
    }
    attr(x, "DStorage") <- value
    x
}

################################################################################
## DStorage helper functions
################################################################################

## helper function for DList -> list coercion
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
    if( is.DStorage(x) )
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
    stopifnot( is.DStorage(storage) )
    dir <- sub("/./", "", file.path(DS_base_dir(storage), dir ))
    storage$dir_create( dir )
}

DS_list_directory <- function( storage, dir = "." ){
    stopifnot( is.DStorage(storage) )
    if( dir == "." )
        dir <- ""
    storage$list_directory( file.path(DS_base_dir(storage), dir ) )
}

DS_fetch_last_line <- function( storage, file ){
    stopifnot( is.DStorage(storage) )
    file <- sub("/./", "", file.path(DS_base_dir(storage), file ))
    storage$fetch_last_line( file )
}

DS_read_lines <- function( storage, file ){
    stopifnot( is.DStorage(storage) )
    #file <- sub("/./", "", file.path(storage$base_directory, file ))
    file <- file.path(DS_base_dir(storage), file )
    storage$read_lines( file )
}

## NOTE: due to security reasons no recursive unlinking is permitted!
## deletes a link on the corresponding storage (file, directory)
DS_unlink <- function( storage, link ){
    stopifnot( is.DStorage(storage) )
    link <- sub("/./", "", file.path(DS_base_dir(storage), link ))
    storage$unlink( link )
}

DS_write_lines <- function( storage, text, file ){
    stopifnot( is.DStorage(storage) )
    file <- sub("/./", "", file.path(DS_base_dir(storage), file ))
    storage$write_lines( text, file )
}

check_storage_for_sanity <- function( storage ){
    stopifnot( is.DStorage(storage) )

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

LFS_remove <- function( x ){
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
