## Functions related to the Hadoop Distributed File System (HDFS)
## Author: Stefan Theussl

## use with caution
## FIXME: not working yet, too dangerous
DFS_format <- function(henv){
  ##machines, DFS_root= "/var/tmp/hadoop"
  stopifnot(hive_stop(henv))
  machines <- unique(c(hive_get_slaves(henv), hive_get_masters(henv)))
  DFS_root <- gsub("\\$\\{user.name\\}", system("whoami", intern = TRUE),
                   hive_get_parameter("hadoop.tmp.dir", henv))
  for(machine in machines){
    ## delete possibly corrupted file system
    command <- sprintf("ssh %s 'rm -rf %s/*
rm -rf %s-*' ", machine, DFS_root, DFS_root)
    system(command)
  }
  ## reformat DFS
  system(sprintf("%s namenode -format", hadoop(henv)))
}

## out of simplicity queries status of / in DFS (Java)
DFS_is_available <- function( henv = hive() ) {
  
  stat <- .DFS_stat( "/", henv )
  if( is.null(stat) || is.na(stat) )
    return( FALSE )
  TRUE
}

## does file exist in DFS? (Java)
DFS_file_exists <- function( file, henv = hive() ) {
  hdfs <- HDFS(henv)
  hdfs$exists(HDFS_path(file))
}

## does dir exist in DFS? (Java)
DFS_dir_exists <- function( path, henv = hive() ) {
  status <- tryCatch(.DFS_getFileStatus(path, henv ), error = identity)
  if(inherits(status, "error"))
    return(FALSE)
  status$isDir()
}

## create dir in DFS (Java)
## TODO: throws getClass error although function calls work in global env
## interestingly, when debugging and calling DFS_mkdir() twice it works ...
DFS_dir_create <- function( path, henv = hive() ) {
  if( DFS_dir_exists(path, henv) ) {
    warning( sprintf("directory '%s' already exists.", path) )
    return( invisible(FALSE) )
  }
  if( DFS_file_exists(path, henv) ) {
    warning( sprintf("'%s' already exists but is not a directory", path) )
    return( invisible(FALSE) )
  }
  status <- .DFS_mkdir( path, henv )
  if( is.null(status) ) {
    warning( sprintf("cannot create dir '%s'.", path) )
    return( invisible(FALSE) )
  }
  invisible( TRUE )
}

## Delete files in DFS (Java)
DFS_delete <- function( file, recursive = FALSE, henv = hive() ) {
  if( DFS_dir_exists(file, henv) && !recursive){
    warning(sprintf("cannot remove directory '%s'. Use 'recursive = TRUE' instead.", file))
    return(FALSE)
  }
  
  status <- .DFS_delete( file, henv )
  if(!status){
    warning(sprintf("cannot remove file '%s'.", file))
    return(FALSE)
  }
  TRUE
}

DFS_dir_remove <- function(path, recursive = TRUE, henv = hive()){
  if( DFS_dir_exists(path, henv) ){
    DFS_delete(path, recursive, henv)
    TRUE
  } else {
    warning(sprintf("'%s' is not a directory.", path))
    FALSE
  }
} 
     

## private int ls(String srcf, boolean recursive) throws IOException {
##    Path srcPath = new Path(srcf);
##    FileSystem srcFs = srcPath.getFileSystem(this.getConf());
##    FileStatus[] srcs = srcFs.globStatus(srcPath);
##    if (srcs==null || srcs.length==0) {
##      throw new FileNotFoundException("Cannot access " + srcf + 
##          ": No such file or directory.");
##    }
## 
##    boolean printHeader = (srcs.length == 1) ? true: false;
##    int numOfErrors = 0;
##    for(int i=0; i<srcs.length; i++) {
##      numOfErrors += ls(srcs[i], srcFs, recursive, printHeader);
##    }
##    return numOfErrors == 0 ? 0 : -1;
##  }


DFS_list <- function( path = ".", henv = hive() ) {
  globstat <- .DFS_stat(path, henv)
  if( is.null(globstat) ){
    warning(sprintf("'%s' is not a readable directory", path))
    return(character(0))
  }
  
  splitted <- strsplit(grep(path, hive:::.DFS_intern("-ls", path, henv), value = TRUE), path)
  sapply(splitted, function(x) basename(x[2]))
}

DFS_cat <- function( x, henv = hive() ){
  stopifnot( DFS_file_exists(x, henv) )
  .DFS("-cat", x, henv)
}

DFS_tail <- function(file, n = 6L, henv = hive() ){
  stopifnot( as.integer(n) > 0L )
  stopifnot( DFS_file_exists(file, henv) )
  out <- .DFS_tail(file, henv = henv)
  len <- length(out)
  out[(len - (n - 1)) : len]
}

.DFS_tail <- function(file, size = 1024, henv = hive()){
  hdfs <- HDFS(henv)
  ioutils <- IOUTILS(henv)
  
  hdfs_file <- HDFS_path(file)
  len <- hdfs$getFileStatus(hdfs_file)$getLen()
  offset <- ifelse(len > size, len - size, 0)


  inputstream <- hdfs$open(hdfs_file)
  inputstream$seek(.jlong(offset))

  ## we need to copy the contents of the file to an output stream
  ## Thus, for the time being we use the JRI class to RConsoleOutputStream divert
  ## the outputstream to the R console
  routput <- .jnew("org/rosuda/JRI/RConsoleOutputStream", .jengine(TRUE), as.integer(0))
  ## now we need to capture the contents from the console usingg a text connection
  ## we save the results in the object out
  con <- textConnection("out", open = "w")
  sink(file = con)
  ioutils$copyBytes(inputstream, routput, as.integer(1024), TRUE)
  sink()
  close(con)
  out
}

## Note that fs -tail only outputs the last kilobyte!!!
## for us this is typically not very practical.
## as long as we have no HDFS C interface:
DFS_tail_long <- function(file, n = 1L, henv = hive() ){
  stopifnot( as.integer(n) == 1L )
  stopifnot( DFS_file_exists(file, henv) )
  out <- paste(suppressWarnings(.DFS_intern( "-cat", paste(file, " | tail -n", n, sep = ""), henv )), collapse = "" )
  out
}

# Load local files into hadoop and distribute them along its nodes
DFS_put <- function( files, to = ".", henv = hive() ) {
  if(length(files) == 1)
    status <- .DFS("-put", paste(files, to), henv )
  else {
    if( !DFS_dir_exists(to, henv) )
      DFS_dir_create( to, henv )
    status <- .DFS("-put", paste(paste(files, collapse = " "), to), henv )
  }
  if( status ){
    warning( sprintf("Cannot put file(s) to '%s'.", to) )
    return( invisible(FALSE) )
  }
  invisible( TRUE )
}

## serialize R object to DFS
DFS_put_object <- function( obj, file, henv = hive() ) {
  con <- .DFS_pipe( "-put", file, open = "w", henv = henv )
  status <- tryCatch(serialize( obj, con ), error = identity)
  close.connection(con)
  if(inherits(status, "error"))
    stop("Serialization failed.")
  invisible(file)
}

## worse performance than read_lines2, reason: paste
DFS_write_lines <- function( text, file, henv = hive(), ... ) {
  if(DFS_file_exists(file)){
    warning(sprintf("file '%s' already exists.", file))
    return(NA)
  }

  if(!length(text))
    stop("text length of zero not supported.")
  
  hdfs <- HDFS(henv)
  
  outputstream <- hdfs$create(HDFS_path(file))
#  outputstream$write(text)
  for( i in 1:length(text) ){
    outputstream$writeBytes(text[i])
    outputstream$writeBytes("\n")
  }
  outputstream$close()

  invisible(file)
}

DFS_write_lines2 <- function( text, file, henv = hive(), ... ) {
  con <- .DFS_pipe( "-put", file, open = "w", henv = henv )
  status <- tryCatch( writeLines(text = text, con = con, ...), error = identity )
  close.connection(con)
  if(inherits(status, "error"))
    stop("Cannot write to connection.")
  invisible(file)
}

## TODO: DFS_read_lines3 should become the default -> a lot more efficient
DFS_read_lines <- function( file, n = -1L, henv = hive(), ... ) {
  if(!DFS_file_exists(file)){
    warning(sprintf("file '%s' does not exists.", file))
    return(NA)
  }
  hdfs <- HDFS(henv)
  
  inputstream <- hdfs$open(HDFS_path(file))
  ## FIXME: allocate vector before reading from inputstream
  out <- character()
  if( n <= 0 ){
    i <- 1
    while( (! is.null(input))  ){
      input <- inputstream$readLine()
      out <- c(out, input)
      i <- i + 1
    }
  }
  else
    for(i in 1:n)
      out[i] <- inputstream$readLine()
  inputstream$close()
  out
}

DFS_read_lines3 <- function( file, n = -1L, henv = hive(), ... ) {
  if(!DFS_file_exists(file)){
    warning(sprintf("file '%s' does not exists.", file))
    return(NA)
  }
  hdfs <- HDFS(henv)
  ioutils <- IOUTILS(henv)
  offset <- 0
  inputstream <- hdfs$open(HDFS_path(file))
  inputstream$seek(.jlong(offset))

  ## we need to copy the contents of the file to an output stream
  ## Thus, for the time being we use the JRI class to RConsoleOutputStream divert
  ## the outputstream to the R console
  routput <- .jnew("org/rosuda/JRI/RConsoleOutputStream", .jengine(TRUE), as.integer(0))
  ## now we need to capture the contents from the console usingg a text connection
  ## we save the results in the object out
  con <- textConnection("out", open = "w")
  sink(file = con)
  ioutils$copyBytes(inputstream, routput, as.integer(1024), TRUE)
  sink()
  close(con)

  #inputstream$close()
  out
}

## serialize R object from DFS
DFS_get_object <- function( file, henv = hive() ) {
  con <- .DFS_pipe( "-cat", file, open = "r", henv = henv )
  obj <- tryCatch( unserialize(con), error = identity)
  close.connection(con)
  if(inherits(obj, "error"))
     return(NA)
  obj
}

## deletes a file or empty directory
## returns TRUE if successful and FALSE otherwise
## caution: always deletes recursively!
.DFS_delete <- function(x, henv){
   hdfs <- HDFS(henv)
   hdfs$delete(HDFS_path(x))
}

## creates directory on DFS
## returns TRUE if successful and NULL otherwise
.DFS_mkdir <- function(x, henv){
  hdfs <- HDFS(henv)
  hdfs$mkdirs(HDFS_path(x))
}

.DFS_stat <- function(x, henv){
  hdfs <- HDFS(henv)
  if(is.null(hdfs)){
    warning("no HDFS found in Hadoop environment")
    return(NA)
  } 
  stat <- hdfs$globStatus(HDFS_path(x))
  if(is.null(stat)){
    warning(sprintf("cannot stat '%s': No such file or directory", x))
    return(NULL)
  }
  ## for the time being return TRUE
  ## TODO: this should return an R object containing the stat information 
  TRUE
}

.DFS_getFileStatus <- function(x, henv){
  hdfs <- HDFS(henv)
  hdfs$getFileStatus(HDFS_path(x))
}

############################################################
## Old command line wrappers
## FIXME: all of them have to be replaced by Java routines

.DFS <- function( cmd, args, henv )
  system( .DFS_create_command(cmd, args, henv), ignore.stderr = TRUE )

.DFS_pipe <- function( cmd, args, open = "w", henv ){
  if(open == "w")
    pipe(.DFS_create_command(cmd, sprintf("- %s", args), henv), open = open)
  else
    pipe(.DFS_create_command(cmd, args, henv), open = open)
}

.DFS_intern <- function( cmd, args, henv )
  system( .DFS_create_command(cmd, args, henv), intern = TRUE, ignore.stderr = TRUE )

.DFS_create_command <- function( cmd, args, henv )
  sprintf("%s fs %s %s", hadoop(henv), cmd, args)
