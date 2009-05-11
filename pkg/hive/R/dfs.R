## Functions related to the Hadoop Distributed File System (HDFS)

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
  system(sprintf("%s namenode -format", hadoop))
}

## out of simplicity queries status of / in DFS 
DFS_is_available <- function( henv = hive() ) {
  msg <- .DFS_intern( "-stat", "/", henv )
  if( length(msg) )
    return( TRUE )
  FALSE
}

## does file exist in DFS?
DFS_file_exists <- function( file, henv = hive() ) {
  status <- .DFS("-test -e", file, henv )
  if( status )
    return( FALSE )
  TRUE
}

## does dir exist in DFS?
DFS_dir_exists <- function( path, henv = hive() ) {
  status <- .DFS( "-test -d", path, henv )
  if( status )
    return( FALSE )
  TRUE
}

## create dir in DFS
DFS_dir_create <- function( path, henv = hive() ) {
  if( DFS_dir_exists(path, henv) ) {
    warning( sprintf("Directory '%s' already exists.", path) )
    return( invisible(FALSE) )
  }
  status <- .DFS( "-mkdir", path, henv )
  if( status ) {
    warning( sprintf("Cannot create dir '%s'.", path) )
    return( invisible(FALSE) )
  }
  invisible( TRUE )
}

# Delete repository
DFS_dir_remove <- function( path, recursive = TRUE, henv = hive() ) {
  if( DFS_dir_exists(path, henv) ){
    status <- .DFS( "-rmr", path, henv )
    if(status){
      warning(sprintf("Cannot remove dir '%s'.", path))
      return(invisible(FALSE))
    }
    TRUE
  }
  else {
    warning(sprintf("There is no dir '%s'.", path))
    FALSE
  }
}

DFS_list <- function( path = ".", henv = hive() ) {
  .DFS( "-ls", path, henv )
}

DFS_cat <- function( file, con = stdout(), henv = hive() ){
  stopifnot( DFS_file_exists( file, henv) )
  cat(.DFS( "-cat", file, henv ), con)
}

DFS_tail <- function(file, con = stdout(), henv = hive() ){
  stopifnot( DFS_file_exists( file, henv) )
  cat(.DFS( "-tail", file, henv ), con)
}

# Load local files into hadoop and distribute them along its nodes
DFS_put <- function( files, path = ".", henv = hive() ) {
  if( !DFS_dir_exists(path, henv) )
    DFS_dir_create( path, henv )
  status <- .DFS("-put", paste(paste(files, collapse = " "), path), henv )
  if( status ){
    warning( sprintf("Cannot put file(s) to '%s'.", path) )
    return( invisible(FALSE) )
  }
  invisible( TRUE )
}

## serialize R object to DFS
DFS_put_object <- function( obj, path = ".", henv = hive() ) {
  if( DFS_file_exists(path, henv) ){
    warning( sprintf("File '%s' already exists in DFS.", path) )
    return( invisible(FALSE) )
  }
  con <- .DFS_pipe( "-put", path, henv )
  serialize( obj, con )
  close.connection(con)
  invisible( TRUE )
}

.DFS <- function( cmd, args, henv )
  system( .DFS_create_command(cmd, args, henv), ignore.stderr = TRUE )

.DFS_pipe <- function( cmd, args, henv )
  pipe(.DFS_create_command(cmd, sprintf("- %s", args), henv), open = "w")

.DFS_intern <- function( cmd, args, henv )
  system( .DFS_create_command(cmd, args, henv), intern = TRUE, ignore.stderr = TRUE )

.DFS_create_command <- function( cmd, args, henv )
  sprintf("%s fs %s %s", hadoop(henv), cmd, args)

## Fetch distributed files and return them as character vector
## hadoop_cat_files <- function(dir = "input", con = stdout())
##    writeLines(system(sprintf("%s fs -cat %s/*", hadoop, dir), intern = TRUE), henv)

## hadoop_get_last_line <- function(file){
##   out <- system(sprintf("%s fs -tail %s", hadoop, file), intern = TRUE)
##   out[length(out)]
## }


# Fetch distributed files and return them as character vector
# provided that the results are in key, value pair format (should we check?)

hive_get_results <- function(path, henv = hive()){
  split_line <- function(line) {
    val <- unlist(strsplit(line, "\t"))
    list(key = val[1], value = as.integer(val[2]))
  }
  lines <- system(sprintf("%s fs -cat %s/part-*", hadoop(henv), path), intern = TRUE)
  splitted <- sapply(lines, split_line)
  keys <- unlist(splitted[1, ])
  values <- unlist(splitted[2, ])
  out <- values
  names(out) <- keys
  out
}
