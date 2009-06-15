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
  system(sprintf("%s namenode -format", hadoop(henv)))
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
  out <- .DFS_intern( "-tail", file, henv )
  len <- length(out)
  out[(len - (n - 1)) : len]
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

DFS_write_lines <- function( text, file, henv = hive(), ... ) {
  con <- .DFS_pipe( "-put", file, open = "w", henv = henv )
  status <- tryCatch( writeLines(text = text, con = con, ...), error = identity )
  close.connection(con)
  if(inherits(status, "error"))
    stop("Cannot write to connection.")
  invisible(file)
}

## serialize R object from DFS
DFS_read_lines <- function( file, n = -1L, henv = hive(), ... ) {
  con <- .DFS_pipe( "-cat", file, open = "r", henv = henv )
  text <- tryCatch( readLines(con = con, n = -1L, ...), error = identity)
  close.connection(con)
  if(inherits(text, "error"))
     return(NA)
  if(n > 0L)
    return(text[1L:n])
  text
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
