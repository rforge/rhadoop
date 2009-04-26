## Handling the Hadoop framework

## .hinit() initializes the Hadoop framework and returns the corresponding environment
.hinit <- function( hadoop_home ) {
  if( missing(hadoop_home) )
    hadoop_home <- Sys.getenv("HADOOP_HOME")
  tmp <- tryCatch(hive_create(hadoop_home), error = identity )
  hive <- if( inherits(tmp, "error") )
    .hive_default_env()
  else
    tmp
  hive
}

hive_create <- function(hadoop_home){
  hive <- .create_hive_from_installation(hadoop_home)
  class(hive) <- "hive"
  hive
}

.create_hive_from_installation <- function(hadoop_home){
  if(!file.exists(hadoop_home))
    stop(sprintf("There is no directory '%s'.", hadoop_home))
  hive <- new.env()
  local({
      hadoop <- file.path(hadoop_home, "bin", "hadoop")
      stopifnot(file.exists(hadoop))
      configuration <- list(hadoop_default = get_hadoop_config("default", hadoop_home),
                            hadoop_site = get_hadoop_config("site", hadoop_home),
                            slaves = readLines(file.path(hadoop_home, "conf", "slaves")),
                            masters = readLines(file.path(hadoop_home, "conf", "masters")))
    }, hive)
  hive
}

## Default Environment: not available
.hive_default_env <- function(){
  NA
}

hive_is_valid <- function(henv){
  inherits(henv, "hive")
}

print.hive <- function(x, ...){
  writeLines("Hadoop Environment")
}

hive_start <- function(henv = hive()){
  if(hive_is_available(henv))
    return(invisible(TRUE))
  hadoop_framework_control("start", henv)
  if(!hive_is_available(henv)){
    # writeLines(msg)
    hive_stop(henv)
  }
  invisible(hive_is_available(henv))
}

hive_stop <- function(henv = hive()){
  if(hive_is_available(henv))
    hadoop_framework_control("stop", henv)
  else
    warning("Hadoop not running. Nothing to stop.")
  invisible(TRUE)
}

## FIXME: Very simple query of hadoop status. Probably better to use pid?
hive_is_available <- function(henv = hive()){
  stopifnot(hive_is_valid(henv))
  DFS_is_available(henv)
}

## internal functions
hadoop <- function(henv)
  get("hadoop", henv)

hadoop_home <- function(henv)
  get("hadoop_home", henv)

hadoop_framework_control <- function(action, henv){
  system(file.path(hadoop_home(henv), "bin", sprintf("%s-all.sh", action)), intern = TRUE)
}
