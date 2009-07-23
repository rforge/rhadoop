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

hive_create <- function( hadoop_home ){
  hive <- .create_hive_from_installation(hadoop_home)
  class(hive) <- "hive"
  hive
}

## given a pointer to a Hadoop installation directory, this function creates an environment
## containing all information about the Hadoop cluster
.create_hive_from_installation <- function( hadoop_home ){
  if(!file.exists(hadoop_home))
    stop(sprintf("There is no directory '%s'.", hadoop_home))
  hive <- new.env()
  hvers <- hadoop_get_version(hadoop_home)
  ## config files are split and located in different places since version 0.20.0
  if(hvers < "0.20.0"){
      local({
          hadoop <- file.path(hadoop_home, "bin", "hadoop")
          version <- hvers
          stopifnot(file.exists(hadoop))
          config_files <- list(hadoop_default = get_hadoop_config("hadoop-default.xml", file.path(hadoop_home, "conf")),
                                hadoop_site = get_hadoop_config("hadoop-site.xml", file.path(hadoop_home, "conf")),
                                slaves = readLines(file.path(hadoop_home, "conf", "slaves")),
                                masters = readLines(file.path(hadoop_home, "conf", "masters")))
      }, hive)
  } else {
      local({
          hadoop <- file.path(hadoop_home, "bin", "hadoop")
          version <- hvers
          stopifnot(file.exists(hadoop))
          config_files <- list(core_default = get_hadoop_config("core-default.xml", file.path(hadoop_home, "src/core")),
                                core_site = get_hadoop_config("core-site.xml", file.path(hadoop_home, "conf")),
                                hdfs_default = get_hadoop_config("hdfs-default.xml", file.path(hadoop_home, "src/hdfs")),
                                hdfs_site = get_hadoop_config("hdfs-site.xml", file.path(hadoop_home, "conf")),
                                mapred_default = get_hadoop_config("mapred-default.xml", file.path(hadoop_home, "src/mapred")),
                                mapred_site = get_hadoop_config("mapred-site.xml", file.path(hadoop_home, "conf")),
                                slaves = readLines(file.path(hadoop_home, "conf", "slaves")),
                                masters = readLines(file.path(hadoop_home, "conf", "masters")))
      }, hive)
  }
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

  ## if there are problems starting hive, close it
  if(!hive_is_available(henv)){
    # writeLines(msg)
    suppressWarnings(hive_stop(henv))
  }
  else {
    add_java_DFS_support(henv = hive())
  }
  invisible(TRUE)
}

hive_stop <- function(henv = hive()){
  if(hive_is_available(henv))
    hadoop_framework_control("stop", henv)
  else
    warning("No Hadoop cluster running. Nothing to stop.")
  invisible(TRUE)
}

## FIXME: Very simple query of hadoop status. Probably better to use pid?
hive_is_available <- function(henv = hive()){
  stopifnot(hive_is_valid(henv))
  suppressWarnings(DFS_is_available(henv))
}

## internal functions
hadoop <- function(henv)
  get("hadoop", henv)

hadoop_home <- function(henv)
  get("hadoop_home", henv)

hadoop_version <- function(henv)
  get("version", henv)

hadoop_get_version <- function(hadoop_home){
  version <- readLines(file.path(hadoop_home, "contrib", "hod", "bin", "VERSION"))
  version
}
                   
hadoop_framework_control <- function(action = c("start", "stop"), henv){
  action <- match.arg(action)
  system(file.path(hadoop_home(henv), "bin", sprintf("%s-all.sh", action)), intern = TRUE)
}
