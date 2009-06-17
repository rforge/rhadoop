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

## TODO: automatically retrieve version number from installation
hive_create <- function(hadoop_home, version = 0.19){
  hive <- .create_hive_from_installation(hadoop_home, version)
  class(hive) <- "hive"
  hive
}

.create_hive_from_installation <- function(hadoop_home, version){
  if(!file.exists(hadoop_home))
    stop(sprintf("There is no directory '%s'.", hadoop_home))
  hive <- new.env()
  if(version <= 0.19){
      local({
          hadoop <- file.path(hadoop_home, "bin", "hadoop")
          stopifnot(file.exists(hadoop))
          configuration <- list(hadoop_default = get_hadoop_config("hadoop-default.xml", file.path(hadoop_home, "conf")),
                                hadoop_site = get_hadoop_config("hadoop-site.xml", file.path(hadoop_home, "conf")),
                                slaves = readLines(file.path(hadoop_home, "conf", "slaves")),
                                masters = readLines(file.path(hadoop_home, "conf", "masters")))
      }, hive)
  } else {
      local({
          hadoop <- file.path(hadoop_home, "bin", "hadoop")
          stopifnot(file.exists(hadoop))
          configuration <- list(core_default = get_hadoop_config("core-default.xml", file.path(hadoop_home, "src/core")),
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
