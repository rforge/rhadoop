## Handling the Hadoop framework

## .hinit() initializes the Hadoop framework and returns the corresponding environment.
.hinit <- function( hadoop_home ) {
  if( missing(hadoop_home) )
    hadoop_home <- Sys.getenv( "HADOOP_HOME" )
  tmp <- tryCatch( hive_create(hadoop_home), error = identity )
  hive <- if( inherits(tmp, "error") )
    .hive_default_env()
  else
    tmp
  hive
}

## See also .create_hive_from_installation.
## Returns on object of class 'hive'.
hive_create <- function( hadoop_home ){
  hive <- .create_hive_from_installation( file_path_as_absolute(hadoop_home) )
  hive_set_nreducer( hive_default_nreducer(hive), hive )
  class( hive ) <- "hive"
  hive
}

## Given a pointer to a Hadoop installation directory, this function
## creates an environment containing all information about the Hadoop
## cluster.  We store the hadoop home directory, the hadoop version,
## and the parsed configuration files in a separate R environment.
.create_hive_from_installation <- function( hadoop_home ){
  if( !file.exists(hadoop_home) )
    stop( sprintf("There is no directory '%s'.", hadoop_home) )
  hive <- new.env()
  hvers <- hadoop_get_version( hadoop_home )
  ## config files are split and located in different places since version 0.20.0
  if( hvers < "0.20.0" ){
      local( {
          hadoop <- file.path(hadoop_home, "bin", "hadoop")
          version <- hvers
          stopifnot(file.exists(hadoop))
          config_files <- list(hadoop_default = get_hadoop_config("hadoop-default.xml", file.path(hadoop_home,"conf")),
                               hadoop_site = get_hadoop_config("hadoop-site.xml", file.path(hadoop_home,"conf")),
                                slaves = readLines(file.path(hadoop_home, "conf", "slaves")),
                                masters = readLines(file.path(hadoop_home, "conf", "masters")))
      }, hive )
  } else {
      local( {
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
          hadoop_jars <- c(file.path(hadoop_home, sprintf("hadoop-%s-core.jar", version)), file.path(hadoop_home, "lib", "commons-logging-1.0.4.jar"))

      }, hive )
  }
  hive
}

## Default environment: NA
.hive_default_env <- function(){
  NA
}

## Checks if henv inherits from class 'hive'
hive_is_valid <- function( henv ){
  inherits(henv, "hive")
}

## Provides information about the "hive"
print.hive <- function( x, ... ){
  writeLines( "HIVE: Hadoop Cluster" )
  writeLines( sprintf("- Avail. datanodes: %d", length(hive_get_slaves(x))) )
  writeLines( sprintf("'- Max. number Map tasks per datanode: %s",
                      hive_get_parameter("mapred.tasktracker.map.tasks.maximum", x)) )
  writeLines( sprintf("'- Configured Reducer tasks: %d",
                      hive_get_nreducer(x)) )
}

summary.hive <- function( object, ... ){
    print(object)
    writeLines( "---" )
    writeLines( sprintf("- Hadoop version: %s", hadoop_version(hive(object))) )
    writeLines( sprintf("- Hadoop home directory: %s", hadoop_home(object)) )
    writeLines( sprintf("- Namenode: %s", hive_get_masters(object)) )
    writeLines( "- Datanodes:")
    writeLines( sprintf("'- %s\n", hive_get_slaves(object)) )
}

## Start and stop a Hadoop cluster.
## NOTE: Java DFS support is only available for the current cluster.
##       Thus, add/remove DFS support in each call to hive_start/stop
hive_start <- function( henv = hive() ){
    if( DFS_is_registered(henv) )
        return( invisible(TRUE) )
    ## does nothing if hadoop daemons already running
    hadoop_framework_control( "start", henv )
    ## NOTE: really important that java DFS support is added AFTER
    ## framework has been started
    status <- add_java_DFS_support( henv = hive() )
    ## if there are problems starting hive, close it
    if( !status ){
        # writeLines(msg)
        suppressWarnings( hive_stop(henv) )
        return( invisible(FALSE) )
    }
    invisible( TRUE )
}

hive_stop <- function( henv = hive() ){
  if( DFS_is_registered(henv) ){
    remove_java_DFS_support( henv )
    hadoop_framework_control( "stop", henv )
  }
  else
    warning( "No Hadoop cluster running. Nothing to stop." )
  invisible( TRUE )
}

## FIXME: Very simple query of hadoop status. Probably better to use pid?
hive_is_available <- function( henv = hive() ){
    stopifnot( hive_is_valid(henv) )
    suppressWarnings( DFS_is_available(henv) )
}


hive_get_nreducer <- function( henv = hive() ){
  get( "nreducer", henv )
}

hive_set_nreducer <- function( n, henv = hive() ) {
  assign( "nreducer", as.integer(n), henv )
}

hive_default_nreducer <- function( henv = hive() ){
  as.integer( length(hive_get_slaves(henv)) / 1.5 )
}

## Internal extractor functions
hadoop <- function( henv )
  get( "hadoop", henv )

hadoop_home <- function( henv )
  get( "hadoop_home", henv )

hadoop_version <- function( henv )
  get( "version", henv )

hadoop_get_jars <- function( henv )
  get( "hadoop_jars", henv )

hadoop_get_version <- function( hadoop_home ){
  version <- readLines( file.path(hadoop_home, "contrib", "hod", "bin", "VERSION") )
  version
}

## Controlling the Hadoop framework: currently using the start/stop_all.sh scripts
## in the $HADOOP_HOME/bin directory
## FIXME: This may not be platform independent
hadoop_framework_control <- function( action = c("start", "stop"), henv ){
  action <- match.arg(action)
  system( file.path(hadoop_home(henv), "bin", sprintf("%s-all.sh", action)), intern = TRUE )
}
