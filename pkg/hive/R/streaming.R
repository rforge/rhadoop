## Function to facilitate the usage of Hadoop Streaming

## High-level wrapper for hadoop-streaming (MapReduce)
## TODO: ncpu argument -> probably via -D mapred.jobtracker.maxtasks.per.job=..
## TODO: command env arg in henv ?
hive_stream <- function(mapper, reducer, input, output, henv = hive(),
                        mapper_args = NULL, reducer_args = NULL, cmdenv_arg=NULL) {  
  ## check directories in DFS
  stopifnot(DFS_dir_exists(input, henv))
  
  ## set additional args to Hadoop Streaming
  streaming_args <- "mapred.reduce.tasks=1"
  if(missing(reducer)){
    streaming_args <- "mapred.reduce.tasks=0"
    reducer <- NULL
  }

  ## check if mapper and reducer scripts exists  
  stopifnot(file.exists(mapper))
  if(!is.null(reducer))
    stopifnot(file.exists(reducer))

  ## check args
  if(is.null(mapper_args))
    mapper_args <- ""
  if(is.null(reducer_args))
    reducer_args <- ""
  if(is.null(cmdenv_arg))
    cmdenv_arg <- ""

  
  ## start hadoop streaming
  msg <- .hadoop_streaming(mapper, reducer, input, output, mapper_args, reducer_args,
                           streaming_args, cmdenv_arg, henv)
  invisible(msg)
}

.hadoop_streaming <- function(mapper, reducer, input, output, mapper_args, reducer_args,
                              streaming_args, cmdenv_arg, henv){
  files <- paste(unique(c("", mapper, reducer)), collapse = " -file ")
  mapper_arg <- sprintf("-mapper '%s %s'", mapper, as.character(mapper_args))
  reducer_arg <- ""
  if(!is.null(reducer))
    reducer_arg <- sprintf("-reducer '%s %s'", reducer, as.character(reducer_args))
  cmdenv_arg <- sprintf("-cmdenv %s", as.character(cmdenv_arg))
  system(sprintf('%s jar %s/contrib/streaming/hadoop-*-streaming.jar -D %s -input %s -output %s %s %s %s %s',
                 hadoop(henv), hadoop_home(henv), streaming_args, input, output,
                 mapper_arg, reducer_arg, files, cmdenv_arg),
         intern = TRUE)
}
