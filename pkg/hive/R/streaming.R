## Function to facilitate the usage of Hadoop Streaming

## High-level wrapper for hadoop-streaming (MapReduce)
## TODO: ncpu argument -> probably via -D mapred.jobtracker.maxtasks.per.job=..
hive_stream <- function(mapper, reducer, input, output, henv = hive(), ...) {  
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

  ## start hadoop streaming
  msg <- .hadoop_streaming(mapper, reducer, input, output, streaming_args, henv)
  invisible(msg)
}

.hadoop_streaming <- function(mapper, reducer, input, output, streaming_args, henv){
  mapper_args <- sprintf("-mapper %s -file %s", mapper, mapper)
  reducer_args <- ""
  if(!is.null(reducer))
    reducer_args <- sprintf("-reducer %s -file %s", reducer, reducer)
  system(sprintf('%s jar %s/contrib/streaming/hadoop-*-streaming.jar -D %s -input %s -output %s %s %s',
                 hadoop(henv), hadoop_home(henv), streaming_args, input, output,
                 mapper_args, reducer_args),
         intern = TRUE)
}
