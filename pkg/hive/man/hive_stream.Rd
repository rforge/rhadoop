\name{hive_stream}
\alias{hive_stream}

\title{Hadoop Streaming with package \pkg{hive}}
\description{
  
  High-level R function for using Hadoop Streaming.
  
}
\usage{
hive_stream( mapper, reducer, input, output, henv = hive(),
             mapper_args = NULL, reducer_args = NULL, cmdenv_arg = NULL,
streaming_args = NULL)
}
\arguments{
  \item{mapper}{a function which is executed on each worker node. The
    so-called mapper typically maps input key/value pairs to a set of
    intermediate key/value pairs.}
  \item{reducer}{a function which is executed on each worker node. The
    so-called reducer reduces a set of intermediate values which share a
    key to a smaller set of values. If no reducer is used leave empty.}
  \item{input}{specifies the directory holding the data in the DFS.}
  \item{output}{specifies the output directory in the DFS containing the
    results after the streaming job finished.}
  \item{henv}{Hadoop local environment.}
  \item{mapper_args}{additional arguments to the mapper.}
  \item{reducer_args}{additional arguments to the reducer.}
  \item{cmdenv_arg}{additional arguments passed as environment variables
    to distributed tasks.}
    \item{streaming_args}{additional arguments passed to the Hadoop
      Streaming utility. By default, only the number of reducers will be
    set using \code{"-D mapred.reduce.tasks="}.}
}
\details{

  The function \code{hive_stream()} starts a MapReduce job on the given
  data located on the HDFS.

}
%\value{
%  \code{hive_is_available()} returns \code{TRUE} if the given Hadoop
%  framework is running.
%}
\references{
  Apache Hadoop Streaming
  (\url{http://hadoop.apache.org/common/docs/current/streaming.html}).
}

\author{Stefan Theussl}
%\seealso{
%  \pkg{RHIPE}
%  \pkg{HadoopStreaming}
%}
\examples{
## A simple word count example

## Put some xml files on the HDFS:
\dontrun{DFS_put(system.file("defaults/core/", package = "hive"), "/tmp/input")}
\dontrun{DFS_put(system.file("defaults/hdfs/hdfs-default.xml", package = "hive"), "/tmp/input")}
\dontrun{DFS_put(system.file("defaults/mapred/mapred-default.xml", package = "hive"), "/tmp/input")}
## Define the mapper and reducer function to be applied:
## Note that a Hadoop map or reduce job retrieves data line by line from stdin.
\dontrun{
mapper <- function(x){
    con <- file( "stdin", open = "r" )
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
        terms <- unlist(strsplit(line, " "))
        terms <- terms[nchar(terms) > 1 ]
        if( length(terms) )
            cat( paste(terms, 1, sep = "\t"), sep = "\n")
    }
}
reducer <- function(x){
    env <- new.env( hash = TRUE )
    con <- file( "stdin", open = "r" )
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
        keyvalue <- unlist( strsplit(line, "\t") )
        if( exists(keyvalue[1], envir = env, inherits = FALSE) ){
            assign( keyvalue[1], get(keyvalue[1], envir = env) + as.integer(keyvalue[2]), envir = env )
        } else {
            assign( keyvalue[1], as.integer(keyvalue[2]), envir = env )
        }
    }
    env <- as.list(env)
    for( term in names(env) )
        writeLines( paste(c(term, env[[term]]), collapse ="\t") )
}
hive_set_nreducer(1)
hive_stream( mapper = mapper, reducer = reducer, input = "/tmp/input", output = "/tmp/output" )
DFS_list("/tmp/output")
head( DFS_read_lines("/tmp/output/part-00000") )
}
## Don't forget to clean file system
\dontrun{DFS_dir_remove("/tmp/input")}
\dontrun{DFS_dir_remove("/tmp/output")}
}
%\keyword{hpc}
