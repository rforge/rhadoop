\name{hive_stream}
\alias{hive_stream}

\title{Hadoop Streaming with hive}
\description{
  
  High-level functions for using Hadoop Streaming.
  
}
\usage{
hive_stream(mapper, reducer, input, output, henv = hive(), ...)
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
  \item{...}{additional arguments to Hadoop Streaming. Currently not in use.}

}
\details{

  The function \code{hive_stream} starts a MapReduce job on the given
  data located in the DFS.

}
%\value{
%  \code{hive_is_available()} returns \code{TRUE} if the given Hadoop
%  framework is running.
%}
\references{
  Apache Hadoop core
  (\url{http://hadoop.apache.org/core/}).
}

\author{Stefan Theussl}
%\seealso{
%  \pkg{RHIPE}
%  \pkg{HadoopStreaming}
%}
%\examples{
%## Start hadoop cluster:
%## hive_start()
%## Stop hadoop cluster:
%## hive_stop()
%}
%\keyword{hpc}
