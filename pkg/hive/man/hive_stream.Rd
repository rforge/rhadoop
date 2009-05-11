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
  \item{mapper}{a character string pointing to the local mapper script.}
  \item{reducer}{a character string pointing to the local reducer
    script. If no reducer script should be used leave empty.}
  \item{input}{specifies the directory holding the data in the DFS.}
  \item{output}{specifies the output directory in the DFS containing the
    results after the streaming job finished.}
  \item{henv}{Hadoop local environment.}
  \item{...}{additional args to Hadoop Streaming. Currently not in use.}

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
