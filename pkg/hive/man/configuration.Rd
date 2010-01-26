\name{configuration}
\alias{hive_get_parameter}
\alias{hive_get_slaves}
\alias{hive_get_masters}
\alias{hive_get_nreducer}
\alias{hive_set_nreducer}

\title{Hadoop configuration}
\description{
  
  Functions for showing/changing Hadoop configuration.
  
}
\usage{
hive_get_parameter( x, henv = hive() )
hive_get_masters( henv = hive() )
hive_get_slaves( henv = hive() )
hive_get_nreducer( henv = hive() )
hive_set_nreducer( n, henv = hive() )
}
\arguments{
  \item{henv}{Hadoop local environment.}
  \item{x}{a character string naming the parameter in the Hadoop
    configuration.}
  \item{n}{an integer specifying the number of reducers to be used in
    \code{hive_stream()}.}
}
\details{
  The function \code{hive_get_parameter()} is used to get parameters from
  the Hadoop cluster configuration.

  The functions \code{hive_get_slaves()} and \code{hive_get_masters()}
  return the hostnames of the configured nodes in the cluster. 

  The functions \code{hive_get_nreducer()} and \code{hive_set_nreducer()}
  are used to get/set the number of reducers which are used in Hadoop
  Streaming using \code{hive_stream()}.

}
\value{
  \code{hive_get_parameter()} returns the specified parameter as a character string.
  
  \code{hive_get_slaves()} returns a character vector naming the hostnames
  of the configured worker nodes in the cluster.

  \code{hive_get_masters()} returns a character vector of the hostnames of
  the configured master nodes in the cluster.

  \code{hive_get_nreducer()} returns an integer representing the number
  of configured reducers.
}
\references{
  Apache Hadoop core
  (\url{http://hadoop.apache.org/core/}).  
}
\author{Stefan Theussl}
%\seealso{
%  \pkg{RHIPE}
%  \pkg{HadoopStreaming}
%}
\examples{

## Which tmp directory is set in the Hadoop configuration?
\dontrun{hive_get_parameter("hadoop.tmp.dir")}

## The master nodes of the cluster
\dontrun{hive_get_masters()}

## The worker nodes of the cluster
\dontrun{hive_get_slaves()}

## The number of configured reducers
\dontrun{hive_get_nreducer()}

}
%\keyword{hpc}
