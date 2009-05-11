\name{configuration}
\alias{hive_get_parameter}
\alias{hive_get_slaves}
\alias{hive_get_masters}

\title{Hadoop configuration}
\description{
  
  Functions for showing/changing Hadoop configuration.
  
}
\usage{

hive_get_parameter( x, henv = hive() )
hive_get_slaves( henv = hive() )
hive_get_masters( henv = hive() )

}
\arguments{
  \item{henv}{Hadoop local environment.}
  \item{x}{a character string naming the parameter in the Hadoop configuration.}
}
\details{

  The function \code{hive_get_parameter} is used to get parameters from
  the Hadoop cluster configuration.

  The functions \code{hive_get_slaves} and \code{hive_get_masters}
  return the hostnames of the configured nodes in the cluster. 

  The function  returns the hostnames of the
  configured master nodes in the cluster.


}
\value{
  
  \code{hive_get_slaves} returns a character vector naming the hostnames
  of the configured worker nodes in the cluster.

  \code{hive_get_masters} returns a character vector of the hostnames of
  the configured master nodes in the cluster.
  
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
hive_get_parameter("hadoop.tmp.dir")

## The worker nodes of the cluster
hive_get_slaves()

## The master nodes of the cluster
hive_get_masters()

}
%\keyword{hpc}
