\name{hive}
\alias{hive}
\alias{.hinit}
\alias{hive_create}
\alias{hive_is_available}
\alias{hive_start}
\alias{hive_stop}
\title{Hadoop Interactive Framework Control}
\description{
  
  High-level functions to control Hadoop framework.
  
}
\usage{
hive( new )
.hinit( hadoop_home )
hive_create( hadoop_home )
hive_start( henv = hive() )
hive_stop( henv = hive() )
hive_is_available( henv = hive() )

}
\arguments{
  \item{hadoop_home}{a character string pointing to the local Hadoop installation.}
  \item{henv}{Hadoop local environment.}
  \item{new}{Object specifying the Hadoop environment.}
}
\details{
  High-level functions to control Hadoop framework.

  The function \code{hive} is used to get/set the Hadoop cluster
  object.

  The function \code{.hinit} is used to initialize a Hadoop cluster.

  The function \code{hive_create} creates a new Hadoop cluster object.

  The functions \code{hive_start} and \code{hive_stop} are used to
  start/stop the Hadoop framework.

  The function \code{hive_is_available()} is used to check Hadoop cluster
  status.

}
\value{
  \code{hive_is_available()} returns \code{TRUE} if the given Hadoop
  framework is running.
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
## Start hadoop cluster:
## hive_start()
## Stop hadoop cluster:
## hive_stop()

}
%\keyword{hpc}
