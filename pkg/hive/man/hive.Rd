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
hive_start( henv = hive() )
hive_stop( henv = hive() )
hive_is_available( henv = hive() )

}
\arguments{
  \item{hadoop_home}{A character string pointing to the local Hadoop
    installation. If not given, then \code{.hinit()} will search the
    default installation directory (given via the environment variable
    \code{HADOOP_HOME}, or \file{/etc/hadoop}, respectively).}
  \item{henv}{An object containing the local Hadoop configuration.}
  \item{new}{An object specifying the Hadoop environment.}
}
\details{
  High-level functions to control Hadoop framework.

  The function \code{hive()} is used to get/set the Hadoop cluster
  object. This object consists of an environment holding information
  about the Hadoop cluster.

  The function \code{.hinit()} is used to initialize a Hadoop cluster. It
  retrieves most configuration options via searching the
  \code{HADOOP_HOME} directory given as an environment variable, or,
  alternatively, by searching the \code{/etc/hadoop} directory in case
  the \url{https://www.cloudera.com} distribution (i.e., CDH3) is used.

  The functions \code{hive_start()} and \code{hive_stop()} are used to
  start/stop the Hadoop framework. The latter is not applicable for
  system-wide installations like CDH3.

  The function \code{hive_is_available()} is used to check the status of
  a Hadoop cluster.

}
\value{
  \code{hive()} returns an object of class \code{"hive"} representing
  the currently used cluster configuration.

  \code{hive_is_available()} returns \code{TRUE} if the given Hadoop
  framework is running.
  
}
\references{
  Apache Hadoop: \url{https://hadoop.apache.org/}.
  
  Cloudera's distribution including Apache Hadoop (CDH):
  \url{https://www.cloudera.com/downloads/cdh.html}.
  
}

\author{Stefan Theussl}
%\seealso{
%  \pkg{RHIPE}
%  \pkg{HadoopStreaming}
%}
\examples{
## read configuration and initialize a Hadoop cluster:
\dontrun{h <- .hinit( "/etc/hadoop" )}
\dontrun{hive( h )}
## Start hadoop cluster:
\dontrun{hive_start()}
## check the status of an Hadoop cluste:
\dontrun{hive_is_available()}
## return cluster configuration 'h':
hive()
## Stop hadoop cluster:
\dontrun{hive_stop()}
}
%%\keyword{HPC,}
