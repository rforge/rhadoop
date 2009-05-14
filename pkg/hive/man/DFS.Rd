\name{DFS}
\alias{DFS_cat}
\alias{DFS_dir_create}
\alias{DFS_dir_exists}
\alias{DFS_dir_remove}
\alias{DFS_file_exists}
\alias{DFS_format}
\alias{DFS_list}
\alias{DFS_put}
\alias{DFS_put_object}
\alias{DFS_tail}

\title{Hadoop Distributed File System}
\description{
  
  Functions providing high-level access to the Hadoop Distributed File System (HDFS).
  
}
\usage{
DFS_cat( file, con = stdout(), henv = hive() )
DFS_dir_create( path, henv = hive() )
DFS_dir_exists( path, henv = hive() )
DFS_dir_remove( path, recursive = TRUE, henv = hive() )
DFS_file_exists( file, henv = hive() )
DFS_format( henv )
DFS_list( path = ".", henv = hive() )
DFS_tail( file, con = stdout(), henv = hive() )
DFS_put( files, to = ".", henv = hive() )
DFS_put_object( obj, path = ".", henv = hive() )

}
\arguments{
  \item{henv}{Hadoop local environment.}
  \item{file}{a character string representing a file in the DFS.}
  \item{files}{a character string representing files to be copied to the DFS.}
  \item{obj}{an R object to be serialized to the DFS.}
  \item{path}{a character string representing a full path name in the
    DFS (without the leading \code{hdfs://}); for many functions the
    default corresponds to the user's home directory in the DFS.}
  \item{recursive}{logical. Should elements of the path other than the last be
    deleted recursively?}
  \item{con}{A connection to be used for printing the output provided by
    \code{cat}. Default: standard output connection}
}
\details{
  The Hadoop Distributed File System (HDFS) is typically part of a Hadoop
  cluster or can be used as a stand-alone general purpose distributed file
  system (DFS). Several high-level functions provide easy access to
  distributed storage. 
  
  \code{DFS_cat} is useful for producing output in user-defined
  functions. It reads from files in the DFS and typically prints the
  output to the standard output. It's behaviour is similar to the base
  function \code{cat}.

  \code{DFS_dir_create} creates directories with the given path names if
  they do not already exist. It's behaviour is similar to the base
  function \code{dir.create}.

  \code{DFS_dir_exists} and \code{DFS_file_exists} return a logical
  vector indicating whether the directory or file respectively named by
  its argument exist. See also function \code{file.exists}.

  \code{DFS_dir_remove} attempts to remove the directory named in its
  argument and if \code{recursive} is set to \code{TRUE} also attempts
  to remove subdirectories in a recursive manner.

  \code{DFS_format} attempts to completely format the HDFS. This also
  involves a manual restart of the Hadoop framework after the namenode
  as been formated. Note that this command is dangerous as all of the
  data stored in the DFS will be lost.

  \code{DFS_list} produces a character vector of the names of files
  in the directory named by its argument.

  \code{DFS_put} copies files named by its argument to a given path in
  the DFS.

  \code{DFS_put_object} serializes an R object to the DFS.  
}
\value{
  \code{DFS_dir_create} returns a logical value indicating if the 
  operation succeeded for the given argument.

  \code{DFS_dir_exists} and \code{DFS_file_exists} return \code{TRUE} if
  the named directories or files exist in the HDFS.

  
}
\references{
  Apache Hadoop core
  (\url{http://hadoop.apache.org/core/}).  
}

\author{Stefan Theussl}
%\seealso{
%  \code{\link[base]{cat}}
%  \code{\link[base]{file.exists}}
%  \code{\link[base]{file.remove}}
%  \pkg{RHIPE}
%  \pkg{HadoopStreaming}
%}
\examples{
## Do we have access to root in the DFS?
status <- DFS_dir_exists("/")
status

## If so, list the contents.
if(status)
  DFS_list()

}
%%\keyword{hpc}
