\name{DFS}
\alias{DFS_cat}
\alias{DFS_delete}
\alias{DFS_dir_create}
\alias{DFS_dir_exists}
\alias{DFS_dir_remove}
\alias{DFS_file_exists}
\alias{DFS_get_object}
\alias{DFS_list}
\alias{DFS_put}
\alias{DFS_put_object}
\alias{DFS_read_lines}
\alias{DFS_rename}
\alias{DFS_tail}
\alias{DFS_write_lines}

\title{Hadoop Distributed File System}
\description{
  
  Functions providing high-level access to the Hadoop Distributed File System (HDFS).
  
}
\usage{
DFS_cat( file, con = stdout(), henv = hive() )
DFS_delete( file, recursive = FALSE, henv = hive() )
DFS_dir_create( path, henv = hive() )
DFS_dir_exists( path, henv = hive() )
DFS_dir_remove( path, recursive = TRUE, henv = hive() )
DFS_file_exists( file, henv = hive() )
DFS_get_object( file, henv = hive() )
DFS_read_lines( file, n = -1L, henv = hive() )
DFS_rename( from, to, henv = hive() )
DFS_list( path = ".", henv = hive() )
DFS_tail( file, n = 6L, size = 1024L, henv = hive() )
DFS_put( files, path = ".", henv = hive() )
DFS_put_object( obj, file, henv = hive() )
DFS_write_lines( text, file, henv = hive() )
}
\arguments{
  \item{henv}{An object containing the local Hadoop configuration.}
  \item{file}{a character string representing a file on the DFS.}
  \item{files}{a character string representing files located on the
    local file system to be copied to the
    DFS.}
  \item{n}{an integer specifying the number of lines to read.}
  \item{obj}{an R object to be serialized to/from the DFS.}
  \item{path}{a character string representing a full path name in the
    DFS (without the leading \code{hdfs://}); for many functions the
    default corresponds to the user's home directory in the DFS.}
  \item{recursive}{logical. Should elements of the path other than the last be
    deleted recursively?}
  \item{size}{an integer specifying the number of bytes to be read. Must
  be sufficiently large otherwise \code{n} does not have the desired effect.}
  \item{text}{a (vector of) character string(s) to be written to the DFS.}
  \item{con}{A connection to be used for printing the output provided by
    \code{cat}. Default: standard output connection, has currently no
    other effect}
  \item{from}{a character string representing a file or directory on the
    DFS to be renamed.}
  \item{to}{a character string representing the new filename on the DFS.}
}
\details{
  The Hadoop Distributed File System (HDFS) is typically part of a Hadoop
  cluster or can be used as a stand-alone general purpose distributed file
  system (DFS). Several high-level functions provide easy access to
  distributed storage. 
  
  \code{DFS_cat} is useful for producing output in user-defined
  functions. It reads from files on the DFS and typically prints the
  output to the standard output. Its behaviour is similar to the base
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

%  \code{DFS_format} attempts to completely format the HDFS. This also
%  involves a manual restart of the Hadoop framework after the namenode
%  as been formated. Note that this command is dangerous as all of the
%  data stored in the DFS will be lost.

  \code{DFS_list} produces a character vector of the names of files
  in the directory named by its argument.

  \code{DFS_read_lines} is a reader for (plain text) files stored on the
  DFS. It returns a vector of character strings representing lines in
  the (text) file. If \code{n} is given as an argument it reads that
  many lines from the given file. It's behaviour is similar to the base
  function \code{readLines}.

  \code{DFS_put} copies files named by its argument to a given path in
  the DFS.

  \code{DFS_put_object} serializes an R object to the DFS.

  \code{DFS_write_lines} writes a given vector of character strings to a
  file stored on the DFS. It's behaviour is similar to the base
  function \code{writeLines}.

}
\value{
  \code{DFS_delete()}, \code{DFS_dir_create()}, and \code{DFS_dir_remove} return a logical value indicating if the 
  operation succeeded for the given argument.

  \code{DFS_dir_exists()} and \code{DFS_file_exists()} return \code{TRUE} if
  the named directories or files exist in the HDFS.

  \code{DFS_get__object()} returns the deserialized object stored in a
  file on the HDFS.
  
  \code{DFS_list()} returns a character vector representing the directory listing of the corresponding
  path on the HDFS.

  \code{DFS_read_lines()} returns a character vector of length the
  number of lines read.
  
  \code{DFS_tail()} returns a character vector of length the number of
  lines to read until the end of a file on the HDFS.
  
}
\references{
  The Hadoop Distributed File System (\url{https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html}).  
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
## Do we have access to the root directory of the DFS?
\dontrun{DFS_dir_exists("/")}
## Some self-explanatory DFS interaction
\dontrun{
DFS_list( "/" )
DFS_dir_create( "/tmp/test" )
DFS_write_lines( c("Hello HDFS", "Bye Bye HDFS"), "/tmp/test/hdfs.txt" )
DFS_list( "/tmp/test" )
DFS_read_lines( "/tmp/test/hdfs.txt" )
}
## Serialize an R object to the HDFS
\dontrun{
foo <- function()
"You got me serialized."
sro <- "/tmp/test/foo.sro"
DFS_put_object(foo, sro)
DFS_get_object( sro )()
}
## finally (recursively) remove the created directory
\dontrun{DFS_dir_remove( "/tmp/test" )}
}
%%\keyword{hpc}
