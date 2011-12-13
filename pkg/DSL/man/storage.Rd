\name{DStorage}
\alias{DStorage}
\alias{is.DStorage}


\title{Virtual Distributed Storage Class}
\description{
  When using class \code{DList} the underlying
  \sQuote{virtual} storage plays an
  important role. It defines how to use the given storage (read/write
  methods, etc.), where the data is to be stored (i.e., the base
  directory on the file system), and how \code{\link{DMap}} as well as
  \code{\link{DReduce}} have to be applied. 
}
\usage{
DStorage(type = c("LFS", "HDFS"), base_dir, chunksize = 1024^2)
is.DStorage( ds )
}
\arguments{
  \item{type}{the type of the storage to be created. Currently only
    \code{"LFS"} and \code{"HDFS"} storage types are supported.}
  \item{base_dir}{specifies the base directory where data is to be
    stored.}
  \item{chunksize}{defines the size of each chunk written to the virtual
    storage.}
  \item{ds}{a virtual possibly distributed storage.}
}
\value{
  An object which inherits from class \code{DStorage}, or, in case of
  \code{is.DStorage()} a logical indicating whether it inherits from
  \code{"DStorage"} or not.
}
\examples{
## creating a new virtual storage using 50MB chunks
ds <- DStorage(type = "LFS", base_dir = tempdir(),
chunksize = 50 * 1024^2)
is.DStorage( ds )
}
