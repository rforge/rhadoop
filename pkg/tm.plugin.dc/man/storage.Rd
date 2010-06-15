\name{dc_storage}
\alias{dc_storage}
\alias{dc_storage<-}
\alias{dc_storage_create}
\alias{as.dc_storage}
\alias{is.dc_storage}


\title{Virtual Storage Class}
\description{
  When using class \code{DistributedCorpus} the underlying
  \sQuote{virtual} storage plays and
  important role. It defines how to use the given storage (read/write
  methods, etc.), where the data is to be stored (i.e., the base
  directory on the file system), and how transformations as well as
  term-document matrix construction have to be performed. 
}
\usage{
dc_storage( x, ... )
`dc_storage<-`( x, value )
dc_storage_create(type = c("local_disk", "HDFS"), base_dir, chunksize = 1024^2)
as.dc_storage( ds, ... )
is.dc_storage( ds )
}
\arguments{
  \item{x}{A distributed corpus.}
  \item{value}{The new storage of class \code{dc_storage} attached to
    the distributed corpus.}
  \item{type}{The type of the storage to be created. Currently only
    \code{local_disk} and \code{HDFS} storage types are supported.}
  \item{base_dir}{specifies the base directory where distributed corpus
    data is to be stored.}
  \item{chunksize}{defines the size of each chunk written to the virtual
    storage.}
  \item{ds}{A virtual storage.}
  \item{...}{Further arguments to the corresponding methods}
}
\value{
  An object which inherits from class \code{dc_storage}, or, in case of
  \code{is.dc_storage()} a logical indicating whether it inherits from
  \code{dc_storage} or not.
}
\examples{
## extract storage from 'DistributedCorpus'
data(crude)
dc <- as.DistributedCorpus( crude )
dc_storage( dc )
## creating a new storage using 50MB chunks
dcs <- dc_storage_create(type = "local_disk", base_dir = tempfile(),
chunksize = 50 * 1024^2)
is.dc_storage( dcs )
}
