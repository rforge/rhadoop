\name{Storage}
\alias{dcStorage}
\title{Storage Types}
\description{
  In the class \code{DistributedCorpus} the underlying storage plays and
  important role. It defines how to use the given storage (read/write
  methods, etc.), where the data has to be stored (i.e., the base
  directory on the file system), and how transformations as well as
  term-document matrix construction have to be performed. 
}
\usage{
dcStorage( new )
}
\arguments{
  \item{new}{The new storage type to use. If missing \code{dcStorage()}
    returns the current active storage.}
}
\value{
  An object which inherits from class \code{dc_storage}.
}
\examples{
## show active storage
dcStorage()
}
