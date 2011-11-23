\name{DGather}
\alias{DGather}
\title{Gather Distributed Data}
\description{
  Retrieves \code{"DList"} data distributed as chunks.
}
\usage{
DGather( x, keys = FALSE, n = -1L, names = TRUE )
}
\arguments{
  \item{x}{a \code{"\link{DList}"} object.}
  \item{keys}{logical; should only keys be retrieved from chunks?
    Default: \code{FALSE}, i.e., only values are retrieved.}
  \item{n}{an integer specifying the number of chunks to be read.}
  \item{names}{logical; should the return value be a named list?
    Default: \code{TRUE}.}
}
\details{
  \code{DGather()} is similar to an \code{MPI_GATHER} (see
  \url{http://www.mpi-forum.org/docs/mpi22-report/node95.htm#Node95})
  where: \dQuote{[...] each process (root process included) sends the
  contents of its send buffer to the root process. The root process
  receives the messages and stores them in rank order.} For
  \code{"\link{DList}"} objects \code{DGather()} will gather data
  contained in chunks possibly distributed on a cluster of workstations
  and store it in a (possibly named) \code{\link{list}}. Note that
  depending of the size of the data, the resulting list may not fit into
  memory.}
\value{
  A (named) list.
}
\examples{
dl <- DList( line1 = "This is the first line.",
             line2 = "Now, the second line." )
DGather( dl )
## retrieve keys
unlist(DGather( dl, keys = TRUE, names = FALSE ))
}

