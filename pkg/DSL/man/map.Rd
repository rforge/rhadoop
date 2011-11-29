\name{MapReduce}
\alias{DLapply}
\alias{DMap}
\alias{DReduce}
\title{MapReduce for \code{"DList"} Objects}
\description{
  Interface to apply functions on elements of \code{"DList"} objects.
}
\usage{
DLapply( x, FUN, parallel, ..., keep = FALSE )
DMap( x, MAP, parallel, keep = FALSE )
DReduce( x, REDUCE, parallel, ... )
}
\arguments{
  \item{x}{a \code{"\link{DList}"} object. Other objects (e.g., lists) will be
    coerced by \code{\link{as.DList}}.}
  \item{FUN}{the function to be applied to each element (i.e., the
    values) of \code{x}.}
  \item{MAP}{the function to be applied to each key/value pair in
    \code{x}.}
  \item{REDUCE}{the function to be applied to each key/value pair in
    \code{x}.}
  \item{\dots}{optional arguments to \code{FUN} or \code{REDUCE}.}
  \item{parallel}{logical; should the provided functions applied in
    parallel? Default: \code{FALSE}.}
  \item{keep}{logical; should the current data be kept as a separate
    revision for further processing later? Default: \code{FALSE}.}
}
\details{
The MapReduce programming model as defined by Dean and Ghemawat (2008)
is as follows: the computation takes a set of input key/value pairs, and produces a
set of output key/value pairs. The user expresses the computation as two
functions: Map and Reduce. The Map function takes an input pair and produces a set of
intermediate key/value pairs. The Reduce function accepts an
intermediate key and a set of values for that key (possibly
grouped by the MapReduce library). It merges these values
together to form a possibly smaller set of values. Typically, just zero or
one output value is produced per reduce invocation. Furthermore, data is
usually stored on a (distributed) file system which is recognized by the
MapReduce library. This allows such a framework to handle lists of
values (here objects of class \code{"\link{DList}"}) that are too
large to fit in main memory (i.e., RAM).}
\value{
  A \code{"\link{DList}"}.
}
\examples{
dl <- DList( line1 = "This is the first line.",
             line2 = "Now, the second line." )
res <- DLapply( dl, function(x) unlist(strsplit(x, " ")) )
as.list( res )

foo <- function( keypair )
    list( key = paste("next_", keypair$key, sep = ""), value =
    gsub("first", "mapped", keypair$value) )

dlm <- DMap( x = dl, MAP = foo)
## retrieve keys
unlist(DGather(dlm, keys = TRUE, names = FALSE))
## retrieve values
as.list( dlm )
## simple wordcount based on two files:
dir(system.file("examples", package = "DSL"))
## first force 1 chunk per file (set max chunk size to 1 byte):
ds <- DStorage_create("LFS", tempdir(), chunksize = 1L)
## make "DList" from files, i.e., read contents and store in chunks
dl <- as.DList(system.file("examples", package = "DSL"), DStorage = ds)
## read files
dl <- DMap(dl, function( keypair ){
    list( key = keypair$key, value = tryCatch(readLines(keypair$value),
error = function(x) NA) )
})
## split into terms
splitwords <- function( keypair ){
    keys <- unlist(strsplit(keypair$value, " "))
    mapply( function(key, value) list( key = key, value = value), keys, rep(1L, length(keys)), SIMPLIFY = FALSE, USE.NAMES = FALSE)
}
res <- DMap( dl, splitwords )
as.list(res)
## now aggregate by term
res <- DReduce( res, sum )
as.list( res )
}
\references{
  J. Dean and S. Ghemawat (2008).
  MapReduce: Simplified Data Processing on Large
  Clusters. \emph{Communications of the ACM}, \bold{51}, 107--113.
}

