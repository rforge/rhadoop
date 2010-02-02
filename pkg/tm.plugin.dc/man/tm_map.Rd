\name{tm_map.DistributedCorpus}
\alias{tm_map.DistributedCorpus}
\title{Transformations on Distributed Corpora}
\description{
  Interface to apply transformation functions to distributed
  corpora. See \code{\link[tm]{tm_map}} in \pkg{tm} for more information.
}
\usage{
\method{tm_map}{DistributedCorpus}(x, FUN, \dots, useMeta = FALSE, lazy = FALSE)
}
\arguments{
  \item{x}{A distributed corpus.}
  \item{FUN}{A transformation function returning a text document.}
  \item{\dots}{Arguments to \code{FUN}.}
  \item{useMeta}{Logical. Should \code{\link[tm]{DMetaData}} be passed over
    to \code{FUN} as argument?}
  \item{lazy}{FALSE. Only available to be compatible with standard
    \pkg{tm} mapping routines. Lazy evaluation is not supported in the
    Hadoop framework.}
}
\value{
  A corpus with \code{FUN} applied to each document in \code{x}.
}
\examples{
data("crude")
tm_map(as.DistributedCorpus(crude), stemDocument)
}
\seealso{
  \code{\link[tm]{getTransformations}} for available transformations.
}
