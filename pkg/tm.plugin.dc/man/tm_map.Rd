\name{tm_map.DistributedCorpus}
\alias{tm_map.DistributedCorpus}
\title{Transformations on Distributed Corpora}
\description{
  Interface to apply transformation functions (maps in Hadoop
  terminology) to distributed corpora.
}
\usage{
\method{tm_map}{DistributedCorpus}(x, FUN, \dots, cmdenv_arg = NULL, useMeta = FALSE, lazy = FALSE)
}
\arguments{
  \item{x}{A distributed corpus.}
  \item{FUN}{A transformation function returning a text document.}
  \item{\dots}{Arguments to \code{FUN}.}
  \item{cmdenv_arg}{Command line arguments to be supplied to the
    underlying Hadoop calls.}
  \item{useMeta}{Logical. Should \code{\link[tm]{DMetaData}} be passed over
    to \code{FUN} as argument?}
  \item{lazy}{FALSE. Only available to be compatible with standard
    \pkg{tm} mapping routines. Lazy evaluation is not supported in the
    Hadoop framework.}
}
\value{
  A corpus with \code{FUN} applied to each document in \code{x}.
}
\seealso{
  \code{\link[tm]{getTransformations}} for available transformations.
}
