\name{tm_map.DCorpus}
\alias{tm_map.DCorpus}
\title{Transformations on Distributed Corpora}
\description{
  Interface to apply transformation functions to distributed
  corpora. See \code{\link[tm]{tm_map}} in \pkg{tm} for more information.
}
\usage{
\method{tm_map}{DCorpus}(x, FUN, \dots, useMeta = FALSE, lazy = FALSE)
}
\arguments{
  \item{x}{A distributed corpus of class \code{DCorpus}.}
  \item{FUN}{A transformation function returning a text document.}
  \item{\dots}{Arguments to \code{FUN}.}
  \item{useMeta}{Logical. Should \code{\link[tm]{DMetaData}} be passed over
    to \code{FUN} as argument?}
  \item{lazy}{FALSE. Only available to be compatible with standard
    \pkg{tm} mapping routines. Lazy evaluation is not supported in the
    \pkg{tm.plugin.dc} package.}
}
\value{
  A \code{DCorpus} with \code{FUN} applied to each document in
  \code{x}. If revisions are enabled, the original documents contained
  in \code{x} can be retrieved via getting back to the corresponding
  revision using the function \code{setRevision()}.
}
\examples{
data("crude")
tm_map(as.DCorpus(crude), tolower)
}
\seealso{
  \code{\link[tm]{getTransformations}} for available transformations.
}
