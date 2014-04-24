\name{tm_map.DCorpus}
\alias{tm_map.DCorpus}
\title{Transformations on Distributed Corpora}
\description{
  Interface to apply transformation functions to distributed
  corpora. See \code{\link[tm]{tm_map}} in \pkg{tm} for more information.
}
\usage{
\method{tm_map}{DCorpus}(x, FUN, \dots)
}
\arguments{
  \item{x}{A distributed corpus of class \code{DCorpus}.}
  \item{FUN}{a transformation function taking a text document as input and
    returning a text document. The function \code{\link[tm]{content_transformer}}
    can be used to create a wrapper to get and set the content of text
    documents.}
  \item{\dots}{arguments to \code{FUN}.}
}
\value{
  A \code{DCorpus} with \code{FUN} applied to each document in
  \code{x}. If revisions are enabled, the original documents contained
  in \code{x} can be retrieved via getting back to the corresponding
  revision using the function \code{setRevision()}.
}
\examples{
data("crude")
tm_map(as.DCorpus(crude), content_transformer(tolower))
}
\seealso{
  \code{\link[tm]{getTransformations}} for available transformations in package
  \pkg{tm}.
}
