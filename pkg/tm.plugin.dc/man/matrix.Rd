\name{TermDocumentMatrix.DistributedCorpus}
\alias{TermDocumentMatrix.DistributedCorpus}
\title{Term-Document Matrix from Distributed Corpora}
\description{
  Constructs a term-document matrix given a distributed corpus.
}
\usage{
\method{TermDocumentMatrix}{DistributedCorpus}(x, control = list())
}
\arguments{
  \item{x}{A distributed corpus.}
  \item{control}{A named list of control options. The component
    \code{weighting} must be a weighting function capable of handling a
    \code{TermDocumentMatrix}. It defaults to \code{weightTf} for term
    frequency weighting. All other options are delegated internally to a
    \code{\link[tm]{termFreq}} call.}
}
\value{
  An object of class \code{TermDocumentMatrix} containing a sparse
  term-document matrix. The attribute \code{Weighting} contains the
  weighting applied to the matrix.
}
\examples{
data("crude")
tdm <- TermDocumentMatrix(as.DistributedCorpus(crude),
control = list(weighting = weightTfIdf, stopwords = TRUE))
inspect(tdm[155:160,1:5])
}
\seealso{
  The documentation of \code{\link[tm]{termFreq}} gives an extensive list of
  possible options.

  \code{\link[tm]{TermDocumentMatrix}}
}
