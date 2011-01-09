\name{Revisions}
\alias{getRevisions}
\alias{setRevision}
\title{Revisions in a Distributed Corpora}
\description{
  Each modification of the documents in the corpus results in a new
  stage, i.e., \emph{revision} of the corpus. To allow fast switching
  between multiple revisions all modifications are kept on the file
  system. The replacement function \code{setRevision()} allows to go
  back to any stage in the history of the corpus.
}
\usage{
getRevisions( corpus )
setRevision( corpus, revision )
}
\arguments{
  \item{corpus}{A distributed corpus.}
  \item{revision}{The revision which is to be set as active.}
}
\value{
  Whereas \code{getRevisions()} returns a list of character strings naming all
  available revisions, \code{setRevision()} returns the distributed
  corpus with the given revision marked as active.
}
\examples{
## provide data on storage
data("crude")
dc <- as.DistributedCorpus(crude)
## do some preprocessing
dc <- tm_map(dc, tolower)
## retrieve available revisions
revs <- getRevisions(dc)
revs
## go back to original revision
setRevision(dc, revs[1])
}
