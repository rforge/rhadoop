\name{Revisions}
\alias{getRevisions}
\alias{removeRevision}
\alias{setRevision}
\alias{keepRevisions}
\alias{keepRevisions<-}
\title{Revisions of a Distributed Corpus}
\description{
  Each modification of the documents in the corpus results in a new
  stage, i.e., \emph{revision} of the corpus. To allow fast switching
  between multiple revisions all modifications may be kept on the file
  system. The function \code{setRevision()} allows to go back to any
  stage in the history of the corpus. The function
  \code{keepRevisions()} shows if revisions are turned on or off; the
  corresponding replacement function is used to set the desired
  behavior.
}
\usage{
getRevisions( corpus )
removeRevision( corpus, revision )
setRevision( corpus, revision )
keepRevisions( corpus )
`keepRevisions<-`( corpus, value )
}
\arguments{
  \item{corpus}{A distributed corpus of class \code{DCorpus}.}
  \item{revision}{The revision which is to be set as active or removed.}
  \item{value}{A logical indicating whether revisions should be kept or not.}
}
\value{
  Whereas \code{getRevisions()} returns a list of character strings naming all
  available revisions, \code{setRevision()} returns the distributed
  corpus with the given revision marked as active. The function
  \code{keepRevisions()} returns a logical indicating whether revisions
  are used or not.
}
\examples{
## provide data on storage
data("crude")
dc <- as.DCorpus(crude)
## do some preprocessing
dc <- tm_map(dc, tm::genMap(tolower))
## retrieve available revisions
revs <- getRevisions(dc)
revs
## go back to original revision
setRevision(dc, revs[2])
keepRevisions(dc)
keepRevisions(dc) <- FALSE
}
