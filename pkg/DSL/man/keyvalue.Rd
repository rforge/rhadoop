\name{KeyValue}
\alias{DKeys}
\title{Key/Value Pairs}
\description{
  Key/value pairs in \code{"DList"} objects.
}
\usage{
DKeys( x )
}
\arguments{
  \item{x}{a \code{"\link{DList}"} object.}}
\value{
  A character vector representing all keys of the key/value pairs stored
  in chunks by \code{"\link{DList}"} objects.
}
\examples{
## create a 2 elements DList
dl <- DList( line1 = "This is the first line.",
             line2 = "Now, the second line." )
## retrieve keys
DKeys( dl )
## remove DList and garbage collect it
rm( dl )
gc()
}

