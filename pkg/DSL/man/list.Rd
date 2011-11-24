\name{DList}
\alias{DList}
\alias{as.DList}
\alias{is.DList}

\title{Distributed List Class}
\description{
  Functions to construct, coerce and check for objects of class
  \code{"DList"}.
}
\usage{
DList( ... )
as.DList( x, DStorage = NULL, ... )
is.DList( x )
}
\arguments{
  \item{...}{objects, possibly named.}
  \item{x}{an object.}
  \item{DStorage}{an object representing the virtual (distributed)
    storage for storing data. See class \code{"\link{DStorage}"} for
    details.}
}
\value{
  An object of class \code{"DList"}.
}
\examples{
## coerce to 'DList' object using a default virtual storage
\dontrun{
l <- list( cow = "girl", bull = "boy" )
dl <- as.DList( l )
is.DList( dl )}
}
