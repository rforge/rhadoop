\name{DList}
\alias{DList}
\alias{as.DList}
\alias{is.DList}
\alias{DL_storage}
\alias{DL_storage<-}


\title{Distributed List Class}
\description{
  Functions to construct, coerce, check for, and interact with storage
  of objects of class \code{"DList"}.
}
\usage{
DList( ... )
as.DList( x, DStorage = NULL, ... )
is.DList( x )
DL_storage( x )
`DL_storage<-`( x, value )
}
\arguments{
  \item{...}{objects, possibly named.}
  \item{x}{an object.}
  \item{DStorage}{an object representing the virtual (distributed)
    storage for storing data. See class \code{"\link{DStorage}"} for
    details.}
  \item{value}{the new storage of class \code{DStorage} attached to
    the \code{"DList"}.}
}
\value{
  An object of class \code{"DList"} or, in case of \code{DL_storage()},
  an object of class \code{"\link{DStorage}"}.
}
\examples{
## coerce to 'DList' object using a default virtual storage
l <- list( cow = "girl", bull = "boy" )
dl <- as.DList( l )
is.DList( dl )
DL_storage(dl)
## remove DList and garbage collect it
rm(dl)
gc()
}
