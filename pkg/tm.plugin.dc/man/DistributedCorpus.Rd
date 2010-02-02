\name{DistributedCorpus}
\alias{DistributedCorpus}
\alias{as.DistributedCorpus}

\title{Distributed Corpus}
\description{
  Data structures and operators for distributed corpora.
}
\usage{
DistributedCorpus( source,
                   readerControl = list(reader   = source$DefaultReader,
                                        language = "eng"),
                   storage = dcStorage(), ... )

as.DistributedCorpus( x, storage = dcStorage(), ... )
}
\arguments{
  \item{source}{A \code{\link[tm]{Source}} object. At the moment only
    \code{\link[tm]{DirSource}} is supported.}
  \item{readerControl}{A list with the named components \code{reader}
    representing a reading function capable of handling the file format
    found in \code{source}, and \code{language} giving the text's language
    (preferably in \acronym{ISO} 639-2 format).}
  \item{storage}{The storage subsystem to use with the
    DistributedCorpus. Currently two types of storages are supported:
    local disk storage (local_disk) and the Hadoop distributed file
    system (HDFS).}
  \item{x}{An object to be coerced to a
    \code{DistributedCorpus}. Currently coercion from classic \pkg{tm}
    corpora is implemented.}
  \item{\dots}{Optional arguments for the \code{reader}.}
}
\details{When constructing a distributed corpus the input source is
  extracted via the supplied reader and stored in the given file
  system (argument \code{storage}). While the dataset resides on the
  corresponding storage (e.g., HDFS), only a symbolic representation is
  held in \R which allows to access the corpus via corresponding methods
  which dispatch on the distributed corpus. Since the available memory
  for the distributed corpus is only restricted by available disk space
  in the given storage (and not main memory like in a classic corpus) we
  also store a set of so-called revisions, i.e., stages of the
  (processed) corpus.
\
  The constructed corpus object inherits from a \pkg{tm} \code{Corpus}
  and has several attributes containing meta information:
  \describe{
    \item{ActiveRevision}{Contains the current revision (random string)
      of the corpus. Each modification of the documents in the corpus
      results in a new revision which allows fast switching between
      multiple snapshots.}. 
    \item{Chunks}{A list of file names on the local disk/HFS representing
      underlying chunks holding the (serialized) documents.}
    \item{\code{CMetaData}}{Corpus Meta Data contains corpus specific
      meta data in form of tag-value pairs and information about
      children in form of a binary tree. This information is useful for
      reconstructing meta data after e.g. merging corpora. This is
      mainly available to be compatible with standard \pkg{tm} corpus
      definitions but not yet actually used in the distributed scenario.}
    \item{\code{DMetaData}}{Document Meta Data of class
      \code{data.frame} contains document specific meta data for the
      corpus. This data frame typically encompasses clustering or
      classification results which basically are metadata for documents
      but form an own entity (e.g., with its name, the value range,
      etc.). This is mainly available to be compatible with standard
      \pkg{tm} corpus definitions but not yet actually used in the
      distributed scenario.}
    \item{Keys}{A character vector identifying each individual document
      in the distributed corpus.}
    \item{Mapping}{Basically a hash table (implemented as a matrix of
      position mappings) holding for each key the chunk and position in
      the file system of the storage.}
    \item{Revisions}{A list of all available revisions.}
    \item{Storage}{An object which inherits from class
      \code{dc_storage}. It specifies how to use the given storage
      (read/write methods, base directory for data, etc.).}
  }					     
}
\value{
  An object of class \code{DistributedCorpus} which extends the classes
  \code{Corpus} and \code{list} containing a collection of text
  documents.
}
\examples{
## Similar to example in package 'tm'
reut21578 <- system.file("texts", "crude", package = "tm")
dc <- DistributedCorpus(DirSource(reut21578),
readerControl = list(reader = readReut21578XMLasPlain) )
dc

## Coercion
data("crude")
as.DistributedCorpus(crude)

}
\author{
  Ingo Feinerer and Stefan Theussl
}
\seealso{
  \code{\link[tm]{Corpus}}
}
