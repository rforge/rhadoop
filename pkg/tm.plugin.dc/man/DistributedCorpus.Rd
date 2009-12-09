\name{DistributedCorpus}
\alias{DistributedCorpus}
\title{Distributed Corpus}
\description{
  Data structures and operators for distributed corpora.
}
\usage{
DistributedCorpus(source, readerControl = list(reader = source$DefaultReader, language = "eng"), chunksize = 8 * 1024^2, \ldots)
}
\arguments{
  \item{source}{A \code{\link[tm]{Source}} object. At the moment only
    \code{\link[tm]{DirSource}} is supported.}
  \item{readerControl}{A list with the named components \code{reader}
    representing a reading function capable of handling the file format
    found in \code{source}, and \code{language} giving the text's language
    (preferably in \acronym{ISO} 639-2 format).}
  \item{chunksize}{An integer value determining the maximal size in
    bytes for chunks in the Hadoop distributed file system.}
  \item{\dots}{Optional arguments for the \code{reader}.}
}
\details{When constructing a distributed corpus the input source is
  extracted via the supplied reader and uploaded into the Hadoop
  distributed file system (HDFS). While the dataset resides on HDFS,
  only a symbolic representation is held in \R which allows to access
  the corpus via corresponding methods which dispatch on the distributed
  corpus.

  The constructed corpus object inherits from a \code{list} and has several
  attributes containing meta information:
  \describe{
    \item{ActiveRevision}{Contains the current revision of the
      corpus. Each modification (map/reduce in the Hadoop terminology)
      of the documents in the corpus results in a new revision which
      allows fast switching between multiple snapshots.}.
    \item{Chunks}{A list of file names in the HDFS representing
      underlying chunks holding the documents.}
    \item{\code{CMetaData}}{Corpus Meta Data contains corpus specific
      meta data in form of tag-value pairs and information about
      children in form of a binary tree. This information is useful for
      reconstructing meta data after e.g. merging corpora. This is
      mainly available to be compatible with standard \pkg{tm} corpus definitions
      but not yet actually used in the distributed scenario.}
    \item{\code{DMetaData}}{Document Meta Data of class
      \code{data.frame} contains document specific meta data for the
      corpus. This data frame typically encompasses clustering or
      classification results which basically are metadata for documents
      but form an own entity (e.g., with its name, the value range,
      etc.). This is mainly available to be compatible with standard \pkg{tm}
      corpus definitions but not yet actually used in the distributed
      scenario.}
    \item{Keys}{A character vector identifying each individual document
      in the corpus on the HDFS.}
    \item{Mapping}{Basically a hash table (implemented as a list of \R
      environments) holding for each key the chunk and position in the
      HDFS.}
    \item{Revisions}{A list of all available revisions.}
  }					     
}
\value{
  An object of class \code{DistributedCorpus} which extends the classes
  \code{Corpus} and \code{list} containing a collection of text
  documents.
}
\author{
  Ingo Feinerer and Stefan Theussl
}
\seealso{
  \code{\link[tm]{Corpus}}
}
