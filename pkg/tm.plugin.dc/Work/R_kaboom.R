## FIXME: max of index i does not correspond to the number of documents
##        nice way to kill R in combination with row_sums

library("tm.corpus.RCV1")
data("RCV1_DTM")

require("slam")

names(RCV1_DTM)
unlist(lapply(RCV1_DTM, typeof))
unlist(lapply(RCV1_DTM, length))

dim(RCV1_DTM)
range(RCV1_DTM$i)
range(RCV1_DTM$j)

## to kill R, e.g. do
require("slam")
x <- row_sums(RCV1_DTM)
head(x)

## lg, st :-)
