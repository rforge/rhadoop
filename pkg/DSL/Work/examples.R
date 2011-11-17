## some DSL examples

library("DSL")

dl <- DList( a = "muh", b = "kuh" )
dl
length(dl)
dl[[1]]
dl[[2]]
names(dl)
names(dl) <- c("c", "d")
summary( dl )

## test garbage collection/finalizer
base_dir <- DSL:::DS_base_dir(DStorage(dl))
d <- dir(base_dir)
d
DSL:::.revisions(dl)
dir(file.path(base_dir, d))
rm(dl)
gc()
stopifnot(length(dir(file.path(base_dir, d))) == 0)
stopifnot(length(dir(base_dir)) == 0)

## distribute/gather
l2 <- list( a = "muh", b = 1:100 )
dl2 <- as.DList(l2)
stopifnot( all(unlist(lapply(seq_along(l2), function(i) identical(l2[[i]], dl2[[i]])))) )
l2a <- as.list(dl2)
stopifnot( identical(l2, l2a) )

## Map
dl <- DList( a = "muh", b = "kuh" )
foo <- function(x)
    c("super", x)
dlm <- DLapply(dl, foo)
l <- as.list( dl )
lm <- lapply(l, foo)
stopifnot( identical(lm, as.list(dlm)) )
## now a second map step
dlm2 <- DLapply(dlm, foo)
lm2 <- lapply(lm, foo)
stopifnot( identical(lm, as.list(dlm)) )

## parallel Map
dlmp <- DLapply(dl, foo, parallel = TRUE)
stopifnot( identical(lm, as.list(dlmp)) )

## more data
#install.packages("tm.corpus.Reuters21578", repos = "http://datacube.wu.ac.at")
require("tm.corpus.Reuters21578")
data(Reuters21578)
reut <- as.DList( Reuters21578 )

# Test 2011-11-16: success
#test <- lapply( seq_along(Reuters21578), function( i ) identical(Reuters21578[[i]], reut[[i]]) )

## new naming scheme:

DLapply
DMap
DReduce
DGather
DStorage
