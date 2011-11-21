## ##################
## some DSL examples
## ##################

require( "DSL" )

## ##################
## DStorage: LFS
## ##################

## test construction/access
dl <- DList( a = "muh", b = "kuh" )
l <- list( a = "muh", b = "kuh" )
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

## test distribute/gather
l2 <- list( a = "muh", b = 1:100 )
dl2 <- as.DList(l2)
stopifnot( all(unlist(lapply(seq_along(l2), function(i) identical(l2[[i]], dl2[[i]])))) )
l2a <- as.list(dl2)
stopifnot( identical(l2, l2a) )

## test map for DLists a key value pair is organized as a named list
## where list elements represent the values and the corresponding
## names the keys. Note that keys cannot be arbitrary R objects,
## rather they are character vectors as generated with the function
## make.names()
dl <- DList( a = "muh", b = "kuh" )

foo <- function( keypair )
    list( key = paste("m", keypair$key, sep = ""), value = c("map", keypair$value) )
dlm <- DMap( x = dl, MAP = foo)
names(dlm)
DGather( dlm, keys = TRUE )
DKeys( dlm )
## same but with new revision; NOTE:: this has the side effect that environment in dlm is changed too!!!!
## generally, keeping revisions works pretty good
dlm2 <- DMap( x = dlm, MAP = foo, keep = TRUE)
DSL:::.revisions( dlm2 )

## test lapply
dl <- DList( a = "muh", b = "kuh" )
foo <- function(x)
    c("super", x)
dlm <- DLapply(dl, foo)
l <- as.list( dl )
lm <- lapply(l, foo)
stopifnot( identical(lm, as.list(dlm)) )
## now a second lapply step
dlm2 <- DLapply(dlm, foo)
lm2 <- lapply(lm, foo)
stopifnot( identical(lm, as.list(dlm)) )

## parallel lapply
dlmp <- DLapply(dl, foo, parallel = TRUE)
stopifnot( identical(lm, as.list(dlmp)) )

## ##################
## DStorage: HDFS
## ##################
require("DSL")

## first create HDFS storage
ds <- DStorage_create("HDFS", tempdir())
ds

## test construction/access
dl <- DList( a = "muh", b = "kuh", DStorage = ds )
dl
length(dl)
dl[[1]]
dl[[2]]
names(dl)
names(dl) <- c("c", "d")
summary( dl )
DStorage( dl )

## test garbage collection/finalizer
base_dir <- DSL:::DS_base_dir(DStorage(dl))
d <- DStorage(dl)$list_directory(base_dir)
d
DSL:::.revisions(dl)
DStorage(dl)$list_directory(file.path(base_dir, d))
rm(dl)
gc()
stopifnot(length(DStorage(ds)$list_directory(file.path(base_dir, d))) == 0)
stopifnot(length(DStorage(ds)$list_directory(base_dir)) == 0)

## test distribute/gather
l2 <- list( a = "muh", b = 1:100 )
dl2 <- as.DList(l2, DStorage = ds)
stopifnot( all(unlist(lapply(seq_along(l2), function(i) identical(l2[[i]], dl2[[i]])))) )
l2a <- as.list(dl2)
stopifnot( identical(l2, l2a) )

## test map
dl <- DList( a = "muh", b = "kuh", DStorage = ds )

bar <- function( keypair )
    list( key = paste("m", keypair$key, sep = ""), value = c("map", keypair$value) )
dlm <- DMap( x = dl, MAP = bar)

dlm2 <- DMap( x = dlm, MAP = bar)
as.list(dlm2)
DKeys(dlm2)

## test lapply (automatically done in parallel if Hadoop environment configured)
dl <- DList( a = "muh", b = "kuh", DStorage = ds )
bar2 <- function(x)
    c("super", x)
dlm <- DLapply(dl, bar2)
l <- as.list( dl )
lm <- lapply(l, bar2)
stopifnot( identical(lm, as.list(dlm)) )

## more data
#install.packages("tm.corpus.Reuters21578", repos = "http://datacube.wu.ac.at")
require("tm.corpus.Reuters21578")
data(Reuters21578)
reut <- as.DList( Reuters21578 )

# Test 2011-11-16: success
#test <- lapply( seq_along(Reuters21578), function( i ) identical(Reuters21578[[i]], reut[[i]]) )


## Useful Examples:

## Read in a bunch of files in key/value pair format
dl <- as.DList("/tmp")
dl <- DMap(dl, function( keypair ){
    list( key = keypair$key, value = tryCatch(readLines(keypair$value), error = function(x) NA) )
})

