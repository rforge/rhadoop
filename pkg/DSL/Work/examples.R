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
base_dir <- DSL:::DS_base_dir(DL_storage(dl))
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
ds <- DStorage("HDFS", tempdir())
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
DL_storage( dl )

## test garbage collection/finalizer
base_dir <- DSL:::DS_base_dir(DL_storage(dl))
d <- DL_storage(dl)$list_directory(base_dir)
d
DSL:::.revisions(dl)
DL_storage(dl)$list_directory(file.path(base_dir, d))
rm(dl)
gc()
stopifnot(length(DL_storage(ds)$list_directory(file.path(base_dir, d))) == 0)
stopifnot(length(DL_storage(ds)$list_directory(base_dir)) == 0)

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
require( "DSL" )
## Read in a bunch of files in key/value pair format
dl <- as.DList("/tmp")
dl <- DMap(dl, function( keypair ){
    list( key = keypair$key, value = tryCatch(readLines(keypair$value), error = function(x) NA) )
})


## Another test example: a key-value pair procuces a set of key value pairs
l <- list( line1 = "This is the first line.", line2 = "Now, the second line." )
lapply( l, function(x) unlist(strsplit(x, " ")) )

splitwords <- function( keypair ){
    keys <- unlist(strsplit(keypair$value, " "))
    mapply( DSL:::DPair, keys, rep(1L, length(keys)), SIMPLIFY = FALSE, USE.NAMES = FALSE)
}


dl <- as.DList( l )
res <- DMap( dl, splitwords )
res[[9]]
DKeys( res )

out <- DReduce( res, sum )
as.list(out)

ds <- DStorage("HDFS", tempdir())
dl <- as.DList( l, DStorage = ds )
res <- DMap( dl, splitwords )
out <- DReduce( res, REDUCE = sum )
as.list(out)

## DL_Storage replacement test

l <- list( line1 = "This is the first line.", line2 = "Now, the second line." )
dl <- as.DList( l )

ds <- DStorage("HDFS", tempdir())

DL_storage(dl) <- ds

## get/put objects
## lm example to create object
ctl <- c(4.17,5.58,5.18,6.11,4.50,4.61,5.17,4.53,5.33,5.14)
trt <- c(4.81,4.17,4.41,3.59,5.87,3.83,6.03,4.89,4.32,4.69)
group <- gl(2,10,20, labels=c("Ctl","Trt"))
weight <- c(ctl, trt)
lm.D9 <- lm(weight ~ group)

## LFS
ds <- DStorage("LFS", tempdir())

file <- "lm.D9"

DSL:::DS_put(ds, lm.D9, file)
DSL:::DS_list_directory( ds )

DSL:::DS_get(ds, file)

## HDFS
ds <- DStorage("HDFS", tempdir())

file <- "lm.D9"

DSL:::DS_put(ds, lm.D9, file)
DSL:::DS_list_directory( ds )

DSL:::DS_get(ds, file)
