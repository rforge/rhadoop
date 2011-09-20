## some DSL examples

library("DSL")

dl <- DistributedList( a = "muh", b = "kuh" )
dl
length(dl)
dl[[1]]
dl[[2]]
names(dl)
## not yet working
#names(dl) <- c("c", "d")
summary( dl )

## test garbage collection/finalizer
base_dir <- DSL:::DS_base_dir(DistributedStorage(dl))
d <- dir(base_dir)
d
dir(file.path(base_dir, d))
rm(dl)
gc()
stopifnot(length(dir(file.path(base_dir, d))) == 0)
stopifnot(length(dir(base_dir)) == 0)

l2 <- list( a = "muh", b = 1:100 )
dl2 <- as.DistributedList(l)
stopifnot( all(unlist(lapply(seq_along(l), function(i) identical(l[[i]], dl2[[i]])))) )
l2a <- as.list(dl2)

stopifnot( identical(l2, l2a) )
