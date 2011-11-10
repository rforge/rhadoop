## some DSL examples

library("DSL")

dl <- DistributedList( a = "muh", b = "kuh" )
dl
length(dl)
dl[[1]]
dl[[2]]
names(dl)
names(dl) <- c("c", "d")
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

## distirbute/gather
l2 <- list( a = "muh", b = 1:100 )
dl2 <- as.DistributedList(l2)
stopifnot( all(unlist(lapply(seq_along(l2), function(i) identical(l2[[i]], dl2[[i]])))) )
l2a <- as.list(dl2)
stopifnot( identical(l2, l2a) )

## Map
l2 <- list( a = 10:200, b = 1:100 )
Map( mean, l2 )

DSL_map( dl2, mean )
