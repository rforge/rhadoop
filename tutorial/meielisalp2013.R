## Examples for the coures
## High Performance and Parallel Computing in R
## Stefan Theussl, 2013

############################################
## (1) Global Optimization
############################################

## --- Sequential Approach

## Define function and visualize
f <- function( x, y ){
    3*( 1-x )^2 * exp( -x^2-(y+1)^2 ) - 10 * (x/5 - x^3 - y^5) * exp( -x^2 - y^2 ) - 1/3 * exp( -(x+1)^2 - y^2 )
}

x <- y <- seq(from = -2.5, to = 2.5, length.out = 100)
z <- outer(x, y, FUN = f )

pp <- persp(x, y, z,
            col = "lightgreen",
            theta = -30, phi = 20, r = 10, d = 1, expand = 1,
            ltheta = 90, lphi = 180,shade = 0.75,
            ticktype = "detailed",
            nticks = 5)


## optimize using multi-start procedure
start <- list( c(0, 0), c(-1, -1), c(0, -1), c(0, 1) )
seqt <- system.time(sol <- lapply(start, function(par) optim(par, function(x) f(x[1], x[2]),
                                                             method = "Nelder-Mead",
          lower = -Inf, upper = Inf, control = list(maxit = 1000000, beta = 0.01, reltol = 1e-16)))
        )["elapsed"]
seqt

do.call( rbind, lapply(sol, function(x) c(x$par, x$value)) )

## show solutions
for(i in seq_along(sol))
    points(trans3d(sol[[i]]$par[1], sol[[i]]$par[2], sol[[i]]$value, pmat = pp), col = i+1, pch =16)




## now in parallel
require( "parallel" )

ncores <- detectCores()
ncores

part <- system.time(sol <- mclapply(start, function(par)
                      optim(par, function(x) f(x[1], x[2]), method = "Nelder-Mead", lower = -Inf, upper = Inf,
                            control = list(maxit = 1000000, beta = 0.01, reltol = 1e-16)), mc.cores = ncores)
                    )["elapsed"]

do.call( rbind, lapply(sol, function(x) c(x$par, x$value)) )

## calculate speedup
seqt/part


############################################
## (2) Text Mining
############################################

## install.packages( "tm", dependencies = TRUE )
require( "tm" )
## install.packages( "tm.corpus.Reuters21578", repos = "http://datacube.wu.ac.at" )
require( "tm.corpus.Reuters21578" )
data( Reuters21578 )

content( Reuters21578[[1]] )
meta( Reuters21578[[1]] )

seqt <- system.time( l <- lapply( Reuters21578, stemDocument ) )["elapsed"]

content( l[[1]] )

## now in parallel
part <- system.time( l <- mclapply( Reuters21578, stemDocument, mc.cores = ncores ) )["elapsed"]

seqt/part


## Further examples of the parallel package can be found in the book
## by Q. Ethan McCallum and Stephen Weston among others.

############################################
## (3) DSL
############################################

## install.packages( "DSL" )
require("DSL")

## simple wordcount based on two files:
dir( system.file("examples", package = "DSL") )

## first force 1 chunk per file (set max chunk size to 1 byte):
ds <- DStorage("LFS", tempdir(), chunksize = 1L)
## make "DList", i.e., read file contents and store in chunks
dl <- as.DList( system.file("examples", package = "DSL"),
                DStorage = ds )

## read files
dl <- DMap(dl, function( keypair ){
    list( key = keypair$key,
          value = tryCatch(readLines(keypair$value),
                           error = function(x) NA) )
})

## split into terms
splitwords <- function( keypair ){
    keys <- unlist(strsplit(keypair$value, " "))
    mapply( function(key, value) list( key = key,
                                       value = value),
            keys, rep(1L, length(keys)),
            SIMPLIFY = FALSE, USE.NAMES = FALSE )
}

## apply the function
res <- DMap( dl, splitwords )
unlist( as.list(res) )

## reduce
res <- DReduce( res, sum )
unlist( as.list( res ) )


############################################
## (4) Text mining
############################################

require( "tm.plugin.dc" )

Reuters21578
print(object.size(Reuters21578), units = "Mb")

dc <- as.DCorpus( Reuters21578 )
summary(dc)
print(object.size(dc), units = "Mb")

dc <- tm_map(dc, stemDocument)

is.DList(dc)
DL_storage(dc)
dc[[2]]

dtm <- DocumentTermMatrix( Reuters21578 )
dtm


require("slam")
head( sort( col_sums( dtm ), decreasing = TRUE), n = 10 )
