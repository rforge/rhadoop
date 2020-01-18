TermDocumentMatrix.DCorpus <- function( x, control = list() ){
    ## control contains preprocessing function, see help page of termFreq()

    ## if empty then termFreq is called with default options (e.g., when
    ## preprocessing has already be done using tm_map())
    ## otherwise call tm_map_reduce where the mapper does the preprocessing
    ## (supplied with the control argument) and the reducer
    ## makes the TDMs

    args <- control
    ## this is borrowed tm code (2011-11-27) to make things as compatible as possible
    MAP <- function(keypair){
        tf <- tm::termFreq(keypair$value, args)
        if(!length(tf))
            list(key = NULL, value = list(id = keypair$key, count = 0))
        else
            mapply( function(key, value) list( key = key, value = value), names(tf),
                   mapply(function(id, count) list(id = id, count = count), as.integer(meta(keypair$value, "id")), tf, SIMPLIFY = FALSE, USE.NAMES = FALSE), SIMPLIFY = FALSE, USE.NAMES = FALSE )
    }
    ## Apply above map function, then reduce, then retrieve partial
    ## results from file system (term / {key / term frequency})
    ## {} indicates serialized object; we use the standard collector in the reduce step
    intermed <- DReduce(DMap(x$content, MAP))

    ## first extract the terms. NOTE: they are not necessarily unique as there may be
    ## some terms duplicated among different chunks. Terms derived from the same chunk are unique.
    terms <- factor(DKeys(intermed))
    uniq_terms <- sort(unique(as.character(terms)))
    levels(terms) <- seq_along(levels(terms))

    results <- unlist(DGather(intermed, names = FALSE), recursive = FALSE)
    i <- rep(as.integer(terms), unlist(lapply(results, function(x) length(x[[ 1 ]]))))
    rmo <- order(i)

    docs <- unlist( lapply(results, function(x) x[[ 1 ]]) )
    j <- match( as.character(docs), names(x) )

    ## .SimpleTripletMatrix not exported from tm
    m <- .fix_TDM( .SimpleTripletMatrix(i = as.integer(i)[rmo],
                                        j = as.integer(j)[rmo],
                                        v = as.numeric(unlist(lapply(results, function(x) x[[ 2 ]])))[rmo],
                                        uniq_terms,
                                        x) )

    ## tm function not exported: filter_global_bounds
    bg <- control$bounds$global
    if (length(bg) == 2L && is.numeric(bg)) {
        rs <- row_sums(m > 0)
        m <- m[(rs >= bg[1]) & (rs <= bg[2]), ]
    }

    tm::as.TermDocumentMatrix(m, weighting = control$weighting)
}

## FIXME: we can do this more efficiently
.fix_TDM <- function(x){
  #x$ncol <- x$ncol + length( not_included )
  #x$dimnames$Docs <- c(x$dimnames$Docs, as.character(not_included))
  ## column major order
  cmo <- order(x$j)
  x$i <- x$i[cmo]
  x$j <- x$j[cmo]
  x$v <- x$v[cmo]
  x
}



tm:::TermDocumentMatrix.VCorpus
function (x, control = list())
{
    stopifnot(is.list(control))
    tflist <- tm_parLapply(unname(content(x)), termFreq, control)
    v <- unlist(tflist)
    i <- names(v)
    terms <- sort(unique(as.character(if (is.null(control$dictionary)) i else control$dictionary)))
    i <- match(i, terms)
    j <- rep.int(seq_along(x), lengths(tflist))
    m <- .SimpleTripletMatrix(i, j, as.numeric(v), terms, x)
    m <- filter_global_bounds(m, control$bounds$global)
    .TermDocumentMatrix(m, control$weighting)
}



## not exported tm function
.SimpleTripletMatrix <- function (i, j, v, terms, corpus)
{
    docs <- as.character(meta(corpus, "id", "local"))
    if (length(docs) != length(corpus)) {
        warning("invalid document identifiers")
        docs <- NULL
    }
    simple_triplet_matrix(i, j, v, nrow = length(terms), ncol = length(corpus),
        dimnames = list(Terms = terms, Docs = docs))
}
