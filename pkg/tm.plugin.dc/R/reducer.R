# NOTE: Works only for term-document matrices (and NOT document-term matrices)
c_TermDocumentMatrix <-function(m, doc) {
    tf <- termFreq(doc)

    m$dimnames <- list(Terms = c(Terms(m), setdiff(names(tf), Terms(m))),
                       Docs = c(Docs(m), ID(doc)))
    m$nrow <- length(Terms(m))
    m$ncol <- ncol(m) + 1 # better use length(Docs(m)) for consistency?

    m$i <- c(m$i, which(Terms(m) %in% names(tf)))
    m$j <- c(m$j, rep(nDocs(m), length(tf)))
    m$v <- c(m$v, tf)

    m
}

tm_map_reduce <- function(x, MAP, REDUCE = NULL, ..., cmdenv_arg = NULL, useMeta = FALSE, lazy = FALSE) {
    stopifnot(inherits(x, "DistributedCorpus"))
    rev <- tempfile()
    cmdenv_arg <- c(cmdenv_arg, sprintf("_HIVE_FUNCTION_TO_APPLY_=%s", as.character(substitute(FUN))))
    ## start the streaming job
    hive_stream(.generate_tm_mapper(), #hive:::hadoop_generate_mapper("tm", deparse(substitute(FUN))),
                input = attr(x, "ActiveRevision"), output = rev,
                cmdenv_arg = cmdenv_arg)
    ## in case the streaming job failed to create output directory return an error
    stopifnot(DFS_dir_exists(rev))
    ## add new revision to corpus meta info
    attr(x, "Revisions") <- c(attr(x, "Revisions"), rev)
    ## update ActiveRevision in dc
    x <- updateRevision(x, rev)
    x
}
