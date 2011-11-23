tm_map.DCorpus <-
function(x, FUN, ..., useMeta = FALSE, lazy = FALSE) {
    ## TODO: shouldn't we check provided function for sanity?

    ## FIXME: what to do with lazy argument?
    result <- x
    if (useMeta)
        result <- tm:::`Content<-.default`(result, DLapply(x, FUN, ..., DMetaData = DMetaData(x), keep = TRUE))
    else
        result <- tm:::`Content<-.default`(result, DLapply(x, FUN, ..., keep = TRUE))

    result
}
