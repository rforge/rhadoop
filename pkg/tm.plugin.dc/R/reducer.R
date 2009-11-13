# TODO: Kurt fragen, ob das Sinn macht!
empty_tdm <- structure(list(i = integer(0), j = integer(0), v = integer(0), nrow = 0, ncol = 0,
                            dimnames = list(Terms = character(0), Docs = character(0)),
                            Weighting = c("W1", "W2")),
                       class = c("TermDocumentMatrix", "simple_triplet_matrix"))

# NOTE: Works only for term-document matrices (and NOT document-term matrices)
c_TermDocumentMatrix <-function(m, doc) {
    tf <- termFreq(doc)

    m$dimnames <- list(Terms = c(Terms(m), setdiff(names(tf), Terms(m))),
                       Docs = c(Docs(m), ID(doc)))
    m$nrow <- length(Terms(m))
    m$ncol <- ncol(m) + 1

    m$i <- c(m$i, which(Terms(m) %in% names(tf)))
    m$j <- c(m$j, rep(nDocs(m), length(tf)))
    m$v <- c(m$v, tf)

    m
}
