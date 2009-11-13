library(tm.plugin.dc)

data(crude)

m <- TermDocumentMatrix(crude[1:10])
mFull <- TermDocumentMatrix(crude[1:11])
mExtended <- c_TermDocumentMatrix(m, crude[[11]])

all(sapply(Docs(mFull), function(x) all(as.matrix(mFull[, as.integer(x)]) == as.matrix(mExtended[, as.integer(x)]))))
