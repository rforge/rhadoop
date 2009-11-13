library(tm.plugin.dc)

data(crude)

m <- TermDocumentMatrix(crude[1:10])
mFull <- TermDocumentMatrix(crude[1:11])
mExtended <- merge_tdm(m, crude[[1]])

all(sapply(Docs(mFull), function(x) all(mFull[, as.integer(x)] == all(mExtended[, as.integer(x)]))))
