library(tm.plugin.dc)

data(crude)

m <- TermDocumentMatrix(crude[1:10])
mFull <- TermDocumentMatrix(crude[1:11])
mExtended <- c_TermDocumentMatrix(m, crude[[11]])

all(sapply(Docs(mFull), function(x) all(as.matrix(mFull[, as.integer(x)]) == as.matrix(mExtended[, as.integer(x)]))))


## check if TDM is constucted correctly compared to the original method

dc <- as.DistributedCorpus(crude)

tdm <- TermDocumentMatrix(dc)
tdm_orig <- TermDocumentMatrix(crude)

all(sort(Terms(tdm)) == sort(Terms(tdm_orig)))
