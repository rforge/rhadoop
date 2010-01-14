library(tm.plugin.dc)

data(crude)

## check if dc and classic corpus contain the same documents
dc <- as.DistributedCorpus(crude)
stopifnot(all(sapply(seq_len(20L), function(x) identical(crude[[x]], dc[[x]]))))

dc <- tm_map(dc, stemDocument)
crude_stemmed <- tm_map(crude, stemDocument)
stopifnot(all(sapply(seq_len(length(crude)), function(x) identical(crude_stemmed[[x]], dc[[x]]))))

## Reuters data set
input <- "~/Data/Reuters/reuters_xml"
dc <- DistributedCorpus(DirSource(input),
                        readerControl = list(reader = readReut21578XMLasPlain))
reuters <- Corpus(DirSource(input),
                        readerControl = list(reader = readReut21578XMLasPlain))
## first 100 documents
stopifnot(all(sapply(seq_len(100), function(x) identical(reuters[[x]], dc[[x]]))))

## last 100 documents
stopifnot(all(sapply((length(reuters)-100):length(reuters), function(x) identical(reuters[[x]], dc[[x]]))))

m <- TermDocumentMatrix(crude[1:10])
mFull <- TermDocumentMatrix(crude[1:11])
mExtended <- c_TermDocumentMatrix(m, crude[[11]])

all(sapply(Docs(mFull), function(x) all(as.matrix(mFull[, as.integer(x)]) == as.matrix(mExtended[, as.integer(x)]))))


## check if TDM is constucted correctly compared to the original method

dc <- as.DistributedCorpus(crude)

tdm <- TermDocumentMatrix(dc)
tdm_orig <- TermDocumentMatrix(crude)

all(sort(Terms(tdm)) == sort(Terms(tdm_orig)))
