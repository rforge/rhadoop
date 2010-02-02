################################################################################
## Initializiation
################################################################################

library(tm.plugin.dc)
data(crude)

################################################################################
## Standard disk storage
################################################################################

dcStorage()

## -> crude: build distributed corpus
################################################################################

dc <- as.DistributedCorpus(crude)
## check if dc and classic corpus contain the same documents
stopifnot( all(sapply(seq_len(length(dc)),
                      function(x) identical(crude[[x]], dc[[x]]))) )

## -> crude: preprocess distributed corpus
################################################################################

dc <- tm_map(dc, stemDocument)
crude_stemmed <- tm_map(crude, stemDocument)
## check if dc and classic corpus contain the same documents after preprocessing
stopifnot( all(sapply(seq_len(length(crude)),
                      function(x) identical(crude_stemmed[[x]], dc[[x]]))) )

## -> crude: construct TermDocumentMatrix
################################################################################

require("slam")
control <- list( removePunctuation = TRUE,
                 removeNumbers = TRUE,
                 stemming = TRUE,
                 stopwords = TRUE )

tdm_dc <- TermDocumentMatrix(dc, control = control )
tdm_c <- TermDocumentMatrix(crude, control = control )
stopifnot( all(sort(Terms(tdm_dc)) == sort(Terms(tdm_c))) )
stopifnot( all(row_sums(tdm_dc)[Terms(tdm_c)] == row_sums(tdm_c)) )

## -> Reuters: build distributed corpus
################################################################################

input <- "~/Data/Reuters/reuters_xml"
run_time_dc <-
    system.time(
                dc <- DistributedCorpus(DirSource(input),
                         readerControl = list(reader = readReut21578XMLasPlain))
                )["elapsed"]
run_time_c <-
    system.time(
                reuters <- Corpus(DirSource(input),
                         readerControl = list(reader = readReut21578XMLasPlain))
)["elapsed"]
c(distributed = run_time_dc, local = run_time_c)

## first 25 documents
stopifnot( all(sapply(1:25, function(x) identical(reuters[[x]], dc[[x]]))) )

## last 25 documents
stopifnot( all(sapply((length(reuters)-25):length(reuters),
                      function(x) identical(reuters[[x]], dc[[x]]))) )

## -> Reuters: preprocess distributed corpus
################################################################################

dc <- tm_map(dc, stemDocument)
reuters_stemmed <- tm_map(reuters, stemDocument)

################################################################################
## Hadoop distributed filesystem (HDFS) storage
################################################################################

require("hive")
if( inherits(tryCatch(hive_is_available(), error = identity), "error") ){
    hive( hive_create("/home/theussl/lib/hadoop-0.20.1") )
    hive_start()
}
stor <- tm.plugin.dc:::dc_HDFS_storage()
stor
dcStorage(stor)

## -> crude: build distributed corpus
################################################################################

hdc <- as.DistributedCorpus(crude)
## check if dc and classic corpus contain the same documents
stopifnot( all(sapply(seq_len(length(hdc)),
                      function(x) identical(crude[[x]], hdc[[x]]))) )

## -> crude: preprocess distributed corpus
################################################################################

hdc <- tm_map(hdc, stemDocument)
crude_stemmed <- tm_map(crude, stemDocument)
## check if dc and classic corpus contain the same documents after preprocessing
stopifnot( all(sapply(seq_len(length(crude)),
                      function(x) identical(crude_stemmed[[x]], dc[[x]]))) )
