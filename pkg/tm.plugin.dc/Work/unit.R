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

## we need do set the revision back to the original
dc <- setRevision(dc, getRevisions(dc)[[1]])
tdm_dc <- TermDocumentMatrix(dc, control = control )
tdm_c <- TermDocumentMatrix(crude, control = control )
stopifnot( all(sort(Terms(tdm_dc)) == sort(Terms(tdm_c))) )
stopifnot( all(row_sums(tdm_dc)[Terms(tdm_c)] == row_sums(tdm_c)) )

##input <- "~/Data/Reuters/reuters_xml"
##run_time_dc <-
#    system.time(
#                dc <- DistributedCorpus(DirSource(input),
#                        readerControl = list(reader = readReut21578XMLasPlain))
#                )["elapsed"]
#run_time_c <-
#    system.time(
#                reuters <- Corpus(DirSource(input),
#                         readerControl = list(reader = readReut21578XMLasPlain))
#)["elapsed"]
#c(distributed = run_time_dc, local = run_time_c)

## first 25 documents
#stopifnot( all(sapply(1:25, function(x) identical(reuters[[x]], dc[[x]]))) )

## last 25 documents
#stopifnot( all(sapply((length(reuters)-25):length(reuters),
#                      function(x) identical(reuters[[x]], dc[[x]]))) )

## -> Reuters: preprocess distributed corpus
################################################################################

#dc <- tm_map(dc, stemDocument)
#reuters_stemmed <- tm_map(reuters, stemDocument)

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
                      function(x) identical(crude_stemmed[[x]], hdc[[x]]))) )

## -> crude: construct TermDocumentMatrix
################################################################################

require("slam")
control <- list( removePunctuation = TRUE,
                 removeNumbers = TRUE,
                 stemming = TRUE,
                 stopwords = TRUE )

tdm_hdc <- TermDocumentMatrix(hdc, control = control )
tdm_c <- TermDocumentMatrix(crude, control = control )
stopifnot( all(sort(Terms(tdm_hdc)) == sort(Terms(tdm_c))) )
stopifnot( all(row_sums(tdm_hdc)[Terms(tdm_c)] == row_sums(tdm_c)) )

###############################################################################
## Construct Reuters 21578 corpus via Hadoop
## Using constructor DistributedCorpus()
###############################################################################

hive_set_nreducer( 20 )

tmp_dir <- "/work_local/hadoop/tmp"
term_freq_control <- list( removePunctuation = TRUE,
                           removeNumbers = TRUE,
                           stemming = TRUE,
                           stopwords = TRUE )

dir.create( tmp_dir )
input <- "~/Data/Reuters/reuters_xml.tar.gz"
system( sprintf("tar xzf %s -C %s", input, tmp_dir) )
xml <- file.path( tmp_dir, dir( tmp_dir ) )

## Configure HDFS Storage
storage <- tm.plugin.dc:::dc_HDFS_storage() 
storage$chunksize <- 2 * 1024^2
dcStorage( storage )

## NOTE: we need 16 chunks to work with 16 CPUs, thus setting chunk size to 2.5 MB
system.time( dcReuters <- DistributedCorpus(DirSource(xml),
                        readerControl = list(reader = readReut21578XMLasPlain)) )
## delete uncompressed sources
unlink( tmp_dir, recursive = TRUE )

## construct the distributed corpus
system.time( ReutersTDM <- TermDocumentMatrix(dcReuters,
                                              control = term_freq_control) )

## compare hadoop results with classic result
ReutersTDM_hadoop <- ReutersTDM
library( "tm.corpus.Reuters" )
data( "Reuters" )
data( "ReutersTDM" )

stopifnot( identical(ReutersTDM_hadoop, ReutersTDM) )
