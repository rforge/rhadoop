library("tm.plugin.dc")

#tgz <-  "/home/stheussl/Data/nyt/nyt_dtm_sources.tar.gz"

#path <- "/tmp/theussl_NYT"
#dir.create(path)
#system(sprintf("tar xzf %s -C %s", tgz, path))
path <- "/tmp/theussl_NYT/incoming"

.read_lines_from_reducer_output <- function( path )
  lapply(.get_chunks_from_revision(path),
         function(x) {
           cat(sprintf("processing chunk '%s'", x), sep = "\n")
           lapply(lapply( readLines(x), function(line) strsplit(line, "\t") ), function(keyvalue) list(keyvalue[[1]][1], tm.plugin.dc:::dc_unserialize_object(keyvalue[[1]][2])))
         })

.get_chunks_from_revision <- function(path)
  file.path( path, grep("part-", dir(path),
                              value =TRUE) )

DTM_results <- .read_lines_from_reducer_output(path)
term_list <- do.call( c, lapply(DTM_results, function(x) lapply(x, function(e) e[[2]])) )
names( term_list ) <- unlist(lapply(DTM_results, function(x) unlist(lapply(x, function(e) e[[1]]))))

NYT_DTM <- tm:::.TermDocumentMatrix( i    = rep(seq_along(term_list),
                                   unlist(lapply(term_list, function(x) length(x[[ 2 ]])))),
                                     j    = unlist(lapply(term_list, function(x) x[[ 1 ]])),
                                     v    = as.numeric(unlist(lapply(term_list, function(x) x[[ 2 ]]))),
                                     nrow = length(term_list),
                                        #ncol = length(x),
                                     ncol = 1700000,
                                     dimnames = list(Terms = names(term_list), Docs = as.character(seq_len(1700000))) )

save(DTM_results, file = "DTM_results.rda")

