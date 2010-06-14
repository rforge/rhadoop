library("tm.plugin.dc")
library("multicore")

tgz <-  "/home/stheussl/Data/Reuters/RCV1_dtm_sources.tar.gz"

path <- "/scratch/9214.1.bignode.q/incoming"
dir.create(path)
system(sprintf("tar xzf %s -C %s", tgz, path))


.read_lines_from_reducer_output <- function( path, cores = 4L )
  mclapply( .get_chunks_from_revision(path),
          function(x) {
            #cat(sprintf("processing chunk '%s'", x), sep = "\n")
            lapply(lapply( readLines(x), function(line) strsplit(line, "\t") ),
                   function(keyvalue) list(keyvalue[[1]][1],
                                           tm.plugin.dc:::dc_unserialize_object(keyvalue[[1]][2])))
         }, mc.cores = cores )

.get_chunks_from_revision <- function(path)
  file.path( path, grep("part-", dir(path),
                              value =TRUE) )

construct_term_list <- function( DTM_results, cores = 2L ){
  structure( do.call(c,
               mclapply(DTM_results, function(x) lapply(x, function(e) e[[2]]),
                        mc.cores = cores) ), names = unlist(mclapply(DTM_results, function(x) unlist(lapply(x, function(e) e[[1]])), mc.cores = cores)) )
}

## should be length(RCV1)
ndoc <- 806791
term_list <- construct_term_list( .read_lines_from_reducer_output(path) )

RCV1_TDM <- tm:::.TermDocumentMatrix( i    = rep(seq_along(term_list),
                                   unlist(lapply(term_list, function(x) length(x[[ 2 ]])))),
                                     j    = unlist(lapply(term_list, function(x) x[[ 1 ]])),
                                     v    = as.numeric(unlist(lapply(term_list, function(x) x[[ 2 ]]))),
                                     nrow = length(term_list),
                                        #ncol = length(x),
                                     ncol = ndoc,
                                     dimnames = list(Terms = names(term_list), Docs = as.character(seq_len(ndoc))) )

RCV1_DTM <- t(RCV1_TDM)
save(RCV1_TDM, file = "RCV1_TDM.rda")

