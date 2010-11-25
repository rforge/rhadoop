library("tm.corpus.NYTimes")
library("multicore")

local_tmp <- "/home/theussl/Data/NYT/dc_builder_storage"
stopifnot(file.exists(local_tmp))
active_rev <- "20100831155759-4-n"
path <- file.path(local_tmp, active_rev)

## new code
.read_lines_from_reducer_output <- function( path  )
  unlist( lapply(.get_chunks_from_revision(path),
                           function(x){
                   cat(sprintf("processing chunk '%s'", x), sep = "\n")
                   readLines(x)
                   }
                   )
         )

.get_chunks_from_revision <- function(path)
  file.path( path, grep("^part-", dir(path),
                              value =TRUE) )


results <- mclapply( .read_lines_from_reducer_output( path ),
                  function(x) strsplit(x, "\t"), mc.cores = 7L )


   terms <- factor(tm.plugin.dc:::dc_decode_term(unlist(lapply(results, function(x) x[[1]][1])) ))
    uniq_terms <- sort(unique(as.character(terms)))
    levels(terms) <- seq_along(levels(terms))

    results <- lapply( results,
                       function(x) tm.plugin.dc:::dc_unserialize_object(x[[ 1 ]][2]) )
    i <- rep(as.integer(terms), unlist(lapply(results, function(x) length(x[[ 1 ]]))))
    rmo <- order(i)
load("/home/theussl/svn/baRpkgs/tm.corpus.NYTimes/data/NYT.rda")
NYT_DTM <- t(tm.plugin.dc:::.fix_TDM( tm:::.TermDocumentMatrix(i    = as.integer(i)[rmo],
                                       j    = unlist(lapply(results, function(x) x[[ 1 ]]))[rmo],
                                       v    = as.numeric(unlist(lapply(results, function(x) x[[ 2 ]])))[rmo],
                                       nrow = length(uniq_terms),
                                       ncol = length(x),
                                       dimnames = list(Terms = uniq_terms,
                                                       Docs = rownames(tm.plugin.dc:::dc_get_text_mapping_from_revision(NYT)))) )
)

## old code
.read_lines_from_reducer_output <- function( path, cores = 4L )
  mclapply( .get_chunks_from_revision(path),
          function(x) {
            cat(sprintf("processing chunk '%s'", x), sep = "\n")
            lapply(lapply( readLines(x), function(line) strsplit(line, "\t") ),
                   function(keyvalue) list(keyvalue[[1]][1],
                                           tm.plugin.dc:::dc_unserialize_object(keyvalue[[1]][2])))
         }, mc.cores = cores )

construct_term_list <- function( DTM_results, cores = 2L ){
  structure( do.call(c,
               mclapply(DTM_results, function(x) lapply(x, function(e) e[[2]]),
                        mc.cores = cores) ), names = unlist(mclapply(DTM_results, function(x) unlist(lapply(x, function(e) e[[1]])), mc.cores = cores)) )
}

system.time(term_list <- construct_term_list( .read_lines_from_reducer_output(path), cores = 7L ))
save(term_list, file = "cache_term_list.rda")
data(NYT)
system.time(
NYT_DTM <- t( tm:::.TermDocumentMatrix( i    = rep(seq_along(term_list),
                                   unlist(lapply(term_list, function(x) length(x[[ 2 ]])))),
                                     j    = unlist(lapply(term_list, function(x) x[[ 1 ]])),
                                     v    = as.numeric(unlist(lapply(term_list, function(x) x[[ 2 ]]))),
                                     nrow = length(term_list),
                                     ncol = length(NYT),
                                     dimnames = list(Terms = names(term_list), Docs = as.character(seq_len(length(NYT))))) )
)
save(NYT_DTM, file = "NYT_DTM.rda", compress = TRUE)


