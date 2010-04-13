tgz <-  "/home/stheussl/Data/nyt/nyt_dtm_sources.tar.gz"

path <- "/tmp/theussl_NYT"
dir.create(path)
system(sprintf("tar xzf %s -C %s", tgz, path)
path <- "/tmp/theussl_NYT/incoming"

.read_lines_from_reducer_output <- function( path )
  unlist( lapply(.get_chunks_from_revision(path),
                           function(x) {
                               cat(sprintf("processing chunk '%s'", x), sep = "\n")
                               lapply(lapply( readLines(x), function(line) strsplit(line, "\t") ), function(keyvalue) list(keyvalue[[1]][1], tm.plugin.dc:::dc_unserialize_object(keyvalue[[1]][2])))
                               }))

.get_chunks_from_revision <- function(path)
  file.path( path, grep("part-", dir(path),
                              value =TRUE) )

results <- .read_lines_from_reducer_output(path)
save(DTM_results, file = "DTM_results.rda", compress = TRUE)
