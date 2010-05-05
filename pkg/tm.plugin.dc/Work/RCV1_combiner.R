source("dc_builder.R")

## Configuration
src_dir <- "~/Data/Reuters"
stor_dir <- "/scratch/hadoop/storage"

dc_subsets <- grep("_dc_part_", dir(src_dir), value = TRUE )
n <- length(dc_subsets)

dc_meta <- list()
for( i in 1:n ){
  load( file.path(src_dir, sprintf("rcv1_dc_subset_%d.Rda",i)) )
  dc_meta[[i]] <- rcv1_data
}

dc <- aggregate_distributed_corpus_subsets(dc_subsets = dc_subsets,
                                           src_dir    = src_dir,
                                           dc_meta    = dc_meta,
                                           stor_dir   = stor_dir)

save(dc, file="~/Data/Reuters/dc.Rda", compress = TRUE)

system(sprintf("tar czf %s %s", file.path(src_dir, "dc_complete.tar.gz"), stor_dir))

stor <- (function(description, chunksize, base_directory, dir_create, list_directory, fetch_last_line, read_lines, unlink, write_lines) environment())(tm.corpus.RCV1:::RCV1_storage()$description, tm.corpus.RCV1:::RCV1_storage()$chunksize, tm.corpus.RCV1:::RCV1_storage()$base_directory, tm.corpus.RCV1:::RCV1_storage()$dir_create, tm.corpus.RCV1:::RCV1_storage()$list_directory, tm.corpus.RCV1:::RCV1_storage()$fetch_last_line, tm.corpus.RCV1:::RCV1_storage()$read_lines, tm.corpus.RCV1:::RCV1_storage()$unlink, tm.corpus.RCV1:::RCV1_storage()$write_lines)

class(stor) <- class(dc_storage(dc))
