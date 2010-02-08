## Configuration
src_dir <- "~/Data/nyt"
stor_dir <- "/scratch/hadoop/storage"

dc_subsets <- grep("_dc_part_", dir(src_dir), value = TRUE )
n <- length(dc_subsets)

dc_meta <- list()
for( i in 1:n ){
  load( file.path(src_dir, sprintf("nyt_dc_subset_%d.Rda",i)) )
  dc_meta[[i]] <- nyt_data
}

dc <- aggregate_distributed_corpus_subsets(dc_subsets = dc_subsets,
                                           src_dir    = src_dir,
                                           dc_meta    = dc_meta,
                                           stor_dir   = stor_dir)

save(dc, file="~/Data/nyt/dc.Rda", compress = TRUE)

system(sprintf("tar czf %s %s", file.path(src_dir, "dc_complete.Rda"), stor_dir))
