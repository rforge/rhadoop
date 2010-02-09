source("dc_builder.R")

## Task ID and number of tasks
id <- as.integer( Sys.getenv("SGE_TASK_ID") )
ntasks <- 30L

## Configuration
src_dir <- "~/Data/nyt"
stor_dir <- "/scratch/hadoop"

## NYT Corpus (FileList)
ds_file <- file.path(src_dir, "nyt_ds.Rda")
if( !file.exists(ds_file) ){
  ds <- make_dir_source_from_gz(src_dir     = src_dir,
                                gzfile      = "nyt_xml.tar.gz",
                                base_dir    = stor_dir,
                                prefix      = "nyt")
  save(ds, file = ds_file, compress = TRUE)
}
load(ds_file)
file_list <- ds 

## prepare corpus in a distributed way
nyt_data <- build_distributed_corpus_subset_from_xml(src_dir     = src_dir,
                                                     gzfile      = "nytimes_xml.tar.gz",
                                                     base_dir    = stor_dir,
                                                     prefix      = "nyt",
                                                     file_list   = file_list,
                                                     n           = ntasks,
                                                     id          = id,
                                                     reader      = readNYTimes)

save( nyt_data, file = file.path(src_dir,
                  sprintf("nyt_dc_subset_%d.Rda", id)),
     compress = TRUE )

