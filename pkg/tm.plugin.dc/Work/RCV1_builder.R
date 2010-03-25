source("dc_builder.R")

## Task ID and number of tasks
id <- as.integer( Sys.getenv("SGE_TASK_ID") )
ntasks <- 30L

## Configuration
src_dir <- "~/Data/Reuters"
stor_dir <- "/scratch/hadoop"

## NYT Corpus (FileList)
ds_file <- file.path(src_dir, "rcv1_ds.Rda")
if( !file.exists(ds_file) ){
  ds <- make_dir_source_from_gz(src_dir     = src_dir,
                                gzfile      = "rcv1_xml.tar.gz",
                                base_dir    = stor_dir,
                                prefix      = "rcv1")
  save(ds, file = ds_file, compress = TRUE)
}
load(ds_file)
file_list <- ds 

## prepare corpus in a distributed way
rcv1_data <- build_distributed_corpus_subset_from_xml(src_dir     = src_dir,
                                                     gzfile      = "rcv1_xml.tar.gz",
                                                     base_dir    = stor_dir,
                                                     prefix      = "rcv1",
                                                     file_list   = file_list,
                                                     n           = ntasks,
                                                     id          = id,
                                                     reader      = readRCV1asPlain)

save( rcv1_data, file = file.path(src_dir,
                  sprintf("rcv1_dc_subset_%d.Rda", id)),
     compress = TRUE )

