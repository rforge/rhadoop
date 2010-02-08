source("dc_builder.R")

## Task ID and number of tasks
id <- as.integer( Sys.getenv("SGE_TASK_ID") )
ntasks <- 30L  ##Sys.getenv("NSLOTS")

## Reuters Corpus (FileList)
ds_file <- "~/Data/Reuters/reuters_ds.Rda"
if( !file.exists(ds_file) ){
  ds <- make_dir_source_from_gz(src_dir     = "~/Data/Reuters",
                                gzfile      = "reuters_xml.tar.gz",
                                base_dir    = "/scratch/hadoop",
                                prefix      = "reuters")
  save(ds, file = ds_file, compress = TRUE)
}
load(ds_file)
file_list <- ds 

reuters_data <- build_distributed_corpus_subset_from_xml(src_dir = "~/Data/Reuters",
                                                     gzfile      = "reuters_xml.tar.gz",
                                                     base_dir    = "/scratch/hadoop",
                                                     prefix      = "reuters",
                                                     file_list   = file_list,
                                                     n           = ntasks,
                                                     id          = id, 
                                                     reader      = readReut21578XML)
  
save(reuters_data, file = file.path("~/Data/Reuters",
                     sprintf("reuters_dc_subset_%d.Rda", id)),
     compress = TRUE )
