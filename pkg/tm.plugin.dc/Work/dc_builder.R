require("tm.plugin.dc")

###############################################################################
## Helper functions
###############################################################################

.make_new_source <- function(x, filelist){
  stopifnot( inherits(x, "DirSource") )
  tm:::.Source( defaultreader = x$DefaultReader,
                encoding      = x$Encoding,
                length        = length(filelist),
                lodsupport    = x$LoDSupport,
                position      = x$Position,
                vectorized    = x$Vectorized )
}

split_source_by_files <- function(x, n){
  delta <- ceiling(x$Length/n)
  cutoffs <- seq_len(n-1) * delta
  cutoffs <- cbind(c(1, cutoffs+1), c(cutoffs, x$Length))
  apply(cutoffs, 1, function(x, src){
    fl <- src$FileList[ x[1]:x[2] ]
    new <- .make_new_source(src, fl)
    new$FileList <- fl
    class(new) = c("DirSource", class(new))
    new}, x )  
}

###############################################################################
## Reader
###############################################################################

## New York Times Annotated Corpus - Reader
readNYTimes <- readXML(spec = list(Content = list("node",
                                     "/nitf/body/body.content/block[@class='full_text']/p"),
                         Heading = list("node", "/nitf/head/title"),
                         ID = list("attribute", "/nitf/head/docdata/doc-id/@id-string")),
                       doc = PlainTextDocument())

###############################################################################
## Configuration and initial setup
###############################################################################

make_dir_source_from_gz <- function(src_dir, gzfile, base_dir, prefix){
  ## needs to correspond with build_distributed_corpus_subset_from_xml
  dest_dir <- file.path( base_dir, "dataset" )
  xml_dir  <- file.path( dest_dir, sprintf("%s_xml", prefix) )
  
  if( !file.exists(dest_dir) )
    dir.create( dest_dir )
  
  ## copy over tarball and extract
  if( !file.exists(xml_dir) ) {
    file.copy( file.path(src_dir, gzfile), file.path(dest_dir, gzfile) )
    wd <- getwd()
    setwd(dest_dir)
    system(sprintf("tar xzf %s", gzfile))
    setwd(wd)
  } 
  DirSource(xml_dir)
}

###############################################################################
## Distributed preparation of corpora
###############################################################################

build_distributed_corpus_subset_from_xml <-
function(src_dir,
         gzfile,
         base_dir,
         prefix, 
         file_list,   ## DirSource
         n,           ## number of parallel tasks
         id,          ## current task number
         reader) {
  
  ## where should the xml go to
  dest_dir <- file.path( base_dir, "dataset" )
  xml_dir  <- file.path( dest_dir, sprintf("%s_xml", prefix) )
  
  if( !file.exists(dest_dir) )
    dir.create( dest_dir )
  ## copy over tarball and extract
  if( !file.exists(xml_dir) ) {
    file.copy( file.path(src_dir, gzfile), file.path(dest_dir, gzfile) )
    wd <- getwd()
    setwd(dest_dir)
    system(sprintf("tar xzf %s", gzfile))
    setwd(wd)
  } 

  ## load the file list. contains paths to all documents in the corpus
  
  ###########################################################################
  ## Build distributed corpus based on filelist and task id
  ###########################################################################

  ## now split the dir sources according to the number of CPUs used
  n <- as.integer( n )
  id <- as.integer( id )
  splitted <- split_source_by_files( file_list, n )
  src <- splitted[[id]]

  ## storage setup
  stor_dir <- file.path( base_dir, sprintf("storage_%d", id) )
  dir.create(stor_dir)
  storage <- dcStorage()
  storage$chunksize <- 50 * 1024^2
  storage$base_directory <- stor_dir
  dcStorage(storage)
  
  ## now prepare dc
  dc <- DistributedCorpus( src,
                          readerControl = list(reader = reader) )

  outgz <- file.path( src_dir, sprintf("%s_dc_part_%d.tar.gz", prefix, id) )
  system( sprintf("tar czf %s %s", outgz, file.path(stor_dir, attr(dc, "ActiveRevision"))) )
  unlink( file.path(stor_dir, attr(dc, "ActiveRevision")), recursive = TRUE )
  ## return relevant objects
  list(ID      = id,
       Mapping = attr(dc, "Mapping"),
       Chunks  = attr(dc, "Chunks"),
       subset  = outgz)
}

###############################################################################
## Combine corpus subsets
###############################################################################

aggregate_distributed_corpus_subsets <- function(dc_subsets, src_dir, dc_meta, stor_dir){
  stopifnot( length(dc_subsets) == length(dc_meta) )

  ## storage setup
  ## TODO: chunksize should be taken from dc_meta
  dir.create(stor_dir)
  storage <- dcStorage()
  storage$chunksize <- 50 * 1024^2
  storage$base_directory <- stor_dir

  new_rev <- tm.plugin.dc:::.generate_random_revision()
  tm.plugin.dc:::dc_dir_create(storage, new_rev)
  new_base_dir <- file.path(storage$base_directory, new_rev)
  
  ## merge corpus subsets
  for(i in seq_along(dc_subsets) ){
    system( sprintf("tar xzf %s --strip 2 -C %s", file.path(src_dir, dc_subsets[i]), stor_dir) )
    src_dir_subset <- sprintf( "storage_%d", i )
    meta <- dc_meta[[ i ]]
    revision <- names( meta$Chunks )
    chunks <- meta$Chunks[[ revision ]]
    rev_dir <- file.path( stor_dir, src_dir_subset, revision )
    chunks_new <- paste(chunks, i, sep = "_")
    for( j in seq_along(chunks) )
      file.copy( file.path(rev_dir, chunks[j]), file.path(new_base_dir, chunks_new[j]) )
    unlink(file.path(stor_dir, src_dir_subset), recursive = TRUE )
  }
  ## Calculate new mapping for merged distributed corpus
  nrows <- unlist(lapply(dc_meta, function(x) nrow(x$Mapping[[1]])))
  nrows_sum <- c( 0L, cumsum(nrows) )
  offset_per_subset <- c( 0L, cumsum( unlist(lapply(dc_meta, function(x) max(x$Mapping[[1L]][, 1L]))))[ -length(nrows) ] )
  chunks_offset <- integer(sum(nrows))
  for( subset in seq_along(nrows) )
    chunks_offset[ (nrows_sum[subset]+1):nrows_sum[subset+1] ] <- rep(offset_per_subset[ subset ], nrows[ subset ])
  Mapping <- Reduce(function(x, y) rbind(x, y$Mapping[[1]]), dc_meta, init = integer())
  Mapping[, 1] <- Mapping[, 1] + chunks_offset
  chunks <- tm.plugin.dc:::dc_list_directory(storage, new_rev)
  ind <- integer()
  subset <- as.integer(unlist(lapply(strsplit(tm.plugin.dc:::dc_list_directory(storage, new_rev), "_"), function(x) x[[2]])))
  for(i in  seq_along(dc_subsets))
    ind <- c(ind, which(subset == i) )
  tm.plugin.dc:::.DistributedCorpus(x = list(),
                                    active_revision = new_rev,
                                    chunks = structure( list(chunks[ind]), names = new_rev ),
                                    cmeta = tm:::.MetaDataNode(),
                                    dmeta = data.frame( MetaID = rep(0L, sum(nrows)),
                                           stringsAsFactors = FALSE ),
                                    keys = 1L:sum(nrows),
                                    mapping = structure(list(Mapping), names = new_rev),
                                    revisions = list(new_rev),
                                    storage = storage)
}
