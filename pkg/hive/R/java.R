add_java_DFS_support <- function(henv){
  ## add paths to Hadoop configuration files
  core_default <- .jnew("org/apache/hadoop/fs/Path", file.path(hadoop_home(henv), "src", "core", "core-default.xml"))
  core_site <- .jnew("org/apache/hadoop/fs/Path", file.path(hadoop_home(henv), "conf", "core-site.xml"))
  hdfs_default <- .jnew("org/apache/hadoop/fs/Path", file.path(hadoop_home(henv), "src", "hdfs", "hdfs-default.xml"))
  hdfs_site <- .jnew("org/apache/hadoop/fs/Path", file.path(hadoop_home(henv), "conf", "hdfs-site.xml"))
  mapred_default <- .jnew("org/apache/hadoop/fs/Path", file.path(hadoop_home(henv), "src", "mapred", "mapred-default.xml"))
  mapred_site <- .jnew("org/apache/hadoop/fs/Path", file.path(hadoop_home(henv), "conf", "mapred-site.xml"))
  
  configuration <- .jnew("org/apache/hadoop/conf/Configuration")
  .jcall(configuration, "V", "addResource", core_default)
  .jcall(configuration, "V", "addResource", core_site)
  .jcall(configuration, "V", "addResource", hdfs_default)
  .jcall(configuration, "V", "addResource", hdfs_site)
  .jcall(configuration, "V", "addResource", mapred_default)
  .jcall(configuration, "V", "addResource", mapred_site)

  ## Update class loader
  jcl <- .jclassLoader()
  .jcall(configuration, "V", "setClassLoader", .jcast(jcl, "java/lang/ClassLoader"))

  ## create hdfs handler
  hdfs <- .jcall("org/apache/hadoop/fs/FileSystem", "Lorg/apache/hadoop/fs/FileSystem;", "get", configuration)
  ## ioutils class
  ioutils <- .jnew("org/apache/hadoop/io/IOUtils")
  ## store everything in hadoop environment
  assign("configuration", configuration, henv)
  assign("hdfs", hdfs, henv)
  assign("ioutils", ioutils, henv)
  
  invisible(TRUE)
}

.DFS_java <- function(...) {

    srcPath <- .jnew("org/apache/hadoop/fs/Path", path)
    ##srcFs <- .jcall("srcPath", "org/apache/hadoop/fs/FileSystem", "getFileSystem", .jcast(hdfs, "org/apache/hadoop/conf/Configuration"))
    ##FileSystem srcFs = srcPath.getFileSystem(this.getConf());

    srcs <- hdfs$globStatus(srcPath)
    
    dos <- hdfs$create(path)
    dos$writeUTF("Hello World")
    dos$close()

    dis <- hdfs$open(path)
    dis$readUTF()
    dis$close()

    # E.g., instead of DFS_file_exists
    # java api
    hdfs$exists(path)
}

############################################################
## henv Extractors

## Returns the Hadoop DFS configuration object (Java) or NULL
HDFS <- function(henv = hive()){
  hdfs <- tryCatch(get("hdfs", henv), error = identity)
  if(inherits(hdfs, "error"))
    hdfs <- NULL
  hdfs
}

## Returns path as Hadoop DFS path object (Java)
HDFS_path <- function(x)
  .jnew("org/apache/hadoop/fs/Path", x)

IOUTILS <- function(henv = hive()){
  ioutils <- tryCatch(get("ioutils", henv), error = identity)
  if(inherits(ioutils, "error"))
    ioutils <- NULL
  ioutils
}
