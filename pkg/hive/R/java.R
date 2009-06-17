
.DFS_java <- function(x){
    configuration <- .jnew("org/apache/hadoop/conf/Configuration")
    configuration
    .jcall("org/apache/hadoop/fs/FileSystem", "Lorg/apache/hadoop/fs/FileSystem;", "get", configuration)
    #.jcall(configuration, "S", "get", "hadoop.tmp.dir")
}
