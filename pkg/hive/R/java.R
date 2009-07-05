.DFS_java <- function(...) {


    hdfs <- .jcall("org/apache/hadoop/fs/FileSystem", "Lorg/apache/hadoop/fs/FileSystem;", "get", configuration)
    path <- .jnew("org/apache/hadoop/fs/Path", "/tmp/testfile")
    dos <- hdfs$create(path)
    dos$writeUTF("Hello World")
    dos$close()

    dis <- hdfs$open(path)
    dis$readUTF()
    dis$close()

    # E.g., instead of DFS_file_exists
    hdfs$exists(path)
}
