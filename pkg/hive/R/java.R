.DFS_java <- function(...) {
    core_default <- .jnew("org/apache/hadoop/fs/Path", file.path(hive:::hadoop_home(hive()), "src", "core", "core-default.xml"))
    core_site <- .jnew("org/apache/hadoop/fs/Path", file.path(hive:::hadoop_home(hive()), "conf", "core-site.xml"))
    hdfs_default <- .jnew("org/apache/hadoop/fs/Path", file.path(hive:::hadoop_home(hive()), "src", "hdfs", "hdfs-default.xml"))
    hdfs_site <- .jnew("org/apache/hadoop/fs/Path", file.path(hive:::hadoop_home(hive()), "conf", "hdfs-site.xml"))
    mapred_default <- .jnew("org/apache/hadoop/fs/Path", file.path(hive:::hadoop_home(hive()), "src", "mapred", "mapred-default.xml"))
    mapred_site <- .jnew("org/apache/hadoop/fs/Path", file.path(hive:::hadoop_home(hive()), "conf", "mapred-site.xml"))

    configuration <- .jnew("org/apache/hadoop/conf/Configuration")
    .jcall(configuration, "V", "addResource", core_default)
    .jcall(configuration, "V", "addResource", core_site)
    .jcall(configuration, "V", "addResource", hdfs_default)
    .jcall(configuration, "V", "addResource", hdfs_site)
    .jcall(configuration, "V", "addResource", mapred_default)
    .jcall(configuration, "V", "addResource", mapred_site)

    .jcall(configuration, "I", "size")

    configuration$getClassLoader()
    cl <- .jclassLoader()
    configuration$setClassLoader(cl)
    configuration$getClassLoader()

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
