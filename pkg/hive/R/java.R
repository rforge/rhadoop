.DFS_java <- function(...) {
    core_default <- .jnew("org/apache/hadoop/fs/Path", file.path(hive:::hadoop_home(hive()), "src", "core", "core-default.xml"))
    core_site <- .jnew("org/apache/hadoop/fs/Path", file.path(hive:::hadoop_home(hive()), "conf", "core-site.xml"))

    configuration <- .jnew("org/apache/hadoop/conf/Configuration")
    .jcall(configuration, "V", "addResource", core_default)
    .jcall(configuration, "V", "addResource", core_site)

    .jcall(configuration, "I", "size")

    .jcall("org/apache/hadoop/fs/FileSystem", "Lorg/apache/hadoop/fs/FileSystem;", "get", configuration)
}
