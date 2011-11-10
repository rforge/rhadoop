## This is the same example as shown in the 'hive_stream' man page but without the \dontrun clauses

require("hive")

DFS_put(system.file("defaults/core/", package = "hive"), "/tmp/input")
DFS_put(system.file("defaults/hdfs/hdfs-default.xml", package = "hive"), "/tmp/input")
DFS_put(system.file("defaults/mapred/mapred-default.xml", package = "hive"), "/tmp/input")
## Define the mapper and reducer function to be applied:
## Note that a Hadoop map or reduce job retrieves data line by line from stdin.

mapper <- function(x){
    con <- file( "stdin", open = "r" )
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
        terms <- unlist(strsplit(line, " "))
        terms <- terms[nchar(terms) > 1 ]
        if( length(terms) )
            cat( paste(terms, 1, sep = "\t"), sep = "\n")
    }
}
reducer <- function(x){
    env <- new.env( hash = TRUE )
    con <- file( "stdin", open = "r" )
    while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
        keyvalue <- unlist( strsplit(line, "\t") )
        if( exists(keyvalue[1], envir = env, inherits = FALSE) ){
            assign( keyvalue[1], get(keyvalue[1], envir = env) +
as.integer(keyvalue[2]), envir = env )
        } else
            assign( keyvalue[1], as.integer(keyvalue[2]), envir = env )
    }
    env <- as.list(env)
    for( term in names(env) )
        cat( sprintf("%s\t%s", term, env[[term]]), sep ="\n" )
}
hive_set_nreducer(1)
hive_stream( mapper = mapper, reducer = reducer, input = "/tmp/input", output = "/tmp/output" )
DFS_list("/tmp/output")
head( DFS_read_lines("/tmp/output/part-00000") )
## Don't forget to clean file system
DFS_dir_remove("/tmp/input")
DFS_dir_remove("/tmp/output")

