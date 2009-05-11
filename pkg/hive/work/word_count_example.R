## Where is the Hadoop home directory
Sys.setenv(HADOOP_HOME="/home/theussl/lib/hadoop-0.19.1")

## Load hive package; initial hive gets automatically created (needs HADOOP_HOME from above)
library(hive)

## Start Hadoop and in case of failure stop
stopifnot(hive_start())

### BEGIN CONFIGURATION ###

# Test data set (plain text files)
input <- "~/Data/OzBooks/"
stopifnot(file.exists(input))

DFS_put(input, "data")

### END CONFIGURATION ###

### EXAMPLE 1: wordcount ###

## generate MapReduce scripts
map_script <- hive:::hadoop_generate_mapper("wordcount")
reduce_script <- hive:::hadoop_generate_reducer("wordcount")

hive_stream(mapper = map_script, reducer = reduce_script, input = "./data/OzBooks", output = "ozout")

## retrieve results
results <- hive:::hive_get_results("./ozout")
head(results)
tail(results)
length(results)
## top occuring words
head(sort(results, decreasing = TRUE))

## delete output repository
DFS_dir_remove("./ozout")

### END EXAMPLE 1: wordcount ###

### EXAMPLE 2: wordcount using package HadoopStreaming ###
map_script <- hive:::hadoop_generate_mapper("hs")
hive_stream(mapper = map_script, reducer = map_script, input = "./data/OzBooks",
            output = "ozout", mapper_args = "--mapper", reducer_args = "--mapper")



### END EXAMPLE 2: wordcount using package HadoopStreaming ###

### CLEANUP ###

## delete output repository
DFS_dir_remove("./ozout")

## delete MapReduce scripts
file.remove(map_script)
file.remove(reduce_script)

hive_stop()

### END CLEANUP ###
