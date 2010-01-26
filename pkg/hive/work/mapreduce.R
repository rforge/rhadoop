# Author: Ingo Feinerer, Stefan Theussl

# Fetch distributed files and return them as character vector
# provided that the results are in key, value pair format (should we check?)

hive_get_results <- function(path, henv = hive()){
  split_line <- function(line) {
    val <- unlist(strsplit(line, "\t"))
    list(key = val[1], value = as.integer(val[2]))
  }
  lines <- system(sprintf("%s fs -cat %s/part-*", hadoop(henv), path), intern = TRUE)
  splitted <- sapply(lines, split_line)
  keys <- unlist(splitted[1, ])
  values <- unlist(splitted[2, ])
  out <- values
  names(out) <- keys
  out
}

## MAPPERS

## all mappers available have to be listed here
.hadoop_mappers <- c("wordcount", "hs", "tm", "tm_test")

## Example from Luke Tierneys course
## Input: standard text file
.hadoop_generate_word_count_mapper <- function(script){
  writeLines('#!/usr/bin/env Rscript

## MapReduce standard output function: <key, value>
mapred_write_output <- function(key, value)
  cat(sprintf("%s\t%s\n", key, value), sep = "")

## Function definitions
trim_white_space <- function(line) gsub("(^ +)|( +$)", "", line)
split_into_words <- function(line) unlist(strsplit(line, "[[:space:]]+"))

## Do the work now
con <- file("stdin", open = "r")
while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    line <- trim_white_space(line)
    words <- split_into_words(line)
    if(length(words))
      mapred_write_output(words, 1)
}
close(con)

', script)
}

.hadoop_generate_hs_mapper <- function(script){
  writeLines('#!/usr/bin/env Rscript
library(HadoopStreaming)

## Additional command line arguments for this script (rest are default in hsCmdLineArgs)
spec = c("chunkSize","c",1,"numeric","Number of lines to read at once, a la scan.",-1)

opts = hsCmdLineArgs(spec, openConnections=T)

if (!opts$set) {
  quit(status=0)
}

mapperOutCols = c("word","cnt")
reducerOutCols = c("word","cnt")

if (opts$mapcols) {
  cat( paste(mapperOutCols,collapse=opts$outsep),"\n", file=opts$outcon )
}

if (opts$reducecols) {
  cat( paste(reducerOutCols,collapse=opts$outsep),"\n", file=opts$outcon )
}

if (opts$mapper) {
  mapper <- function(d) {
    words <- strsplit(paste(d,collapse=" "),"[[:punct:][:space:]]+")[[1]] # split on punctuation and spaces
    words <- words[!(words=="")]  # get rid of empty words caused by whitespace at beginning of lines
    df = data.frame(word=words)
    df[,"cnt"]=1
    hsWriteTable(df[,mapperOutCols],file=opts$outcon,sep=opts$outsep)
  }

  hsLineReader(opts$incon,chunkSize=opts$chunkSize,FUN=mapper)

} else if (opts$reducer) {

  reducer <- function(d) {
    cat(d[1,"word"],sum(d$cnt),"\n",sep=opts$outsep)
  }
  cols=list(word="",cnt=0)  # define the column names and types (""-->string 0-->numeric)
  hsTableReader(opts$incon,cols,chunkSize=opts$chunkSize,skip=opts$skip,sep=opts$insep,keyCol="word",singleKey=T, ignoreKey= F, FUN=reducer)
}


', script)
}

# TODO: Think about where to store generated map file (HDFS?)
.hadoop_generate_tm_mapper <- function(script, FUN, ...) {
  writeLines(sprintf('#!/usr/bin/env Rscript
require("tm")
fun <- %s

split_line <- function(line) {
    val <- unlist(strsplit(line, "\t"))
    ## four/three backslashes necessary as we have to write this code to disk. Otherwise backslash n would be interpreted as newline.
    list(key = val[1], value = unserialize(charToRaw(gsub("\\\\n", "\\\n", val[2], fixed = TRUE))))
}

mapred_write_output <- function(key, value)
    cat(paste(key, gsub("\\\n", "\\\\n", rawToChar(serialize(value, NULL, TRUE)), fixed = TRUE), sep = "\t"), sep = "\\\n")

## very important: hash table for this chunk (necessary to create mapping between part-x and original texts)
mapping <- new.env()
chunkname <- paste(paste(sample(c(letters, 0:9), 10, replace = TRUE), collapse = ""), system("hostname", intern = TRUE), sep = "-")
position <- 1L

con <- file("stdin", open = "r")
while (length(line <- readLines(con, n = 1L, warn = FALSE)) > 0) {
    input <- split_line(line)
    
    result <- fun(input$value)
    if(length(result)){
      mapping[[input$key]] <- c(chunk = chunkname, position = position)
      position <- position + 1L
      mapred_write_output(input$key, result)
    }
}
mapred_write_output(chunkname, mapping)
close(con)

', FUN), script)
}

## here without loading heavy weight tm package
.hadoop_generate_tm_test_mapper <- function(script, FUN, ...) {
  local_library <- "/home/theussl/lib/R" # FIXME: this needs to be changed to something like Sys.getenv("R_LIBS")
  #local_library <-"/home/feinerer/lib/R/library"
  writeLines(sprintf('#!/usr/bin/env Rscript

## MapReduce standard output function: <key, value>

.libPaths(new = "%s")
# require("tm")

fun <- %s
stopifnot(is.function(fun))
con <- file("stdin", open = "r")
while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    result <- fun(line)
    if(length(result))
      writeLines(result)
}
close(con)

', local_library, FUN), script)
}

.hadoop_get_mapper <- function(mapper){
  mapper <- match.arg(mapper, .hadoop_mappers)
  generator <- switch(mapper,
                      "wordcount" = .hadoop_generate_word_count_mapper,
                      "tm" = .hadoop_generate_tm_mapper,
                      "hs" = .hadoop_generate_hs_mapper,
                      "tm_test" = .hadoop_generate_tm_test_mapper,
                      stop(sprintf("There is no mapper named '%s'!", mapper)))
  generator
}

.hadoop_mapper_filename <- function(mapper){
  sprintf("_hadoop_%s_mapper_", mapper)
}


hadoop_generate_mapper <- function(mapper = .hadoop_mappers, ...){
  script <- file.path(".", .hadoop_mapper_filename(mapper))
  if(file.exists(script))
    file.remove(script)
  hadoop_mapper <- .hadoop_get_mapper(mapper)
  hadoop_mapper(script, ...)
  ## make mapper script executable, automatically checks if script is available
  status <- system(sprintf("chmod 775 %s", script), ignore.stderr = TRUE)
  if(status){
    warning("No mapper script to make executable found!")
    invisible(FALSE)
  }
  invisible(script)
}

## REDUCERS

## all mappers available have to be listed here
.hadoop_reducers <- c("wordcount")

## example from Luke Tierneys course
.hadoop_generate_word_count_reducer <- function(script){
  writeLines('#!/usr/bin/env Rscript

## MapReduce standard output function: <key, value>
mapred_write_output <- function(key, value)
  cat(sprintf("%s\t%s\n", key, value), sep = "")

## Function definitions
trim_white_space <- function(line) gsub("(^ +)|( +$)", "", line)
split_line <- function(line) {
    val <- unlist(strsplit(line, "\t"))
    list(word = val[1], count = as.integer(val[2]))
}

## Create new env to host word counts
env <- new.env(hash = TRUE)

## Do the work now
con <- file("stdin", open = "r")
while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    line <- trim_white_space(line)
    split <- split_line(line)
    word <- split$word
    count <- split$count
    if(nchar(word) > 0){
      if(exists(word, envir = env, inherits = FALSE)) {
        oldcount <- get(word, envir = env)
        assign(word, oldcount + count, envir = env)
      }
      else assign(word, count, envir = env)
    }
}
close(con)

for (w in ls(env, all = TRUE))
    cat(w, "\t", get(w, envir = env), "\n", sep = "")

', script)
}

.hadoop_get_reducer <- function(reducer){
  reducer <- match.arg(reducer, .hadoop_reducers)
  generator <- switch(reducer,
                      "wordcount" = .hadoop_generate_word_count_reducer,
                      stop(sprintf("There is no reducer named '%s'!", reducer)))
  generator
}

.hadoop_reducer_filename <- function(reducer){
  sprintf("_hadoop_%s_reducer_", reducer)
}

hadoop_generate_reducer <- function(reducer = .hadoop_reducers, ...){
  script <- file.path(".", .hadoop_reducer_filename(reducer))
  if(file.exists(script))
    file.remove(script)
  hadoop_reducer <- .hadoop_get_reducer(reducer)
  hadoop_reducer(script, ...)
  ## make reducer script executable, automatically checks if script is available
  status <- system(sprintf("chmod 775 %s", script), ignore.stderr = TRUE)
  if(status){
    warning("No reducer script to make executable found!")
    invisible(FALSE)
  }
  invisible(script)
}


