# Author: Ingo Feinerer, Stefan Theussl

## MAPPERS

## all mappers available have to be listed here
.hadoop_mappers <- c("wordcount", "tm", "tm_test")

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

# TODO: Think about where to store generated map file (HFS?)
## Does not work on my laptop, most likely due to open mpi!!!
.hadoop_generate_tm_mapper <- function(script, FUN, ...) {
  local_library <- "/usr/local/tmp/lib/R" # FIXME: this needs to be changed to something like Sys.getenv("R_LIBS")
  #local_library <-"/home/feinerer/lib/R/library"
  writeLines(sprintf('#!/usr/bin/env Rscript
.libPaths(new = "%s")
require("tm")
fun <- %s
input <- readLines(file("stdin"))
doc <- new("PlainTextDocument", .Data = input[1:(length(input) - 1)], DateTimeStamp = Sys.time())
result <- fun(doc)
writeLines(Content(result))
writeLines(input[length(input)])
', local_library, FUN), script)
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

## High-level wrapper for hadoop-streaming (map-only)
## TODO: ncpu argument -> probably via -D mapred.jobtracker.maxtasks.per.job=..
hadoop_map <- function(mapper, input, output, ...) {
  mapper_script <- hadoop_generate_mapper(mapper, ...)
  msg <- system(sprintf('%s jar %s/contrib/streaming/hadoop-*-streaming.jar -D mapred.reduce.tasks=0 -input %s -mapper %s -output %s -file %s',
                        hadoop, hadoop_home, input, mapper_script,
                        output, mapper_script), intern = TRUE)
  if(file.exists(mapper_script))
    file.remove(mapper_script)
  invisible(msg)
}

## High-level wrapper for hadoop-streaming (MapReduce)

hadoop_map_reduce <- function(mapper, reducer, input, output, ...) {
  mapper_script <- hadoop_generate_mapper(mapper)
  reducer_script <- hadoop_generate_reducer(reducer)
  msg <- system(sprintf('%s jar %s/contrib/streaming/hadoop-*-streaming.jar -D mapred.reduce.tasks=1 -input %s -mapper %s -reducer %s -output %s -file %s -file %s',
                        hadoop, hadoop_home, input, mapper_script,
                        reducer_script, output, mapper_script, reducer_script),
                intern = TRUE)
  if(file.exists(mapper_script))
    file.remove(mapper_script)
  invisible(msg)
}


