#Packages##########################################################################################################

Sys.setenv(HADOOP_HOME="/home/riesenh/lib/hadoop-0.20.0")
require(hive)

#Check############################################################################################################

hive_is_available()
DFS_list("/")

#Move Files########################################################################################################

DFS_put("/home/riesenh/data/OzBooks1","/user/riesenh/inputoz1")
DFS_put("/home/riesenh/data/OzBooks2","/user/riesenh/inputoz2")
DFS_put("/home/riesenh/data/OzBooks3","/user/riesenh/inputoz3")

system("${HADOOP_HOME}/bin/hadoop dfs -put /home/riesenh/data/wordcount.jar /user")

#Functions#########################################################################################################

mapper<-function(){

mapred_write_output <- function(key, value) cat(sprintf("%s\t%s\n", key, value), sep = "")
trim_white_space <- function(line) gsub("(^ +)|( +$)", "", line)
split_into_words <- function(line) unlist(strsplit(line, "[[:space:]]+"))

con <- file("stdin", open = "r")
while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    line <- trim_white_space(line)
    words <- split_into_words(line)
    if(length(words))
      mapred_write_output(words, 1)
}
close(con)

}

reducer<-function(){

split_line <- function(line) {	
    splitline <- unlist(strsplit(line, "\t"))
    list(key = splitline[1], value = as.numeric(splitline[2]))
}

mapred_output <- function(key, value) {
	cat(sprintf("%s\t%s\n", key, value), sep = "")
}

con <- file("stdin", open = "r")
lastkey<-"NULL";sumvalues=0

while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    split = split_line(line)
	 if (lastkey==split[[1]]){
		sumvalues=sumvalues+split[[2]]
	 } else {
		if (lastkey!="NULL") mapred_output(lastkey,sumvalues);sumvalues=split[[2]]	
	 }   
	 lastkey=split$key
}

if (lastkey!="NULL") mapred_output(lastkey,sumvalues)	
close(con)

}

mapper2 <- function (){

mapred_write_output <- function(key, value) cat(sprintf("%s\t%s\n", key, value), sep = "")
trim_white_space <- function(line) gsub("(^ +)|( +$)", "", line)
split_into_words <- function(line) unlist(strsplit(line, "[[:space:]]+"))

con <- file("stdin", open = "r")
while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    line <- trim_white_space(line)
    words <- split_into_words(line)
    if(length(words))
      mapred_write_output(words, 1)
}
close(con)

}

reducer2 <- function (){

mapred_write_output <- function(key, value) cat(sprintf("%s\t%s\n", key, value), sep = "")


trim_white_space <- function(line) gsub("(^ +)|( +$)", "", line)
split_line <- function(line) {
    val <- unlist(strsplit(line, "\t"))
    list(word = val[1], count = as.integer(val[2]))
}

env <- new.env(hash = TRUE)
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

for (w in ls(env, all = TRUE)) cat(w, "\t", get(w, envir = env), "\n", sep = "")

}


#Run Job#########################################################################################################

hive_stream(mapper,reducer,"/user/riesenh/inputoz3","/user/riesenh/outputwordcount1")

hive_stream(mapper2,reducer2,"/user/riesenh/inputoz3","/user/riesenh/outputwordcount2")

hive_stream(mapper3,reducer3,"/user/riesenh/inputoz3","/user/riesenh/outputwordcount33")

hive_stream(mapper,reducer,"/user/riesenh/inputoz3","/user/riesenh/outputwordcount332")
hive_stream(mapper3,reducer,"/user/riesenh/inputoz3","/user/riesenh/outputwordcount333")

system("${HADOOP_HOME}/bin/hadoop jar /home/riesenh/data/wordcount.jar org.myorg.WordCount /user/riesenh/inputoz3 /user/riesenh/outputwordcount3")

#Results#########################################################################################################

DFS_list("/user/riesenh/output")

DFS_cat("/user/riesenh/outputwordcount1/part-00000")
DFS_cat("/user/riesenh/outputwordcount2/part-00000")
DFS_cat("/user/riesenh/outputwordcount3/part-00000")

#Remove##########################################################################################################
DFS_dir_remove("/user/riesenh/input")
DFS_dir_remove("/user/riesenh/output")
hive_stop()

#Without Hadoop##################################################################################################

wordcount<-function(){

	parentnode="/home/riesenh/data/OzBooks1"
	env <- new.env(hash = TRUE)

	for (i in dir(parentnode)) {
		cat(paste(i,"\n",sep=""))

		pathfile=file.path(parentnode,i)
		inputlines=readLines(pathfile)
		words=unlist(strsplit(gsub("(^ +)|( +$)", "", inputlines), "[[:space:]]+"))
		wordtable=table(words)

		for (j in 1:length(wordtable)){
   			key=names(wordtable[j])
			value=wordtable[j]
			if (key=="") next()
			if(exists(key, envir = env, inherits = FALSE)) {
        			oldcount <- get(key, envir = env)
        			assign(key, oldcount + value, envir = env)
   			} else assign(key, value, envir = env)
		}
	}

	w= ls(env, all = TRUE)
	key=character(length(w))
	value=numeric(length(w))
	for (i in 1:length(w)){
		key[[i]]=w[[i]]
		value[[i]]=get(w[[i]],envir=env)
	}
	data.frame(key,value)

}

system.time(wordcount())
ans=wordcount()


















