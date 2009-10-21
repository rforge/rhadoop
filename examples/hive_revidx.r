#Packages##########################################################################################################

Sys.setenv(HADOOP_HOME="/home/riesenh/lib/hadoop-0.20.0")
require(hive)

#Check############################################################################################################

hive_is_available()
DFS_list("/")

#Move Files########################################################################################################

DFS_put("/home/riesenh/data/OzBooks","/user/riesenh/input")


#Functions#########################################################################################################
mapper<-function(){

trim_white_space <- function(line) gsub("(^ +)|( +$)", "", line)
split_into_words <- function(line) unlist(strsplit(line, "[[:space:]]+"))
remove_punctuation_marks <- function(line) gsub('[:punct:]', "", line)
mapred_output <- function(key, value) cat(sprintf("%s\t%s\n", key, value), sep = "")

	lines=c()

	con <- file("stdin", open = "r")
	while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    		lines <- c(lines,line)
	}
	close(con)

	filename=trim_white_space(lines[1])

	for (i in 1:length(lines)){

		keys=tolower(remove_punctuation_marks(split_into_words(trim_white_space(lines[i]))))
       	values=paste(filename,i,1:length(keys),sep=",")
	
		mapred_output(keys,values)	

	}
}


reducer<-function(){

split_line <- function(line) {	
    splitline <- unlist(strsplit(line, "\t"))
    list(key = splitline[1], value = splitline[2])
}

mapred_output <- function(key, value) {
	cat(sprintf("%s\t%s\n", key, value), sep = "")
}

con <- file("stdin", open = "r")
lastkey<-"NULL";listvalues=c()

while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    split = split_line(line)
	 if (lastkey==split[[1]]){
		if (length(listvalues)==0) {
			paste(listvalues,split[[2]],sep="")
		} else {
			listvalues=paste(listvalues,split[[2]],sep=";")
		}
	 } else {
		if (lastkey!="NULL") mapred_output(lastkey,listvalues);listvalues=split[[2]]	
	 }   
	 lastkey=split[[1]]
}

if (lastkey!="NULL") mapred_output(lastkey,listvalues)	
close(con)

}


#Run Job#########################################################################################################

hive_stream(mapper,reducer,"/user/riesenh/input","/user/riesenh/outputrevidx")

#Results#########################################################################################################

DFS_list("/user/riesenh/outputrevidx")

DFS_cat("/user/riesenh/outputrevidx/part-00000")


#Remove##########################################################################################################
DFS_dir_remove("/user/riesenh/html")
DFS_dir_remove("/user/riesenh/outputrevidx")
hive_stop()


#Without Hadoop##################################################################################################

trim_white_space <- function(line) gsub("(^ +)|( +$)", "", line)
split_into_words <- function(line) unlist(strsplit(line, "[[:space:]]+"))
remove_punctuation_marks <- function(line) gsub('\\?|!|\\.|,|"|;', "", line)
#remove_punctuation_marks <- function(line) gsub("^[:punct:]", "", line)
mapred_output <- function(key, value) cat(sprintf("%s\t%s\n", key, value), sep = "")

revidx<-function(){

	parentnode="/home/riesenh/data/OzBooks"
	env <- new.env(hash = TRUE)

	for (i in dir(parentnode)[1]) {
		print(i)
		pathfile=file.path(parentnode,i)
		inputlines=readLines(pathfile)

		for (j in 1:length(inputlines)){
			
			keys=tolower(remove_punctuation_marks(split_into_words(trim_white_space(inputlines[j]))))
       		values=paste(i,j,1:length(keys),sep=",")
			if (length(keys)!=0){ 
   				for (k in 1:length(values)){
					key=keys[k];value=values[k]
					if (length(key)==0) next()
					if (key=="") next()
					if(exists(key, envir = env, inherits = FALSE)) {
        					oldvalue <- get(key, envir = env)
        					assign(key, paste(oldvalue,value,sep=";"), envir = env)
   					} else assign(key, value, envir = env)
				}
			}
		}
	}

	w= ls(env, all = TRUE)
	key=character(length(w))
	value=numeric(length(w))
	for (i in 1:length(w)){
		key[[i]]=w[[i]]
		value[[i]]=get(w[[i]],envir=env)
	}
	list(key=key,value=paste(value,"\n",sep="",collapse=""))

}

ans=revidx()

system.time(revidx())


