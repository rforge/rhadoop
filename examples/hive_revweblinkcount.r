#Packages#########################################################################################################

Sys.setenv(HOME_HADOOP="/home/riesenh/lib/hadoop-0.20.0")
require(hive)

#Check#########################################################################################################

hive_is_available()
DFS_list("/")

#Move Files
DFS_put("/home/riesenh/data/HTMLBSP","/user/riesenh/html")

#Functions#####################################################################################################

mapper<-function(){

hyperlink<-function(inputline){
elements=character(0)
if (nchar(inputline)>0) {
	elements=unlist(strsplit(inputline,"<"))
	elements=unlist(strsplit(elements,">"))
	elements=unlist(strsplit(elements," "))
  	links=elements[grep("^href=",elements)]
  	links=gsub('^href=','',links)
  	links=gsub('"','',links)
	elements=links
}
elements
}

mapred_output <- function(key, value) cat(sprintf("%s\t%s\n", key, value), sep = "")

	value=1

	con <- file("stdin", open = "r")
		while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
   			key=hyperlink(line)
   			mapred_output(key,value)	 	
		}
	close(con)
}


mapper2<-function(){

hyperlink<-function(inputline){
elements=character(0)
if (nchar(inputline)>0) {
	elements=unlist(strsplit(inputline,"<"))
	elements=unlist(strsplit(elements,">"))
	elements=unlist(strsplit(elements," "))
  	links=elements[grep("^href=",elements)]
  	links=gsub('^href=','',links)
  	links=gsub('"','',links)
	elements=links
}
elements
}
mapred_output <- function(key, value) cat(sprintf("%s\t%s\n", key, value), sep = "")


	lines=c()

	con <- file("stdin", open = "r")
	while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    		lines <- c(lines,line)
	}
	close(con)

	value=1
	
	idx=grep('href=',lines)	
	keys=c()

	for (i in idx) keys=c(keys,hyperlink(lines[i]))


       mapred_output(keys,value)	
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

#Run Job#########################################################################################################

hive_stream(mapper,reducer,"/user/riesenh/html","/user/riesenh/htmlrevcountout1")

hive_stream(mapper2,reducer,"/user/riesenh/html","/user/riesenh/htmlrevcountout2")

#Results#########################################################################################################

DFS_list("/user/riesenh/htmlout")
results=DFS_read_lines("/user/riesenh/htmlout/part-00000")

DFS_cat("/user/riesenh/htmlrevcountout1/part-00000")
DFS_cat("/user/riesenh/htmlrevcountout2/part-00000")

#Remove##########################################################################################################
DFS_dir_remove("/user/riesenh/html")
DFS_dir_remove("/user/riesenh/htmlout")
hive_stop()

