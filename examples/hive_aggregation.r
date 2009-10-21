#Packages#########################################################################################################

Sys.setenv(HOME_HADOOP="/home/riesenh/lib/hadoop-0.20.0")
require(hive)

#Check#########################################################################################################

hive_is_available(henv)
DFS_list("/")

#Move Files
DFS_put("/home/riesenh/data/Control","/user/riesenh/control")
DFS_put("/home/riesenh/data/RD50","/user/riesenh/rd")

#Functions#####################################################################################################

mapper<-function(){

col_names=c("Interest","Currency","Price","Volatility","Commodity","Spread","Spread2","Total")

mapred_write_output <- function(key, value)
  cat(sprintf("%s\t%s\n", key, value), sep = "")

transform_report<-function(reportlines,loaddate=Sys.time()-86400,first="1900-01-01",last="1900-01-01"){
    
    #'pre-allocate'
    error=FALSE;loaddate=as.Date(loaddate)
    errors<-matrix(0,0,3)
    colnames(errors)=c("Error","Job","Date")
	
    reportname=strsplit(reportlines[1],";")[[1]][2]
    reportname=sub("\\\"","",reportname)
    reportname=sub("\\\"","",reportname)

    #delete header
    if (!error){
      rows=length(reportlines)
      if (rows<10) {
        errors=rbind(errors,c("Not enough rows.",reportname,format(loaddate,"%Y-%m-%d")))
        error=TRUE
      }
    }

    if (!error){
      reportlines=reportlines[-c(1:8)]
      rows=rows-8
    }

    if (!error){
      repfirst=as.Date(unlist(strsplit(reportlines[2],";"))[1])
      replast=as.Date(unlist(strsplit(reportlines[length(reportlines)],";"))[1])
      if (first=="1900-01-01") first=repfirst
      if (last=="1900-01-01") last=replast
       first=as.Date(first);last=as.Date(last)

      if (!(repfirst<=first&replast>=last)) {
        errors=rbind(errors,c("Invalid Date Range.",reportname,format(loaddate,"%Y-%m-%d")))
        error=TRUE
      }
    }

    first=as.Date(first);last=as.Date(last)

    #'pre-allocate' output
    col_names=c("Interest","Currency","Price","Volatility","Commodity","Spread","Spread2","Total")
    row_names=format(as.Date(0:unclass(last-first)[1],origin=first),"%Y-%m-%d")
    cols=length(col_names)
    output=matrix(NA,length(row_names),cols)

    #reorder column names
    if (!error){
      col_names_text=strsplit(reportlines[1],";")[[1]]
      col_names_text=sub("\\\"","",col_names_text)
      col_names_text=sub("\\\"","",col_names_text)
      col_names_text=col_names_text[2:length(col_names_text)]
      col_index=rep(NA,cols)
      for (i in 1:cols){
        column=(1:length(col_names_text))[col_names_text==col_names[i]]
        if (length(column)!=0) col_index[i]=column
      }
      if (sum(is.na(col_index))==cols) {
        errors=rbind(errors,c("Missing header.",reportname,format(loaddate,"%Y-%m-%d")))
        error=TRUE
      }
    }
    
    #write output matrix
    if (!error){
        for (i in 2:rows){
          values=strsplit(reportlines[i],";")[[1]]
          datval=values[1]
          values=values[col_index+1]
          output[datval==row_names,]=values
        }
    }

    options(warn=-1)
    output=matrix(as.numeric(output),length(row_names),cols)
    options(warn=1)
    colnames(output)=col_names
    rownames(output)=row_names

    #set empty columns
    output[,apply(is.na(output),2,sum)==dim(output)[1]]=0

    #transform output matrix to a list
    output=data.frame(output)
    output=list(output,errors)
    names(output)=c(paste(reportname,"_",loaddate,sep=""),"Errors")
    output
}

read_map<-function(hadoop,path_file){ 
  lines=system(sprintf("%s/bin/hadoop fs -cat %s",hadoop, path_file),intern=TRUE)
  output=matrix(unlist(strsplit(sub("\r","",lines),";")),,2,byrow=TRUE)
  output
}
output_pl<-function(reportlist,aggregationvector) { 
  jobname=names(reportlist)
  risktypes= names(reportlist[[1]])

    values=paste(cbind(rownames(reportlist[[1]]),as.matrix(reportlist[[1]])),collapse=";")    
	for (j in aggregationvector){
    		key=sprintf("%s",j)
    		mapred_write_output(key,values)
  	}
  ##cat(sprintf("%s\t%s\n","Loaded",jobname),sep="")
}


## Do the work now
con <- file("stdin", open = "r")
reportlines=c()
	while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
    		reportlines <- c(reportlines,line)
	}
close(con)

reportlist = transform_report(reportlines,"2009-03-10","2007-03-11","2009-03-10")
reportmap = read_map("${HADOOP_HOME}","/user/riesenh/control/HIERARCHY_250.txt")

reportname = names(reportlist)[1]
reportname = substr(reportname,1,nchar(reportname)-11)

aggregationvector = reportmap [reportmap [,2]==reportname,1]
keys=c(aggregationvector)

if (dim(reportlist$Errors)[1]==0){
	output_pl(reportlist,keys)
}

}



##########################################################################################################


reducer<-function(){

## Function definitions
col_names=c("Interest","Currency","Price","Volatility","Commodity","Spread","Spread2","Total")
quant=0.01

mapred_write_output <- function(key, value)
  cat(sprintf("%s\t%s\n", key, value), sep = "")


split_line <- function(line) {
    #line <-sub("\n","",line)	
    val <- unlist(strsplit(line, "\t"))
    list(key = val[1], values = val[2])
}

mapred_read_input <- function(line){
	split <- split_line(line)
	unstrseries=matrix(strsplit(split$values,";")[[1]],,length(col_names)+1)
	options(warn=-1)
	unstrseriesmatrix=matrix(as.numeric(unstrseries[,-1]),,length(col_names))
	options(warn=1)
	rownames(unstrseriesmatrix)=unstrseries[,1]
	colnames(unstrseriesmatrix)=col_names
	ans=list(unstrseriesmatrix)
	names(ans)=split$key
	ans
}

## Do the work now
con <- file("stdin", open = "r")
lastkey <- "NULL"
while (length(line <- readLines(con, n = 1, warn = FALSE)) > 0) {
	
	reportlist=mapred_read_input(line)
	key=names(reportlist)
	values=reportlist[[1]]	
	
	if (lastkey==key){
		sumvalues=sumvalues+values
	} else {
		if (lastkey!="NULL") {		
			mapred_write_output(paste(lastkey,col_names,sep=":"),apply(sumvalues,2,quantile,probs=quant,na.rm=TRUE))	
		}
		sumvalues=values
	}
	lastkey=key
}

if (lastkey!="NULL") {		
	mapred_write_output(paste(lastkey,col_names,sep=":"),apply(sumvalues,2,quantile,probs=quant,na.rm=TRUE))	
}	

close(con)

}

hive_stream(mapper,reducer,"/user/riesenh/rd","/user/riesenh/var")

#Results#########################################################################################################

DFS_list("/user/riesenh/var")
results=DFS_read_lines("/user/riesenh/var/part-00000")

#Remove##########################################################################################################
DFS_dir_remove("/user/riesenh/rd")
DFS_dir_remove("/user/riesenh/var")
hive_stop()






