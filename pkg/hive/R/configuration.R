## Getters
hadoop_get_parameter <- function(x, henv = hive()){
  ## first search in hadoop-site configuration (overrules defaults)
  site <- .hadoop_configuration("hadoop_site", henv)[x]
  if(is.na(site))
    ## if not found then return value from default configuration
    return(.hadoop_configuration("hadoop_default", henv)[x])
  site
}

hadoop_get_slaves <- function(henv = hive()){
 .hadoop_configuration("slaves", henv)
}

hadoop_get_masters <- function(henv = hive()){
  .hadoop_configuration("masters", henv)
}

.hadoop_configuration <- function(x, henv){
  get("configuration", con)[[x]]
}

## Setters

## FIXME: not updated yet
hadoop_set_slaves <- function(slaves, henv){
  hadoop_stop()
  slave_conf <- file.path(hadoop_home(henv), "conf", "slaves")
  writeLines(slaves, henv = slave_conf)
  hadoop_start()
}

## Hadoop config XML parser

## FIXME: con argument
get_hadoop_config <- function(x, dir){
  infile <- xmlRoot(xmlTreeParse(file.path(dir, "conf", sprintf("hadoop-%s.xml", x))))
  out <- hadoop_parse_xml(infile, "value")
  names(out) <- hadoop_parse_xml(infile, "name")
  out
}

## returns the right function for coercion
## FIXME: 'value' can be of different type (integer, character, logical, NA)
hadoop_xml_return_type <- function(x){
  switch(x,
         "name"  = return(as.character),
         "value" = return(as.character),
         "description" = return(as.character)
         )
  stop("'x' can only be 'name', 'value' or 'description'")
}

## parse values from xml tree
hadoop_parse_xml <- function(x, what){
  as_type <- hadoop_xml_return_type(what)
  as_type(unlist(xmlApply(x, function(x){
    if(xmlName(x) == "property"){xmlApply(x, function(x){if(xmlName(x) == what) {out <- xmlValue(x)
                                                                                 if(!length(out))
                                                                                 out <- ""
                                                                                 out}})}
  })))
}

