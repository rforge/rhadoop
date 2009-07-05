## hadoop root directory has to be set

hive <- local({
               .henv <- .hive_default_env()
               function(new){
                 if(missing(new))
                   .henv
                 else
                   .henv <<- new
               }}
              )

.onLoad <- function(libname, pkgname){
    ## initialize hive environment
    hive(.hinit())
    if( is.environment(hive()))
    {
        config_dirs <- c(file.path(hadoop_home(hive()), sprintf("hadoop-%s-core.jar", hadoop_version(hive()))),
                         file.path(hadoop_home(hive()), "lib", "commons-logging-1.0.4.jar"))
        ## add Java support
        if(.jpackage(pkgname, morePaths = config_dirs, lib.loc = libname)){
          add_java_DFS_support(henv = hive())
          
        }
    }
}

