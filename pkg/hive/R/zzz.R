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
  hive(.hinit())
}

