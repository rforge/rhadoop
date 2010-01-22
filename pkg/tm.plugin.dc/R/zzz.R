storage <- local({
               .storage <- .dc_empty_storage()
               function(new){
                 if(missing(new))
                   .storage
                 else
                   .storage <<- new
               }}
                )

.onLoad <- function(libname, pkgname){
    ## initialize storage
    storage(.dc_storage_init())
}

