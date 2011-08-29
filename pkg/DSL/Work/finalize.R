

foo <- function( x ){
    out <-  list(x)
    e <- new.env()
    f <- "/tmp/sometmpfile"
    assign("file", f, envir = e )
    reg.finalizer(e, function(x) file.remove(get("file", envir = e)))
    attr(out, "FileList") <- e
    file.create( f )
    structure( out, class = "MyClass" )
}
