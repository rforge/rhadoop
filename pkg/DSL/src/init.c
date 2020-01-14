#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

SEXP _collector2(SEXP x, SEXP y);

static const R_CallMethodDef CallEntries[] = {
    {"_collector2", (DL_FUNC) &_collector2, 2},
    {NULL, NULL, 0}
};

void R_init_DSL(DllInfo *dll)
{
    R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
