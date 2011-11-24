
#include <R.h>
#include <Rinternals.h>

/* Push y (a list of two components) to the pairlist 
 * x (initially NULL) or, if y = NULL, concatenate the
 * parallel components of x. If the components of y are
 * NULL or of zero length y is not added to x. Otherwise,
 * the components of y must be two integer vectors of
 * equal length.
 *
 * NOTE memory consumption is twice the data collected.
 *
 * ceeboo 2010/03
 */
SEXP _collector2(SEXP x, SEXP y) {
    const char *_class = "_collector2";
    SEXP c0, c1;
    // concatenate
    if (isNull(y)) {
	if (isNull(x))
	    return x;
	if (!inherits(x, _class))
	    error("'x' not of class %s", _class);
	// count
	int n = 0;
	for (SEXP q = x; q != R_NilValue; q = CDR(q))
	    n += LENGTH(VECTOR_ELT(CAR(q), 0));
	// we do not collect empty components,
	// so something is wrong.
	if (!n)
	    error("'x' empty");
	// allocate
	y = PROTECT(allocVector(VECSXP, 2));
	SET_VECTOR_ELT(y, 0, (c0 = allocVector(INTSXP, n)));
	SET_VECTOR_ELT(y, 1, (c1 = allocVector(INTSXP, n)));
	// copy
	for (; x != R_NilValue; x = CDR(x)) {
	    SEXP _c0, _c1;

	    _c1 = CAR(x);
	    _c0 = VECTOR_ELT(_c1, 0);
	    _c1 = VECTOR_ELT(_c1, 1);

	    int k = n - LENGTH(_c0);
	    for (int i = 0; i < LENGTH(_c0); i++, k++, n--) {
		INTEGER(c0)[k] = INTEGER(_c0)[i];
		INTEGER(c1)[k] = INTEGER(_c1)[i];
	    }
	}
	UNPROTECT(1);

	return y;
    }
    // check
    if (LENGTH(y) != 2)
	error("'y' invalid length");
    if (isNull((c0 = VECTOR_ELT(y, 0))))
	return x;
    if (!LENGTH(c0))
	return x;
    if (LENGTH( c0) !=
	LENGTH((c1 = VECTOR_ELT(y, 1)))) 
	error("'y' components do not conform");
    if (TYPEOF(c0) != INTSXP ||
	TYPEOF(c1) != INTSXP)
	error("'y' component(s) not of type intsxp");
    // push
    if (isNull(x)) {
	y =  PROTECT(CONS(y, R_NilValue));
	setAttrib(y, R_ClassSymbol, mkString(_class));
	UNPROTECT(1);
    }
    else {
	if (!inherits(x, _class))
	    error("'x' not of class %s", _class);
	y = CONS(y, x);
	setAttrib(y, R_ClassSymbol, getAttrib(x, R_ClassSymbol));
    }

    return y;
}

//
