#include <R.h>
#include <Rinternals.h>


// node in a linked-list
typedef struct rnode {
  int ind;
  int cnt;
  struct rnode *next;
} RECN;


static void recnfree(RECN *p){
  if (!p)
    return;
  recnfree(p->next);
  free(p);
}

// if NULL -> finish; if EXTRPTR free all nodes in linked list
static void recnfinalizer(SEXP x){
  if(TYPEOF(x) != EXTPTRSXP) 
    return;
  recnfree( (RECN *) EXTPTR_PTR(x));
}

SEXP add_to_record_list(SEXP x, SEXP addme){
  //TODO: check input arguments (is.null, etc.)
  // e.g. addme is list with two elements
  // 
  SEXP ind, cnt;
  RECN *p, *q, *r;

  if(isNull(x))
    r = NULL;
  else 
    r = (RECN *) EXTPTR_PTR(x);

  if(isNull(addme)){
    int n;
    for( n = 0, p = r; p; p = p->next )
      n++;
    if(!n)
      return R_NilValue;
    x = PROTECT( allocVector(VECSXP, 2) );
    SET_VECTOR_ELT( x, 0, (ind = allocVector(INTSXP, n)) );
    SET_VECTOR_ELT( x, 1, (cnt = allocVector(INTSXP, n)) );
    for( p = r; p; p = p->next ){
      n--;
      INTEGER(ind)[n] = p->ind;
      INTEGER(cnt)[n] = p->cnt;
    }
    UNPROTECT(1);
    return x;
  }

  // TODO need the same length for ind and cnt
  ind = VECTOR_ELT(addme, 0);
  cnt = VECTOR_ELT(addme, 1);

  for(int i = 0; i < LENGTH(ind); i++){
    p = (RECN *) malloc(sizeof(RECN));
    p->ind  = INTEGER(ind)[i];
    p->cnt  = INTEGER(cnt)[i];
    if(r)
      p->next = r;
    else
      p->next = NULL;
    r = p;
  }
  x = R_MakeExternalPtr(r, R_NilValue, R_NilValue);
  R_RegisterCFinalizerEx(x, recnfinalizer, TRUE);
  return x;
}
