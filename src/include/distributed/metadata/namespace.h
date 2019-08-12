/* */

/* Created by Nils Dijk on 2019-08-07. */
/* */

#ifndef CITUS_NAMESPACE_H
#define CITUS_NAMESPACE_H

#include "postgres.h"

#include "nodes/primnodes.h"

extern List * makeNameListFromRangeVar(const RangeVar *var);

#endif /*CITUS_NAMESPACE_H */
