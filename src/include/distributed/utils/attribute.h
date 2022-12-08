//
// Created by Nils Dijk on 02/12/2022.
//

#ifndef CITUS_ATTRIBUTE_H
#define CITUS_ATTRIBUTE_H

#include "executor/execdesc.h"
#include "executor/executor.h"

extern void CitusAttributeToEnd(QueryDesc *queryDesc);
extern void AttributeQueryIfAnnotated(const char *queryString);

extern ExecutorEnd_hook_type prev_ExecutorEnd;

#endif //CITUS_ATTRIBUTE_H
