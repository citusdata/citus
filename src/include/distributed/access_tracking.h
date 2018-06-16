/*
 * access_tracking.h
 *
 * Function declartions for transaction access tracking.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 */

#ifndef ACCESS_TRACKING_H_
#define ACCESS_TRACKING_H_

#include "nodes/pg_list.h"

extern bool RelationAccessedInTransactionBlock(List *relationIds);


#endif /* ACCESS_TRACKING_H_ */
