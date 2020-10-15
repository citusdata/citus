/*-------------------------------------------------------------------------
 *
 * tuple_destination.h
 *	  Tuple destination generic struct.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef TUPLE_DESTINATION_H
#define TUPLE_DESTINATION_H

#include "access/tupdesc.h"
#include "distributed/multi_physical_planner.h"
#include "tcop/dest.h"
#include "utils/tuplestore.h"


typedef struct TupleDestination TupleDestination;


/*
 * TupleDestinationStats holds the size related stats.
 *
 * totalIntermediateResultSize is a counter to keep the size
 * of the intermediate results of complex subqueries and CTEs
 * so that we can put a limit on the size.
 */
typedef struct TupleDestinationStats
{
	uint64 totalIntermediateResultSize;
} TupleDestinationStats;


/*
 * TupleDestination provides a generic interface for where to send tuples.
 *
 * Users of the executor can set task->tupleDest for custom processing of
 * the result tuples.
 *
 * Since a task can have multiple queries, methods of TupleDestination also
 * accept a queryNumber parameter which denotes the index of the query that
 * tuple belongs to.
 */
struct TupleDestination
{
	/* putTuple implements custom processing of a tuple */
	void (*putTuple)(TupleDestination *self, Task *task,
					 int placementIndex, int queryNumber,
					 HeapTuple tuple, uint64 tupleLibpqSize);

	/* tupleDescForQuery returns tuple descriptor for a query number. Can return NULL. */
	TupleDesc (*tupleDescForQuery)(TupleDestination *self, int queryNumber);

	/*
	 * Used to enforce citus.max_intermediate_result_size, could be NULL
	 * if the caller is not interested in the size.
	 */
	TupleDestinationStats *tupleDestinationStats;
};

extern TupleDestination * CreateTupleStoreTupleDest(Tuplestorestate *tupleStore, TupleDesc
													tupleDescriptor);
extern TupleDestination * CreateTupleDestNone(void);
extern DestReceiver * CreateTupleDestDestReceiver(TupleDestination *tupleDest,
												  Task *task, int placementIndex);

#endif
