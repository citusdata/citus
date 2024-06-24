/*-------------------------------------------------------------------------
 *
 * type_utils.c
 *
 * Utility functions related to types.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "libpq-fe.h"

#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "nodes/pg_list.h"
#include "utils/syscache.h"

#include "distributed/causal_clock.h"

#define NUM_CLUSTER_CLOCK_ARGS 2
#define LDELIM '('
#define RDELIM ')'
#define DELIM ','

static ClusterClock * cluster_clock_in_internal(char *clockString);

PG_FUNCTION_INFO_V1(cluster_clock_in);
PG_FUNCTION_INFO_V1(cluster_clock_out);
PG_FUNCTION_INFO_V1(cluster_clock_recv);
PG_FUNCTION_INFO_V1(cluster_clock_send);

/*
 * cluster_clock_in_internal generic routine to parse the cluster_clock format of (logical, counter),
 * (%lu, %u), in string format to ClusterClock struct internal format.
 */
static ClusterClock *
cluster_clock_in_internal(char *clockString)
{
	char *clockFields[NUM_CLUSTER_CLOCK_ARGS];
	int numClockField = 0;

	for (char *currentChar = clockString;
		 (*currentChar) && (numClockField < NUM_CLUSTER_CLOCK_ARGS) && (*currentChar !=
																		RDELIM);
		 currentChar++)
	{
		if (*currentChar == DELIM || (*currentChar == LDELIM && !numClockField))
		{
			clockFields[numClockField++] = currentChar + 1;
		}
	}

	if (numClockField < NUM_CLUSTER_CLOCK_ARGS)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"cluster_clock",
						clockString)));
	}

	char *endingChar = NULL;
	errno = 0;
	int64 logical = strtoul(clockFields[0], &endingChar, 10);

	if (errno || (*endingChar != DELIM) || (logical > MAX_LOGICAL) || logical < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"cluster_clock",
						clockString)));
	}

	int64 counter = strtol(clockFields[1], &endingChar, 10);

	if (errno || (*endingChar != RDELIM) || (counter > MAX_COUNTER) || counter < 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
				 errmsg("invalid input syntax for type %s: \"%s\"",
						"cluster_clock",
						clockString)));
	}

	ClusterClock *clusterClock = (ClusterClock *) palloc(sizeof(ClusterClock));
	clusterClock->logical = logical;
	clusterClock->counter = counter;

	return clusterClock;
}


/*
 * cluster_clock_in converts the cstring input format to the ClusterClock type.
 */
Datum
cluster_clock_in(PG_FUNCTION_ARGS)
{
	char *clockString = PG_GETARG_CSTRING(0);

	PG_RETURN_POINTER(cluster_clock_in_internal(clockString));
}


/*
 * cluster_clock_out converts the internal ClusterClock format to cstring output.
 */
Datum
cluster_clock_out(PG_FUNCTION_ARGS)
{
	ClusterClock *clusterClock = (ClusterClock *) PG_GETARG_POINTER(0);

	if (clusterClock == NULL)
	{
		PG_RETURN_CSTRING("");
	}

	char *clockString = psprintf("(%lu,%u)", clusterClock->logical,
								 clusterClock->counter);

	PG_RETURN_CSTRING(clockString);
}


/*
 * cluster_clock_recv converts external binary format to ClusterClock.
 */
Datum
cluster_clock_recv(PG_FUNCTION_ARGS)
{
	StringInfo clockBuffer = (StringInfo) PG_GETARG_POINTER(0);
	ClusterClock *clusterClock = (ClusterClock *) palloc(sizeof(ClusterClock));

	clusterClock->logical = pq_getmsgint64(clockBuffer);
	clusterClock->counter = pq_getmsgint(clockBuffer, sizeof(int));

	PG_RETURN_POINTER(clusterClock);
}


/*
 * cluster_clock_send converts ClusterClock to binary format.
 */
Datum
cluster_clock_send(PG_FUNCTION_ARGS)
{
	ClusterClock *clusterClock = (ClusterClock *) PG_GETARG_POINTER(0);
	StringInfoData clockBuffer;

	pq_begintypsend(&clockBuffer);
	pq_sendint64(&clockBuffer, clusterClock->logical);
	pq_sendint32(&clockBuffer, clusterClock->counter);

	PG_RETURN_BYTEA_P(pq_endtypsend(&clockBuffer));
}


/*****************************************************************************
*       PUBLIC ROUTINES                                                     *
*****************************************************************************/

PG_FUNCTION_INFO_V1(cluster_clock_lt);
PG_FUNCTION_INFO_V1(cluster_clock_le);
PG_FUNCTION_INFO_V1(cluster_clock_eq);
PG_FUNCTION_INFO_V1(cluster_clock_ne);
PG_FUNCTION_INFO_V1(cluster_clock_gt);
PG_FUNCTION_INFO_V1(cluster_clock_ge);
PG_FUNCTION_INFO_V1(cluster_clock_cmp);
PG_FUNCTION_INFO_V1(cluster_clock_logical);


/*
 * cluster_clock_cmp_internal generic compare routine, and must be used for all
 * operators, including Btree Indexes when comparing cluster_clock data type.
 * Return values are
 *   1 -- clock1 is > clock2
 *   0 -- clock1 is = clock2
 *  -1 -- clock1 is < clock2
 */
int
cluster_clock_cmp_internal(ClusterClock *clusterClock1, ClusterClock *clusterClock2)
{
	Assert(clusterClock1 && clusterClock2);

	int retcode = 0;

	/* Logical value takes precedence when comparing two clocks */
	if (clusterClock1->logical != clusterClock2->logical)
	{
		retcode = (clusterClock1->logical > clusterClock2->logical) ? 1 : -1;
		return retcode;
	}

	/* Logical values are equal, let's compare ticks */
	if (clusterClock1->counter != clusterClock2->counter)
	{
		retcode = (clusterClock1->counter > clusterClock2->counter) ? 1 : -1;
		return retcode;
	}

	/* Ticks are equal too, return zero */
	return retcode;
}


/*
 * cluster_clock_lt returns true if clock1 is less than clock2.
 */
Datum
cluster_clock_lt(PG_FUNCTION_ARGS)
{
	ClusterClock *clock1 = (ClusterClock *) PG_GETARG_POINTER(0);
	ClusterClock *clock2 = (ClusterClock *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(cluster_clock_cmp_internal(clock1, clock2) < 0);
}


/*
 * cluster_clock_le returns true if clock1 is less than or equal to clock2.
 */
Datum
cluster_clock_le(PG_FUNCTION_ARGS)
{
	ClusterClock *clock1 = (ClusterClock *) PG_GETARG_POINTER(0);
	ClusterClock *clock2 = (ClusterClock *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(cluster_clock_cmp_internal(clock1, clock2) <= 0);
}


/*
 * cluster_clock_eq returns true if clock1 is equal to clock2.
 */
Datum
cluster_clock_eq(PG_FUNCTION_ARGS)
{
	ClusterClock *clock1 = (ClusterClock *) PG_GETARG_POINTER(0);
	ClusterClock *clock2 = (ClusterClock *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(cluster_clock_cmp_internal(clock1, clock2) == 0);
}


/*
 * cluster_clock_ne returns true if clock1 is not equal to clock2.
 */
Datum
cluster_clock_ne(PG_FUNCTION_ARGS)
{
	ClusterClock *clock1 = (ClusterClock *) PG_GETARG_POINTER(0);
	ClusterClock *clock2 = (ClusterClock *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(cluster_clock_cmp_internal(clock1, clock2) != 0);
}


/*
 * cluster_clock_gt returns true if clock1 is greater than clock2.
 */
Datum
cluster_clock_gt(PG_FUNCTION_ARGS)
{
	ClusterClock *clock1 = (ClusterClock *) PG_GETARG_POINTER(0);
	ClusterClock *clock2 = (ClusterClock *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(cluster_clock_cmp_internal(clock1, clock2) > 0);
}


/*
 * cluster_clock_ge returns true if clock1 is greater than or equal to clock2
 */
Datum
cluster_clock_ge(PG_FUNCTION_ARGS)
{
	ClusterClock *clock1 = (ClusterClock *) PG_GETARG_POINTER(0);
	ClusterClock *clock2 = (ClusterClock *) PG_GETARG_POINTER(1);

	PG_RETURN_BOOL(cluster_clock_cmp_internal(clock1, clock2) >= 0);
}


/*
 * cluster_clock_cmp returns 1 if clock1 is greater than clock2, returns -1 if
 * clock1 is less than clock2, and zero if they are equal.
 */
Datum
cluster_clock_cmp(PG_FUNCTION_ARGS)
{
	ClusterClock *clock1 = (ClusterClock *) PG_GETARG_POINTER(0);
	ClusterClock *clock2 = (ClusterClock *) PG_GETARG_POINTER(1);

	PG_RETURN_INT32(cluster_clock_cmp_internal(clock1, clock2));
}


/*
 * cluster_clock_logical return the logical part from <logical, counter> type
 * clock, which is basically the epoch value in milliseconds.
 */
Datum
cluster_clock_logical(PG_FUNCTION_ARGS)
{
	ClusterClock *clusterClock = (ClusterClock *) PG_GETARG_POINTER(0);

	PG_RETURN_INT64(clusterClock->logical);
}


/*
 * ParseClusterClockPGresult parses a ClusterClock remote result and returns the value or
 * returns 0 if the result is NULL.
 */
ClusterClock *
ParseClusterClockPGresult(PGresult *result, int rowIndex, int colIndex)
{
	if (PQgetisnull(result, rowIndex, colIndex))
	{
		return 0;
	}

	char *resultString = PQgetvalue(result, rowIndex, colIndex);
	return cluster_clock_in_internal(resultString);
}
