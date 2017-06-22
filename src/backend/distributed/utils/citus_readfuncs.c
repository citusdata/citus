/*-------------------------------------------------------------------------
 *
 * citus_readfuncs.c
 *    Citus specific node functions
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2012-2015, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "distributed/citus_nodefuncs.h"
#include "distributed/errormessage.h"
#include "distributed/multi_planner.h"
#include "nodes/parsenodes.h"
#include "nodes/readfuncs.h"


/*
 * Macros to simplify reading of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in a Read
 * routine.
 */

/* Macros for declaring appropriate local variables */
/* A few guys need only local_node */
#if (PG_VERSION_NUM >= 90600)
static inline Node *
CitusSetTag(Node *node, int tag)
{
	CitusNode *citus_node = (CitusNode *) node;
	citus_node->citus_tag = tag;
	return node;
}


/* *INDENT-OFF* */
#define READ_LOCALS_NO_FIELDS(nodeTypeName) \
	nodeTypeName *local_node = (nodeTypeName *) CitusSetTag((Node *) node, T_##nodeTypeName)
#else
#define READ_LOCALS_NO_FIELDS(nodeTypeName) \
	nodeTypeName *local_node = CitusMakeNode(nodeTypeName)
#endif

/* And a few guys need only the citus_pg_strtok support fields */
#define READ_TEMP_LOCALS()	\
	char	   *token;		\
	int			length

/* ... but most need both */
#define READ_LOCALS(nodeTypeName)			\
	READ_LOCALS_NO_FIELDS(nodeTypeName);	\
	READ_TEMP_LOCALS()

/* Read an integer field (anything written as ":fldname %d") */
#define READ_INT_FIELD(fldname) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	token = citus_pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoi(token)

/* Read an unsigned integer field (anything written as ":fldname %u") */
#define READ_UINT_FIELD(fldname) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	token = citus_pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoui(token)

/* XXX: CITUS Read an uint64 field (anything written as ":fldname %u") */
#define READ_UINT64_FIELD(fldname) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	token = citus_pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atoull(token)

/* Read an OID field (don't hard-wire assumption that OID is same as uint) */
#define READ_OID_FIELD(fldname) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	token = citus_pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atooid(token)

/* Read a char field (ie, one ascii character) */
#define READ_CHAR_FIELD(fldname) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	token = citus_pg_strtok(&length);		/* get field value */ \
	local_node->fldname = token[0]

/* Read an enumerated-type field that was written as an integer code */
#define READ_ENUM_FIELD(fldname, enumtype) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	token = citus_pg_strtok(&length);		/* get field value */ \
	local_node->fldname = (enumtype) atoi(token)

/* Read a float field */
#define READ_FLOAT_FIELD(fldname) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	token = citus_pg_strtok(&length);		/* get field value */ \
	local_node->fldname = atof(token)

/* Read a boolean field */
#define READ_BOOL_FIELD(fldname) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	token = citus_pg_strtok(&length);		/* get field value */ \
	local_node->fldname = strtobool(token)

/* Read a character-string field */
#define READ_STRING_FIELD(fldname) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	token = citus_pg_strtok(&length);		/* get field value */ \
	local_node->fldname = nullable_string(token, length)

/* Read a parse location field (and throw away the value, per notes above) */
#define READ_LOCATION_FIELD(fldname) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	token = citus_pg_strtok(&length);		/* get field value */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = -1	/* set field to "unknown" */

/* Read a Node field XXX: Citus: replaced call to nodeRead with CitusNodeRead */
#define READ_NODE_FIELD(fldname) \
	token = citus_pg_strtok(&length);		/* skip :fldname */ \
	(void) token;				/* in case not used elsewhere */ \
	local_node->fldname = CitusNodeRead(NULL, 0)

/* Routine exit */
#if (PG_VERSION_NUM >= 90600)
#define READ_DONE() \
	return;
#else
#define READ_DONE() \
	return (Node *) local_node
#endif


/*
 * NOTE: use atoi() to read values written with %d, or atoui() to read
 * values written with %u in outfuncs.c.  An exception is OID values,
 * for which use atooid().  (As of 7.1, outfuncs.c writes OIDs as %u,
 * but this will probably change in the future.)
 */
#define atoui(x)  ((unsigned int) strtoul((x), NULL, 10))

#define atooid(x)  ((Oid) strtoul((x), NULL, 10))

/* XXX: Citus */
#define atoull(x)  ((uint64) strtoull((x), NULL, 10))

#define strtobool(x)  ((*(x) == 't') ? true : false)

#define nullable_string(token,length)  \
	((length) == 0 ? NULL : debackslash(token, length))


static void
readJobInfo(Job *local_node)
{
	READ_TEMP_LOCALS();

	READ_UINT64_FIELD(jobId);
	READ_NODE_FIELD(jobQuery);
	READ_NODE_FIELD(taskList);
	READ_NODE_FIELD(dependedJobList);
	READ_BOOL_FIELD(subqueryPushdown);
	READ_BOOL_FIELD(requiresMasterEvaluation);
	READ_BOOL_FIELD(deferredPruning);
}


READFUNC_RET
ReadJob(READFUNC_ARGS)
{
	READ_LOCALS_NO_FIELDS(Job);

	readJobInfo(local_node);

	READ_DONE();
}


READFUNC_RET
ReadMultiPlan(READFUNC_ARGS)
{
	READ_LOCALS(MultiPlan);

	READ_INT_FIELD(operation);
	READ_BOOL_FIELD(hasReturning);

	READ_NODE_FIELD(workerJob);
	READ_NODE_FIELD(masterQuery);
	READ_BOOL_FIELD(routerExecutable);

	READ_NODE_FIELD(insertSelectSubquery);
	READ_NODE_FIELD(insertTargetList);
	READ_OID_FIELD(targetRelationId);

	READ_NODE_FIELD(planningError);

	READ_DONE();
}


READFUNC_RET
ReadShardInterval(READFUNC_ARGS)
{
	READ_LOCALS(ShardInterval);

	READ_OID_FIELD(relationId);
	READ_CHAR_FIELD(storageType);
	READ_OID_FIELD(valueTypeId);
	READ_INT_FIELD(valueTypeLen);
	READ_BOOL_FIELD(valueByVal);
	READ_BOOL_FIELD(minValueExists);
	READ_BOOL_FIELD(maxValueExists);

	token = citus_pg_strtok(&length); /* skip :minValue */
	if (!local_node->minValueExists)
		token = citus_pg_strtok(&length);               /* skip "<>" */
	else
		local_node->minValue = readDatum(local_node->valueByVal);

	token = citus_pg_strtok(&length); /* skip :maxValue */
	if (!local_node->minValueExists)
		token = citus_pg_strtok(&length);               /* skip "<>" */
	else
		local_node->maxValue = readDatum(local_node->valueByVal);

	READ_UINT64_FIELD(shardId);

	READ_DONE();
}


READFUNC_RET
ReadMapMergeJob(READFUNC_ARGS)
{
	int arrayLength;
	int i;

	READ_LOCALS(MapMergeJob);

	readJobInfo(&local_node->job);

	READ_NODE_FIELD(reduceQuery);
	READ_ENUM_FIELD(partitionType, PartitionType);
	READ_NODE_FIELD(partitionColumn);
	READ_UINT_FIELD(partitionCount);
	READ_INT_FIELD(sortedShardIntervalArrayLength);

	arrayLength = local_node->sortedShardIntervalArrayLength;

	/* now build & read sortedShardIntervalArray */
	local_node->sortedShardIntervalArray =
			(ShardInterval**) palloc(arrayLength * sizeof(ShardInterval *));

	for (i = 0; i < arrayLength; ++i)
	{
		/* can't use READ_NODE_FIELD, no field names */
		local_node->sortedShardIntervalArray[i] = CitusNodeRead(NULL, 0);
	}

	READ_NODE_FIELD(mapTaskList);
	READ_NODE_FIELD(mergeTaskList);

	READ_DONE();
}


READFUNC_RET
ReadShardPlacement(READFUNC_ARGS)
{
	READ_LOCALS(ShardPlacement);

	READ_UINT64_FIELD(placementId);
	READ_UINT64_FIELD(shardId);
	READ_UINT64_FIELD(shardLength);
	READ_ENUM_FIELD(shardState, RelayFileState);
	READ_STRING_FIELD(nodeName);
	READ_UINT_FIELD(nodePort);
	/* so we can deal with 0 */
	READ_INT_FIELD(partitionMethod);
	READ_UINT_FIELD(colocationGroupId);
	READ_UINT_FIELD(representativeValue);

	READ_DONE();
}


READFUNC_RET
ReadRelationShard(READFUNC_ARGS)
{
	READ_LOCALS(RelationShard);

	READ_OID_FIELD(relationId);
	READ_UINT64_FIELD(shardId);

	READ_DONE();
}


READFUNC_RET
ReadTask(READFUNC_ARGS)
{
	READ_LOCALS(Task);

	READ_ENUM_FIELD(taskType, TaskType);
	READ_UINT64_FIELD(jobId);
	READ_UINT_FIELD(taskId);
	READ_STRING_FIELD(queryString);
	READ_UINT64_FIELD(anchorShardId);
	READ_NODE_FIELD(taskPlacementList);
	READ_NODE_FIELD(dependedTaskList);
	READ_UINT_FIELD(partitionId);
	READ_UINT_FIELD(upstreamTaskId);
	READ_NODE_FIELD(shardInterval);
	READ_BOOL_FIELD(assignmentConstrained);
	READ_NODE_FIELD(taskExecution);
	READ_BOOL_FIELD(upsertQuery);
	READ_CHAR_FIELD(replicationModel);
	READ_BOOL_FIELD(insertSelectQuery);
	READ_NODE_FIELD(relationShardList);

	READ_DONE();
}


READFUNC_RET
ReadDeferredErrorMessage(READFUNC_ARGS)
{
	READ_LOCALS(DeferredErrorMessage);

	READ_INT_FIELD(code);
	READ_STRING_FIELD(message);
	READ_STRING_FIELD(detail);
	READ_STRING_FIELD(hint);
	READ_STRING_FIELD(filename);
	READ_INT_FIELD(linenumber);
	READ_STRING_FIELD(functionname);

	READ_DONE();
}


READFUNC_RET
ReadUnsupportedCitusNode(READFUNC_ARGS)
{
	ereport(ERROR, (errmsg("not implemented")));
}


#if (PG_VERSION_NUM < 90600)

/*
 * readDatum
 *
 * Given a string representation of a constant, recreate the appropriate
 * Datum.  The string representation embeds length info, but not byValue,
 * so we must be told that.
 */
Datum
readDatum(bool typbyval)
{
	Size		length,
				i;
	int			tokenLength;
	char	   *token;
	Datum		res;
	char	   *s;

	/*
	 * read the actual length of the value
	 */
	token = citus_pg_strtok(&tokenLength);
	length = atoui(token);

	token = citus_pg_strtok(&tokenLength);	/* read the '[' */
	if (token == NULL || token[0] != '[')
		elog(ERROR, "expected \"[\" to start datum, but got \"%s\"; length = %zu",
			 token ? (const char *) token : "[NULL]", length);

	if (typbyval)
	{
		if (length > (Size) sizeof(Datum))
			elog(ERROR, "byval datum but length = %zu", length);
		res = (Datum) 0;
		s = (char *) (&res);
		for (i = 0; i < (Size) sizeof(Datum); i++)
		{
			token = citus_pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
	}
	else if (length <= 0)
		res = (Datum) NULL;
	else
	{
		s = (char *) palloc(length);
		for (i = 0; i < length; i++)
		{
			token = citus_pg_strtok(&tokenLength);
			s[i] = (char) atoi(token);
		}
		res = PointerGetDatum(s);
	}

	token = citus_pg_strtok(&tokenLength);	/* read the ']' */
	if (token == NULL || token[0] != ']')
		elog(ERROR, "expected \"]\" to end datum, but got \"%s\"; length = %zu",
			 token ? (const char *) token : "[NULL]", length);

	return res;
}
#endif


#if (PG_VERSION_NUM >= 90600)

/* *INDENT-ON* */

/*
 * For 9.6+ we can just use the, now extensible, parseNodeString(). Before
 * that citus_readfuncs_$ver.c has a version specific implementation.
 */
Node *
CitusParseNodeString(void)
{
	return parseNodeString();
}


#endif
