/*-------------------------------------------------------------------------
 *
 * citus_outfuncs.c
 *	  Output functions for Citus tree nodes.
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * NOTES
 *	  This is a wrapper around postgres' nodeToString() that additionally
 *	  supports Citus node types.
 *
 *    Keep as closely aligned with the upstream version as possible.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <ctype.h>

#include "distributed/citus_nodefuncs.h"
#include "distributed/citus_nodes.h"
#include "distributed/errormessage.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/master_metadata_utility.h"
#include "lib/stringinfo.h"
#include "nodes/plannodes.h"
#include "nodes/relation.h"
#include "utils/datum.h"


/*
 * Macros to simplify output of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire conventions about the names of the local variables in an Out
 * routine.
 */

/* Store const reference to raw input node in local named 'node' */
#define WRITE_LOCALS(nodeTypeName) \
		const nodeTypeName *node = (const nodeTypeName *) raw_node

/* Write the label for the node type */
#if (PG_VERSION_NUM >= 90600)
#define WRITE_NODE_TYPE(nodelabel) \
	(void) 0

#else
#define WRITE_NODE_TYPE(nodelabel) \
	appendStringInfoString(str, nodelabel)

#endif

/* Write an integer field (anything written as ":fldname %d") */
#define WRITE_INT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write an unsigned integer field (anything written as ":fldname %u") */
#define WRITE_UINT_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* XXX: Citus: Write an unsigned 64-bit integer field */
#define WRITE_UINT64_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " " UINT64_FORMAT, node->fldname)

/* Write an OID field (don't hard-wire assumption that OID is same as uint) */
#define WRITE_OID_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %u", node->fldname)

/* Write a long-integer field */
#define WRITE_LONG_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %ld", node->fldname)

/* Write a char field (ie, one ascii character) */
#define WRITE_CHAR_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %c", node->fldname)

/* Write an enumerated-type field as an integer code */
#define WRITE_ENUM_FIELD(fldname, enumtype) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", \
					 (int) node->fldname)

/* Write a float field --- caller must give format to define precision */
#define WRITE_FLOAT_FIELD(fldname,format) \
	appendStringInfo(str, " :" CppAsString(fldname) " " format, node->fldname)

/* Write a boolean field */
#define WRITE_BOOL_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %s", \
					 booltostr(node->fldname))

/* Write a character-string (possibly NULL) field */
#define WRITE_STRING_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outToken(str, node->fldname))

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 outNode(str, node->fldname))

/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 _outBitmapset(str, node->fldname))


#define booltostr(x)  ((x) ? "true" : "false")

#if (PG_VERSION_NUM < 90600)
static void outNode(StringInfo str, const void *obj);

/*
 * outToken
 *	  Convert an ordinary string (eg, an identifier) into a form that
 *	  will be decoded back to a plain token by read.c's functions.
 *
 *	  If a null or empty string is given, it is encoded as "<>".
 */
static void
outToken(StringInfo str, const char *s)
{
	if (s == NULL || *s == '\0')
	{
		appendStringInfoString(str, "<>");
		return;
	}

	/*
	 * Look for characters or patterns that are treated specially by read.c
	 * (either in pg_strtok() or in nodeRead()), and therefore need a
	 * protective backslash.
	 */
	/* These characters only need to be quoted at the start of the string */
	if (*s == '<' ||
		*s == '\"' ||
		isdigit((unsigned char) *s) ||
		((*s == '+' || *s == '-') &&
		 (isdigit((unsigned char) s[1]) || s[1] == '.')))
		appendStringInfoChar(str, '\\');
	while (*s)
	{
		/* These chars must be backslashed anywhere in the string */
		if (*s == ' ' || *s == '\n' || *s == '\t' ||
			*s == '(' || *s == ')' || *s == '{' || *s == '}' ||
			*s == '\\')
			appendStringInfoChar(str, '\\');
		appendStringInfoChar(str, *s++);
	}
}


static void
_outList(StringInfo str, const List *node)
{
	const ListCell *lc;

	appendStringInfoChar(str, '(');

	if (IsA(node, IntList))
		appendStringInfoChar(str, 'i');
	else if (IsA(node, OidList))
		appendStringInfoChar(str, 'o');

	foreach(lc, node)
	{
		/*
		 * For the sake of backward compatibility, we emit a slightly
		 * different whitespace format for lists of nodes vs. other types of
		 * lists. XXX: is this necessary?
		 */
		if (IsA(node, List))
		{
			outNode(str, lfirst(lc));
			if (lnext(lc))
				appendStringInfoChar(str, ' ');
		}
		else if (IsA(node, IntList))
			appendStringInfo(str, " %d", lfirst_int(lc));
		else if (IsA(node, OidList))
			appendStringInfo(str, " %u", lfirst_oid(lc));
		else
			elog(ERROR, "unrecognized list node type: %d",
				 (int) node->type);
	}

	appendStringInfoChar(str, ')');
}


/*
 * Print the value of a Datum given its type.
 */
static void
outDatum(StringInfo str, Datum value, int typlen, bool typbyval)
{
	Size		length,
				i;
	char	   *s;

	length = datumGetSize(value, typbyval, typlen);

	if (typbyval)
	{
		s = (char *) (&value);
		appendStringInfo(str, "%u [ ", (unsigned int) length);
		for (i = 0; i < (Size) sizeof(Datum); i++)
			appendStringInfo(str, "%d ", (int) (s[i]));
		appendStringInfoChar(str, ']');
	}
	else
	{
		s = (char *) DatumGetPointer(value);
		if (!PointerIsValid(s))
			appendStringInfoString(str, "0 [ ]");
		else
		{
			appendStringInfo(str, "%u [ ", (unsigned int) length);
			for (i = 0; i < length; i++)
				appendStringInfo(str, "%d ", (int) (s[i]));
			appendStringInfoChar(str, ']');
		}
	}
}

#endif

/*****************************************************************************
 *	Output routines for Citus node types
 *****************************************************************************/

static void
OutMultiUnaryNodeFields(StringInfo str, const MultiUnaryNode *node)
{
	WRITE_NODE_FIELD(childNode);
}


static void
OutMultiBinaryNodeFields(StringInfo str, const MultiBinaryNode *node)
{
	WRITE_NODE_FIELD(leftChildNode);
	WRITE_NODE_FIELD(rightChildNode);
}

void
OutMultiNode(OUTFUNC_ARGS)
{
	WRITE_NODE_TYPE("MULTINODE");
}


void
OutMultiTreeRoot(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiTreeRoot);

	WRITE_NODE_TYPE("MULTITREEROOT");

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiPlan(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiPlan);

	WRITE_NODE_TYPE("MULTIPLAN");

	WRITE_INT_FIELD(operation);
	WRITE_BOOL_FIELD(hasReturning);

	WRITE_NODE_FIELD(workerJob);
	WRITE_NODE_FIELD(masterQuery);
	WRITE_BOOL_FIELD(routerExecutable);
	WRITE_NODE_FIELD(planningError);
}


void
OutMultiProject(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiProject);
	WRITE_NODE_TYPE("MULTIPROJECT");

	WRITE_NODE_FIELD(columnList);

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiCollect(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiCollect);
	WRITE_NODE_TYPE("MULTICOLLECT");

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiSelect(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiSelect);
	WRITE_NODE_TYPE("MULTISELECT");

	WRITE_NODE_FIELD(selectClauseList);

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiTable(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiTable);
	WRITE_NODE_TYPE("MULTITABLE");

	WRITE_OID_FIELD(relationId);
	WRITE_INT_FIELD(rangeTableId);

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiJoin(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiJoin);
	WRITE_NODE_TYPE("MULTIJOIN");

	WRITE_NODE_FIELD(joinClauseList);
	WRITE_ENUM_FIELD(joinRuleType, JoinRuleType);
	WRITE_ENUM_FIELD(joinType, JoinType);

	OutMultiBinaryNodeFields(str, (const MultiBinaryNode *) node);
}


void
OutMultiPartition(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiPartition);
	WRITE_NODE_TYPE("MULTIPARTITION");

	WRITE_NODE_FIELD(partitionColumn);

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}


void
OutMultiCartesianProduct(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiCartesianProduct);
	WRITE_NODE_TYPE("MULTICARTESIANPRODUCT");

	OutMultiBinaryNodeFields(str, (const MultiBinaryNode *) node);
}




void
OutMultiExtendedOp(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MultiExtendedOp);
	WRITE_NODE_TYPE("MULTIEXTENDEDOP");

	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(groupClauseList);
	WRITE_NODE_FIELD(sortClauseList);
	WRITE_NODE_FIELD(limitCount);
	WRITE_NODE_FIELD(limitOffset);
	WRITE_NODE_FIELD(havingQual);

	OutMultiUnaryNodeFields(str, (const MultiUnaryNode *) node);
}

static void
OutJobFields(StringInfo str, const Job *node)
{
	WRITE_UINT64_FIELD(jobId);
	WRITE_NODE_FIELD(jobQuery);
	WRITE_NODE_FIELD(taskList);
	WRITE_NODE_FIELD(dependedJobList);
	WRITE_BOOL_FIELD(subqueryPushdown);
	WRITE_BOOL_FIELD(requiresMasterEvaluation);
	WRITE_BOOL_FIELD(deferredPruning);
}


void
OutJob(OUTFUNC_ARGS)
{
	WRITE_LOCALS(Job);
	WRITE_NODE_TYPE("JOB");

	OutJobFields(str, node);
}


void
OutShardInterval(OUTFUNC_ARGS)
{
	WRITE_LOCALS(ShardInterval);
	WRITE_NODE_TYPE("SHARDINTERVAL");

	WRITE_OID_FIELD(relationId);
	WRITE_CHAR_FIELD(storageType);
	WRITE_OID_FIELD(valueTypeId);
	WRITE_INT_FIELD(valueTypeLen);
	WRITE_BOOL_FIELD(valueByVal);
	WRITE_BOOL_FIELD(minValueExists);
	WRITE_BOOL_FIELD(maxValueExists);

	appendStringInfoString(str, " :minValue ");
	if (!node->minValueExists)
		appendStringInfoString(str, "<>");
	else
		outDatum(str, node->minValue, node->valueTypeLen, node->valueByVal);

	appendStringInfoString(str, " :maxValue ");
	if (!node->maxValueExists)
		appendStringInfoString(str, "<>");
	else
		outDatum(str, node->maxValue, node->valueTypeLen, node->valueByVal);

	WRITE_UINT64_FIELD(shardId);
}


void
OutMapMergeJob(OUTFUNC_ARGS)
{
	WRITE_LOCALS(MapMergeJob);
	int arrayLength = node->sortedShardIntervalArrayLength;
	int i;

	WRITE_NODE_TYPE("MAPMERGEJOB");

	OutJobFields(str, (Job *) node);
	WRITE_NODE_FIELD(reduceQuery);
	WRITE_ENUM_FIELD(partitionType, PartitionType);
	WRITE_NODE_FIELD(partitionColumn);
	WRITE_UINT_FIELD(partitionCount);
	WRITE_INT_FIELD(sortedShardIntervalArrayLength);

	for (i = 0; i < arrayLength; ++i)
	{
		outNode(str, node->sortedShardIntervalArray[i]);
	}

	WRITE_NODE_FIELD(mapTaskList);
	WRITE_NODE_FIELD(mergeTaskList);
}


void
OutShardPlacement(OUTFUNC_ARGS)
{
	WRITE_LOCALS(ShardPlacement);
	WRITE_NODE_TYPE("SHARDPLACEMENT");

	WRITE_UINT64_FIELD(placementId);
	WRITE_UINT64_FIELD(shardId);
	WRITE_UINT64_FIELD(shardLength);
	WRITE_ENUM_FIELD(shardState, RelayFileState);
	WRITE_STRING_FIELD(nodeName);
	WRITE_UINT_FIELD(nodePort);
	/* so we can deal with 0 */
	WRITE_INT_FIELD(partitionMethod);
	WRITE_UINT_FIELD(colocationGroupId);
	WRITE_UINT_FIELD(representativeValue);
}


void
OutRelationShard(OUTFUNC_ARGS)
{
	WRITE_LOCALS(RelationShard);
	WRITE_NODE_TYPE("RELATIONSHARD");

	WRITE_OID_FIELD(relationId);
	WRITE_UINT64_FIELD(shardId);
}


void
OutTask(OUTFUNC_ARGS)
{
	WRITE_LOCALS(Task);
	WRITE_NODE_TYPE("TASK");

	WRITE_ENUM_FIELD(taskType, TaskType);
	WRITE_UINT64_FIELD(jobId);
	WRITE_UINT_FIELD(taskId);
	WRITE_STRING_FIELD(queryString);
	WRITE_UINT64_FIELD(anchorShardId);
	WRITE_NODE_FIELD(taskPlacementList);
	WRITE_NODE_FIELD(dependedTaskList);
	WRITE_UINT_FIELD(partitionId);
	WRITE_UINT_FIELD(upstreamTaskId);
	WRITE_NODE_FIELD(shardInterval);
	WRITE_BOOL_FIELD(assignmentConstrained);
	WRITE_NODE_FIELD(taskExecution);
	WRITE_BOOL_FIELD(upsertQuery);
	WRITE_CHAR_FIELD(replicationModel);
	WRITE_BOOL_FIELD(insertSelectQuery);
	WRITE_NODE_FIELD(relationShardList);
}


void
OutDeferredErrorMessage(OUTFUNC_ARGS)
{
	WRITE_LOCALS(DeferredErrorMessage);
	WRITE_NODE_TYPE("DEFERREDERRORMESSAGE");

	WRITE_INT_FIELD(code);
	WRITE_STRING_FIELD(message);
	WRITE_STRING_FIELD(detail);
	WRITE_STRING_FIELD(hint);
	WRITE_STRING_FIELD(filename);
	WRITE_INT_FIELD(linenumber);
	WRITE_STRING_FIELD(functionname);
}


#if (PG_VERSION_NUM < 90600)

/*
 * outNode -
 *	  converts a Node into ascii string and append it to 'str'
 */
static void
outNode(StringInfo str, const void *obj)
{
	if (obj == NULL)
	{
		appendStringInfoString(str, "<>");
		return;
	}


	switch (CitusNodeTag(obj))
	{
		case T_List:
		case T_IntList:
		case T_OidList:
			_outList(str, obj);
			break;

		case T_MultiTreeRoot:
			appendStringInfoChar(str, '{');
			OutMultiTreeRoot(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiProject:
			appendStringInfoChar(str, '{');
			OutMultiProject(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiCollect:
			appendStringInfoChar(str, '{');
			OutMultiCollect(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiSelect:
			appendStringInfoChar(str, '{');
			OutMultiSelect(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiTable:
			appendStringInfoChar(str, '{');
			OutMultiTable(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiJoin:
			appendStringInfoChar(str, '{');
			OutMultiJoin(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiPartition:
			appendStringInfoChar(str, '{');
			OutMultiPartition(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiCartesianProduct:
			appendStringInfoChar(str, '{');
			OutMultiCartesianProduct(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiExtendedOp:
			appendStringInfoChar(str, '{');
			OutMultiExtendedOp(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_Job:
			appendStringInfoChar(str, '{');
			OutJob(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MapMergeJob:
			appendStringInfoChar(str, '{');
			OutMapMergeJob(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiPlan:
			appendStringInfoChar(str, '{');
			OutMultiPlan(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_Task:
			appendStringInfoChar(str, '{');
			OutTask(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_ShardInterval:
			appendStringInfoChar(str, '{');
			OutShardInterval(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_ShardPlacement:
			appendStringInfoChar(str, '{');
			OutShardPlacement(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_RelationShard:
			appendStringInfoChar(str, '{');
			OutRelationShard(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_DeferredErrorMessage:
			appendStringInfoChar(str, '{');
			OutDeferredErrorMessage(str, obj);
			appendStringInfoChar(str, '}');
			break;

		default:
			/* fall back into postgres' normal nodeToString machinery */
			appendStringInfoString(str, nodeToString(obj));
	}
}

#endif

/*
 * CitusNodeToString -
 *	   returns the ascii representation of the Node as a palloc'd string
 */
char *
CitusNodeToString(const void *obj)
{
#if (PG_VERSION_NUM >= 90600)
	return nodeToString(obj);
#else
	StringInfoData str;

	initStringInfo(&str);
	outNode(&str, obj);
	return str.data;
#endif
}
