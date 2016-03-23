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
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_physical_planner.h"
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

/* Write the label for the node type */
#define WRITE_NODE_TYPE(nodelabel) \
	appendStringInfoString(str, nodelabel)

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
	 _outToken(str, node->fldname))

/* Write a parse location field (actually same as INT case) */
#define WRITE_LOCATION_FIELD(fldname) \
	appendStringInfo(str, " :" CppAsString(fldname) " %d", node->fldname)

/* Write a Node field */
#define WRITE_NODE_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 _outNode(str, node->fldname))

/* Write a bitmapset field */
#define WRITE_BITMAPSET_FIELD(fldname) \
	(appendStringInfo(str, " :" CppAsString(fldname) " "), \
	 _outBitmapset(str, node->fldname))


#define booltostr(x)  ((x) ? "true" : "false")

static void _outNode(StringInfo str, const void *obj);


/*
 * _outToken
 *	  Convert an ordinary string (eg, an identifier) into a form that
 *	  will be decoded back to a plain token by read.c's functions.
 *
 *	  If a null or empty string is given, it is encoded as "<>".
 */
static void
_outToken(StringInfo str, const char *s)
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
			_outNode(str, lfirst(lc));
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
_outDatum(StringInfo str, Datum value, int typlen, bool typbyval)
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


/*****************************************************************************
 *	Output routines for Citus node types
 *****************************************************************************/

static void
_outMultiUnaryNode(StringInfo str, const MultiUnaryNode *node)
{
	WRITE_NODE_FIELD(childNode);
}


static void
_outMultiBinaryNode(StringInfo str, const MultiBinaryNode *node)
{
	WRITE_NODE_FIELD(leftChildNode);
	WRITE_NODE_FIELD(rightChildNode);
}


static void
_outMultiTreeRoot(StringInfo str, const MultiTreeRoot *node)
{
	WRITE_NODE_TYPE("MULTITREEROOT");

	_outMultiUnaryNode(str, (const MultiUnaryNode *) node);
}


static void
_outMultiPlan(StringInfo str, const MultiPlan *node)
{
	WRITE_NODE_TYPE("MULTIPLAN");

	WRITE_NODE_FIELD(workerJob);
	WRITE_NODE_FIELD(masterQuery);
	WRITE_STRING_FIELD(masterTableName);
}


static void
_outMultiProject(StringInfo str, const MultiProject *node)
{
	WRITE_NODE_TYPE("MULTIPROJECT");

	WRITE_NODE_FIELD(columnList);

	_outMultiUnaryNode(str, (const MultiUnaryNode *) node);
}


static void
_outMultiCollect(StringInfo str, const MultiCollect *node)
{
	WRITE_NODE_TYPE("MULTICOLLECT");

	_outMultiUnaryNode(str, (const MultiUnaryNode *) node);
}


static void
_outMultiSelect(StringInfo str, const MultiSelect *node)
{
	WRITE_NODE_TYPE("MULTISELECT");

	WRITE_NODE_FIELD(selectClauseList);

	_outMultiUnaryNode(str, (const MultiUnaryNode *) node);
}


static void
_outMultiTable(StringInfo str, const MultiTable *node)
{
	WRITE_NODE_TYPE("MULTITABLE");

	WRITE_OID_FIELD(relationId);
	WRITE_INT_FIELD(rangeTableId);

	_outMultiUnaryNode(str, (const MultiUnaryNode *) node);
}


static void
_outMultiJoin(StringInfo str, const MultiJoin *node)
{
	WRITE_NODE_TYPE("MULTIJOIN");

	WRITE_NODE_FIELD(joinClauseList);
	WRITE_ENUM_FIELD(joinRuleType, JoinRuleType);
	WRITE_ENUM_FIELD(joinType, JoinType);

	_outMultiBinaryNode(str, (const MultiBinaryNode *) node);
}


static void
_outMultiPartition(StringInfo str, const MultiPartition *node)
{
	WRITE_NODE_TYPE("MULTIPARTITION");

	WRITE_NODE_FIELD(partitionColumn);

	_outMultiUnaryNode(str, (const MultiUnaryNode *) node);
}


static void
_outMultiCartesianProduct(StringInfo str, const MultiCartesianProduct *node)
{
	WRITE_NODE_TYPE("MULTICARTESIANPRODUCT");

	_outMultiBinaryNode(str, (const MultiBinaryNode *) node);
}




static void
_outMultiExtendedOp(StringInfo str, const MultiExtendedOp *node)
{
	WRITE_NODE_TYPE("MULTIEXTENDEDOP");

	WRITE_NODE_FIELD(targetList);
	WRITE_NODE_FIELD(groupClauseList);
	WRITE_NODE_FIELD(sortClauseList);
	WRITE_NODE_FIELD(limitCount);
	WRITE_NODE_FIELD(limitOffset);

	_outMultiUnaryNode(str, (const MultiUnaryNode *) node);
}

static void
_outJobInfo(StringInfo str, const Job *node)
{
	WRITE_UINT64_FIELD(jobId);
	WRITE_NODE_FIELD(jobQuery);
	WRITE_NODE_FIELD(taskList);
	WRITE_NODE_FIELD(dependedJobList);
	WRITE_BOOL_FIELD(subqueryPushdown);
}


static void
_outJob(StringInfo str, const Job *node)
{
	WRITE_NODE_TYPE("JOB");

	_outJobInfo(str, node);
}


static void
_outShardInterval(StringInfo str, const ShardInterval *node)
{
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
		_outDatum(str, node->minValue, node->valueTypeLen, node->valueByVal);

	appendStringInfoString(str, " :maxValue ");
	if (!node->maxValueExists)
		appendStringInfoString(str, "<>");
	else
		_outDatum(str, node->maxValue, node->valueTypeLen, node->valueByVal);

	WRITE_UINT64_FIELD(shardId);
}


static void
_outMapMergeJob(StringInfo str, const MapMergeJob *node)
{
	int arrayLength = node->sortedShardIntervalArrayLength;
	int i;

	WRITE_NODE_TYPE("MAPMERGEJOB");

	_outJobInfo(str, (Job *) node);
	WRITE_NODE_FIELD(reduceQuery);
	WRITE_ENUM_FIELD(partitionType, PartitionType);
	WRITE_NODE_FIELD(partitionColumn);
	WRITE_UINT_FIELD(partitionCount);
	WRITE_INT_FIELD(sortedShardIntervalArrayLength);

	for (i = 0; i < arrayLength; ++i)
	{
		ShardInterval *writeElement = node->sortedShardIntervalArray[i];

		_outShardInterval(str, writeElement);
	}

	WRITE_NODE_FIELD(mapTaskList);
	WRITE_NODE_FIELD(mergeTaskList);
}


static void
_outShardPlacement(StringInfo str, const ShardPlacement *node)
{
	WRITE_NODE_TYPE("SHARDPLACEMENT");

	WRITE_OID_FIELD(tupleOid);
	WRITE_UINT64_FIELD(shardId);
	WRITE_UINT64_FIELD(shardLength);
	WRITE_ENUM_FIELD(shardState, RelayFileState);
	WRITE_STRING_FIELD(nodeName);
	WRITE_UINT_FIELD(nodePort);
}


static void
_outTask(StringInfo str, const Task *node)
{
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
}


/*
 * _outNode -
 *	  converts a Node into ascii string and append it to 'str'
 */
static void
_outNode(StringInfo str, const void *obj)
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
			_outMultiTreeRoot(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiProject:
			appendStringInfoChar(str, '{');
			_outMultiProject(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiCollect:
			appendStringInfoChar(str, '{');
			_outMultiCollect(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiSelect:
			appendStringInfoChar(str, '{');
			_outMultiSelect(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiTable:
			appendStringInfoChar(str, '{');
			_outMultiTable(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiJoin:
			appendStringInfoChar(str, '{');
			_outMultiJoin(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiPartition:
			appendStringInfoChar(str, '{');
			_outMultiPartition(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiCartesianProduct:
			appendStringInfoChar(str, '{');
			_outMultiCartesianProduct(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiExtendedOp:
			appendStringInfoChar(str, '{');
			_outMultiExtendedOp(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_Job:
			appendStringInfoChar(str, '{');
			_outJob(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MapMergeJob:
			appendStringInfoChar(str, '{');
			_outMapMergeJob(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_MultiPlan:
			appendStringInfoChar(str, '{');
			_outMultiPlan(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_Task:
			appendStringInfoChar(str, '{');
			_outTask(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_ShardInterval:
			appendStringInfoChar(str, '{');
			_outShardInterval(str, obj);
			appendStringInfoChar(str, '}');
			break;

		case T_ShardPlacement:
			appendStringInfoChar(str, '{');
			_outShardPlacement(str, obj);
			appendStringInfoChar(str, '}');
			break;

		default:
			/* fall back into postgres' normal nodeToString machinery */
			appendStringInfoString(str, nodeToString(obj));
	}
}


/*
 * CitusNodeToString -
 *	   returns the ascii representation of the Node as a palloc'd string
 */
char *
CitusNodeToString(const void *obj)
{
	StringInfoData str;

	initStringInfo(&str);
	_outNode(&str, obj);
	return str.data;
}
