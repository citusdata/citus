/*-------------------------------------------------------------------------
 *
 * citus_nodefuncs.h
 *	  Node (de-)serialization support for Citus.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_NODEFUNCS_H
#define CITUS_NODEFUNCS_H

#include "distributed/multi_physical_planner.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"

/* citus_nodefuncs.c */
extern void SetRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind rteKind,
								 char *fragmentSchemaName, char *fragmentTableName,
								 List *tableIdList);
extern void ModifyRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind rteKind,
									char *fragmentSchemaName, char *fragmentTableName,
									List *tableIdList);
extern void ExtractRangeTblExtraData(RangeTblEntry *rte, CitusRTEKind *rteKind,
									 char **fragmentSchemaName, char **fragmentTableName,
									 List **tableIdList);
extern CitusRTEKind GetRangeTblKind(RangeTblEntry *rte);

/* citus_outfuncs.c */
extern char * CitusNodeToString(const void *obj);

/* citus_read.c */
extern void * CitusStringToNode(char *str);
extern char * citus_pg_strtok(int *length);
extern void * CitusNodeRead(char *token, int tok_len);

/* citus_readfuncs.c */
extern Node * CitusParseNodeString(void);
extern Datum readDatum(bool typbyval);

extern void RegisterNodes(void);

/*
 * Define read functions for citus nodes in a way they're usable across
 * several major versions. That requires some macro-uglyness as 9.6+ is quite
 * different from before.
 */

#if (PG_VERSION_NUM >= 90600)
#define READFUNC_ARGS struct ExtensibleNode *node
#define READFUNC_RET void
#else
#define READFUNC_ARGS void
#define READFUNC_RET Node *
#endif

#if (PG_VERSION_NUM >= 90600)
#define OUTFUNC_ARGS StringInfo str, const struct ExtensibleNode *raw_node
#else
#define OUTFUNC_ARGS StringInfo str, const Node *raw_node
#endif

extern READFUNC_RET ReadJob(READFUNC_ARGS);
extern READFUNC_RET ReadMultiPlan(READFUNC_ARGS);
extern READFUNC_RET ReadShardInterval(READFUNC_ARGS);
extern READFUNC_RET ReadMapMergeJob(READFUNC_ARGS);
extern READFUNC_RET ReadShardPlacement(READFUNC_ARGS);
extern READFUNC_RET ReadRelationShard(READFUNC_ARGS);
extern READFUNC_RET ReadTask(READFUNC_ARGS);

extern READFUNC_RET ReadUnsupportedCitusNode(READFUNC_ARGS);

extern void OutJob(OUTFUNC_ARGS);
extern void OutMultiPlan(OUTFUNC_ARGS);
extern void OutShardInterval(OUTFUNC_ARGS);
extern void OutMapMergeJob(OUTFUNC_ARGS);
extern void OutShardPlacement(OUTFUNC_ARGS);
extern void OutRelationShard(OUTFUNC_ARGS);
extern void OutTask(OUTFUNC_ARGS);

extern void OutMultiNode(OUTFUNC_ARGS);
extern void OutMultiTreeRoot(OUTFUNC_ARGS);
extern void OutMultiProject(OUTFUNC_ARGS);
extern void OutMultiCollect(OUTFUNC_ARGS);
extern void OutMultiSelect(OUTFUNC_ARGS);
extern void OutMultiTable(OUTFUNC_ARGS);
extern void OutMultiJoin(OUTFUNC_ARGS);
extern void OutMultiPartition(OUTFUNC_ARGS);
extern void OutMultiCartesianProduct(OUTFUNC_ARGS);
extern void OutMultiExtendedOp(OUTFUNC_ARGS);

#endif /* CITUS_NODEFUNCS_H */
