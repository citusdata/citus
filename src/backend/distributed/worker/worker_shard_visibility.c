/*
 * worker_shard_visibility.c
 *
 * Implements the functions for hiding shards on the Citus MX
 * worker (data) nodes.
 *
 * Copyright (c) Citus Data, Inc.
 */

#include "postgres.h"

#include "miscadmin.h"

#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

#include "distributed/backend_data.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/listutils.h"
#include "distributed/local_executor.h"
#include "distributed/metadata_cache.h"
#include "distributed/query_colocation_checker.h"
#include "distributed/worker_protocol.h"
#include "distributed/worker_shard_visibility.h"


/* HideShardsMode is used to determine whether to hide shards */
typedef enum HideShardsMode
{
	CHECK_APPLICATION_NAME,
	HIDE_SHARDS_FROM_APPLICATION,
	DO_NOT_HIDE_SHARDS
} HideShardsMode;

/* Config variable managed via guc.c */
bool OverrideTableVisibility = true;
bool EnableManualChangesToShards = false;

/* show shards when the application_name starts with one of: */
char *ShowShardsForAppNamePrefixes = "";

/* cache of whether or not to hide shards */
static HideShardsMode HideShards = CHECK_APPLICATION_NAME;

static bool ShouldHideShards(void);
static bool ShouldHideShardsInternal(void);
static bool IsPgBgWorker(void);
static bool FilterShardsFromPgclass(Node *node, void *context);
static Node * CreateRelationIsAKnownShardFilter(int pgClassVarno);
static bool HasRangeTableRef(Node *node, int *varno);

PG_FUNCTION_INFO_V1(citus_table_is_visible);
PG_FUNCTION_INFO_V1(relation_is_a_known_shard);


/*
 * relation_is_a_known_shard a wrapper around RelationIsAKnownShard(), so
 * see the details there. The function also treats the indexes on shards
 * as if they were shards.
 */
Datum
relation_is_a_known_shard(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	PG_RETURN_BOOL(RelationIsAKnownShard(relationId));
}


/*
 * citus_table_is_visible aims to behave exactly the same with
 * pg_table_is_visible with only one exception. The former one
 * returns false for the relations that are known to be shards.
 */
Datum
citus_table_is_visible(PG_FUNCTION_ARGS)
{
	CheckCitusVersion(ERROR);

	Oid relationId = PG_GETARG_OID(0);
	char relKind = '\0';

	/*
	 * We don't want to deal with not valid/existing relations
	 * as pg_table_is_visible does.
	 */
	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)))
	{
		PG_RETURN_NULL();
	}

	if (!RelationIsVisible(relationId))
	{
		/* relation is not on the search path */
		PG_RETURN_BOOL(false);
	}

	if (RelationIsAKnownShard(relationId))
	{
		/*
		 * If the input relation is an index we simply replace the
		 * relationId with the corresponding relation to hide indexes
		 * as well. See RelationIsAKnownShard() for the details and give
		 * more meaningful debug message here.
		 */
		relKind = get_rel_relkind(relationId);
		if (relKind == RELKIND_INDEX || relKind == RELKIND_PARTITIONED_INDEX)
		{
			ereport(DEBUG2, (errmsg("skipping index \"%s\" since it belongs to a shard",
									get_rel_name(relationId))));
		}
		else
		{
			ereport(DEBUG2, (errmsg("skipping relation \"%s\" since it is a shard",
									get_rel_name(relationId))));
		}

		PG_RETURN_BOOL(false);
	}

	PG_RETURN_BOOL(RelationIsVisible(relationId));
}


/*
 * ErrorIfRelationIsAKnownShard errors out if the relation with relationId is
 * a shard relation.
 */
void
ErrorIfRelationIsAKnownShard(Oid relationId)
{
	if (!RelationIsAKnownShard(relationId))
	{
		return;
	}

	const char *relationName = get_rel_name(relationId);

	ereport(ERROR, (errmsg("relation \"%s\" is a shard relation ", relationName)));
}


/*
 * ErrorIfIllegallyChangingKnownShard errors out if the relation with relationId is
 * a known shard and manual changes on known shards are disabled. This is
 * valid for only non-citus (external) connections.
 */
void
ErrorIfIllegallyChangingKnownShard(Oid relationId)
{
	/* allow Citus to make changes, and allow the user if explicitly enabled */
	if (LocalExecutorShardId != INVALID_SHARD_ID ||
		IsCitusInternalBackend() ||
		IsRebalancerInternalBackend() ||
		EnableManualChangesToShards)
	{
		return;
	}

	if (RelationIsAKnownShard(relationId))
	{
		const char *relationName = get_rel_name(relationId);
		ereport(ERROR, (errmsg("cannot modify \"%s\" because it is a shard of "
							   "a distributed table",
							   relationName),
						errhint("Use the distributed table or set "
								"citus.enable_manual_changes_to_shards to on "
								"to modify shards directly")));
	}
}


/*
 * RelationIsAKnownShard gets a relationId, check whether it's a shard of
 * any distributed table.
 */
bool
RelationIsAKnownShard(Oid shardRelationId)
{
	bool missingOk = true;
	char relKind = '\0';

	if (!OidIsValid(shardRelationId))
	{
		/* we cannot continue without a valid Oid */
		return false;
	}

	if (IsCoordinator())
	{
		bool coordinatorIsKnown = false;
		PrimaryNodeForGroup(0, &coordinatorIsKnown);

		if (!coordinatorIsKnown)
		{
			/*
			 * We're not interested in shards in the coordinator
			 * or non-mx worker nodes, unless the coordinator is
			 * in pg_dist_node.
			 */
			return false;
		}
	}

	/*
	 * We do not take locks here, because that might block a query on pg_class.
	 */

	if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(shardRelationId)))
	{
		/* relation does not exist */
		return false;
	}

	/*
	 * If the input relation is an index we simply replace the
	 * relationId with the corresponding relation to hide indexes
	 * as well.
	 */
	relKind = get_rel_relkind(shardRelationId);
	if (relKind == RELKIND_INDEX || relKind == RELKIND_PARTITIONED_INDEX)
	{
		shardRelationId = IndexGetRelation(shardRelationId, false);
	}

	/* get the shard's relation name */
	char *shardRelationName = get_rel_name(shardRelationId);

	uint64 shardId = ExtractShardIdFromTableName(shardRelationName, missingOk);
	if (shardId == INVALID_SHARD_ID)
	{
		/*
		 * The format of the table name does not align with
		 * our shard name definition.
		 */
		return false;
	}

	/* try to get the relation id */
	Oid relationId = LookupShardRelationFromCatalog(shardId, true);
	if (!OidIsValid(relationId))
	{
		/* there is no such relation */
		return false;
	}

	/* verify that their namespaces are the same */
	if (get_rel_namespace(shardRelationId) != get_rel_namespace(relationId))
	{
		return false;
	}

	/*
	 * Now get the relation name and append the shardId to it. We need
	 * to do that because otherwise a local table with a valid shardId
	 * appended to its name could be misleading.
	 */
	char *generatedRelationName = get_rel_name(relationId);
	AppendShardIdToName(&generatedRelationName, shardId);
	if (strncmp(shardRelationName, generatedRelationName, NAMEDATALEN) == 0)
	{
		/* we found the distributed table that the input shard belongs to */
		return true;
	}

	return false;
}


/*
 * HideShardsFromSomeApplications transforms queries to pg_class to
 * filter out known shards if the application_name does not match any of
 * the prefixes in citus.show_shards_for_app_name_prefix.
 */
void
HideShardsFromSomeApplications(Query *query)
{
	if (!OverrideTableVisibility || HideShards == DO_NOT_HIDE_SHARDS ||
		!CitusHasBeenLoaded() || !CheckCitusVersion(DEBUG2))
	{
		return;
	}

	if (ShouldHideShards())
	{
		FilterShardsFromPgclass((Node *) query, NULL);
	}
}


/*
 * ShouldHideShards returns whether we should hide shards in the current
 * session. It only checks the application_name once and then uses a
 * cached response unless either the application_name or
 * citus.show_shards_for_app_name_prefix changes.
 */
static bool
ShouldHideShards(void)
{
	if (HideShards == CHECK_APPLICATION_NAME)
	{
		if (ShouldHideShardsInternal())
		{
			HideShards = HIDE_SHARDS_FROM_APPLICATION;
			return true;
		}
		else
		{
			HideShards = DO_NOT_HIDE_SHARDS;
			return false;
		}
	}
	else
	{
		return HideShards == HIDE_SHARDS_FROM_APPLICATION;
	}
}


/*
 * ResetHideShardsDecision resets the decision whether to hide shards.
 */
void
ResetHideShardsDecision(void)
{
	HideShards = CHECK_APPLICATION_NAME;
}


/*
 * ShouldHideShardsInternal determines whether we should hide shards based on
 * the current application name.
 */
static bool
ShouldHideShardsInternal(void)
{
	if (MyBackendType == B_BG_WORKER)
	{
		if (IsPgBgWorker())
		{
			/*
			 * If a background worker belongs to Postgres, we should
			 * never hide shards. For other background workers, enforce
			 * the application_name check below.
			 */
			return false;
		}
	}
	else if (MyBackendType != B_BACKEND && MyBackendType != B_WAL_SENDER)
	{
		/*
		 * We are aiming only to hide shards from client
		 * backends or certain background workers(see above),
		 */
		return false;
	}

	if (IsCitusInternalBackend() || IsRebalancerInternalBackend() ||
		IsCitusRunCommandBackend() || IsCitusShardTransferBackend())
	{
		/* we never hide shards from Citus */
		return false;
	}

	List *prefixList = NIL;

	/* SplitGUCList scribbles on the input */
	char *splitCopy = pstrdup(ShowShardsForAppNamePrefixes);

	if (!SplitGUCList(splitCopy, ',', &prefixList))
	{
		/* invalid GUC value, ignore */
		return true;
	}

	char *appNamePrefix = NULL;
	foreach_declared_ptr(appNamePrefix, prefixList)
	{
		/* never hide shards when one of the prefixes is * */
		if (strcmp(appNamePrefix, "*") == 0)
		{
			return false;
		}

		/* compare only the first first <prefixLength> characters */
		int prefixLength = strlen(appNamePrefix);
		if (strncmp(application_name, appNamePrefix, prefixLength) == 0)
		{
			return false;
		}
	}

	/* default behaviour: hide shards */
	return true;
}


/*
 * IsPgBgWorker returns true if the current background worker
 * belongs to Postgres.
 */
static bool
IsPgBgWorker(void)
{
	Assert(MyBackendType == B_BG_WORKER);

	if (MyBgworkerEntry)
	{
		return strcmp(MyBgworkerEntry->bgw_library_name, "postgres") == 0;
	}

	return false;
}


/*
 * FilterShardsFromPgclass adds a "relation_is_a_known_shard(oid) IS NOT TRUE"
 * filter to the quals of queries that query pg_class.
 */
static bool
FilterShardsFromPgclass(Node *node, void *context)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		MemoryContext queryContext = GetMemoryChunkContext(query);

		/*
		 * We process the whole rtable rather than visiting individual RangeTblEntry's
		 * in the walker, since we need to know the varno to generate the right
		 * filter.
		 */
		int varno = 0;
		RangeTblEntry *rangeTableEntry = NULL;

		foreach_declared_ptr(rangeTableEntry, query->rtable)
		{
			varno++;

			if (rangeTableEntry->rtekind != RTE_RELATION ||
				rangeTableEntry->relid != RelationRelationId)
			{
				/* not pg_class */
				continue;
			}

			/*
			 * Skip if pg_class is not actually queried. This is possible on
			 * INSERT statements that insert into pg_class.
			 */
			if (!expression_tree_walker((Node *) query->jointree->fromlist,
										HasRangeTableRef, &varno))
			{
				/* the query references pg_class */
				continue;
			}

			/* make sure the expression is in the right memory context */
			MemoryContext originalContext = MemoryContextSwitchTo(queryContext);

			/* add relation_is_a_known_shard(oid) IS NOT TRUE to the quals of the query */
			Node *newQual = CreateRelationIsAKnownShardFilter(varno);

#if PG_VERSION_NUM >= PG_VERSION_17

			/*
			 * In PG17, MERGE queries introduce a new struct `mergeJoinCondition`.
			 * We need to handle this condition safely.
			 */
			if (query->mergeJoinCondition != NULL)
			{
				/* Add the filter to mergeJoinCondition */
				query->mergeJoinCondition = (Node *) makeBoolExpr(
					AND_EXPR,
					list_make2(query->mergeJoinCondition, newQual),
					-1);
			}
			else
#endif
			{
				/* Handle older versions or queries without mergeJoinCondition */
				Node *oldQuals = query->jointree->quals;
				if (oldQuals)
				{
					query->jointree->quals = (Node *) makeBoolExpr(
						AND_EXPR,
						list_make2(oldQuals, newQual),
						-1);
				}
				else
				{
					query->jointree->quals = newQual;
				}
			}

			MemoryContextSwitchTo(originalContext);
		}

		return query_tree_walker((Query *) node, FilterShardsFromPgclass, context, 0);
	}

	return expression_tree_walker(node, FilterShardsFromPgclass, context);
}


/*
 * HasRangeTableRef passed to expression_tree_walker to check if a node is a
 * RangeTblRef of the given varno is present in a fromlist.
 */
static bool
HasRangeTableRef(Node *node, int *varno)
{
	if (node == NULL)
	{
		return false;
	}

	if (IsA(node, RangeTblRef))
	{
		RangeTblRef *rangeTblRef = (RangeTblRef *) node;
		return rangeTblRef->rtindex == *varno;
	}

	return expression_tree_walker(node, HasRangeTableRef, varno);
}


/*
 * CreateRelationIsAKnownShardFilter constructs an expression of the form:
 * pg_catalog.relation_is_a_known_shard(oid) IS NOT TRUE
 *
 * The difference between "NOT pg_catalog.relation_is_a_known_shard(oid)" and
 * "pg_catalog.relation_is_a_known_shard(oid) IS NOT TRUE" is that the former
 * will return FALSE if the function returns NULL, while the second will return
 * TRUE. This difference is important in the case of outer joins, because this
 * filter might be applied on an oid that is then NULL.
 */
static Node *
CreateRelationIsAKnownShardFilter(int pgClassVarno)
{
	/* oid is always the first column */
	AttrNumber oidAttNum = 1;

	Var *oidVar = makeVar(pgClassVarno, oidAttNum, OIDOID, -1, InvalidOid, 0);

	/* build the call to read_intermediate_result */
	FuncExpr *funcExpr = makeNode(FuncExpr);
	funcExpr->funcid = RelationIsAKnownShardFuncId();
	funcExpr->funcretset = false;
	funcExpr->funcvariadic = false;
	funcExpr->funcformat = 0;
	funcExpr->funccollid = 0;
	funcExpr->inputcollid = 0;
	funcExpr->location = -1;
	funcExpr->args = list_make1(oidVar);

	BooleanTest *notExpr = makeNode(BooleanTest);
	notExpr->booltesttype = IS_NOT_TRUE;
	notExpr->arg = (Expr *) funcExpr;
	notExpr->location = -1;

	return (Node *) notExpr;
}
