/*
 * relation_restriction_equivalence.c
 *
 * This file contains functions helper functions for planning
 * queries with colocated tables and subqueries.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "distributed/pg_version_constants.h"

#include "distributed/colocation_utils.h"
#include "distributed/distributed_planner.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/multi_router_planner.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/query_utils.h"
#include "distributed/relation_restriction_equivalence.h"
#include "distributed/shard_pruning.h"

#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "nodes/makefuncs.h"
#include "optimizer/paths.h"
#include "parser/parsetree.h"
#include "optimizer/pathnode.h"


static uint32 AttributeEquivalenceId = 1;


/*
 * AttributeEquivalenceClass
 *
 * Whenever we find an equality clause A = B, where both A and B originates from
 * relation attributes (i.e., not random expressions), we create an
 * AttributeEquivalenceClass to record this knowledge. If we later find another
 * equivalence B = C, we create another AttributeEquivalenceClass. Finally, we can
 * apply transitivity rules and generate a new AttributeEquivalenceClass which includes
 * A, B and C.
 *
 * Note that equality among the members are identified by the varattno and rteIdentity.
 */
typedef struct AttributeEquivalenceClass
{
	uint32 equivalenceId;
	List *equivalentAttributes;

	Index unionQueryPartitionKeyIndex;
} AttributeEquivalenceClass;

typedef struct FindQueryContainingRteIdentityContext
{
	int targetRTEIdentity;
	Query *query;
}FindQueryContainingRteIdentityContext;

/*
 *  AttributeEquivalenceClassMember - one member expression of an
 *  AttributeEquivalenceClass. The important thing to consider is that
 *  the class member contains "rteIndentity" field. Note that each RTE_RELATION
 *  is assigned a unique rteIdentity in AssignRTEIdentities() function.
 *
 *  "varno" and "varattno" is directly used from a Var clause that is being added
 *  to the attribute equivalence. Since we only use this class for relations, the member
 *  also includes the relation id field.
 */
typedef struct AttributeEquivalenceClassMember
{
	Oid relationId;
	int rteIdentity;
	Index varno;
	AttrNumber varattno;
} AttributeEquivalenceClassMember;


static bool ContextContainsLocalRelation(RelationRestrictionContext *restrictionContext);
static bool ContextContainsAppendRelation(RelationRestrictionContext *restrictionContext);
static int RangeTableOffsetCompat(PlannerInfo *root, AppendRelInfo *appendRelInfo);
static Var * FindUnionAllVar(PlannerInfo *root, List *translatedVars, Oid relationOid,
							 Index relationRteIndex, Index *partitionKeyIndex);
static bool ContainsMultipleDistributedRelations(PlannerRestrictionContext *
												 plannerRestrictionContext);
static List * GenerateAttributeEquivalencesForRelationRestrictions(
	RelationRestrictionContext *restrictionContext);
static AttributeEquivalenceClass * AttributeEquivalenceClassForEquivalenceClass(
	EquivalenceClass *plannerEqClass, RelationRestriction *relationRestriction);
static void AddToAttributeEquivalenceClass(AttributeEquivalenceClass *
										   attributeEquivalenceClass,
										   PlannerInfo *root, Var *varToBeAdded);
static void AddRteSubqueryToAttributeEquivalenceClass(AttributeEquivalenceClass *
													  attributeEquivalenceClass,
													  RangeTblEntry *
													  rangeTableEntry,
													  PlannerInfo *root,
													  Var *varToBeAdded);
static Query * GetTargetSubquery(PlannerInfo *root, RangeTblEntry *rangeTableEntry,
								 Var *varToBeAdded);
static void AddUnionAllSetOperationsToAttributeEquivalenceClass(
	AttributeEquivalenceClass *
	attributeEquivalenceClass,
	PlannerInfo *root,
	Var *varToBeAdded);
static void AddUnionSetOperationsToAttributeEquivalenceClass(AttributeEquivalenceClass *
															 attributeEquivalenceClass,
															 PlannerInfo *root,
															 SetOperationStmt *
															 setOperation,
															 Var *varToBeAdded);
static void AddRteRelationToAttributeEquivalenceClass(AttributeEquivalenceClass *
													  attrEquivalenceClass,
													  RangeTblEntry *rangeTableEntry,
													  Var *varToBeAdded);
static Var * GetVarFromAssignedParam(List *outerPlanParamsList, Param *plannerParam,
									 PlannerInfo **rootContainingVar);
static Var * SearchPlannerParamList(List *plannerParamList, Param *plannerParam);
static List * GenerateAttributeEquivalencesForJoinRestrictions(JoinRestrictionContext
															   *joinRestrictionContext);
static bool AttributeClassContainsAttributeClassMember(AttributeEquivalenceClassMember *
													   inputMember,
													   AttributeEquivalenceClass *
													   attributeEquivalenceClass);
static List * AddAttributeClassToAttributeClassList(List *attributeEquivalenceList,
													AttributeEquivalenceClass *
													attributeEquivalence);
static bool AttributeEquivalencesAreEqual(AttributeEquivalenceClass *
										  firstAttributeEquivalence,
										  AttributeEquivalenceClass *
										  secondAttributeEquivalence);
static AttributeEquivalenceClass * GenerateCommonEquivalence(List *
															 attributeEquivalenceList,
															 RelationRestrictionContext *
															 relationRestrictionContext);
static AttributeEquivalenceClass * GenerateEquivalenceClassForRelationRestriction(
	RelationRestrictionContext
	*
	relationRestrictionContext);
static void ListConcatUniqueAttributeClassMemberLists(AttributeEquivalenceClass *
													  firstClass,
													  AttributeEquivalenceClass *
													  secondClass);
static Var * PartitionKeyForRTEIdentityInQuery(Query *query, int targetRTEIndex,
											   Index *partitionKeyIndex);
static bool AllDistributedRelationsInRestrictionContextColocated(
	RelationRestrictionContext *
	restrictionContext);
static bool IsNotSafeRestrictionToRecursivelyPlan(Node *node);
static bool HasPlaceHolderVar(Node *node);
static JoinRestrictionContext * FilterJoinRestrictionContext(
	JoinRestrictionContext *joinRestrictionContext, Relids
	queryRteIdentities);
static bool RangeTableArrayContainsAnyRTEIdentities(RangeTblEntry **rangeTableEntries, int
													rangeTableArrayLength, Relids
													queryRteIdentities);
static Relids QueryRteIdentities(Query *queryTree);

static Query * FindQueryContainingRTEIdentity(Query *mainQuery, int rteIndex);
static bool FindQueryContainingRTEIdentityInternal(Node *node,
												   FindQueryContainingRteIdentityContext *
												   context);

static int ParentCountPriorToAppendRel(List *appendRelList, AppendRelInfo *appendRelInfo);


/*
 * AllDistributionKeysInQueryAreEqual returns true if either
 *    (i)  there exists join in the query and all relations joined on their
 *         partition keys
 *    (ii) there exists only union set operations and all relations has
 *         partition keys in the same ordinal position in the query
 */
bool
AllDistributionKeysInQueryAreEqual(Query *originalQuery,
								   PlannerRestrictionContext *plannerRestrictionContext)
{
	/* we don't support distribution key equality checks for CTEs yet */
	if (originalQuery->cteList != NIL)
	{
		return false;
	}

	/* we don't support distribution key equality checks for local tables */
	RelationRestrictionContext *restrictionContext =
		plannerRestrictionContext->relationRestrictionContext;
	if (ContextContainsLocalRelation(restrictionContext))
	{
		return false;
	}

	bool restrictionEquivalenceForPartitionKeys =
		RestrictionEquivalenceForPartitionKeys(plannerRestrictionContext);
	if (restrictionEquivalenceForPartitionKeys)
	{
		return true;
	}

	if (originalQuery->setOperations || ContainsUnionSubquery(originalQuery))
	{
		return SafeToPushdownUnionSubquery(originalQuery, plannerRestrictionContext);
	}

	return false;
}


/*
 * ContextContainsLocalRelation determines whether the given
 * RelationRestrictionContext contains any local tables.
 */
static bool
ContextContainsLocalRelation(RelationRestrictionContext *restrictionContext)
{
	ListCell *relationRestrictionCell = NULL;

	foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction = lfirst(relationRestrictionCell);

		if (!relationRestriction->citusTable)
		{
			return true;
		}
	}

	return false;
}


/*
 * ContextContainsAppendRelation determines whether the given
 * RelationRestrictionContext contains any append-distributed tables.
 */
static bool
ContextContainsAppendRelation(RelationRestrictionContext *restrictionContext)
{
	ListCell *relationRestrictionCell = NULL;

	foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction = lfirst(relationRestrictionCell);

		if (IsCitusTableType(relationRestriction->relationId, APPEND_DISTRIBUTED))
		{
			return true;
		}
	}

	return false;
}


/*
 * SafeToPushdownUnionSubquery returns true if all the relations are returns
 * partition keys in the same ordinal position and there is no reference table
 * exists.
 *
 * Note that the function expects (and asserts) the input query to be a top
 * level union query defined by TopLevelUnionQuery().
 *
 * Lastly, the function fails to produce correct output if the target lists contains
 * multiple partition keys on the target list such as the following:
 *
 *   select count(*) from (
 *       select user_id, user_id from users_table
 *   union
 *       select 2, user_id from users_table) u;
 *
 * For the above query, although the second item in the target list make this query
 * safe to push down, the function would fail to return true.
 */
bool
SafeToPushdownUnionSubquery(Query *originalQuery,
							PlannerRestrictionContext *plannerRestrictionContext)
{
	RelationRestrictionContext *restrictionContext =
		plannerRestrictionContext->relationRestrictionContext;
	JoinRestrictionContext *joinRestrictionContext =
		plannerRestrictionContext->joinRestrictionContext;

	AttributeEquivalenceClass *attributeEquivalence =
		palloc0(sizeof(AttributeEquivalenceClass));
	ListCell *relationRestrictionCell = NULL;

	attributeEquivalence->equivalenceId = AttributeEquivalenceId++;

	/*
	 * Ensure that the partition column is in the same place across all
	 * leaf queries in the UNION and construct an equivalence class for
	 * these columns.
	 */
	foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction = lfirst(relationRestrictionCell);
		Index partitionKeyIndex = InvalidAttrNumber;
		PlannerInfo *relationPlannerRoot = relationRestriction->plannerInfo;

		int targetRTEIndex = GetRTEIdentity(relationRestriction->rte);
		Var *varToBeAdded =
			PartitionKeyForRTEIdentityInQuery(originalQuery, targetRTEIndex,
											  &partitionKeyIndex);

		/* union does not have partition key in the target list */
		if (partitionKeyIndex == 0)
		{
			continue;
		}

		/*
		 * This should never happen but to be on the safe side, we have this
		 */
		if (relationPlannerRoot->simple_rel_array_size < relationRestriction->index)
		{
			continue;
		}

		/*
		 * We update the varno because we use the original parse tree for finding the
		 * var. However the rest of the code relies on a query tree that might be different
		 * than the original parse tree because of postgres optimizations.
		 * That's why we update the varno to reflect the rteIndex in the modified query tree.
		 */
		varToBeAdded->varno = relationRestriction->index;


		/*
		 * The current relation does not have its partition key in the target list.
		 */
		if (partitionKeyIndex == InvalidAttrNumber)
		{
			continue;
		}

		/*
		 * We find the first relations partition key index in the target list. Later,
		 * we check whether all the relations have partition keys in the
		 * same position.
		 */
		if (attributeEquivalence->unionQueryPartitionKeyIndex == InvalidAttrNumber)
		{
			attributeEquivalence->unionQueryPartitionKeyIndex = partitionKeyIndex;
		}
		else if (attributeEquivalence->unionQueryPartitionKeyIndex != partitionKeyIndex)
		{
			continue;
		}

		Assert(varToBeAdded != NULL);
		AddToAttributeEquivalenceClass(attributeEquivalence, relationPlannerRoot,
									   varToBeAdded);
	}

	/*
	 * For queries of the form:
	 * (SELECT ... FROM a JOIN b ...) UNION (SELECT .. FROM c JOIN d ... )
	 *
	 * we determine whether all relations are joined on the partition column
	 * by adding the equivalence classes that can be inferred from joins.
	 */
	List *relationRestrictionAttributeEquivalenceList =
		GenerateAttributeEquivalencesForRelationRestrictions(restrictionContext);
	List *joinRestrictionAttributeEquivalenceList =
		GenerateAttributeEquivalencesForJoinRestrictions(joinRestrictionContext);

	List *allAttributeEquivalenceList =
		list_concat(relationRestrictionAttributeEquivalenceList,
					joinRestrictionAttributeEquivalenceList);

	allAttributeEquivalenceList = lappend(allAttributeEquivalenceList,
										  attributeEquivalence);

	if (!EquivalenceListContainsRelationsEquality(allAttributeEquivalenceList,
												  restrictionContext))
	{
		/* cannot confirm equality for all distribution colums */
		return false;
	}

	if (!AllDistributedRelationsInRestrictionContextColocated(restrictionContext))
	{
		/* distribution columns are equal, but tables are not co-located */
		return false;
	}

	return true;
}


/*
 * RangeTableOffsetCompat returns the range table offset(in glob->finalrtable) for the appendRelInfo.
 */
static int
RangeTableOffsetCompat(PlannerInfo *root, AppendRelInfo *appendRelInfo)
{
	int parentCount = ParentCountPriorToAppendRel(root->append_rel_list, appendRelInfo);
	int skipParentCount = parentCount - 1;

	int i = 1;
	for (; i < root->simple_rel_array_size; i++)
	{
		RangeTblEntry *rte = root->simple_rte_array[i];
		if (rte->inh)
		{
			/*
			 * We skip the previous parents because we want to find the offset
			 * for the given append rel info.
			 */
			if (skipParentCount > 0)
			{
				skipParentCount--;
				continue;
			}
			break;
		}
	}
	int indexInRtable = (i - 1);

	/*
	 * Postgres adds the global rte array size to parent_relid as an offset.
	 * Here we do the reverse operation: Commit on postgres side:
	 * 6ef77cf46e81f45716ec981cb08781d426181378
	 */
	int parentRelIndex = appendRelInfo->parent_relid - 1;
	return parentRelIndex - indexInRtable;
}


/*
 * FindUnionAllVar finds the variable used in union all for the side that has
 * relationRteIndex as its index and the same varattno as the partition key of
 * the given relation with relationOid.
 */
static Var *
FindUnionAllVar(PlannerInfo *root, List *translatedVars, Oid relationOid,
				Index relationRteIndex, Index *partitionKeyIndex)
{
	if (!IsCitusTableType(relationOid, STRICTLY_PARTITIONED_DISTRIBUTED_TABLE))
	{
		/* we only care about hash and range partitioned tables */
		*partitionKeyIndex = 0;
		return NULL;
	}

	Var *relationPartitionKey = DistPartitionKeyOrError(relationOid);

	AttrNumber childAttrNumber = 0;
	*partitionKeyIndex = 0;
	ListCell *translatedVarCell;
	foreach(translatedVarCell, translatedVars)
	{
		Node *targetNode = (Node *) lfirst(translatedVarCell);
		childAttrNumber++;

		if (!IsA(targetNode, Var))
		{
			continue;
		}

		Var *targetVar = (Var *) lfirst(translatedVarCell);
		if (targetVar->varno == relationRteIndex &&
			targetVar->varattno == relationPartitionKey->varattno)
		{
			*partitionKeyIndex = childAttrNumber;

			return targetVar;
		}
	}
	return NULL;
}


/*
 * RestrictionEquivalenceForPartitionKeys aims to deduce whether each of the RTE_RELATION
 * is joined with at least one another RTE_RELATION on their partition keys. If each
 * RTE_RELATION follows the above rule, we can conclude that all RTE_RELATIONs are
 * joined on their partition keys.
 *
 * Before doing the expensive equality checks, we do a cheaper check to understand
 * whether there are more than one distributed relations. Otherwise, we exit early.
 *
 * The function returns true if all relations are joined on their partition keys.
 * Otherwise, the function returns false. We ignore reference tables at all since
 * they don't have partition keys.
 *
 * In order to do that, we invented a new equivalence class namely:
 * AttributeEquivalenceClass. In very simple words, a AttributeEquivalenceClass is
 * identified by an unique id and consists of a list of AttributeEquivalenceMembers.
 *
 * Each AttributeEquivalenceMember is designed to identify attributes uniquely within the
 * whole query. The necessity of this arise since varno attributes are defined within
 * a single level of a query. Instead, here we want to identify each RTE_RELATION uniquely
 * and try to find equality among each RTE_RELATION's partition key.
 *
 * Each equality among RTE_RELATION is saved using an AttributeEquivalenceClass where
 * each member attribute is identified by a AttributeEquivalenceMember. In the final
 * step, we try generate a common attribute equivalence class that holds as much as
 * AttributeEquivalenceMembers whose attributes are a partition keys.
 *
 * RestrictionEquivalenceForPartitionKeys uses both relation restrictions and join restrictions
 * to find as much as information that Postgres planner provides to extensions. For the
 * details of the usage, please see GenerateAttributeEquivalencesForRelationRestrictions()
 * and GenerateAttributeEquivalencesForJoinRestrictions().
 */
bool
RestrictionEquivalenceForPartitionKeys(PlannerRestrictionContext *restrictionContext)
{
	if (ContextContainsLocalRelation(restrictionContext->relationRestrictionContext))
	{
		return false;
	}
	else if (!ContainsMultipleDistributedRelations(restrictionContext))
	{
		/* there is a single distributed relation, no need to continue */
		return true;
	}
	else if (ContextContainsAppendRelation(
				 restrictionContext->relationRestrictionContext))
	{
		/* we never consider append-distributed tables co-located */
		return false;
	}

	List *attributeEquivalenceList = GenerateAllAttributeEquivalences(restrictionContext);

	return RestrictionEquivalenceForPartitionKeysViaEquivalences(restrictionContext,
																 attributeEquivalenceList);
}


/*
 * RestrictionEquivalenceForPartitionKeysViaEquivalences follows the same rules
 * with RestrictionEquivalenceForPartitionKeys(). The only difference is that
 * this function allows passing pre-computed attribute equivalences along with
 * the planner restriction context.
 */
bool
RestrictionEquivalenceForPartitionKeysViaEquivalences(PlannerRestrictionContext *
													  plannerRestrictionContext,
													  List *allAttributeEquivalenceList)
{
	RelationRestrictionContext *restrictionContext =
		plannerRestrictionContext->relationRestrictionContext;

	/* there is a single distributed relation, no need to continue */
	if (!ContainsMultipleDistributedRelations(plannerRestrictionContext))
	{
		return true;
	}

	return EquivalenceListContainsRelationsEquality(allAttributeEquivalenceList,
													restrictionContext);
}


/*
 * ContainsMultipleDistributedRelations returns true if the input planner
 * restriction context contains more than one distributed relation.
 */
static bool
ContainsMultipleDistributedRelations(PlannerRestrictionContext *
									 plannerRestrictionContext)
{
	RelationRestrictionContext *restrictionContext =
		plannerRestrictionContext->relationRestrictionContext;

	uint32 distributedRelationCount =
		UniqueRelationCount(restrictionContext, DISTRIBUTED_TABLE);

	/*
	 * If the query includes a single relation which is not a reference table,
	 * we should not check the partition column equality.
	 * Consider two example cases:
	 *   (i)   The query includes only a single colocated relation
	 *   (ii)  A colocated relation is joined with a (or multiple) reference
	 *         table(s) where colocated relation is not joined on the partition key
	 *
	 * For the above two cases, we don't need to execute the partition column equality
	 * algorithm. The reason is that the essence of this function is to ensure that the
	 * tasks that are going to be created should not need data from other tasks. In both
	 * cases mentioned above, the necessary data per task would be on available.
	 */
	if (distributedRelationCount <= 1)
	{
		return false;
	}

	return true;
}


/*
 * GenerateAllAttributeEquivalences gets the planner restriction context and returns
 * the list of all attribute equivalences based on both join restrictions and relation
 * restrictions.
 */
List *
GenerateAllAttributeEquivalences(PlannerRestrictionContext *plannerRestrictionContext)
{
	RelationRestrictionContext *relationRestrictionContext =
		plannerRestrictionContext->relationRestrictionContext;
	JoinRestrictionContext *joinRestrictionContext =
		plannerRestrictionContext->joinRestrictionContext;

	/* reset the equivalence id counter per call to prevent overflows */
	AttributeEquivalenceId = 1;

	List *relationRestrictionAttributeEquivalenceList =
		GenerateAttributeEquivalencesForRelationRestrictions(relationRestrictionContext);
	List *joinRestrictionAttributeEquivalenceList =
		GenerateAttributeEquivalencesForJoinRestrictions(joinRestrictionContext);

	List *allAttributeEquivalenceList = list_concat(
		relationRestrictionAttributeEquivalenceList,
		joinRestrictionAttributeEquivalenceList);

	return allAttributeEquivalenceList;
}


/*
 * UniqueRelationCount iterates over the relations and returns the
 * unique relation count. We use RTEIdentity as the identifiers, so if
 * the same relation appears twice in the restrictionContext, we count
 * it as a single item.
 */
uint32
UniqueRelationCount(RelationRestrictionContext *restrictionContext, CitusTableType
					tableType)
{
	ListCell *relationRestrictionCell = NULL;
	List *rteIdentityList = NIL;

	foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(relationRestrictionCell);
		Oid relationId = relationRestriction->relationId;

		CitusTableCacheEntry *cacheEntry = LookupCitusTableCacheEntry(relationId);
		if (cacheEntry == NULL)
		{
			/* we  don't expect non-distributed tables, still be no harm to skip */
			continue;
		}

		if (IsCitusTableTypeCacheEntry(cacheEntry, tableType))
		{
			int rteIdentity = GetRTEIdentity(relationRestriction->rte);
			rteIdentityList = list_append_unique_int(rteIdentityList, rteIdentity);
		}
	}

	return list_length(rteIdentityList);
}


/*
 * EquivalenceListContainsRelationsEquality gets a list of attributed equivalence
 * list and a relation restriction context. The function first generates a common
 * equivalence class out of the attributeEquivalenceList. Later, the function checks
 * whether all the relations exists in the common equivalence class.
 *
 */
bool
EquivalenceListContainsRelationsEquality(List *attributeEquivalenceList,
										 RelationRestrictionContext *restrictionContext)
{
	ListCell *commonEqClassCell = NULL;
	ListCell *relationRestrictionCell = NULL;
	Relids commonRteIdentities = NULL;

	/*
	 * In general we're trying to expand existing the equivalence classes to find a
	 * common equivalence class. The main goal is to test whether this main class
	 * contains all partition keys of the existing relations.
	 */
	AttributeEquivalenceClass *commonEquivalenceClass = GenerateCommonEquivalence(
		attributeEquivalenceList,
		restrictionContext);

	/* add the rte indexes of relations to a bitmap */
	foreach(commonEqClassCell, commonEquivalenceClass->equivalentAttributes)
	{
		AttributeEquivalenceClassMember *classMember =
			(AttributeEquivalenceClassMember *) lfirst(commonEqClassCell);
		int rteIdentity = classMember->rteIdentity;

		commonRteIdentities = bms_add_member(commonRteIdentities, rteIdentity);
	}

	/* check whether all relations exists in the main restriction list */
	foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(relationRestrictionCell);
		int rteIdentity = GetRTEIdentity(relationRestriction->rte);

		/* we shouldn't check for the equality of non-distributed tables */
		if (IsCitusTable(relationRestriction->relationId) &&
			!HasDistributionKey(relationRestriction->relationId))
		{
			continue;
		}

		if (!bms_is_member(rteIdentity, commonRteIdentities))
		{
			return false;
		}
	}

	return true;
}


/*
 * GenerateAttributeEquivalencesForRelationRestrictions gets a relation restriction
 * context and returns a list of AttributeEquivalenceClass.
 *
 * The algorithm followed can be summarized as below:
 *
 * - Per relation restriction
 *     - Per plannerInfo's eq_class
 *         - Create an AttributeEquivalenceClass
 *         - Add all Vars that appear in the plannerInfo's
 *           eq_class to the AttributeEquivalenceClass
 *               - While doing that, consider LATERAL vars as well.
 *                 See GetVarFromAssignedParam() for the details. Note
 *                 that we're using parentPlannerInfo while adding the
 *                 LATERAL vars given that we rely on that plannerInfo.
 *
 */
static List *
GenerateAttributeEquivalencesForRelationRestrictions(RelationRestrictionContext
													 *restrictionContext)
{
	List *attributeEquivalenceList = NIL;
	ListCell *relationRestrictionCell = NULL;

	if (restrictionContext == NULL)
	{
		return attributeEquivalenceList;
	}

	foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(relationRestrictionCell);
		List *equivalenceClasses = relationRestriction->plannerInfo->eq_classes;
		ListCell *equivalenceClassCell = NULL;

		foreach(equivalenceClassCell, equivalenceClasses)
		{
			EquivalenceClass *plannerEqClass =
				(EquivalenceClass *) lfirst(equivalenceClassCell);

			AttributeEquivalenceClass *attributeEquivalence =
				AttributeEquivalenceClassForEquivalenceClass(plannerEqClass,
															 relationRestriction);

			attributeEquivalenceList =
				AddAttributeClassToAttributeClassList(attributeEquivalenceList,
													  attributeEquivalence);
		}
	}

	return attributeEquivalenceList;
}


/*
 * AttributeEquivalenceClassForEquivalenceClass is a helper function for
 * GenerateAttributeEquivalencesForRelationRestrictions. The function takes an
 * EquivalenceClass and the relation restriction that the equivalence class
 * belongs to. The function returns an AttributeEquivalenceClass that is composed
 * of ec_members that are simple Var references.
 *
 * The function also takes case of LATERAL joins by simply replacing the PARAM_EXEC
 * with the corresponding expression.
 */
static AttributeEquivalenceClass *
AttributeEquivalenceClassForEquivalenceClass(EquivalenceClass *plannerEqClass,
											 RelationRestriction *relationRestriction)
{
	AttributeEquivalenceClass *attributeEquivalence =
		palloc0(sizeof(AttributeEquivalenceClass));
	ListCell *equivilanceMemberCell = NULL;
	PlannerInfo *plannerInfo = relationRestriction->plannerInfo;

	attributeEquivalence->equivalenceId = AttributeEquivalenceId++;

	foreach(equivilanceMemberCell, plannerEqClass->ec_members)
	{
		EquivalenceMember *equivalenceMember =
			(EquivalenceMember *) lfirst(equivilanceMemberCell);
		Node *equivalenceNode = strip_implicit_coercions(
			(Node *) equivalenceMember->em_expr);
		Expr *strippedEquivalenceExpr = (Expr *) equivalenceNode;

		Var *expressionVar = NULL;

		if (IsA(strippedEquivalenceExpr, Param))
		{
			PlannerInfo *outerNodeRoot = NULL;
			Param *equivalenceParam = (Param *) strippedEquivalenceExpr;

			expressionVar =
				GetVarFromAssignedParam(relationRestriction->outerPlanParamsList,
										equivalenceParam, &outerNodeRoot);
			if (expressionVar)
			{
				AddToAttributeEquivalenceClass(attributeEquivalence, outerNodeRoot,
											   expressionVar);
			}
		}
		else if (IsA(strippedEquivalenceExpr, Var))
		{
			expressionVar = (Var *) strippedEquivalenceExpr;
			AddToAttributeEquivalenceClass(attributeEquivalence, plannerInfo,
										   expressionVar);
		}
	}

	return attributeEquivalence;
}


/*
 * GetVarFromAssignedParam returns the Var that is assigned to the given
 * plannerParam if its kind is PARAM_EXEC.
 *
 * If the paramkind is not equal to PARAM_EXEC the function returns NULL. Similarly,
 * if there is no Var corresponding to the given param is, the function returns NULL.
 *
 * Rationale behind this function:
 *
 *   While iterating through the equivalence classes of RTE_RELATIONs, we
 *   observe that there are PARAM type of equivalence member expressions for
 *   the RTE_RELATIONs which actually belong to lateral vars from the other query
 *   levels.
 *
 *   We're also keeping track of the RTE_RELATION's outer nodes'
 *   plan_params lists which is expected to hold the parameters that are required
 *   for its lower level queries as it is documented:
 *
 *        plan_params contains the expressions that this query level needs to
 *        make available to a lower query level that is currently being planned.
 *
 *   This function is a helper function to iterate through the outer node's query's
 *   plan_params and looks for the param that the equivalence member has. The
 *   comparison is done via the "paramid" field. Finally, if the found parameter's
 *   item is a Var, we conclude that Postgres standard_planner replaced the Var
 *   with the Param on assign_param_for_var() function
 *   @src/backend/optimizer/plan/subselect.c.
 */
static Var *
GetVarFromAssignedParam(List *outerPlanParamsList, Param *plannerParam,
						PlannerInfo **rootContainingVar)
{
	Var *assignedVar = NULL;
	ListCell *rootPlanParamsCell = NULL;

	Assert(plannerParam != NULL);

	/* we're only interested in parameters that Postgres added for execution */
	if (plannerParam->paramkind != PARAM_EXEC)
	{
		return NULL;
	}

	foreach(rootPlanParamsCell, outerPlanParamsList)
	{
		RootPlanParams *outerPlanParams = lfirst(rootPlanParamsCell);

		assignedVar = SearchPlannerParamList(outerPlanParams->plan_params,
											 plannerParam);
		if (assignedVar != NULL)
		{
			*rootContainingVar = outerPlanParams->root;
			break;
		}
	}

	return assignedVar;
}


/*
 * SearchPlannerParamList searches in plannerParamList and returns the Var that
 * corresponds to the given plannerParam. If there is no Var corresponding to the
 * given param is, the function returns NULL.
 */
static Var *
SearchPlannerParamList(List *plannerParamList, Param *plannerParam)
{
	Var *assignedVar = NULL;
	ListCell *plannerParameterCell = NULL;

	foreach(plannerParameterCell, plannerParamList)
	{
		PlannerParamItem *plannerParamItem =
			(PlannerParamItem *) lfirst(plannerParameterCell);

		if (plannerParamItem->paramId != plannerParam->paramid)
		{
			continue;
		}

		/* TODO: Should we consider PlaceHolderVar? */
		if (!IsA(plannerParamItem->item, Var))
		{
			continue;
		}

		assignedVar = (Var *) plannerParamItem->item;

		break;
	}

	return assignedVar;
}


/*
 * GenerateCommonEquivalence gets a list of unrelated AttributeEquiavalenceClass
 * whose all members are partition keys.
 *
 * With the equivalence classes, the function follows the algorithm
 * outlined below:
 *
 *     - Add the first equivalence class to the common equivalence class
 *     - Then, iterate on the remaining equivalence classes
 *          - If any of the members equal to the common equivalence class
 *            add all the members of the equivalence class to the common
 *            class
 *          - Start the iteration from the beginning. The reason is that
 *            in case any of the classes we've passed is equivalent to the
 *            newly added one. To optimize the algorithm, we utilze the
 *            equivalence class ids and skip the ones that are already added.
 *      - Finally, return the common equivalence class.
 */
static AttributeEquivalenceClass *
GenerateCommonEquivalence(List *attributeEquivalenceList,
						  RelationRestrictionContext *relationRestrictionContext)
{
	Bitmapset *addedEquivalenceIds = NULL;
	uint32 equivalenceListSize = list_length(attributeEquivalenceList);
	uint32 equivalenceClassIndex = 0;

	AttributeEquivalenceClass *commonEquivalenceClass = palloc0(
		sizeof(AttributeEquivalenceClass));
	commonEquivalenceClass->equivalenceId = 0;

	/*
	 * We seed the common equivalence class with a the first distributed
	 * table since we always want the input distributed relations to be
	 * on the common class.
	 */
	AttributeEquivalenceClass *firstEquivalenceClass =
		GenerateEquivalenceClassForRelationRestriction(relationRestrictionContext);

	/* we skip the calculation if there are not enough information */
	if (equivalenceListSize < 1 || firstEquivalenceClass == NULL)
	{
		return commonEquivalenceClass;
	}

	commonEquivalenceClass->equivalentAttributes =
		firstEquivalenceClass->equivalentAttributes;
	addedEquivalenceIds = bms_add_member(addedEquivalenceIds,
										 firstEquivalenceClass->equivalenceId);

	while (equivalenceClassIndex < equivalenceListSize)
	{
		ListCell *equivalenceMemberCell = NULL;
		bool restartLoop = false;

		AttributeEquivalenceClass *currentEquivalenceClass = list_nth(
			attributeEquivalenceList,
			equivalenceClassIndex);

		/*
		 * This is an optimization. If we already added the same equivalence class,
		 * we could skip it since we've already added all the relevant equivalence
		 * members.
		 */
		if (bms_is_member(currentEquivalenceClass->equivalenceId, addedEquivalenceIds))
		{
			equivalenceClassIndex++;

			continue;
		}

		foreach(equivalenceMemberCell, currentEquivalenceClass->equivalentAttributes)
		{
			AttributeEquivalenceClassMember *attributeEquialanceMember =
				(AttributeEquivalenceClassMember *) lfirst(equivalenceMemberCell);

			if (AttributeClassContainsAttributeClassMember(attributeEquialanceMember,
														   commonEquivalenceClass))
			{
				ListConcatUniqueAttributeClassMemberLists(commonEquivalenceClass,
														  currentEquivalenceClass);

				addedEquivalenceIds = bms_add_member(addedEquivalenceIds,
													 currentEquivalenceClass->
													 equivalenceId);

				/*
				 * It seems inefficient to start from the beginning.
				 * But, we should somehow restart from the beginning to test that
				 * whether the already skipped ones are equal or not.
				 */
				restartLoop = true;

				break;
			}
		}

		if (restartLoop)
		{
			equivalenceClassIndex = 0;
		}
		else
		{
			++equivalenceClassIndex;
		}
	}

	return commonEquivalenceClass;
}


/*
 * GenerateEquivalenceClassForRelationRestriction generates an AttributeEquivalenceClass
 * with a single AttributeEquivalenceClassMember.
 */
static AttributeEquivalenceClass *
GenerateEquivalenceClassForRelationRestriction(
	RelationRestrictionContext *relationRestrictionContext)
{
	ListCell *relationRestrictionCell = NULL;
	AttributeEquivalenceClassMember *eqMember = NULL;
	AttributeEquivalenceClass *eqClassForRelation = NULL;

	foreach(relationRestrictionCell, relationRestrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(relationRestrictionCell);
		Var *relationPartitionKey = DistPartitionKey(relationRestriction->relationId);

		if (relationPartitionKey)
		{
			eqClassForRelation = palloc0(sizeof(AttributeEquivalenceClass));
			eqMember = palloc0(sizeof(AttributeEquivalenceClassMember));
			eqMember->relationId = relationRestriction->relationId;
			eqMember->rteIdentity = GetRTEIdentity(relationRestriction->rte);
			eqMember->varno = relationRestriction->index;
			eqMember->varattno = relationPartitionKey->varattno;

			eqClassForRelation->equivalentAttributes =
				lappend(eqClassForRelation->equivalentAttributes, eqMember);

			break;
		}
	}

	return eqClassForRelation;
}


/*
 * ListConcatUniqueAttributeClassMemberLists gets two attribute equivalence classes. It
 * basically concatenates attribute equivalence member lists uniquely and updates the
 * firstClass' member list with the list.
 *
 * Basically, the function iterates over the secondClass' member list and checks whether
 * it already exists in the firstClass' member list. If not, the member is added to the
 * firstClass.
 */
static void
ListConcatUniqueAttributeClassMemberLists(AttributeEquivalenceClass *firstClass,
										  AttributeEquivalenceClass *secondClass)
{
	ListCell *equivalenceClassMemberCell = NULL;
	List *equivalenceMemberList = secondClass->equivalentAttributes;

	foreach(equivalenceClassMemberCell, equivalenceMemberList)
	{
		AttributeEquivalenceClassMember *newEqMember =
			(AttributeEquivalenceClassMember *) lfirst(equivalenceClassMemberCell);

		if (AttributeClassContainsAttributeClassMember(newEqMember, firstClass))
		{
			continue;
		}

		firstClass->equivalentAttributes = lappend(firstClass->equivalentAttributes,
												   newEqMember);
	}
}


/*
 * GenerateAttributeEquivalencesForJoinRestrictions gets a join restriction
 * context and returns a list of AttrributeEquivalenceClass.
 *
 * The algorithm followed can be summarized as below:
 *
 * - Per join restriction
 *     - Per RestrictInfo of the join restriction
 *     - Check whether the join restriction is in the form of (Var1 = Var2)
 *         - Create an AttributeEquivalenceClass
 *         - Add both Var1 and Var2 to the AttributeEquivalenceClass
 */
static List *
GenerateAttributeEquivalencesForJoinRestrictions(JoinRestrictionContext *
												 joinRestrictionContext)
{
	List *attributeEquivalenceList = NIL;
	ListCell *joinRestrictionCell = NULL;

	if (joinRestrictionContext == NULL)
	{
		return attributeEquivalenceList;
	}

	foreach(joinRestrictionCell, joinRestrictionContext->joinRestrictionList)
	{
		JoinRestriction *joinRestriction =
			(JoinRestriction *) lfirst(joinRestrictionCell);
		ListCell *restrictionInfoList = NULL;

		foreach(restrictionInfoList, joinRestriction->joinRestrictInfoList)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(restrictionInfoList);
			Expr *restrictionClause = rinfo->clause;

			if (!IsA(restrictionClause, OpExpr))
			{
				continue;
			}

			OpExpr *restrictionOpExpr = (OpExpr *) restrictionClause;
			if (list_length(restrictionOpExpr->args) != 2)
			{
				continue;
			}
			if (!OperatorImplementsEquality(restrictionOpExpr->opno))
			{
				continue;
			}

			Node *leftNode = linitial(restrictionOpExpr->args);
			Node *rightNode = lsecond(restrictionOpExpr->args);

			/* we also don't want implicit coercions */
			Expr *strippedLeftExpr = (Expr *) strip_implicit_coercions((Node *) leftNode);
			Expr *strippedRightExpr = (Expr *) strip_implicit_coercions(
				(Node *) rightNode);

			if (!(IsA(strippedLeftExpr, Var) && IsA(strippedRightExpr, Var)))
			{
				continue;
			}

			Var *leftVar = (Var *) strippedLeftExpr;
			Var *rightVar = (Var *) strippedRightExpr;

			AttributeEquivalenceClass *attributeEquivalence = palloc0(
				sizeof(AttributeEquivalenceClass));
			attributeEquivalence->equivalenceId = AttributeEquivalenceId++;

			AddToAttributeEquivalenceClass(attributeEquivalence,
										   joinRestriction->plannerInfo, leftVar);

			AddToAttributeEquivalenceClass(attributeEquivalence,
										   joinRestriction->plannerInfo, rightVar);

			attributeEquivalenceList =
				AddAttributeClassToAttributeClassList(attributeEquivalenceList,
													  attributeEquivalence);
		}
	}

	return attributeEquivalenceList;
}


/*
 * AddToAttributeEquivalenceClass is a key function for building the attribute
 * equivalences. The function gets a plannerInfo, var and attribute equivalence
 * class. It searches for the RTE_RELATION(s) that the input var belongs to and
 * adds the found Var(s) to the input attribute equivalence class.
 *
 * Note that the input var could come from a subquery (i.e., not directly from an
 * RTE_RELATION). That's the reason we recursively call the function until the
 * RTE_RELATION found.
 *
 * The algorithm could be summarized as follows:
 *
 *    - If the RTE that corresponds to a relation
 *        - Generate an AttributeEquivalenceMember and add to the input
 *          AttributeEquivalenceClass
 *    - If the RTE that corresponds to a subquery
 *        - If the RTE that corresponds to a UNION ALL subquery
 *            - Iterate on each of the appendRels (i.e., each of the UNION ALL query)
 *            - Recursively add all children of the set operation's
 *              corresponding target entries
 *        - If the corresponding subquery entry is a UNION set operation
 *             - Recursively add all children of the set operation's
 *               corresponding target entries
 *        - If the corresponding subquery is a regular subquery (i.e., No set operations)
 *             - Recursively try to add the corresponding target entry to the
 *               equivalence class
 */
static void
AddToAttributeEquivalenceClass(AttributeEquivalenceClass *attributeEquivalenceClass,
							   PlannerInfo *root, Var *varToBeAdded)
{
	/* punt if it's a whole-row var rather than a plain column reference */
	if (varToBeAdded->varattno == InvalidAttrNumber)
	{
		return;
	}

	/* we also don't want to process ctid, tableoid etc */
	if (varToBeAdded->varattno < InvalidAttrNumber)
	{
		return;
	}

	/* outer join checks in PG16 */
	if (IsRelOptOuterJoin(root, varToBeAdded->varno))
	{
		return;
	}

	RangeTblEntry *rangeTableEntry = root->simple_rte_array[varToBeAdded->varno];
	if (rangeTableEntry->rtekind == RTE_RELATION)
	{
		AddRteRelationToAttributeEquivalenceClass(attributeEquivalenceClass,
												  rangeTableEntry,
												  varToBeAdded);
	}
	else if (rangeTableEntry->rtekind == RTE_SUBQUERY)
	{
		AddRteSubqueryToAttributeEquivalenceClass(attributeEquivalenceClass,
												  rangeTableEntry, root,
												  varToBeAdded);
	}
}


/*
 * AddRteSubqueryToAttributeEquivalenceClass adds the given var to the given
 * attribute equivalence class.
 *
 * The main algorithm is outlined in AddToAttributeEquivalenceClass().
 */
static void
AddRteSubqueryToAttributeEquivalenceClass(AttributeEquivalenceClass
										  *attributeEquivalenceClass,
										  RangeTblEntry *rangeTableEntry,
										  PlannerInfo *root,
										  Var *varToBeAdded)
{
	RelOptInfo *baseRelOptInfo = find_base_rel(root, varToBeAdded->varno);
	Query *targetSubquery = GetTargetSubquery(root, rangeTableEntry, varToBeAdded);

	/*
	 * We might not always get the subquery because the subquery might be a
	 * referencing to RELOPT_DEADREL such that the corresponding join is
	 * removed via join_is_removable().
	 *
	 * Returning here implies that PostgreSQL doesn't need to plan the
	 * subquery because it doesn't contribute to the query result at all.
	 * Since the relations in the subquery does not appear in the query
	 * plan as well, Citus would simply ignore the subquery and treat that
	 * as a safe-to-pushdown subquery.
	 */
	if (targetSubquery == NULL)
	{
		return;
	}

	TargetEntry *subqueryTargetEntry = get_tle_by_resno(targetSubquery->targetList,
														varToBeAdded->varattno);

	/* if we fail to find corresponding target entry, do not proceed */
	if (subqueryTargetEntry == NULL || subqueryTargetEntry->resjunk)
	{
		return;
	}

	/* we're only interested in Vars */
	if (!IsA(subqueryTargetEntry->expr, Var))
	{
		return;
	}

	varToBeAdded = (Var *) subqueryTargetEntry->expr;

	/*
	 *  "inh" flag is set either when inheritance or "UNION ALL" exists in the
	 *  subquery. Here we're only interested in the "UNION ALL" case.
	 *
	 *  Else, we check one more thing: Does the subquery contain a "UNION" query.
	 *  If so, we recursively traverse all "UNION" tree and add the corresponding
	 *  target list elements to the attribute equivalence.
	 *
	 *  Finally, if it is a regular subquery (i.e., does not contain UNION or UNION ALL),
	 *  we simply recurse to find the corresponding RTE_RELATION to add to the
	 *  equivalence class.
	 *
	 *  Note that we're treating "UNION" and "UNION ALL" clauses differently given
	 *  that postgres planner process/plans them separately.
	 */
	if (rangeTableEntry->inh)
	{
		AddUnionAllSetOperationsToAttributeEquivalenceClass(attributeEquivalenceClass,
															root, varToBeAdded);
	}
	else if (targetSubquery->setOperations)
	{
		AddUnionSetOperationsToAttributeEquivalenceClass(attributeEquivalenceClass,
														 baseRelOptInfo->subroot,
														 (SetOperationStmt *)
														 targetSubquery->setOperations,
														 varToBeAdded);
	}
	else if (varToBeAdded && IsA(varToBeAdded, Var) && varToBeAdded->varlevelsup == 0)
	{
		AddToAttributeEquivalenceClass(attributeEquivalenceClass,
									   baseRelOptInfo->subroot, varToBeAdded);
	}
}


/*
 * GetTargetSubquery returns the corresponding subquery for the given planner root,
 * range table entry and the var.
 *
 * The aim of this function is to simplify extracting the subquery in case of "UNION ALL"
 * queries.
 */
static Query *
GetTargetSubquery(PlannerInfo *root, RangeTblEntry *rangeTableEntry, Var *varToBeAdded)
{
	Query *targetSubquery = NULL;

	/*
	 * For subqueries other than "UNION ALL", find the corresponding targetSubquery. See
	 * the details of how we process subqueries in the below comments.
	 */
	if (!rangeTableEntry->inh)
	{
		RelOptInfo *baseRelOptInfo = find_base_rel(root, varToBeAdded->varno);

		/* If the targetSubquery was not planned, we have to punt */
		if (baseRelOptInfo->subroot == NULL)
		{
			return NULL;
		}

		Assert(IsA(baseRelOptInfo->subroot, PlannerInfo));

		targetSubquery = baseRelOptInfo->subroot->parse;
		Assert(IsA(targetSubquery, Query));
	}
	else
	{
		targetSubquery = rangeTableEntry->subquery;
	}

	return targetSubquery;
}


/*
 * IsRelOptOuterJoin returns true if the RelOpt referenced
 * by varNo is an outer join, false otherwise.
 */
bool
IsRelOptOuterJoin(PlannerInfo *root, int varNo)
{
#if PG_VERSION_NUM >= PG_VERSION_16
	if (root->simple_rel_array_size <= varNo)
	{
		return true;
	}

	RelOptInfo *rel = root->simple_rel_array[varNo];
	if (rel == NULL)
	{
		/* must be an outer join */
		return true;
	}
#endif
	return false;
}


/*
 * AddUnionAllSetOperationsToAttributeEquivalenceClass recursively iterates on all the
 * append rels, sets the varno's accordingly and adds the
 * var the given equivalence class.
 */
static void
AddUnionAllSetOperationsToAttributeEquivalenceClass(AttributeEquivalenceClass *
													attributeEquivalenceClass,
													PlannerInfo *root,
													Var *varToBeAdded)
{
	List *appendRelList = root->append_rel_list;
	ListCell *appendRelCell = NULL;

	/* iterate on the queries that are part of UNION ALL subqueries */
	foreach(appendRelCell, appendRelList)
	{
		AppendRelInfo *appendRelInfo = (AppendRelInfo *) lfirst(appendRelCell);

		/*
		 * We're only interested in UNION ALL clauses and parent_reloid is invalid
		 * only for UNION ALL (i.e., equals to a legitimate Oid for inheritance)
		 */
		if (appendRelInfo->parent_reloid != InvalidOid)
		{
			continue;
		}
		int rtoffset = RangeTableOffsetCompat(root, appendRelInfo);
		int childRelId = appendRelInfo->child_relid - rtoffset;

		if (root->simple_rel_array_size <= childRelId)
		{
			/* we prefer to return over an Assert or error to be defensive */
			return;
		}

		RangeTblEntry *rte = root->simple_rte_array[childRelId];
		if (rte->inh)
		{
			/*
			 * This code-path may require improvements. If a leaf of a UNION ALL
			 * (e.g., an entry in appendRelList) itself is another UNION ALL
			 * (e.g., rte->inh = true), the logic here might get into an infinite
			 * recursion.
			 *
			 * The downside of "continue" here is that certain UNION ALL queries
			 * that are safe to pushdown may not be pushed down.
			 */
			continue;
		}
		else if (rte->rtekind == RTE_RELATION)
		{
			Index partitionKeyIndex = 0;
			List *translatedVars = TranslatedVarsForRteIdentity(GetRTEIdentity(rte));
			Var *varToBeAddedOnUnionAllSubquery =
				FindUnionAllVar(root, translatedVars, rte->relid, childRelId,
								&partitionKeyIndex);
			if (partitionKeyIndex == 0)
			{
				/* no partition key on the target list */
				continue;
			}

			if (attributeEquivalenceClass->unionQueryPartitionKeyIndex == 0)
			{
				/* the first partition key index we found */
				attributeEquivalenceClass->unionQueryPartitionKeyIndex =
					partitionKeyIndex;
			}
			else if (attributeEquivalenceClass->unionQueryPartitionKeyIndex !=
					 partitionKeyIndex)
			{
				/*
				 * Partition keys on the leaves of the UNION ALL queries on
				 * different ordinal positions. We cannot pushdown, so skip.
				 */
				continue;
			}

			if (varToBeAddedOnUnionAllSubquery != NULL)
			{
				AddToAttributeEquivalenceClass(attributeEquivalenceClass, root,
											   varToBeAddedOnUnionAllSubquery);
			}
		}
		else
		{
			/* set the varno accordingly for this specific child */
			varToBeAdded->varno = childRelId;

			AddToAttributeEquivalenceClass(attributeEquivalenceClass, root,
										   varToBeAdded);
		}
	}
}


/*
 * ParentCountPriorToAppendRel returns the number of parents that come before
 * the given append rel info.
 */
static int
ParentCountPriorToAppendRel(List *appendRelList, AppendRelInfo *targetAppendRelInfo)
{
	int targetParentIndex = targetAppendRelInfo->parent_relid;
	Bitmapset *parent_ids = NULL;
	AppendRelInfo *appendRelInfo = NULL;
	foreach_ptr(appendRelInfo, appendRelList)
	{
		int curParentIndex = appendRelInfo->parent_relid;
		if (curParentIndex <= targetParentIndex)
		{
			parent_ids = bms_add_member(parent_ids, curParentIndex);
		}
	}
	return bms_num_members(parent_ids);
}


/*
 * AddUnionSetOperationsToAttributeEquivalenceClass recursively iterates on all the
 * setOperations and adds each corresponding target entry to the given equivalence
 * class.
 *
 * Although the function silently accepts INTERSECT and EXPECT set operations, they are
 * rejected later in the planning. We prefer this behavior to provide better error
 * messages.
 */
static void
AddUnionSetOperationsToAttributeEquivalenceClass(AttributeEquivalenceClass *
												 attributeEquivalenceClass,
												 PlannerInfo *root,
												 SetOperationStmt *setOperation,
												 Var *varToBeAdded)
{
	List *rangeTableIndexList = NIL;
	ListCell *rangeTableIndexCell = NULL;

	ExtractRangeTableIndexWalker((Node *) setOperation, &rangeTableIndexList);

	foreach(rangeTableIndexCell, rangeTableIndexList)
	{
		int rangeTableIndex = lfirst_int(rangeTableIndexCell);

		varToBeAdded->varno = rangeTableIndex;
		AddToAttributeEquivalenceClass(attributeEquivalenceClass, root, varToBeAdded);
	}
}


/*
 * AddRteRelationToAttributeEquivalenceClass adds the given var to the given equivalence
 * class using the rteIdentity provided by the rangeTableEntry. Note that
 * rteIdentities are only assigned to RTE_RELATIONs and this function asserts
 * the input rte to be an RTE_RELATION.
 */
static void
AddRteRelationToAttributeEquivalenceClass(AttributeEquivalenceClass *
										  attrEquivalenceClass,
										  RangeTblEntry *rangeTableEntry,
										  Var *varToBeAdded)
{
	Oid relationId = rangeTableEntry->relid;

	/* we don't consider local tables in the equality on columns */
	if (!IsCitusTable(relationId))
	{
		return;
	}

	Var *relationPartitionKey = DistPartitionKey(relationId);

	Assert(rangeTableEntry->rtekind == RTE_RELATION);

	/*
	 * we only calculate the equivalence of distributed tables.
	 * This leads to certain shortcomings in the query planning when reference
	 * tables and/or intermediate results are involved in the query. For example,
	 * the following query patterns could actually be pushed-down in a single iteration
	 *    "(intermediate_res INNER JOIN dist dist1) INNER JOIN dist dist2 " or
	 *    "(ref INNER JOIN dist dist1) JOIN dist dist2"
	 *
	 * However, if there are no explicit join conditions between distributed tables,
	 * the planner cannot deduce the equivalence between the distributed tables.
	 *
	 * Instead, we should be able to track all the equivalences between range table
	 * entries, and expand distributed table equivalences that happens via
	 * reference table/intermediate results
	 */
	if (relationPartitionKey == NULL)
	{
		return;
	}

	/* we're only interested in distribution columns */
	if (relationPartitionKey->varattno != varToBeAdded->varattno)
	{
		return;
	}

	AttributeEquivalenceClassMember *attributeEqMember = palloc0(
		sizeof(AttributeEquivalenceClassMember));

	attributeEqMember->varattno = varToBeAdded->varattno;
	attributeEqMember->varno = varToBeAdded->varno;
	attributeEqMember->rteIdentity = GetRTEIdentity(rangeTableEntry);
	attributeEqMember->relationId = rangeTableEntry->relid;

	attrEquivalenceClass->equivalentAttributes =
		lappend(attrEquivalenceClass->equivalentAttributes,
				attributeEqMember);
}


/*
 * AttributeClassContainsAttributeClassMember returns true if it the input class member
 * is already exists in the attributeEquivalenceClass. An equality is identified by the
 * varattno and rteIdentity.
 */
static bool
AttributeClassContainsAttributeClassMember(AttributeEquivalenceClassMember *inputMember,
										   AttributeEquivalenceClass *
										   attributeEquivalenceClass)
{
	ListCell *classCell = NULL;
	foreach(classCell, attributeEquivalenceClass->equivalentAttributes)
	{
		AttributeEquivalenceClassMember *memberOfClass =
			(AttributeEquivalenceClassMember *) lfirst(classCell);
		if (memberOfClass->rteIdentity == inputMember->rteIdentity &&
			memberOfClass->varattno == inputMember->varattno)
		{
			return true;
		}
	}

	return false;
}


/*
 * AddAttributeClassToAttributeClassList checks for certain properties of the
 * input attributeEquivalence before adding it to the attributeEquivalenceList.
 *
 * Firstly, the function skips adding NULL attributeEquivalence to the list.
 * Secondly, since an attribute equivalence class with a single member does
 * not contribute to our purposes, we skip such classed adding to the list.
 * Finally, we don't want to add an equivalence class whose exact equivalent
 * already exists in the list.
 */
static List *
AddAttributeClassToAttributeClassList(List *attributeEquivalenceList,
									  AttributeEquivalenceClass *attributeEquivalence)
{
	ListCell *attributeEquivalenceCell = NULL;

	if (attributeEquivalence == NULL)
	{
		return attributeEquivalenceList;
	}

	/*
	 * Note that in some cases we allow having equivalentAttributes with zero or
	 * one elements. For the details, see AddToAttributeEquivalenceClass().
	 */
	List *equivalentAttributes = attributeEquivalence->equivalentAttributes;
	if (list_length(equivalentAttributes) < 2)
	{
		return attributeEquivalenceList;
	}

	/* we don't want to add an attributeEquivalence which already exists */
	foreach(attributeEquivalenceCell, attributeEquivalenceList)
	{
		AttributeEquivalenceClass *currentAttributeEquivalence =
			(AttributeEquivalenceClass *) lfirst(attributeEquivalenceCell);

		if (AttributeEquivalencesAreEqual(currentAttributeEquivalence,
										  attributeEquivalence))
		{
			return attributeEquivalenceList;
		}
	}

	attributeEquivalenceList = lappend(attributeEquivalenceList,
									   attributeEquivalence);

	return attributeEquivalenceList;
}


/*
 *  AttributeEquivalencesAreEqual returns true if both input attribute equivalence
 *  classes contains exactly the same members.
 */
static bool
AttributeEquivalencesAreEqual(AttributeEquivalenceClass *firstAttributeEquivalence,
							  AttributeEquivalenceClass *secondAttributeEquivalence)
{
	List *firstEquivalenceMemberList =
		firstAttributeEquivalence->equivalentAttributes;
	List *secondEquivalenceMemberList =
		secondAttributeEquivalence->equivalentAttributes;
	ListCell *firstAttributeEquivalenceCell = NULL;
	ListCell *secondAttributeEquivalenceCell = NULL;

	if (list_length(firstEquivalenceMemberList) != list_length(
			secondEquivalenceMemberList))
	{
		return false;
	}

	foreach(firstAttributeEquivalenceCell, firstEquivalenceMemberList)
	{
		AttributeEquivalenceClassMember *firstEqMember =
			(AttributeEquivalenceClassMember *) lfirst(firstAttributeEquivalenceCell);
		bool foundAnEquivalentMember = false;

		foreach(secondAttributeEquivalenceCell, secondEquivalenceMemberList)
		{
			AttributeEquivalenceClassMember *secondEqMember =
				(AttributeEquivalenceClassMember *) lfirst(
					secondAttributeEquivalenceCell);

			if (firstEqMember->rteIdentity == secondEqMember->rteIdentity &&
				firstEqMember->varattno == secondEqMember->varattno)
			{
				foundAnEquivalentMember = true;
				break;
			}
		}

		/* we couldn't find an equivalent member */
		if (!foundAnEquivalentMember)
		{
			return false;
		}
	}

	return true;
}


/*
 * ContainsUnionSubquery gets a queryTree and returns true if the query
 * contains
 *      - a subquery with UNION set operation
 *      - no joins above the UNION set operation in the query tree
 *
 * Note that the function allows top level unions being wrapped into aggregations
 * queries and/or simple projection queries that only selects some fields from
 * the lower level queries.
 *
 * If there exists joins before the set operations, the function returns false.
 * Similarly, if the query does not contain any union set operations, the
 * function returns false.
 */
bool
ContainsUnionSubquery(Query *queryTree)
{
	List *rangeTableList = queryTree->rtable;
	List *joinTreeTableIndexList = NIL;

	ExtractRangeTableIndexWalker((Node *) queryTree->jointree, &joinTreeTableIndexList);
	uint32 joiningRangeTableCount = list_length(joinTreeTableIndexList);

	/* don't allow joins on top of unions */
	if (joiningRangeTableCount > 1)
	{
		return false;
	}

	/* subquery without FROM */
	if (joiningRangeTableCount == 0)
	{
		return false;
	}

	Index subqueryRteIndex = linitial_int(joinTreeTableIndexList);
	RangeTblEntry *rangeTableEntry = rt_fetch(subqueryRteIndex, rangeTableList);
	if (rangeTableEntry->rtekind != RTE_SUBQUERY)
	{
		return false;
	}

	Query *subqueryTree = rangeTableEntry->subquery;
	Node *setOperations = subqueryTree->setOperations;
	if (setOperations != NULL)
	{
		SetOperationStmt *setOperationStatement = (SetOperationStmt *) setOperations;

		/*
		 * Note that the set operation tree is traversed elsewhere for ensuring
		 * that we only support UNIONs.
		 */
		if (setOperationStatement->op != SETOP_UNION)
		{
			return false;
		}

		return true;
	}

	return ContainsUnionSubquery(subqueryTree);
}


/*
 * PartitionKeyForRTEIdentityInQuery finds the partition key var(if exists),
 * in the given original query for the rte that has targetRTEIndex.
 */
static Var *
PartitionKeyForRTEIdentityInQuery(Query *originalQuery, int targetRTEIndex,
								  Index *partitionKeyIndex)
{
	Query *originalQueryContainingRTEIdentity =
		FindQueryContainingRTEIdentity(originalQuery, targetRTEIndex);
	if (!originalQueryContainingRTEIdentity)
	{
		/*
		 * We should always find the query but we have this check for sanity.
		 * This check makes sure that if there is a bug while finding the query,
		 * we don't get a crash etc. and the only downside will be we might be recursively
		 * planning a query that could be pushed down.
		 */
		return NULL;
	}

	/*
	 * This approach fails to detect when
	 * the top level query might have the column indexes in different order:
	 * explain
	 * SELECT count(*) FROM
	 * (
	 * SELECT user_id,value_2 FROM events_table
	 * UNION
	 * SELECT value_2, user_id FROM (SELECT user_id, value_2, random() FROM events_table) as foo
	 * ) foobar;
	 * So we hit https://github.com/citusdata/citus/issues/5093.
	 */
	List *relationTargetList = originalQueryContainingRTEIdentity->targetList;

	ListCell *targetEntryCell = NULL;
	Index partitionKeyTargetAttrIndex = 0;
	foreach(targetEntryCell, relationTargetList)
	{
		TargetEntry *targetEntry = (TargetEntry *) lfirst(targetEntryCell);
		Expr *targetExpression = targetEntry->expr;

		partitionKeyTargetAttrIndex++;

		bool skipOuterVars = false;
		if (!targetEntry->resjunk &&
			IsA(targetExpression, Var) &&
			IsPartitionColumn(targetExpression, originalQueryContainingRTEIdentity,
							  skipOuterVars))
		{
			Var *targetColumn = (Var *) targetExpression;

			/*
			 * We find the referenced table column to support distribution
			 * columns that are correlated.
			 */
			RangeTblEntry *rteContainingPartitionKey = NULL;
			FindReferencedTableColumn(targetExpression, NIL,
									  originalQueryContainingRTEIdentity,
									  &targetColumn,
									  &rteContainingPartitionKey,
									  skipOuterVars);

			if (rteContainingPartitionKey->rtekind == RTE_RELATION &&
				GetRTEIdentity(rteContainingPartitionKey) == targetRTEIndex)
			{
				*partitionKeyIndex = partitionKeyTargetAttrIndex;
				return (Var *) copyObject(targetColumn);
			}
		}
	}

	return NULL;
}


/*
 * FindQueryContainingRTEIdentity finds the query/subquery that has an RTE
 * with rteIndex in its rtable.
 */
static Query *
FindQueryContainingRTEIdentity(Query *query, int rteIndex)
{
	FindQueryContainingRteIdentityContext *findRteIdentityContext =
		palloc0(sizeof(FindQueryContainingRteIdentityContext));
	findRteIdentityContext->targetRTEIdentity = rteIndex;
	FindQueryContainingRTEIdentityInternal((Node *) query, findRteIdentityContext);
	return findRteIdentityContext->query;
}


/*
 * FindQueryContainingRTEIdentityInternal walks on the given node to find a query
 * which has an RTE that has a given rteIdentity.
 */
static bool
FindQueryContainingRTEIdentityInternal(Node *node,
									   FindQueryContainingRteIdentityContext *context)
{
	if (node == NULL)
	{
		return false;
	}
	if (IsA(node, Query))
	{
		Query *query = (Query *) node;
		Query *parentQuery = context->query;
		context->query = query;
		if (query_tree_walker(query, FindQueryContainingRTEIdentityInternal, context,
							  QTW_EXAMINE_RTES_BEFORE))
		{
			return true;
		}
		context->query = parentQuery;
		return false;
	}

	if (!IsA(node, RangeTblEntry))
	{
		return expression_tree_walker(node, FindQueryContainingRTEIdentityInternal,
									  context);
	}
	RangeTblEntry *rte = (RangeTblEntry *) node;
	if (rte->rtekind == RTE_RELATION)
	{
		if (GetRTEIdentity(rte) == context->targetRTEIdentity)
		{
			return true;
		}
	}
	return false;
}


/*
 * AllDistributedRelationsInRestrictionContextColocated determines whether all of the
 * distributed  relations in the given relation restrictions list are co-located.
 */
static bool
AllDistributedRelationsInRestrictionContextColocated(
	RelationRestrictionContext *restrictionContext)
{
	RelationRestriction *relationRestriction = NULL;
	List *relationIdList = NIL;

	/* check whether all relations exists in the main restriction list */
	foreach_ptr(relationRestriction, restrictionContext->relationRestrictionList)
	{
		relationIdList = lappend_oid(relationIdList, relationRestriction->relationId);
	}

	return AllDistributedRelationsInListColocated(relationIdList);
}


/*
 * AllDistributedRelationsInRTEListColocated determines whether all of the
 * distributed relations in the given RangeTableEntry list are co-located.
 */
bool
AllDistributedRelationsInRTEListColocated(List *rangeTableEntryList)
{
	RangeTblEntry *rangeTableEntry = NULL;
	List *relationIdList = NIL;

	foreach_ptr(rangeTableEntry, rangeTableEntryList)
	{
		relationIdList = lappend_oid(relationIdList, rangeTableEntry->relid);
	}

	return AllDistributedRelationsInListColocated(relationIdList);
}


/*
 * AllDistributedRelationsInListColocated determines whether all of the
 * distributed relations in the given list are co-located.
 */
bool
AllDistributedRelationsInListColocated(List *relationList)
{
	int initialColocationId = INVALID_COLOCATION_ID;
	Oid relationId = InvalidOid;

	foreach_oid(relationId, relationList)
	{
		if (!IsCitusTable(relationId))
		{
			/* not interested in Postgres tables */
			continue;
		}

		if (!IsCitusTableType(relationId, DISTRIBUTED_TABLE))
		{
			/* not interested in non-distributed tables */
			continue;
		}

		if (IsCitusTableType(relationId, APPEND_DISTRIBUTED))
		{
			/*
			 * If we got to this point, it means there are multiple distributed
			 * relations and at least one of them is append-distributed. Since
			 * we do not consider append-distributed tables to be co-located,
			 * we can immediately return false.
			 */
			return false;
		}

		int colocationId = TableColocationId(relationId);

		if (initialColocationId == INVALID_COLOCATION_ID)
		{
			initialColocationId = colocationId;
		}
		else if (colocationId != initialColocationId)
		{
			return false;
		}
	}

	return true;
}


/*
 * RelationIdList returns list of unique relation ids in query tree.
 */
List *
DistributedRelationIdList(Query *query)
{
	List *rangeTableList = NIL;
	List *relationIdList = NIL;
	ListCell *tableEntryCell = NULL;

	ExtractRangeTableRelationWalker((Node *) query, &rangeTableList);
	List *tableEntryList = TableEntryList(rangeTableList);

	foreach(tableEntryCell, tableEntryList)
	{
		TableEntry *tableEntry = (TableEntry *) lfirst(tableEntryCell);
		Oid relationId = tableEntry->relationId;

		if (!IsCitusTable(relationId))
		{
			continue;
		}

		relationIdList = list_append_unique_oid(relationIdList, relationId);
	}

	return relationIdList;
}


/*
 * FilterPlannerRestrictionForQuery gets a planner restriction context and
 * set of rte identities. It returns the restrictions that that appear
 * in the queryRteIdentities and returns a newly allocated
 * PlannerRestrictionContext. The function also sets all the other fields of
 * the PlannerRestrictionContext with respect to the filtered restrictions.
 */
PlannerRestrictionContext *
FilterPlannerRestrictionForQuery(PlannerRestrictionContext *plannerRestrictionContext,
								 Query *query)
{
	Relids queryRteIdentities = QueryRteIdentities(query);

	RelationRestrictionContext *relationRestrictionContext =
		plannerRestrictionContext->relationRestrictionContext;
	JoinRestrictionContext *joinRestrictionContext =
		plannerRestrictionContext->joinRestrictionContext;

	RelationRestrictionContext *filteredRelationRestrictionContext =
		FilterRelationRestrictionContext(relationRestrictionContext, queryRteIdentities);

	JoinRestrictionContext *filtererdJoinRestrictionContext =
		FilterJoinRestrictionContext(joinRestrictionContext, queryRteIdentities);

	/* allocate the filtered planner restriction context and set all the fields */
	PlannerRestrictionContext *filteredPlannerRestrictionContext = palloc0(
		sizeof(PlannerRestrictionContext));
	filteredPlannerRestrictionContext->fastPathRestrictionContext = palloc0(
		sizeof(FastPathRestrictionContext));

	filteredPlannerRestrictionContext->memoryContext =
		plannerRestrictionContext->memoryContext;

	int totalRelationCount = UniqueRelationCount(
		filteredRelationRestrictionContext, ANY_CITUS_TABLE_TYPE);
	int referenceRelationCount = UniqueRelationCount(
		filteredRelationRestrictionContext, REFERENCE_TABLE);

	filteredRelationRestrictionContext->allReferenceTables =
		(totalRelationCount == referenceRelationCount);

	/* finally set the relation and join restriction contexts */
	filteredPlannerRestrictionContext->relationRestrictionContext =
		filteredRelationRestrictionContext;
	filteredPlannerRestrictionContext->joinRestrictionContext =
		filtererdJoinRestrictionContext;

	return filteredPlannerRestrictionContext;
}


/*
 * GetRestrictInfoListForRelation gets a range table entry and planner
 * restriction context. The function returns a list of expressions that
 * appear in the restriction context for only the given relation. And,
 * all the varnos are set to 1.
 */
List *
GetRestrictInfoListForRelation(RangeTblEntry *rangeTblEntry,
							   PlannerRestrictionContext *plannerRestrictionContext)
{
	RelationRestriction *relationRestriction =
		RelationRestrictionForRelation(rangeTblEntry, plannerRestrictionContext);
	if (relationRestriction == NULL)
	{
		return NIL;
	}

	RelOptInfo *relOptInfo = relationRestriction->relOptInfo;
	List *baseRestrictInfo = relOptInfo->baserestrictinfo;

	bool joinConditionIsOnFalse = JoinConditionIsOnFalse(relOptInfo->joininfo);
	if (joinConditionIsOnFalse)
	{
		/* found WHERE false, no need  to continue, we just return a false clause */
		bool value = false;
		bool isNull = false;
		Node *falseClause = makeBoolConst(value, isNull);
		return list_make1(falseClause);
	}


	List *restrictExprList = NIL;
	RestrictInfo *restrictInfo = NULL;
	foreach_ptr(restrictInfo, baseRestrictInfo)
	{
		Expr *restrictionClause = restrictInfo->clause;

		/*
		 * we cannot process some restriction clauses because they are not
		 * safe to recursively plan.
		 */
		if (FindNodeMatchingCheckFunction((Node *) restrictionClause,
										  IsNotSafeRestrictionToRecursivelyPlan))
		{
			continue;
		}

		/*
		 * If the restriction involves multiple tables, we cannot add it to
		 * input relation's expression list.
		 */
		Relids varnos = pull_varnos(relationRestriction->plannerInfo,
									(Node *) restrictionClause);
		if (bms_num_members(varnos) != 1)
		{
			continue;
		}

		/*
		 * PlaceHolderVar is not relevant to be processed inside a restriction clause.
		 * Otherwise, pull_var_clause_default would throw error. PG would create
		 * the restriction to physical Var that PlaceHolderVar points anyway, so it is
		 * safe to skip this restriction.
		 */
		if (FindNodeMatchingCheckFunction((Node *) restrictionClause, HasPlaceHolderVar))
		{
			continue;
		}

		/*
		 * We're going to add this restriction expression to a subquery
		 * which consists of only one relation in its jointree. Thus,
		 * simply set the varnos accordingly.
		 */
		Expr *copyOfRestrictClause = (Expr *) copyObject((Node *) restrictionClause);
		List *varClauses = pull_var_clause_default((Node *) copyOfRestrictClause);
		Var *column = NULL;
		foreach_ptr(column, varClauses)
		{
			column->varno = SINGLE_RTE_INDEX;
			column->varnosyn = SINGLE_RTE_INDEX;
		}

		restrictExprList = lappend(restrictExprList, copyOfRestrictClause);
	}

	return restrictExprList;
}


/*
 * RelationRestrictionForRelation gets the relation restriction for the given
 * range table entry.
 */
RelationRestriction *
RelationRestrictionForRelation(RangeTblEntry *rangeTableEntry,
							   PlannerRestrictionContext *plannerRestrictionContext)
{
	int rteIdentity = GetRTEIdentity(rangeTableEntry);
	RelationRestrictionContext *relationRestrictionContext =
		plannerRestrictionContext->relationRestrictionContext;
	Relids queryRteIdentities = bms_make_singleton(rteIdentity);
	RelationRestrictionContext *filteredRelationRestrictionContext =
		FilterRelationRestrictionContext(relationRestrictionContext, queryRteIdentities);
	List *filteredRelationRestrictionList =
		filteredRelationRestrictionContext->relationRestrictionList;

	if (list_length(filteredRelationRestrictionList) < 1)
	{
		return NULL;
	}

	RelationRestriction *relationRestriction =
		(RelationRestriction *) linitial(filteredRelationRestrictionList);
	return relationRestriction;
}


/*
 * IsNotSafeRestrictionToRecursivelyPlan returns true if the given node
 * is not a safe restriction to be recursivelly planned.
 */
static bool
IsNotSafeRestrictionToRecursivelyPlan(Node *node)
{
	if (IsA(node, Param) || IsA(node, SubLink) || IsA(node, SubPlan) || IsA(node,
																			AlternativeSubPlan))
	{
		return true;
	}
	return false;
}


/*
 * HasPlaceHolderVar returns true if given node contains any PlaceHolderVar.
 */
static bool
HasPlaceHolderVar(Node *node)
{
	return IsA(node, PlaceHolderVar);
}


/*
 * FilterRelationRestrictionContext gets a relation restriction context and
 * set of rte identities. It returns the relation restrictions that that appear
 * in the queryRteIdentities and returns a newly allocated
 * RelationRestrictionContext.
 */
RelationRestrictionContext *
FilterRelationRestrictionContext(RelationRestrictionContext *relationRestrictionContext,
								 Relids queryRteIdentities)
{
	RelationRestrictionContext *filteredRestrictionContext =
		palloc0(sizeof(RelationRestrictionContext));

	ListCell *relationRestrictionCell = NULL;

	foreach(relationRestrictionCell, relationRestrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction =
			(RelationRestriction *) lfirst(relationRestrictionCell);

		int rteIdentity = GetRTEIdentity(relationRestriction->rte);

		if (bms_is_member(rteIdentity, queryRteIdentities))
		{
			filteredRestrictionContext->relationRestrictionList =
				lappend(filteredRestrictionContext->relationRestrictionList,
						relationRestriction);
		}
	}

	return filteredRestrictionContext;
}


/*
 * FilterJoinRestrictionContext gets a join restriction context and
 * set of rte identities. It returns the join restrictions that that appear
 * in the queryRteIdentities and returns a newly allocated
 * JoinRestrictionContext.
 *
 * Note that the join restriction is added to the return context as soon as
 * any range table entry that appear in the join belongs to queryRteIdentities.
 */
static JoinRestrictionContext *
FilterJoinRestrictionContext(JoinRestrictionContext *joinRestrictionContext, Relids
							 queryRteIdentities)
{
	JoinRestrictionContext *filtererdJoinRestrictionContext =
		palloc0(sizeof(JoinRestrictionContext));

	ListCell *joinRestrictionCell = NULL;

	foreach(joinRestrictionCell, joinRestrictionContext->joinRestrictionList)
	{
		JoinRestriction *joinRestriction =
			(JoinRestriction *) lfirst(joinRestrictionCell);
		RangeTblEntry **rangeTableEntries =
			joinRestriction->plannerInfo->simple_rte_array;
		int rangeTableArrayLength = joinRestriction->plannerInfo->simple_rel_array_size;

		if (RangeTableArrayContainsAnyRTEIdentities(rangeTableEntries,
													rangeTableArrayLength,
													queryRteIdentities))
		{
			filtererdJoinRestrictionContext->joinRestrictionList = lappend(
				filtererdJoinRestrictionContext->joinRestrictionList,
				joinRestriction);
		}
	}

	/*
	 * No need to re calculate has join fields as we are still operating on
	 * the same query and as these values are calculated per-query basis.
	 */
	filtererdJoinRestrictionContext->hasSemiJoin = joinRestrictionContext->hasSemiJoin;
	filtererdJoinRestrictionContext->hasOuterJoin = joinRestrictionContext->hasOuterJoin;

	return filtererdJoinRestrictionContext;
}


/*
 * RangeTableArrayContainsAnyRTEIdentities returns true if any of the range table entries
 * int rangeTableEntries array is an range table relation specified in queryRteIdentities.
 */
static bool
RangeTableArrayContainsAnyRTEIdentities(RangeTblEntry **rangeTableEntries, int
										rangeTableArrayLength, Relids queryRteIdentities)
{
	/* simple_rte_array starts from 1, see plannerInfo struct */
	for (int rteIndex = 1; rteIndex < rangeTableArrayLength; ++rteIndex)
	{
		RangeTblEntry *rangeTableEntry = rangeTableEntries[rteIndex];
		List *rangeTableRelationList = NULL;
		ListCell *rteRelationCell = NULL;

		/*
		 * Get list of all RTE_RELATIONs in the given range table entry
		 * (i.e.,rangeTableEntry could be a subquery where we're interested
		 * in relations).
		 */
		if (rangeTableEntry->rtekind == RTE_SUBQUERY)
		{
			ExtractRangeTableRelationWalker((Node *) rangeTableEntry->subquery,
											&rangeTableRelationList);
		}
		else if (rangeTableEntry->rtekind == RTE_RELATION)
		{
			ExtractRangeTableRelationWalker((Node *) rangeTableEntry,
											&rangeTableRelationList);
		}
		else
		{
			/* we currently do not accept any other RTE types here */
			continue;
		}

		foreach(rteRelationCell, rangeTableRelationList)
		{
			RangeTblEntry *rteRelation = (RangeTblEntry *) lfirst(rteRelationCell);

			Assert(rteRelation->rtekind == RTE_RELATION);

			int rteIdentity = GetRTEIdentity(rteRelation);
			if (bms_is_member(rteIdentity, queryRteIdentities))
			{
				return true;
			}
		}
	}

	return false;
}


/*
 * QueryRteIdentities gets a queryTree, find get all the rte identities assigned by
 * us.
 */
static Relids
QueryRteIdentities(Query *queryTree)
{
	List *rangeTableList = NULL;
	ListCell *rangeTableCell = NULL;
	Relids queryRteIdentities = NULL;

	/* extract range table entries for simple relations only */
	ExtractRangeTableRelationWalker((Node *) queryTree, &rangeTableList);

	foreach(rangeTableCell, rangeTableList)
	{
		RangeTblEntry *rangeTableEntry = (RangeTblEntry *) lfirst(rangeTableCell);

		/* we're only interested in relations */
		Assert(rangeTableEntry->rtekind == RTE_RELATION);

		int rteIdentity = GetRTEIdentity(rangeTableEntry);

		queryRteIdentities = bms_add_member(queryRteIdentities, rteIdentity);
	}

	return queryRteIdentities;
}
