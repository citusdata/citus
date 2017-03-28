/*
 * multi_colocated_subquery_planner.c
 *
 * This file contains functions helper functions for planning
 * queries with colocated tables and subqueries.
 *
 * Copyright (c) 2017-2017, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "c.h"

#include "distributed/multi_colocated_subquery_planner.h"
#include "distributed/multi_planner.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/pg_dist_partition.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/relation.h"
#include "parser/parsetree.h"
#include "optimizer/pathnode.h"

static uint32 attributeEquivalenceId = 1;


/*
 * AttributeEquivalenceClass
 *
 * Whenever we find an equality clause A = B, where both A and B originates from
 * relation attributes (i.e., not random expressions), we create an
 * AttributeEquivalenceClass to record this knowledge. If we later find another
 * equivalence B = C, we create another AttributeEquivalenceClass. Finally, we can
 * apply transitity rules and generate a new AttributeEquivalenceClass which includes
 * A, B and C.
 *
 * Note that equality among the members are identified by the varattno and rteIdentity.
 */
typedef struct AttributeEquivalenceClass
{
	uint32 equivalenceId;
	List *equivalentAttributes;
} AttributeEquivalenceClass;

/*
 *  AttributeEquivalenceClassMember - one member expression of an
 *  AttributeEquivalenceClassMember. The important thing to consider is that
 *  the class member contains "rteIndentity" field. Note that each RTE_RELATION
 *  is assigned a unique rteIdentity in AssignRTEIdentities() function.
 *
 *  "varno" and "varattrno" is directly used from a Var clause that is being added
 *  to the attribute equivalence. Since we only use this class for relations, the member
 *  also includes the relation id field.
 */
typedef struct AttributeEquivalenceClassMember
{
	Index varno;
	AttrNumber varattno;
	Oid relationId;
	int rteIdendity;
} AttributeEquivalenceClassMember;


static uint32 ReferenceRelationCount(RelationRestrictionContext *restrictionContext);
static List * GenerateAttributeEquivalencesForRelationRestrictions(
	RelationRestrictionContext *restrictionContext);
static AttributeEquivalenceClass * AttributeEquivalenceClassForEquivalenceClass(
	EquivalenceClass *plannerEqClass, RelationRestriction *relationRestriction);
static void AddToAttributeEquivalenceClass(PlannerInfo *root, Var *varToBeAdded,
										   AttributeEquivalenceClass **
										   attributeEquivalanceClass);
static Var * GetVarFromAssignedParam(List *parentPlannerParamList,
									 Param *plannerParam);
static List * GenerateAttributeEquivalencesForJoinRestrictions(JoinRestrictionContext
															   *joinRestrictionContext);
static bool AttributeClassContainsAttributeClassMember(AttributeEquivalenceClassMember *
													   inputMember,
													   AttributeEquivalenceClass *
													   attributeEquivalenceClass);
static List * AddAttributeClassToAttributeClassList(AttributeEquivalenceClass *
													attributeEquivalance,
													List *attributeEquivalenceList);
static AttributeEquivalenceClass * GenerateCommonEquivalence(List *
															 attributeEquivalenceList);
static void ListConcatUniqueAttributeClassMemberLists(AttributeEquivalenceClass **
													  firstClass,
													  AttributeEquivalenceClass *
													  secondClass);

/*
 * AllRelationsJoinedOnPartitionKey aims to deduce whether each of the RTE_RELATION
 * is joined with at least one another RTE_RELATION on their partition keys. If each
 * RTE_RELATION follows the above rule, we can conclude that all RTE_RELATIONs are
 * joined on their partition keys.
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
 * AllRelationsJoinedOnPartitionKey uses both relation restrictions and join restrictions
 * to find as much as information that Postgres planner provides to extensions. For the
 * details of the usage, please see GenerateAttributeEquivalencesForRelationRestrictions()
 * and GenerateAttributeEquivalencesForJoinRestrictions()
 *
 * Finally, as the name of the function reveals, the function returns true if all relations
 * are joined on their partition keys. Otherwise, the function returns false.
 */
bool
AllRelationsJoinedOnPartitionKey(RelationRestrictionContext *restrictionContext,
								 JoinRestrictionContext *joinRestrictionContext)
{
	List *relationRestrictionAttributeEquivalenceList = NIL;
	List *joinRestrictionAttributeEquivalenceList = NIL;
	List *allAttributeEquivalenceList = NIL;
	AttributeEquivalenceClass *commonEquivalenceClass = NULL;
	uint32 referenceRelationCount = ReferenceRelationCount(restrictionContext);
	uint32 totalRelationCount = list_length(restrictionContext->relationRestrictionList);
	uint32 nonReferenceRelationCount = totalRelationCount - referenceRelationCount;

	ListCell *commonEqClassCell = NULL;
	ListCell *relationRestrictionCell = NULL;
	Relids commonRteIdentities = NULL;

	/*
	 * If the query includes a single relation which is not a reference table,
	 * we should not check the partition column equality.
	 * Consider two example cases:
	 *   (i)   The query includes only a single colocated relation
	 *   (ii)  A colocated relation is joined with a reference
	 *         table where colocated relation is not joined on the partition key
	 *
	 * For the above two cases, we should not execute the partition column equality
	 * algorithm.
	 */
	if (nonReferenceRelationCount <= 1)
	{
		return true;
	}

	/* reset the equivalence id counter per call to prevent overflows */
	attributeEquivalenceId = 1;

	relationRestrictionAttributeEquivalenceList =
		GenerateAttributeEquivalencesForRelationRestrictions(restrictionContext);
	joinRestrictionAttributeEquivalenceList =
		GenerateAttributeEquivalencesForJoinRestrictions(joinRestrictionContext);

	allAttributeEquivalenceList =
		list_concat(relationRestrictionAttributeEquivalenceList,
					joinRestrictionAttributeEquivalenceList);

	/*
	 * In general we're trying to expand existing the equivalence classes to find a
	 * common equivalence class. The main goal is to test whether this main class
	 * contains all partition keys of the existing relations.
	 */
	commonEquivalenceClass = GenerateCommonEquivalence(allAttributeEquivalenceList);

	/* add the rte indexes of relations to a bitmap */
	foreach(commonEqClassCell, commonEquivalenceClass->equivalentAttributes)
	{
		AttributeEquivalenceClassMember *classMember = lfirst(commonEqClassCell);
		int rteIdentity = classMember->rteIdendity;

		commonRteIdentities = bms_add_member(commonRteIdentities, rteIdentity);
	}

	/* check whether all relations exists in the main restriction list */
	foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction = lfirst(relationRestrictionCell);
		int rteIdentity = GetRTEIdentity(relationRestriction->rte);

		if (PartitionKey(relationRestriction->relationId) &&
			!bms_is_member(rteIdentity, commonRteIdentities))
		{
			return false;
		}
	}

	return true;
}


/*
 * ReferenceRelationCount iterates over the relations and returns the reference table
 * relation count.
 */
static uint32
ReferenceRelationCount(RelationRestrictionContext *restrictionContext)
{
	ListCell *relationRestrictionCell = NULL;
	uint32 referenceRelationCount = 0;

	foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction = lfirst(relationRestrictionCell);

		if (PartitionMethod(relationRestriction->relationId) == DISTRIBUTE_BY_NONE)
		{
			referenceRelationCount++;
		}
	}

	return referenceRelationCount;
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

	foreach(relationRestrictionCell, restrictionContext->relationRestrictionList)
	{
		RelationRestriction *relationRestriction = lfirst(relationRestrictionCell);
		List *equivalenceClasses = relationRestriction->plannerInfo->eq_classes;
		ListCell *equivilanceClassCell = NULL;

		foreach(equivilanceClassCell, equivalenceClasses)
		{
			EquivalenceClass *plannerEqClass = lfirst(equivilanceClassCell);

			AttributeEquivalenceClass *attributeEquivalance =
				AttributeEquivalenceClassForEquivalenceClass(plannerEqClass,
															 relationRestriction);

			attributeEquivalenceList =
				AddAttributeClassToAttributeClassList(attributeEquivalance,
													  attributeEquivalenceList);
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
	AttributeEquivalenceClass *attributeEquivalance =
		palloc0(sizeof(AttributeEquivalenceClass));
	ListCell *equivilanceMemberCell = NULL;
	PlannerInfo *plannerInfo = relationRestriction->plannerInfo;

	attributeEquivalance->equivalenceId = attributeEquivalenceId++;

	foreach(equivilanceMemberCell, plannerEqClass->ec_members)
	{
		EquivalenceMember *equivalenceMember = lfirst(equivilanceMemberCell);
		Node *equivalenceNode = strip_implicit_coercions(
			(Node *) equivalenceMember->em_expr);
		Expr *strippedEquivalenceExpr = (Expr *) equivalenceNode;

		Var *expressionVar = NULL;

		if (IsA(strippedEquivalenceExpr, Param))
		{
			List *parentParamList = relationRestriction->parentPlannerParamList;
			Param *equivalenceParam = (Param *) strippedEquivalenceExpr;

			expressionVar = GetVarFromAssignedParam(parentParamList,
													equivalenceParam);
			if (expressionVar)
			{
				AddToAttributeEquivalenceClass(
					relationRestriction->parentPlannerInfo,
					expressionVar,
					&attributeEquivalance);
			}
		}
		else if (IsA(strippedEquivalenceExpr, Var))
		{
			expressionVar = (Var *) strippedEquivalenceExpr;
			AddToAttributeEquivalenceClass(plannerInfo, expressionVar,
										   &attributeEquivalance);
		}
	}

	return attributeEquivalance;
}


/*
 *
 * GetVarFromAssignedParam returns the Var that is assigned to the given
 * plannerParam if its kind is PARAM_EXEC.
 *
 * If the paramkind is not equal to PARAM_EXEC the function returns NULL. Similarly,
 * if there is no var that the given param is assigned to, the function returns NULL.
 *
 * Rationale behind this function:
 *
 *   While iterating through the equivalence classes of RTE_RELATIONs, we
 *   observe that there are PARAM type of equivalence member expressions for
 *   the RTE_RELATIONs which actually belong to lateral vars from the other query
 *   levels.
 *
 *   We're also keeping track of the RTE_RELATION's parent_root's
 *   plan_param list which is expected to hold the parameters that are required
 *   for its lower level queries as it is documented:
 *
 *        plan_params contains the expressions that this query level needs to
 *        make available to a lower query level that is currently being planned.
 *
 *   This function is a helper function to iterate through the parent query's
 *   plan_params and looks for the param that the equivalence member has. The
 *   comparison is done via the "paramid" field. Finally, if the found parameter's
 *   item is a Var, we conclude that Postgres standard_planner replaced the Var
 *   with the Param on assign_param_for_var() function
 *   @src/backend/optimizer//plan/subselect.c.
 *
 */
static Var *
GetVarFromAssignedParam(List *parentPlannerParamList, Param *plannerParam)
{
	Var *assignedVar = NULL;
	ListCell *plannerParameterCell = NULL;

	Assert(plannerParam != NULL);

	/* we're only interested in parameters that Postgres added for execution */
	if (plannerParam->paramkind != PARAM_EXEC)
	{
		return NULL;
	}

	foreach(plannerParameterCell, parentPlannerParamList)
	{
		PlannerParamItem *plannerParamItem = lfirst(plannerParameterCell);

		if (plannerParamItem->paramId != plannerParam->paramid)
		{
			continue;
		}

		/* TODO: Should we consider PlaceHolderVar?? */
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
 * GenerateCommonEquivalence gets a list of unrelated AttributeEquiavalanceClass
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
GenerateCommonEquivalence(List *attributeEquivalenceList)
{
	AttributeEquivalenceClass *commonEquivalenceClass = NULL;
	AttributeEquivalenceClass *firstEquivalenceClass = NULL;
	Bitmapset *addedEquivalenceIds = NULL;
	uint32 equivalenceListSize = list_length(attributeEquivalenceList);
	uint32 equivalenceClassIndex = 0;

	commonEquivalenceClass = palloc0(sizeof(AttributeEquivalenceClass));
	commonEquivalenceClass->equivalenceId = 0;

	/* think more on this. */
	if (equivalenceListSize < 1)
	{
		return commonEquivalenceClass;
	}

	/* setup the initial state of the main equivalence class */
	firstEquivalenceClass = linitial(attributeEquivalenceList);
	commonEquivalenceClass->equivalentAttributes =
		firstEquivalenceClass->equivalentAttributes;
	addedEquivalenceIds = bms_add_member(addedEquivalenceIds,
										 firstEquivalenceClass->equivalenceId);

	for (; equivalenceClassIndex < equivalenceListSize; ++equivalenceClassIndex)
	{
		AttributeEquivalenceClass *currentEquivalenceClass =
			list_nth(attributeEquivalenceList, equivalenceClassIndex);
		ListCell *equivalenceMemberCell = NULL;

		/*
		 * This is an optimization. If we already added the same equivalence class,
		 * we could skip it since we've already added all the relevant equivalence
		 * members.
		 */
		if (bms_is_member(currentEquivalenceClass->equivalenceId, addedEquivalenceIds))
		{
			continue;
		}

		foreach(equivalenceMemberCell, currentEquivalenceClass->equivalentAttributes)
		{
			AttributeEquivalenceClassMember *attributeEquialanceMember =
				(AttributeEquivalenceClassMember *) lfirst(equivalenceMemberCell);

			if (AttributeClassContainsAttributeClassMember(attributeEquialanceMember,
														   commonEquivalenceClass))
			{
				ListConcatUniqueAttributeClassMemberLists(&commonEquivalenceClass,
														  currentEquivalenceClass);

				addedEquivalenceIds = bms_add_member(addedEquivalenceIds,
													 currentEquivalenceClass->
													 equivalenceId);

				/*
				 * It seems inefficient to start from the beginning.
				 * But, we should somehow restart from the beginning to test that
				 * whether the already skipped ones are equal or not.
				 */
				equivalenceClassIndex = 0;

				break;
			}
		}
	}

	return commonEquivalenceClass;
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
ListConcatUniqueAttributeClassMemberLists(AttributeEquivalenceClass **firstClass,
										  AttributeEquivalenceClass *secondClass)
{
	ListCell *equivalenceClassMemberCell = NULL;
	List *equivalenceMemberList = secondClass->equivalentAttributes;

	foreach(equivalenceClassMemberCell, equivalenceMemberList)
	{
		AttributeEquivalenceClassMember *newEqMember = lfirst(equivalenceClassMemberCell);

		if (AttributeClassContainsAttributeClassMember(newEqMember, *firstClass))
		{
			continue;
		}

		(*firstClass)->equivalentAttributes = lappend((*firstClass)->equivalentAttributes,
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

	foreach(joinRestrictionCell, joinRestrictionContext->joinRestrictionList)
	{
		JoinRestriction *joinRestriction = lfirst(joinRestrictionCell);
		ListCell *restrictionInfoList = NULL;

		foreach(restrictionInfoList, joinRestriction->joinRestrictInfoList)
		{
			RestrictInfo *rinfo = (RestrictInfo *) lfirst(restrictionInfoList);
			OpExpr *restrictionOpExpr = NULL;
			Node *leftNode = NULL;
			Node *rightNode = NULL;
			Expr *strippedLeftExpr = NULL;
			Expr *strippedRightExpr = NULL;
			Var *leftVar = NULL;
			Var *rightVar = NULL;
			Expr *restrictionClause = rinfo->clause;
			AttributeEquivalenceClass *attributeEquivalance = NULL;

			if (!IsA(restrictionClause, OpExpr))
			{
				continue;
			}

			restrictionOpExpr = (OpExpr *) restrictionClause;
			if (list_length(restrictionOpExpr->args) != 2)
			{
				continue;
			}
			if (!OperatorImplementsEquality(restrictionOpExpr->opno))
			{
				continue;
			}

			leftNode = linitial(restrictionOpExpr->args);
			rightNode = lsecond(restrictionOpExpr->args);

			/* we also don't want implicit coercions */
			strippedLeftExpr = (Expr *) strip_implicit_coercions((Node *) leftNode);
			strippedRightExpr = (Expr *) strip_implicit_coercions((Node *) rightNode);

			if (!(IsA(strippedLeftExpr, Var) && IsA(strippedRightExpr, Var)))
			{
				continue;
			}

			leftVar = (Var *) strippedLeftExpr;
			rightVar = (Var *) strippedRightExpr;

			attributeEquivalance = palloc0(sizeof(AttributeEquivalenceClass));
			attributeEquivalance->equivalenceId = attributeEquivalenceId++;

			AddToAttributeEquivalenceClass(joinRestriction->plannerInfo, leftVar,
										   &attributeEquivalance);
			AddToAttributeEquivalenceClass(joinRestriction->plannerInfo, rightVar,
										   &attributeEquivalance);

			attributeEquivalenceList =
				AddAttributeClassToAttributeClassList(attributeEquivalance,
													  attributeEquivalenceList);
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
 *        - Find the corresponding target entry via varno
 *        - if subquery entry is a set operation (i.e., only UNION/UNION ALL allowed)
 *             - recursively add both left and right sides of the set operation's
 *               corresponding target entries
 *        - if subquery is not a set operation
 *             - recursively try to add the corresponding target entry to the
 *               equivalence class
 *
 * Note that this function only adds partition keys to the attributeEquivalanceClass.
 * This implies that there wouldn't be any columns for reference tables.
 */
static void
AddToAttributeEquivalenceClass(PlannerInfo *root, Var *varToBeAdded,
							   AttributeEquivalenceClass **attributeEquivalanceClass)
{
	RangeTblEntry *rangeTableEntry = root->simple_rte_array[varToBeAdded->varno];

	if (rangeTableEntry->rtekind == RTE_RELATION)
	{
		AttributeEquivalenceClassMember *attributeEqMember = NULL;
		Oid relationId = rangeTableEntry->relid;
		Var *relationPartitionKey = NULL;

		if (PartitionMethod(relationId) == DISTRIBUTE_BY_NONE)
		{
			return;
		}

		relationPartitionKey = PartitionKey(relationId);
		if (relationPartitionKey->varattno != varToBeAdded->varattno)
		{
			return;
		}

		attributeEqMember = palloc0(sizeof(AttributeEquivalenceClassMember));

		attributeEqMember->varattno = varToBeAdded->varattno;
		attributeEqMember->varno = varToBeAdded->varno;
		attributeEqMember->rteIdendity = GetRTEIdentity(rangeTableEntry);
		attributeEqMember->relationId = rangeTableEntry->relid;

		(*attributeEquivalanceClass)->equivalentAttributes =
			lappend((*attributeEquivalanceClass)->equivalentAttributes,
					attributeEqMember);
	}
	else if (rangeTableEntry->rtekind == RTE_SUBQUERY && !rangeTableEntry->inh)
	{
		Query *subquery = rangeTableEntry->subquery;
		RelOptInfo *baseRelOptInfo = NULL;
		TargetEntry *subqueryTargetEntry = NULL;

		/* punt if it's a whole-row var rather than a plain column reference */
		if (varToBeAdded->varattno == InvalidAttrNumber)
		{
			return;
		}

		baseRelOptInfo = find_base_rel(root, varToBeAdded->varno);

		/* If the subquery hasn't been planned yet, we have to punt */
		if (baseRelOptInfo->subroot == NULL)
		{
			return;
		}

		Assert(IsA(baseRelOptInfo->subroot, PlannerInfo));

		subquery = baseRelOptInfo->subroot->parse;
		Assert(IsA(subquery, Query));

		/* Get the subquery output expression referenced by the upper Var */
		subqueryTargetEntry = get_tle_by_resno(subquery->targetList,
											   varToBeAdded->varattno);
		if (subqueryTargetEntry == NULL || subqueryTargetEntry->resjunk)
		{
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("subquery %s does not have attribute %d",
								   rangeTableEntry->eref->aliasname,
								   varToBeAdded->varattno)));
		}

		if (!IsA(subqueryTargetEntry->expr, Var))
		{
			return;
		}

		varToBeAdded = (Var *) subqueryTargetEntry->expr;

		/* we need to handle set operations separately */
		if (subquery->setOperations)
		{
			SetOperationStmt *unionStatement =
				(SetOperationStmt *) subquery->setOperations;

			RangeTblRef *leftRangeTableReference = (RangeTblRef *) unionStatement->larg;
			RangeTblRef *rightRangeTableReference = (RangeTblRef *) unionStatement->rarg;

			varToBeAdded->varno = leftRangeTableReference->rtindex;
			AddToAttributeEquivalenceClass(baseRelOptInfo->subroot, varToBeAdded,
										   attributeEquivalanceClass);

			varToBeAdded->varno = rightRangeTableReference->rtindex;
			AddToAttributeEquivalenceClass(baseRelOptInfo->subroot, varToBeAdded,
										   attributeEquivalanceClass);
		}
		else if (varToBeAdded && IsA(varToBeAdded, Var) && varToBeAdded->varlevelsup == 0)
		{
			AddToAttributeEquivalenceClass(baseRelOptInfo->subroot, varToBeAdded,
										   attributeEquivalanceClass);
		}
	}
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
		AttributeEquivalenceClassMember *memberOfClass = lfirst(classCell);
		if (memberOfClass->rteIdendity == inputMember->rteIdendity &&
			memberOfClass->varattno == inputMember->varattno)
		{
			return true;
		}
	}

	return false;
}


/*
 * AddAttributeClassToAttributeClassList checks for certain properties of the
 * input attributeEquivalance before adding it to the attributeEquivalenceList.
 *
 * Firstly, the function skips adding NULL attributeEquivalance to the list.
 * Secondly, since an attribute equivalence class with a single member does
 * not contribute to our purposes, we skip such classed adding to the list.
 */
static List *
AddAttributeClassToAttributeClassList(AttributeEquivalenceClass *attributeEquivalance,
									  List *attributeEquivalenceList)
{
	List *equivalentAttributes = NULL;

	if (attributeEquivalance == NULL)
	{
		return attributeEquivalenceList;
	}

	equivalentAttributes = attributeEquivalance->equivalentAttributes;
	if (list_length(equivalentAttributes) < 2)
	{
		return attributeEquivalenceList;
	}

	attributeEquivalenceList = lappend(attributeEquivalenceList,
									   attributeEquivalance);

	return attributeEquivalenceList;
}
