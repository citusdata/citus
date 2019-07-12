/*-------------------------------------------------------------------------
 *
 * foreign_key_relationship.c
 *   This file contains functions for creating foreign key relationship graph
 *   between distributed tables. Created relationship graph will be hold by
 *   a static variable defined in this file until an invalidation comes in.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if PG_VERSION_NUM >= 120000
#include "access/genam.h"
#endif
#include "access/htup_details.h"
#include "access/stratnum.h"
#if PG_VERSION_NUM >= 120000
#include "access/table.h"
#endif
#include "catalog/pg_constraint.h"
#include "distributed/foreign_key_relationship.h"
#include "distributed/hash_helpers.h"
#include "distributed/listutils.h"
#include "distributed/version_compat.h"
#include "nodes/pg_list.h"
#include "storage/lockdefs.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


/*
 * ForeignConstraintRelationshipGraph holds the graph data structure for foreign constraint relationship
 * between relations. We will only have single static instance of that struct and it
 * will be invalidated after change on any foreign constraint.
 */
typedef struct ForeignConstraintRelationshipGraph
{
	HTAB *nodeMap;
	bool isValid;
}ForeignConstraintRelationshipGraph;

/*
 * ForeignConstraintRelationshipNode holds the data for each node of the ForeignConstraintRelationshipGraph
 * For each node we have relation id, which is the Oid of that relation, visiting
 * information for that node in the latest DFS and the list of adjacency nodes.
 * Note that we also hold back adjacency nodes for getting referenced node over
 * that one.
 */
typedef struct ForeignConstraintRelationshipNode
{
	Oid relationId;
	bool visited;
	List *adjacencyList;
	List *backAdjacencyList;
}ForeignConstraintRelationshipNode;


/*
 * ForeignConstraintRelationshipEdge will only be used while creating the ForeignConstraintRelationshipGraph.
 * It won't show edge information on the graph, yet will be used in the pre-processing
 * phase.
 */
typedef struct ForeignConstraintRelationshipEdge
{
	Oid referencingRelationOID;
	Oid referencedRelationOID;
}ForeignConstraintRelationshipEdge;


static ForeignConstraintRelationshipGraph *fConstraintRelationshipGraph = NULL;

static void CreateForeignConstraintRelationshipGraph(void);
static void PopulateAdjacencyLists(void);
static int CompareForeignConstraintRelationshipEdges(const void *leftElement, const
													 void *rightElement);
static void AddForeignConstraintRelationshipEdge(Oid referencingOid, Oid referencedOid);
static ForeignConstraintRelationshipNode * CreateOrFindNode(HTAB *adjacencyLists, Oid
															relid);
static void GetConnectedListHelper(ForeignConstraintRelationshipNode *node,
								   List **adjacentNodeList, bool
								   isReferencing);
static List * GetForeignConstraintRelationshipHelper(Oid relationId, bool isReferencing);


/*
 * ReferencedRelationIdList is a wrapper function around GetForeignConstraintRelationshipHelper
 * to get list of relation IDs which are referenced by the given relation id.
 *
 * Note that, if relation A is referenced by relation B and relation B is referenced
 * by relation C, then the result list for relation A consists of the relation
 * IDs of relation B and relation C.
 */
List *
ReferencedRelationIdList(Oid relationId)
{
	return GetForeignConstraintRelationshipHelper(relationId, false);
}


/*
 * ReferencingRelationIdList is a wrapper function around GetForeignConstraintRelationshipHelper
 * to get list of relation IDs which are referencing by the given relation id.
 *
 * Note that, if relation A is referenced by relation B and relation B is referenced
 * by relation C, then the result list for relation C consists of the relation
 * IDs of relation A and relation B.
 */
List *
ReferencingRelationIdList(Oid relationId)
{
	return GetForeignConstraintRelationshipHelper(relationId, true);
}


/*
 * GetForeignConstraintRelationshipHelper returns the list of oids referenced or
 * referencing given relation id. It is a helper function for providing results
 * to public functions ReferencedRelationIdList and ReferencingRelationIdList.
 */
static List *
GetForeignConstraintRelationshipHelper(Oid relationId, bool isReferencing)
{
	List *foreignConstraintList = NIL;
	List *foreignNodeList = NIL;
	ListCell *nodeCell = NULL;
	bool isFound = false;
	ForeignConstraintRelationshipNode *relationNode = NULL;

	CreateForeignConstraintRelationshipGraph();

	relationNode = (ForeignConstraintRelationshipNode *) hash_search(
		fConstraintRelationshipGraph->nodeMap, &relationId,
		HASH_FIND, &isFound);

	if (!isFound)
	{
		/*
		 * If there is no node with the given relation id, that means given table
		 * is not referencing and is not referenced by any table
		 */
		return NIL;
	}

	GetConnectedListHelper(relationNode, &foreignNodeList, isReferencing);

	/*
	 * We need only their OIDs, we get back node list to make their visited
	 * variable to false for using them iteratively.
	 */
	foreach(nodeCell, foreignNodeList)
	{
		ForeignConstraintRelationshipNode *currentNode =
			(ForeignConstraintRelationshipNode *) lfirst(nodeCell);

		foreignConstraintList = lappend_oid(foreignConstraintList,
											currentNode->relationId);
		currentNode->visited = false;
	}

	/* set to false separately, since we don't add itself to foreign node list */
	relationNode->visited = false;

	return foreignConstraintList;
}


/*
 * CreateForeignConstraintRelationshipGraph creates the foreign constraint relation graph using
 * foreign constraint provided by pg_constraint metadata table.
 */
static void
CreateForeignConstraintRelationshipGraph()
{
	MemoryContext oldContext;
	MemoryContext fConstraintRelationshipMemoryContext = NULL;
	HASHCTL info;
	uint32 hashFlags = 0;

	/* if we have already created the graph, use it */
	if (IsForeignConstraintRelationshipGraphValid())
	{
		return;
	}

	ClearForeignConstraintRelationshipGraphContext();

	fConstraintRelationshipMemoryContext = AllocSetContextCreateExtended(
		CacheMemoryContext,
		"Forign Constraint Relationship Graph Context",
		ALLOCSET_DEFAULT_MINSIZE,
		ALLOCSET_DEFAULT_INITSIZE,
		ALLOCSET_DEFAULT_MAXSIZE);

	oldContext = MemoryContextSwitchTo(fConstraintRelationshipMemoryContext);

	fConstraintRelationshipGraph = (ForeignConstraintRelationshipGraph *) palloc(
		sizeof(ForeignConstraintRelationshipGraph));
	fConstraintRelationshipGraph->isValid = false;

	/* create (oid) -> [ForeignConstraintRelationshipNode] hash */
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(ForeignConstraintRelationshipNode);
	info.hash = oid_hash;
	info.hcxt = CurrentMemoryContext;
	hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	fConstraintRelationshipGraph->nodeMap = hash_create(
		"foreign key relationship map (oid)",
		32, &info, hashFlags);

	PopulateAdjacencyLists();

	fConstraintRelationshipGraph->isValid = true;
	MemoryContextSwitchTo(oldContext);
}


/*
 * IsForeignConstraintGraphValid check whether there is a valid graph.
 */
bool
IsForeignConstraintRelationshipGraphValid()
{
	if (fConstraintRelationshipGraph != NULL && fConstraintRelationshipGraph->isValid)
	{
		return true;
	}

	return false;
}


/*
 * SetForeignConstraintGraphInvalid sets the validity of the graph to false.
 */
void
SetForeignConstraintRelationshipGraphInvalid()
{
	if (fConstraintRelationshipGraph != NULL)
	{
		fConstraintRelationshipGraph->isValid = false;
	}
}


/*
 * GetConnectedListHelper is the function for getting nodes connected (or connecting) to
 * the given relation. adjacentNodeList holds the result for recursive calls and
 * by changing isReferencing caller function can select connected or connecting
 * adjacency list.
 *
 */
static void
GetConnectedListHelper(ForeignConstraintRelationshipNode *node, List **adjacentNodeList,
					   bool isReferencing)
{
	ListCell *nodeCell = NULL;
	List *neighbourList = NIL;

	node->visited = true;

	if (isReferencing)
	{
		neighbourList = node->backAdjacencyList;
	}
	else
	{
		neighbourList = node->adjacencyList;
	}

	foreach(nodeCell, neighbourList)
	{
		ForeignConstraintRelationshipNode *neighborNode =
			(ForeignConstraintRelationshipNode *) lfirst(nodeCell);
		if (neighborNode->visited == false)
		{
			*adjacentNodeList = lappend(*adjacentNodeList, neighborNode);
			GetConnectedListHelper(neighborNode, adjacentNodeList, isReferencing);
		}
	}
}


/*
 * PopulateAdjacencyLists gets foreign constraint relationship information from pg_constraint
 * metadata table and populates them to the foreign constraint relation graph.
 */
static void
PopulateAdjacencyLists(void)
{
	SysScanDesc scanDescriptor;
	HeapTuple tuple;
	Relation pgConstraint;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;

	Oid prevReferencingOid = InvalidOid;
	Oid prevReferencedOid = InvalidOid;
	List *frelEdgeList = NIL;
	ListCell *frelEdgeCell = NULL;

	pgConstraint = heap_open(ConstraintRelationId, AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_constraint_contype, BTEqualStrategyNumber, F_CHAREQ,
				CharGetDatum(CONSTRAINT_FOREIGN));
	scanDescriptor = systable_beginscan(pgConstraint, InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	while (HeapTupleIsValid(tuple = systable_getnext(scanDescriptor)))
	{
		Form_pg_constraint constraintForm = (Form_pg_constraint) GETSTRUCT(tuple);
		ForeignConstraintRelationshipEdge *currentFConstraintRelationshipEdge = NULL;

		currentFConstraintRelationshipEdge = palloc(
			sizeof(ForeignConstraintRelationshipEdge));
		currentFConstraintRelationshipEdge->referencingRelationOID =
			constraintForm->conrelid;
		currentFConstraintRelationshipEdge->referencedRelationOID =
			constraintForm->confrelid;

		frelEdgeList = lappend(frelEdgeList, currentFConstraintRelationshipEdge);
	}

	/*
	 * Since there is no index on columns we are planning to sort tuples
	 * sorting tuples manually instead of using scan keys
	 */
	frelEdgeList = SortList(frelEdgeList, CompareForeignConstraintRelationshipEdges);

	foreach(frelEdgeCell, frelEdgeList)
	{
		ForeignConstraintRelationshipEdge *currentFConstraintRelationshipEdge =
			(ForeignConstraintRelationshipEdge *) lfirst(frelEdgeCell);

		/* we just saw this edge, no need to add it twice */
		if (currentFConstraintRelationshipEdge->referencingRelationOID ==
			prevReferencingOid &&
			currentFConstraintRelationshipEdge->referencedRelationOID ==
			prevReferencedOid)
		{
			continue;
		}

		AddForeignConstraintRelationshipEdge(
			currentFConstraintRelationshipEdge->referencingRelationOID,
			currentFConstraintRelationshipEdge->
			referencedRelationOID);

		prevReferencingOid = currentFConstraintRelationshipEdge->referencingRelationOID;
		prevReferencedOid = currentFConstraintRelationshipEdge->referencedRelationOID;
	}

	systable_endscan(scanDescriptor);
	heap_close(pgConstraint, AccessShareLock);
}


/*
 * CompareForeignConstraintRelationshipEdges is a helper function to compare two
 * ForeignConstraintRelationshipEdge using referencing and referenced ids respectively.
 */
static int
CompareForeignConstraintRelationshipEdges(const void *leftElement, const
										  void *rightElement)
{
	const ForeignConstraintRelationshipEdge *leftEdge = *((const
														   ForeignConstraintRelationshipEdge
														   **) leftElement);
	const ForeignConstraintRelationshipEdge *rightEdge = *((const
															ForeignConstraintRelationshipEdge
															**) rightElement);

	int referencingDiff = leftEdge->referencingRelationOID -
						  rightEdge->referencingRelationOID;
	int referencedDiff = leftEdge->referencedRelationOID -
						 rightEdge->referencedRelationOID;

	if (referencingDiff != 0)
	{
		return referencingDiff;
	}

	return referencedDiff;
}


/*
 * AddForeignConstraintRelationshipEdge adds edge between the nodes having given OIDs
 * by adding referenced node to the adjacency list referencing node and adding
 * referencing node to the back adjacency list of referenced node.
 */
static void
AddForeignConstraintRelationshipEdge(Oid referencingOid, Oid referencedOid)
{
	ForeignConstraintRelationshipNode *referencingNode = CreateOrFindNode(
		fConstraintRelationshipGraph->nodeMap, referencingOid);
	ForeignConstraintRelationshipNode *referencedNode = CreateOrFindNode(
		fConstraintRelationshipGraph->nodeMap, referencedOid);

	referencingNode->adjacencyList = lappend(referencingNode->adjacencyList,
											 referencedNode);
	referencedNode->backAdjacencyList = lappend(referencedNode->backAdjacencyList,
												referencingNode);
}


/*
 * CreateOrFindNode either gets or adds new node to the foreign constraint relation graph
 */
static ForeignConstraintRelationshipNode *
CreateOrFindNode(HTAB *adjacencyLists, Oid relid)
{
	bool found = false;
	ForeignConstraintRelationshipNode *node =
		(ForeignConstraintRelationshipNode *) hash_search(adjacencyLists,
														  &relid, HASH_ENTER,
														  &found);

	if (!found)
	{
		node->adjacencyList = NIL;
		node->backAdjacencyList = NIL;
		node->visited = false;
	}

	return node;
}


/*
 * ClearForeignConstraintRelationshipGraphContext clear all the allocated memory obtained
 * for foreign constraint relationship graph. Since all the variables of relationship
 * graph was obtained within the same context, destroying hash map is enough as
 * it deletes the context.
 */
void
ClearForeignConstraintRelationshipGraphContext()
{
	if (fConstraintRelationshipGraph == NULL)
	{
		return;
	}

	hash_destroy(fConstraintRelationshipGraph->nodeMap);
	fConstraintRelationshipGraph = NULL;
}
