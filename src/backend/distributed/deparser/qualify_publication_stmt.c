/*-------------------------------------------------------------------------
 *
 * qualify_publication_stmt.c
 *	  Functions specialized in fully qualifying all publication statements. These
 *	  functions are dispatched from qualify.c
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "nodes/nodes.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"

#include "distributed/deparser.h"
#include "distributed/listutils.h"

static void QualifyPublicationObjects(List *publicationObjects);
static void QualifyPublicationRangeVar(RangeVar *publication);


/*
 * QualifyCreatePublicationStmt quailifies the publication names of the
 * CREATE PUBLICATION statement.
 */
void
QualifyCreatePublicationStmt(Node *node)
{
	CreatePublicationStmt *stmt = castNode(CreatePublicationStmt, node);

	QualifyPublicationObjects(stmt->pubobjects);
}


/*
 * QualifyPublicationObjects ensures all table names in a list of
 * publication objects are fully qualified.
 */
static void
QualifyPublicationObjects(List *publicationObjects)
{
	PublicationObjSpec *publicationObject = NULL;

	foreach_declared_ptr(publicationObject, publicationObjects)
	{
		if (publicationObject->pubobjtype == PUBLICATIONOBJ_TABLE)
		{
			/* FOR TABLE ... */
			PublicationTable *publicationTable = publicationObject->pubtable;

			QualifyPublicationRangeVar(publicationTable->relation);
		}
	}
}


/*
 * QualifyPublicationObjects ensures all table names in a list of
 * publication objects are fully qualified.
 */
void
QualifyAlterPublicationStmt(Node *node)
{
	AlterPublicationStmt *stmt = castNode(AlterPublicationStmt, node);

	QualifyPublicationObjects(stmt->pubobjects);
}


/*
 * QualifyPublicationRangeVar qualifies the given publication RangeVar if it is not qualified.
 */
static void
QualifyPublicationRangeVar(RangeVar *publication)
{
	if (publication->schemaname == NULL)
	{
		Oid publicationOid = RelnameGetRelid(publication->relname);
		Oid schemaOid = get_rel_namespace(publicationOid);
		publication->schemaname = get_namespace_name(schemaOid);
	}
}
