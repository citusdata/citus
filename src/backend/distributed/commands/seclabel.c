/*-------------------------------------------------------------------------
 *
 * seclabel.c
 *
 *    This file contains the logic of SECURITY LABEL statement propagation.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/coordinator_protocol.h"
#include "distributed/deparser.h"
#include "distributed/log_utils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"


/*
 * PostprocessSecLabelStmt prepares the commands that need to be run on all workers to assign
 * security labels on distributed objects, currently supporting just Role objects.
 * It also ensures that all object dependencies exist on all
 * nodes for the object in the SecLabelStmt.
 */
List *
PostprocessSecLabelStmt(Node *node, const char *queryString)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	SecLabelStmt *secLabelStmt = castNode(SecLabelStmt, node);

	List *objectAddresses = GetObjectAddressListFromParseTree(node, false, true);
	if (!IsAnyObjectDistributed(objectAddresses))
	{
		return NIL;
	}

	if (secLabelStmt->objtype != OBJECT_ROLE)
	{
		/*
		 * If we are not in the coordinator, we don't want to interrupt the security
		 * label command with notices, the user expects that from the worker node
		 * the command will not be propagated
		 */
		if (EnableUnsupportedFeatureMessages && IsCoordinator())
		{
			ereport(NOTICE, (errmsg("not propagating SECURITY LABEL commands whose "
									"object type is not role"),
							 errhint("Connect to worker nodes directly to manually "
									 "run the same SECURITY LABEL command.")));
		}
		return NIL;
	}

	if (!EnableCreateRolePropagation)
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureAllObjectDependenciesExistOnAllNodes(objectAddresses);

	const char *sql = DeparseTreeNode((Node *) secLabelStmt);

	List *commandList = list_make3(DISABLE_DDL_PROPAGATION,
								   (void *) sql,
								   ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commandList);
}


/*
 * SecLabelStmtObjectAddress returns the object address of the object on
 * which this statement operates (secLabelStmt->object). Note that it has no limitation
 * on the object type being OBJECT_ROLE. This is intentionally implemented like this
 * since it is fairly simple to implement and we might extend SECURITY LABEL propagation
 * in the future to include more object types.
 */
List *
SecLabelStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	SecLabelStmt *secLabelStmt = castNode(SecLabelStmt, node);

	Relation rel = NULL;
	ObjectAddress address = get_object_address(secLabelStmt->objtype,
											   secLabelStmt->object, &rel,
											   AccessShareLock, missing_ok);
	if (rel != NULL)
	{
		relation_close(rel, AccessShareLock);
	}

	ObjectAddress *addressPtr = palloc0(sizeof(ObjectAddress));
	*addressPtr = address;
	return list_make1(addressPtr);
}


/*
 * citus_test_object_relabel is a dummy function for check_object_relabel_type hook.
 * It is meant to be used in tests combined with citus_test_register_label_provider
 */
void
citus_test_object_relabel(const ObjectAddress *object, const char *seclabel)
{
	if (seclabel == NULL ||
		strcmp(seclabel, "citus_unclassified") == 0 ||
		strcmp(seclabel, "citus_classified") == 0 ||
		strcmp(seclabel, "citus '!unclassified") == 0)
	{
		return;
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_NAME),
			 errmsg("'%s' is not a valid security label for Citus tests.", seclabel)));
}
