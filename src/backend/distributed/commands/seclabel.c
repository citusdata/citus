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
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"

/*
 * PreprocessSecLabelStmt is executed before the statement is applied to the local
 * postgres instance.
 *
 * In this stage we can prepare the commands that need to be run on all workers to assign
 * security labels on distributed objects, currently supporting just Role objects.
 */
List *
PreprocessSecLabelStmt(Node *node, const char *queryString,
							  ProcessUtilityContext processUtilityContext)
{
	if (!IsCoordinator() || !ShouldPropagate())
	{
		return NIL;
	}

	SecLabelStmt *secLabelStmt = castNode(SecLabelStmt, node);

	List * objectAddresses = GetObjectAddressListFromParseTree(node, false, false);
	if(!IsAnyObjectDistributed(objectAddresses))
	{
		return NIL;
	}

	if (secLabelStmt->objtype != OBJECT_ROLE)
	{
		if (EnableUnsupportedFeatureMessages)
		{
			ereport(NOTICE, (errmsg("not propagating SECURITY LABEL commands whose"
									" object type is not role"),
							 errhint(
								 "Connect to worker nodes directly to manually run the same"
								 " SECURITY LABEL command after disabling DDL propagation.")));
		}
		return NIL;
	}

	if (!EnableCreateRolePropagation)
	{
		return NIL;
	}

	const char *sql = DeparseTreeNode((Node *) secLabelStmt);

	List *commandList = list_make3(DISABLE_DDL_PROPAGATION,
								   (void *) sql,
								   ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commandList);
}

/*
 * PostprocessSecLabelStmt
 */
List *
PostprocessSecLabelStmt(Node *node, const char *queryString)
{
	if (!EnableCreateRolePropagation || !IsCoordinator() || !ShouldPropagate())
	{
		return NIL;
	}

	SecLabelStmt *secLabelStmt = castNode(SecLabelStmt, node);

	if (secLabelStmt->objtype != OBJECT_ROLE)
	{
		return NIL;
	}

	List * objectAddresses = GetObjectAddressListFromParseTree(node, false, false);
	if(IsAnyObjectDistributed(objectAddresses))
	{
		EnsureAllObjectDependenciesExistOnAllNodes(objectAddresses);
	}

	return NIL;
}


/*
 * SecLabelStmtObjectAddress
 */
List *
SecLabelStmtObjectAddress(Node *node, bool missing_ok, bool isPostprocess)
{
	SecLabelStmt *secLabelStmt = castNode(SecLabelStmt, node);

	Relation rel = NULL; /* not used, but required to pass to get_object_address */
	ObjectAddress address = get_object_address(secLabelStmt->objtype, secLabelStmt->object, &rel,
												   AccessShareLock, missing_ok);

	ObjectAddress *addressPtr = palloc0(sizeof(ObjectAddress));
	*addressPtr = address;
	return list_make1(addressPtr);
}
