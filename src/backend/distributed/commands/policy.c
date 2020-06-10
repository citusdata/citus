/*-------------------------------------------------------------------------
 * policy.c
 *
 * This file contains functions to create, alter and drop policies on
 * distributed tables.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/namespace.h"
#include "commands/policy.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "distributed/metadata_cache.h"
#include "miscadmin.h"
#include "rewrite/rowsecurity.h"
#include "utils/builtins.h"
#include "utils/ruleutils.h"


static const char * unparse_policy_command(const char aclchar);
static List * GetPolicyListForRelation(Oid relationId);
static char * CreatePolicyCommandForPolicy(Oid relationId, RowSecurityPolicy *policy);


/*
 * CreatePolicyCommands takes in a relationId, and returns the list of create policy
 * commands needed to reconstruct the policies of that table.
 */
List *
CreatePolicyCommands(Oid relationId)
{
	List *commands = NIL;

	List *policyList = GetPolicyListForRelation(relationId);

	RowSecurityPolicy *policy;
	foreach_ptr(policy, policyList)
	{
		char *createPolicyCommand = CreatePolicyCommandForPolicy(relationId, policy);
		commands = lappend(commands, createPolicyCommand);
	}

	return commands;
}


/*
 * GetPolicyListForRelation returns a list of RowSecurityPolicy objects identifying
 * the policies on the relation with relationId. Note that this function acquires
 * AccessShareLock on relation and does not release it in the end to make sure that
 * caller will process valid policies through the transaction.
 */
static List *
GetPolicyListForRelation(Oid relationId)
{
	Relation relation = heap_open(relationId, AccessShareLock);

	if (!relation_has_policies(relation))
	{
		heap_close(relation, NoLock);

		return NIL;
	}

	if (relation->rd_rsdesc == NULL)
	{
		/*
		 * there are policies, but since RLS is not enabled they are not loaded into
		 * cache, we will do so here for us to access
		 */
		RelationBuildRowSecurity(relation);
	}

	List *policyList = NIL;

	RowSecurityPolicy *policy;
	foreach_ptr(policy, relation->rd_rsdesc->policies)
	{
		policyList = lappend(policyList, policy);
	}

	heap_close(relation, NoLock);

	return policyList;
}


/*
 * CreatePolicyCommandForPolicy takes a relationId and a policy, returns
 * the CREATE POLICY command needed to reconstruct the policy identified
 * by the "policy" object on the relation with relationId.
 */
static char *
CreatePolicyCommandForPolicy(Oid relationId, RowSecurityPolicy *policy)
{
	char *relationName = generate_qualified_relation_name(relationId);
	List *relationContext = deparse_context_for(relationName, relationId);

	StringInfo createPolicyCommand = makeStringInfo();

	appendStringInfo(createPolicyCommand, "CREATE POLICY %s ON %s FOR %s",
					 quote_identifier(policy->policy_name),
					 relationName,
					 unparse_policy_command(policy->polcmd));


	appendStringInfoString(createPolicyCommand, " TO ");

	/*
	 * iterate over all roles and append them to the ddl command with commas
	 * separating the role names
	 */
	Oid *roles = (Oid *) ARR_DATA_PTR(policy->roles);
	for (int roleIndex = 0; roleIndex < ARR_DIMS(policy->roles)[0]; roleIndex++)
	{
		const char *roleName;

		if (roleIndex > 0)
		{
			appendStringInfoString(createPolicyCommand, ", ");
		}

		if (roles[roleIndex] == ACL_ID_PUBLIC)
		{
			roleName = "PUBLIC";
		}
		else
		{
			roleName = quote_identifier(GetUserNameFromId(roles[roleIndex], false));
		}

		appendStringInfoString(createPolicyCommand, roleName);
	}

	if (policy->qual)
	{
		char *qualString = deparse_expression((Node *) (policy->qual),
											  relationContext, false, false);
		appendStringInfo(createPolicyCommand, " USING (%s)", qualString);
	}

	if (policy->with_check_qual)
	{
		char *withCheckQualString = deparse_expression(
			(Node *) (policy->with_check_qual), relationContext, false, false);
		appendStringInfo(createPolicyCommand, " WITH CHECK (%s)",
						 withCheckQualString);
	}

	return createPolicyCommand->data;
}


/*
 * unparse_policy_command takes the type of a policy command and converts it to its full
 * command string. This function is the exact inverse of parse_policy_command that is in
 * postgres.
 */
static const char *
unparse_policy_command(const char aclchar)
{
	switch (aclchar)
	{
		case '*':
		{
			return "ALL";
		}

		case ACL_SELECT_CHR:
		{
			return "SELECT";
		}

		case ACL_INSERT_CHR:
		{
			return "INSERT";
		}

		case ACL_UPDATE_CHR:
		{
			return "UPDATE";
		}

		case ACL_DELETE_CHR:
		{
			return "DELETE";
		}

		default:
		{
			elog(ERROR, "unrecognized aclchar: %d", aclchar);
			return NULL;
		}
	}
}


/* placeholder for PreprocessCreatePolicyStmt */
List *
PreprocessCreatePolicyStmt(Node *node, const char *queryString)
{
	CreatePolicyStmt *stmt = castNode(CreatePolicyStmt, node);
	Oid relationId = RangeVarGetRelid(stmt->table,
									  AccessExclusiveLock,
									  false);
	if (IsCitusTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("policies on distributed tables are only supported in "
							   "Citus Enterprise")));
	}

	/* placeholder for future implementation */
	return NIL;
}


/* placeholder for PreprocessAlterPolicyStmt */
List *
PreprocessAlterPolicyStmt(Node *node, const char *queryString)
{
	/* placeholder for future implementation */
	return NIL;
}


/* placeholder for ErrorIfUnsupportedPolicy */
void
ErrorIfUnsupportedPolicy(Relation relation)
{
	if (relation_has_policies(relation))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("policies on distributed tables are only supported in "
							   "Citus Enterprise"),
						errhint("Remove any policies on a table before distributing")));
	}
}


/* placeholder for ErrorIfUnsupportedPolicyExpr */
void
ErrorIfUnsupportedPolicyExpr(Node *expr)
{
	/* placeholder for future implementation */
}


/* placeholder for PreprocessDropPolicyStmt */
List *
PreprocessDropPolicyStmt(Node *node, const char *queryString)
{
	/* placeholder for future implementation */
	return NIL;
}


/* placeholder for IsPolicyRenameStmt */
bool
IsPolicyRenameStmt(RenameStmt *stmt)
{
	/* placeholder for future implementation */
	return false;
}


/* placeholder for CreatePolicyEventExtendNames */
void
CreatePolicyEventExtendNames(CreatePolicyStmt *stmt, const char *schemaName, uint64
							 shardId)
{
	/* placeholder for future implementation */
}


/* placeholder for AlterPolicyEventExtendNames */
void
AlterPolicyEventExtendNames(AlterPolicyStmt *stmt, const char *schemaName, uint64 shardId)
{
	/* placeholder for future implementation */
}


/* placeholder for RenamePolicyEventExtendNames */
void
RenamePolicyEventExtendNames(RenameStmt *stmt, const char *schemaName, uint64 shardId)
{
	/* placeholder for future implementation */
}


/* placeholder for DropPolicyEventExtendNames */
void
DropPolicyEventExtendNames(DropStmt *dropStmt, const char *schemaName, uint64 shardId)
{
	/* placeholder for future implementation */
}
