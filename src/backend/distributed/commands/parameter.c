#include "postgres.h"

#include "access/genam.h"
#include "catalog/namespace.h"
#include "catalog/pg_parameter_acl.h"
#include "commands/defrem.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

#include "distributed/commands.h"
#include "distributed/deparser.h"
#include "distributed/grant_utils.h"
#include "distributed/listutils.h"
#include "distributed/metadata_sync.h"

static List * GenerateGrantOnParameterFromAclItem(char *parameterName, AclItem *aclItem);
static bool HasAclGrantOption(AclItem *aclItem, AclMode aclMode);
static void CheckPermissionsAndGrants(AclItem *aclItem, AclMode modes[], int numModes);
static void CheckAndAppendQuery(List **queries, AclItem *aclItem, Oid granteeOid,
								char *parameterName, AclMode mode, char *modeStr);


List *
PostprocessGrantParameterStmt(Node *node, const char *queryString)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	EnsurePropagationToCoordinator();

	char *command = DeparseTreeNode(node);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) command,
								ENABLE_DDL_PROPAGATION);

	return NontransactionalNodeDDLTaskList(REMOTE_NODES, commands);
}


/*
 * GenerateGrantOnParameterFromAclItem generates a query string for replicating a users permissions
 * on a database.
 */
static List *
GenerateGrantOnParameterFromAclItem(char *parameterName, AclItem *aclItem)
{
	/*
	 * seems unlikely but we check if there is a grant option in the list without the actual permission
	 */
	CheckPermissionsAndGrants(aclItem, (AclMode[]) { ACL_SET, ACL_ALTER_SYSTEM }, 2);
	Oid granteeOid = aclItem->ai_grantee;
	List *queries = NIL;

	queries = lappend(queries, GenerateSetRoleQuery(aclItem->ai_grantor));

	CheckAndAppendQuery(&queries, aclItem, granteeOid, parameterName, ACL_SET, "SET");
	CheckAndAppendQuery(&queries, aclItem, granteeOid, parameterName, ACL_ALTER_SYSTEM,
						"ALTER SYSTEM");

	queries = lappend(queries, "RESET ROLE");

	return queries;
}

/*
 * CheckAndAppendQuery checks if the aclItem has the given mode and if it has, it appends the
 * corresponding query to the queries list.
*/
static void
CheckAndAppendQuery(List **queries, AclItem *aclItem, Oid granteeOid, char *parameterName,
					AclMode mode, char *modeStr)
{
	AclResult aclresult = pg_parameter_aclcheck(parameterName, granteeOid, mode);
	if (aclresult == ACLCHECK_OK)
	{
		char *query = DeparseTreeNode((Node *) GenerateGrantStmtForRightsWithObjectName(
										  OBJECT_PARAMETER_ACL, granteeOid, parameterName,
										  modeStr,
										  HasAclGrantOption(aclItem, mode)));

		/* remove the semicolon at the end of the query since it is already */
		/* appended in metadata_sync phase */
		query[strlen(query) - 1] = '\0';

		*queries = lappend(*queries, query);
	}
}

/*
* CheckPermissionsAndGrants checks if the aclItem has the valid permissions and grants
* for the given modes.
*/
static void
CheckPermissionsAndGrants(AclItem *aclItem, AclMode modes[], int numModes)
{
	AclMode permissions = ACLITEM_GET_PRIVS(*aclItem) & ACL_ALL_RIGHTS_PARAMETER_ACL;
	AclMode grants = ACLITEM_GET_GOPTIONS(*aclItem) & ACL_ALL_RIGHTS_PARAMETER_ACL;

	for (int i = 0; i < numModes; i++)
	{
		AclMode mode = modes[i];
		if(!(grants & mode) || (permissions & mode)){
			ereport(ERROR, (errmsg("ACL item has no grant option for mode %lu", mode)));
		}
	}
}


static bool
HasAclGrantOption(AclItem *aclItem, AclMode aclMode)
{
	return (aclItem->ai_privs & ACL_GRANT_OPTION_FOR(aclMode)) != 0;
}


List *
GrantOnParameters(void)
{
	/* Open pg_shdescription catalog */
	Relation paramPermissionRelation = table_open(ParameterAclRelationId,
												  AccessShareLock);


	int scanKeyCount = 0;
	bool indexOk = false;
	SysScanDesc scan = systable_beginscan(paramPermissionRelation, InvalidOid,
										  indexOk, NULL, scanKeyCount, NULL);
	HeapTuple tuple;
	List *commands = NIL;
	while ((tuple = systable_getnext(scan)) != NULL)
	{
		bool isNull = false;

		TupleDesc tupdesc = RelationGetDescr(paramPermissionRelation);

		Datum aclDatum = heap_getattr(tuple, Anum_pg_parameter_acl_paracl, tupdesc,
									  &isNull);
		Datum parameterNameDatum = heap_getattr(tuple, Anum_pg_parameter_acl_parname,
												tupdesc,
												&isNull);

		char *parameterName = TextDatumGetCString(parameterNameDatum);

		Acl *acl = DatumGetAclPCopy(aclDatum);
		AclItem *aclDat = ACL_DAT(acl);
		int aclNum = ACL_NUM(acl);


		for (int i = 0; i < aclNum; i++)
		{
			commands = list_concat(commands,
								   GenerateGrantOnParameterFromAclItem(
									   parameterName, &aclDat[i]));
		}
	}

	/* End the scan and close the catalog */
	systable_endscan(scan);
	table_close(paramPermissionRelation, AccessShareLock);

	return commands;
}
