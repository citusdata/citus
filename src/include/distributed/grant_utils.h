/*-------------------------------------------------------------------------
 *
 * grant_utils.h
 *
 * Routines for grant operations.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CITUS_GRANT_UTILS_H
#define CITUS_GRANT_UTILS_H
#include "postgres.h"

#include "nodes/parsenodes.h"

#if PG_VERSION_NUM >= PG_VERSION_15
extern List * GenerateGrantStmtOnParametersFromCatalogTable(void);
#endif /* PG_VERSION_NUM >= PG_VERSION_15 */

extern char * GenerateSetRoleQuery(Oid roleOid);
extern GrantStmt * GenerateGrantStmtForRights(ObjectType objectType,
											  Oid roleOid,
											  Oid objectId,
											  char *permission,
											  bool withGrantOption);
extern GrantStmt * GenerateGrantStmtForRightsWithObjectName(ObjectType objectType,
															Oid roleOid,
															char *objectName,
															char *permission,
															bool withGrantOption);
extern GrantStmt * BaseGenerateGrantStmtForRights(ObjectType objectType,
												  Oid roleOid,
												  Oid objectId,
												  char *objectName,
												  char *permission,
												  bool withGrantOption);


#endif   /* CITUS_GRANT_UTILS_H */
