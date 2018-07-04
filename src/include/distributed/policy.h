/*-------------------------------------------------------------------------
 * policy.h
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_POLICY_H
#define CITUS_POLICY_H

#include "utils/rel.h"

extern List * CreatePolicyCommands(Oid relationId);
extern void ErrorIfUnsupportedPolicy(Relation relation);
extern void ErrorIfUnsupportedPolicyExpr(Node *expr);

extern List * PlanCreatePolicyStmt(CreatePolicyStmt *stmt);
extern List * PlanAlterPolicyStmt(AlterPolicyStmt *stmt);
extern List * PlanDropPolicyStmt(DropStmt *stmt, const char *queryString);
extern bool IsPolicyRenameStmt(RenameStmt *stmt);

extern void CreatePolicyEventExtendNames(CreatePolicyStmt *stmt, const char *schemaName,
										 uint64 shardId);
extern void AlterPolicyEventExtendNames(AlterPolicyStmt *stmt, const char *schemaName,
										uint64 shardId);
extern void RenamePolicyEventExtendNames(RenameStmt *stmt, const char *schemaName, uint64
										 shardId);
extern void DropPolicyEventExtendNames(DropStmt *stmt, const char *schemaName, uint64
									   shardId);

#endif /*CITUS_POLICY_H */
