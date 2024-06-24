/*-------------------------------------------------------------------------
 * foreign_key_relationship.h
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef FOREIGN_KEY_RELATIONSHIP_H
#define FOREIGN_KEY_RELATIONSHIP_H

#include "postgres.h"

#include "postgres_ext.h"

#include "nodes/primnodes.h"
#include "utils/hsearch.h"
#include "utils/relcache.h"

extern List * GetForeignKeyConnectedRelationIdList(Oid relationId);
extern bool ShouldUndistributeCitusLocalTable(Oid relationId);
extern List * ReferencedRelationIdList(Oid relationId);
extern List * ReferencingRelationIdList(Oid relationId);
extern void SetForeignConstraintRelationshipGraphInvalid(void);
extern bool OidVisited(HTAB *oidVisitedMap, Oid oid);
extern void VisitOid(HTAB *oidVisitedMap, Oid oid);

#endif
