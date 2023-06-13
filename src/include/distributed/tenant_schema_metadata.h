/*-------------------------------------------------------------------------
 *
 * tenant_schema_metadata.h
 *
 * This file contains functions to query and modify tenant schema metadata,
 * which is used to track the schemas used for schema-based sharding in
 * Citus.
 *
 * -------------------------------------------------------------------------
 */

#ifndef TENANT_SCHEMA_METADATA_H
#define TENANT_SCHEMA_METADATA_H

#include "postgres.h"

/* accessors */
extern Oid ColocationIdGetTenantSchemaId(uint32 colocationId);
extern uint32 SchemaIdGetTenantColocationId(Oid schemaId);
extern bool IsTenantSchema(Oid schemaId);
extern bool IsTenantSchemaColocationGroup(uint32 colocationId);

/*
 * Local only modifiers.
 *
 * These functions may not make much sense by themselves. They are mainly
 * exported for tenant-schema management (schema_based_sharding.c) and
 * metadata-sync layer (metadata_sync.c).
 */
extern void InsertTenantSchemaLocally(Oid schemaId, uint32 colocationId);
extern void DeleteTenantSchemaLocally(Oid schemaId);

#endif /* TENANT_SCHEMA_METADATA_H */
