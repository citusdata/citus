/*-------------------------------------------------------------------------
 *
 * serialize_distributed_ddls.h
 *
 * Declarations for public functions related to serializing distributed
 * DDLs.
 *
 *-------------------------------------------------------------------------
 */

#ifndef SERIALIZE_DDLS_OVER_CATALOG_H
#define SERIALIZE_DDLS_OVER_CATALOG_H

#include "postgres.h"

#include "catalog/dependency.h"

/*
 * Note that those two lock types don't conflict with each other and are
 * acquired for different purposes. The lock on the object class
 * --SerializeDistributedDDLsOnObjectClass()-- is used to serialize DDLs
 * that target the object class itself, e.g., when creating a new object
 * of that class, and the latter one --SerializeDistributedDDLsOnObjectClassObject()--
 * is used to serialize DDLs that target a specific object of that class,
 * e.g., when altering an object.
 *
 * In some cases, we may want to acquire both locks at the same time. For
 * example, when renaming a database, we want to acquire both lock types
 * because while the object class lock is used to ensure that another session
 * doesn't create a new database with the same name, the object lock is used
 * to ensure that another session doesn't alter the same database.
 */
extern void SerializeDistributedDDLsOnObjectClass(ObjectClass objectClass);
extern void SerializeDistributedDDLsOnObjectClassObject(ObjectClass objectClass,
														char *qualifiedObjectName);

#endif /* SERIALIZE_DDLS_OVER_CATALOG_H */
