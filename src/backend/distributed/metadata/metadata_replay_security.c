/*-------------------------------------------------------------------------
 *
 * metadata_replay_security.c
 *    Security boundary for evaluating expressions during metadata replay.
 *
 * Citus recreates distributed objects on worker nodes through a superuser
 * connection. PostgreSQL evaluates an expression wherever it parses one, so a
 * definition a tenant authored would otherwise run with superuser rights while
 * the object is being recreated.
 *
 * Replayed commands are prefixed with the owner of the object, unless the object
 * is already owned by a superuser or is of a kind that cannot evaluate anything.
 * The worker runs the marked statement as that owner under
 * SECURITY_RESTRICTED_OPERATION. Switching identity rather than intercepting
 * calls is what makes this cover builtin, cached and type-system evaluation,
 * none of which reach a function-call hook. The marker is consumed by the
 * statement it applies to.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"

#include "access/htup_details.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_class.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_database.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_statistic_ext.h"
#include "catalog/pg_ts_config.h"
#include "catalog/pg_ts_dict.h"
#include "catalog/pg_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

#include "distributed/listutils.h"
#include "distributed/metadata_replay_security.h"


char *MetadataReplayOwner = "";
int MetadataReplayProtection = METADATA_REPLAY_PROTECTION_WARN;

static char * MetadataReplayObjectOwnerName(const ObjectAddress *address);
static bool ReplayObjectIsExpressionFree(const ObjectAddress *address);
static Oid ReplayObjectOwnerId(const ObjectAddress *address);
static Oid ReplayCatalogOid(int cacheId, Oid objectId, AttrNumber attributeNumber);
static char * CommandWithoutTrailingSemicolon(const char *command);


/*
 * ProtectMetadataReplayCommandList marks every command with the owner of the object
 * that is being replayed. The marker is part of the same command string because
 * callers may execute each list element in its own transaction.
 *
 * A node that predates the marker does not have the parameter at all. Rather than
 * pass silently, the emitted command tests for it and reports that such a node
 * evaluates the command with the rights of the replaying superuser, and
 * citus.metadata_replay_protection decides whether that is fatal.
 *
 * The list is returned unchanged when there is no owner to reduce the rights to.
 */
List *
ProtectMetadataReplayCommandList(List *commandList, const ObjectAddress *object)
{
	if (MetadataReplayProtection == METADATA_REPLAY_PROTECTION_OFF)
	{
		return commandList;
	}

	char *ownerName = MetadataReplayObjectOwnerName(object);
	if (ownerName == NULL)
	{
		return commandList;
	}

	List *protectedCommandList = NIL;

	/*
	 * A node that predates this protection does not know the marker, so it
	 * reports itself instead of silently evaluating with superuser rights.
	 */
	const char *unsupportedAction =
		MetadataReplayProtection == METADATA_REPLAY_PROTECTION_REQUIRE ?
		"RAISE EXCEPTION" : "RAISE WARNING";

	/*
	 * Presence is tested rather than an error caught: before PG15 an unknown
	 * citus.* name is accepted as a placeholder instead of being rejected.
	 */
	char *setMarkerBody = psprintf(
		"BEGIN "
		"IF current_setting('citus.metadata_replay_owner', true) IS NULL THEN "
		"%s 'this node evaluates replayed expressions with the rights of the "
		"replaying superuser' "
		"USING HINT = 'Upgrade Citus on this node to restore metadata replay "
		"protection, or set citus.metadata_replay_protection to off to accept it.'; "
		"ELSE PERFORM set_config('citus.metadata_replay_owner', %s, true); "
		"END IF; END",
		unsupportedAction, quote_literal_cstr(ownerName));
	char *clearMarkerBody =
		"BEGIN "
		"IF current_setting('citus.metadata_replay_owner', true) IS NOT NULL THEN "
		"PERFORM set_config('citus.metadata_replay_owner', '', true); "
		"END IF; END";

	char *command = NULL;
	foreach_declared_ptr(command, commandList)
	{
		/*
		 * The statement itself consumes the marker. The trailing reset only
		 * matters for commands that turn out not to be utility statements.
		 */
		protectedCommandList =
			lappend(protectedCommandList,
					psprintf("DO %s; %s; DO %s",
							 quote_literal_cstr(setMarkerBody),
							 CommandWithoutTrailingSemicolon(command),
							 quote_literal_cstr(clearMarkerBody)));
	}

	return protectedCommandList;
}


/*
 * MetadataReplayObjectOwnerName returns the name of the role owning the given object,
 * or NULL when the object is already owned by a superuser or belongs to a kind that
 * cannot evaluate an expression while it is recreated.
 *
 * Anything not named as expression free is replayed as its owner. PostgreSQL
 * evaluates an expression wherever it parses one, so enumerating the places that
 * carry one is a list that fails open when it is incomplete.
 */
static char *
MetadataReplayObjectOwnerName(const ObjectAddress *address)
{
	if (ReplayObjectIsExpressionFree(address))
	{
		return NULL;
	}

	Oid ownerId = ReplayObjectOwnerId(address);
	if (!OidIsValid(ownerId))
	{
		/*
		 * The object can evaluate an expression but there is no owner to reduce
		 * the rights to, so it would replay privileged. Report that rather than
		 * let it pass as though it had been considered.
		 */
		int reportLevel = MetadataReplayProtection == METADATA_REPLAY_PROTECTION_REQUIRE ?
						  ERROR : WARNING;
		char *identity = getObjectIdentity(address, true);

		ereport(reportLevel,
				(errmsg("cannot determine which role owns %s",
						identity != NULL ? identity : "an object being replayed"),
				 errdetail("Citus replays it with the rights of the replaying "
						   "superuser, so an expression it carries is not restricted "
						   "to the rights of its author."),
				 errhint("Report this object kind so that metadata replay can "
						 "restrict it, or set citus.metadata_replay_protection to "
						 "off to accept it.")));

		return NULL;
	}

	if (superuser_arg(ownerId))
	{
		return NULL;
	}

	return GetUserNameFromId(ownerId, false);
}


/*
 * ReplayObjectIsExpressionFree returns whether recreating the object is known not to
 * evaluate an expression, which is what makes replaying it privileged safe.
 *
 * Every entry here was verified by recreating the object with a tenant owned domain
 * reachable from its definition and observing that the domain constraint did not run.
 * Roles, databases and extensions additionally have to stay privileged to replay.
 */
static bool
ReplayObjectIsExpressionFree(const ObjectAddress *address)
{
	switch (address->classId)
	{
		case CollationRelationId:
		case NamespaceRelationId:
		case TSDictionaryRelationId:
		case TSConfigRelationId:
		case AuthIdRelationId:
		case DatabaseRelationId:
		case ExtensionRelationId:
		case ForeignServerRelationId:
		{
			return true;
		}

		case RelationRelationId:
		{
			char relKind = get_rel_relkind(address->objectId);

			/* a sequence stores only literals of builtin types */
			return relKind == RELKIND_SEQUENCE || relKind == RELKIND_COMPOSITE_TYPE;
		}

		case TypeRelationId:
		{
			/* a composite type holds no default, unlike a domain */
			return get_typtype(address->objectId) == TYPTYPE_COMPOSITE;
		}

		default:
		{
			return false;
		}
	}
}


/*
 * ReplayObjectOwnerId returns the role owning the given object, or InvalidOid when
 * the object class is not one this node knows how to replay on behalf of an owner.
 */
static Oid
ReplayObjectOwnerId(const ObjectAddress *address)
{
	switch (address->classId)
	{
		case RelationRelationId:
		{
			return ReplayCatalogOid(RELOID, address->objectId,
									Anum_pg_class_relowner);
		}

		case TypeRelationId:
		{
			return ReplayCatalogOid(TYPEOID, address->objectId,
									Anum_pg_type_typowner);
		}

		case ProcedureRelationId:
		{
			return ReplayCatalogOid(PROCOID, address->objectId,
									Anum_pg_proc_proowner);
		}

		case PublicationRelationId:
		{
			return ReplayCatalogOid(PUBLICATIONOID, address->objectId,
									Anum_pg_publication_pubowner);
		}

		case StatisticExtRelationId:
		{
			return ReplayCatalogOid(STATEXTOID, address->objectId,
									Anum_pg_statistic_ext_stxowner);
		}

		case ConstraintRelationId:
		{
			/* a constraint is replayed with the rights of what it constrains */
			Oid relationId = ReplayCatalogOid(CONSTROID, address->objectId,
											  Anum_pg_constraint_conrelid);
			if (OidIsValid(relationId))
			{
				return ReplayCatalogOid(RELOID, relationId, Anum_pg_class_relowner);
			}

			/* a domain constraint has no table, it belongs to the domain */
			Oid typeId = ReplayCatalogOid(CONSTROID, address->objectId,
										  Anum_pg_constraint_contypid);
			return OidIsValid(typeId) ?
				   ReplayCatalogOid(TYPEOID, typeId, Anum_pg_type_typowner) :
				   InvalidOid;
		}

		default:
		{
			return InvalidOid;
		}
	}
}


/*
 * ReplayCatalogOid returns an Oid valued attribute of a catalog tuple, or InvalidOid
 * when the tuple is gone.
 */
static Oid
ReplayCatalogOid(int cacheId, Oid objectId, AttrNumber attributeNumber)
{
	HeapTuple tuple = SearchSysCache1(cacheId, ObjectIdGetDatum(objectId));
	if (!HeapTupleIsValid(tuple))
	{
		return InvalidOid;
	}

	bool isNull = false;
	Datum value = SysCacheGetAttr(cacheId, tuple, attributeNumber, &isNull);
	Oid ownerId = isNull ? InvalidOid : DatumGetObjectId(value);

	ReleaseSysCache(tuple);

	return ownerId;
}


/*
 * MetadataReplayStatementIsMarked returns whether the next statement replays an
 * object on behalf of a non-superuser owner.
 */
bool
MetadataReplayStatementIsMarked(void)
{
	return MetadataReplayOwner != NULL && MetadataReplayOwner[0] != '\0';
}


/*
 * SaveAndClearMetadataReplayMarker hands the marker to a caller that runs the
 * marked statement itself, so that a statement it has to run first does not
 * consume it. Returns NULL when nothing is marked.
 */
char *
SaveAndClearMetadataReplayMarker(void)
{
	if (!MetadataReplayStatementIsMarked())
	{
		return NULL;
	}

	char *ownerName = pstrdup(MetadataReplayOwner);
	SetConfigOption("citus.metadata_replay_owner", "", PGC_USERSET, PGC_S_SESSION);

	return ownerName;
}


/*
 * RestoreMetadataReplayMarker puts a saved marker back, so the statement that
 * follows is replayed as the owner it names.
 */
void
RestoreMetadataReplayMarker(const char *ownerName)
{
	if (ownerName == NULL)
	{
		return;
	}

	SetConfigOption("citus.metadata_replay_owner", ownerName, PGC_USERSET,
					PGC_S_SESSION);
}


/*
 * EnterMetadataReplayOwnerContext switches to the marked owner with security
 * restrictions in place, so that everything the statement evaluates runs with
 * the rights of the object owner. Callers must pair this with
 * ExitMetadataReplayOwnerContext, including on error.
 */
void
EnterMetadataReplayOwnerContext(MetadataReplayOwnerContext *ownerContext)
{
	bool missingOk = false;
	Oid ownerId = get_role_oid(MetadataReplayOwner, missingOk);

	/* consume the marker first, so it cannot outlive this statement */
	SetConfigOption("citus.metadata_replay_owner", "", PGC_USERSET, PGC_S_SESSION);

	ownerContext->ownerId = ownerId;
	GetUserIdAndSecContext(&ownerContext->savedUserId,
						   &ownerContext->savedSecurityContext);
	ownerContext->gucNestLevel = NewGUCNestLevel();

	SetUserIdAndSecContext(ownerId,
						   ownerContext->savedSecurityContext |
						   SECURITY_LOCAL_USERID_CHANGE |
						   SECURITY_RESTRICTED_OPERATION);
}


/*
 * ExitMetadataReplayOwnerContext restores the identity, security context and
 * settings that were in effect before the replayed statement ran.
 */
void
ExitMetadataReplayOwnerContext(MetadataReplayOwnerContext *ownerContext)
{
	AtEOXact_GUC(false, ownerContext->gucNestLevel);
	SetUserIdAndSecContext(ownerContext->savedUserId,
						   ownerContext->savedSecurityContext);
}


/*
 * CommandWithoutTrailingSemicolon lets the caller append another statement to a
 * generated command without producing an empty statement.
 */
static char *
CommandWithoutTrailingSemicolon(const char *command)
{
	int length = strlen(command);

	while (length > 0 &&
		   (command[length - 1] == ';' ||
			command[length - 1] == ' ' ||
			command[length - 1] == '\t' ||
			command[length - 1] == '\n' ||
			command[length - 1] == '\r'))
	{
		length--;
	}

	return pnstrdup(command, length);
}
