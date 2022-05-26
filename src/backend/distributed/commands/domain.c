/*-------------------------------------------------------------------------
 *
 * domain.c
 *    Hooks to handle the creation, altering and removal of domains.
 *    These hooks are responsible for duplicating the changes to the
 *    workers nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "parser/parse_type.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/regproc.h"
#include "utils/syscache.h"

#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/metadata/distobject.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata_utility.h"
#include "distributed/multi_executor.h"
#include "distributed/worker_create_or_replace.h"
#include "distributed/worker_transaction.h"


static CollateClause * MakeCollateClauseFromOid(Oid collationOid);
static ObjectAddress GetDomainAddressByName(TypeName *domainName, bool missing_ok);

/*
 * GetDomainAddressByName returns the ObjectAddress of the domain identified by
 * domainName. When missing_ok is true the object id part of the ObjectAddress can be
 * InvalidOid. When missing_ok is false this function will raise an error instead when the
 * domain can't be found.
 */
static ObjectAddress
GetDomainAddressByName(TypeName *domainName, bool missing_ok)
{
	ObjectAddress address = { 0 };
	Oid domainOid = LookupTypeNameOid(NULL, domainName, missing_ok);
	ObjectAddressSet(address, TypeRelationId, domainOid);
	return address;
}


/*
 * RecreateDomainStmt returns a CreateDomainStmt pointer where the statement represents
 * the creation of the domain to recreate the domain on a different postgres node based on
 * the current representation in the local catalog.
 */
CreateDomainStmt *
RecreateDomainStmt(Oid domainOid)
{
	CreateDomainStmt *stmt = makeNode(CreateDomainStmt);
	stmt->domainname = stringToQualifiedNameList(format_type_be_qualified(domainOid));

	HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(domainOid));
	if (!HeapTupleIsValid(tup))
	{
		elog(ERROR, "cache lookup failed for type %u", domainOid);
	}

	Form_pg_type typTup = (Form_pg_type) GETSTRUCT(tup);
	if (typTup->typtype != TYPTYPE_DOMAIN)
	{
		elog(ERROR, "type is not a domain type");
	}

	stmt->typeName = makeTypeNameFromOid(typTup->typbasetype, typTup->typtypmod);

	if (OidIsValid(typTup->typcollation))
	{
		stmt->collClause = MakeCollateClauseFromOid(typTup->typcollation);
	}

	/*
	 * typdefault and typdefaultbin are potentially null, so don't try to
	 * access 'em as struct fields. Must do it the hard way with
	 * SysCacheGetAttr.
	 */
	bool isNull = false;
	Datum typeDefaultDatum = SysCacheGetAttr(TYPEOID,
											 tup,
											 Anum_pg_type_typdefaultbin,
											 &isNull);
	if (!isNull)
	{
		/* when not null there is default value which we should add as a constraint */
		Constraint *constraint = makeNode(Constraint);
		constraint->contype = CONSTR_DEFAULT;
		constraint->cooked_expr = TextDatumGetCString(typeDefaultDatum);

		stmt->constraints = lappend(stmt->constraints, constraint);
	}

	/* NOT NULL constraints are non-named on the actual type */
	if (typTup->typnotnull)
	{
		Constraint *constraint = makeNode(Constraint);
		constraint->contype = CONSTR_NOTNULL;

		stmt->constraints = lappend(stmt->constraints, constraint);
	}

	/* lookup and look all constraints to add them to the CreateDomainStmt */
	Relation conRel = table_open(ConstraintRelationId, AccessShareLock);

	/* Look for CHECK Constraints on this domain */
	ScanKeyData key[1];
	ScanKeyInit(&key[0],
				Anum_pg_constraint_contypid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(domainOid));

	SysScanDesc scan = systable_beginscan(conRel, ConstraintTypidIndexId, true, NULL, 1,
										  key);

	HeapTuple conTup = NULL;
	while (HeapTupleIsValid(conTup = systable_getnext(scan)))
	{
		Form_pg_constraint c = (Form_pg_constraint) GETSTRUCT(conTup);

		if (c->contype != CONSTRAINT_CHECK)
		{
			/* Ignore non-CHECK constraints, shouldn't be any */
			continue;
		}

		/*
		 * We create a constraint, completely ignoring c->convalidated because we can't
		 * create a domain with an invalidated constraint. Once a constraint is added to
		 * a domain -even non valid-, all new data is validated. Meaning, creating a
		 * domain with a non-valid constraint doesn't make any sense.
		 *
		 * Given it will be too hard to defer the creation of a constraint till we
		 * validate the constraint on the coordinator we will simply create the
		 * non-validated constraint to ad hear to validating all new data.
		 *
		 * An edgecase here would be when moving existing data, that hasn't been validated
		 * before to an other node. This behaviour is consistent with sending it to an
		 * already existing node (that has the constraint created but not validated) and a
		 * new node.
		 */

		Constraint *constraint = makeNode(Constraint);

		constraint->conname = pstrdup(NameStr(c->conname));
		constraint->contype = CONSTR_CHECK; /* we only come here with check constraints */

		/* Not expecting conbin to be NULL, but we'll test for it anyway */
		Datum conbin = heap_getattr(conTup, Anum_pg_constraint_conbin, conRel->rd_att,
									&isNull);
		if (isNull)
		{
			elog(ERROR, "domain \"%s\" constraint \"%s\" has NULL conbin",
				 NameStr(typTup->typname), NameStr(c->conname));
		}

		/*
		 * The conbin containes the cooked expression from when the constraint was
		 * inserted into the catalog. We store it here for the deparser to distinguish
		 * between cooked expressions and raw expressions.
		 *
		 * There is no supported way to go from a cooked expression to a raw expression.
		 */
		constraint->cooked_expr = TextDatumGetCString(conbin);

		stmt->constraints = lappend(stmt->constraints, constraint);
	}

	systable_endscan(scan);
	table_close(conRel, NoLock);

	ReleaseSysCache(tup);

	QualifyTreeNode((Node *) stmt);

	return stmt;
}


/*
 * MakeCollateClauseFromOid returns a CollateClause describing the COLLATE segment of a
 * CREATE DOMAIN statement based on the Oid of the collation used for the domain.
 */
static CollateClause *
MakeCollateClauseFromOid(Oid collationOid)
{
	CollateClause *collateClause = makeNode(CollateClause);

	ObjectAddress collateAddress = { 0 };
	ObjectAddressSet(collateAddress, CollationRelationId, collationOid);

	List *objName = NIL;
	List *objArgs = NIL;

	#if PG_VERSION_NUM >= PG_VERSION_14
	getObjectIdentityParts(&collateAddress, &objName, &objArgs, false);
	#else
	getObjectIdentityParts(&collateAddress, &objName, &objArgs);
	#endif

	char *name = NULL;
	foreach_ptr(name, objName)
	{
		collateClause->collname = lappend(collateClause->collname, makeString(name));
	}

	collateClause->location = -1;

	return collateClause;
}


/*
 * CreateDomainStmtObjectAddress returns the ObjectAddress of the domain that would be
 * created by the statement. When missing_ok is false the function will raise an error if
 * the domain cannot be found in the local catalog.
 */
ObjectAddress
CreateDomainStmtObjectAddress(Node *node, bool missing_ok)
{
	CreateDomainStmt *stmt = castNode(CreateDomainStmt, node);

	TypeName *typeName = makeTypeNameFromNameList(stmt->domainname);
	Oid typeOid = LookupTypeNameOid(NULL, typeName, missing_ok);
	ObjectAddress address = { 0 };
	ObjectAddressSet(address, TypeRelationId, typeOid);

	return address;
}


/*
 * AlterDomainStmtObjectAddress returns the ObjectAddress of the domain being altered.
 * When missing_ok is false this function will raise an error when the domain is not
 * found.
 */
ObjectAddress
AlterDomainStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterDomainStmt *stmt = castNode(AlterDomainStmt, node);

	TypeName *domainName = makeTypeNameFromNameList(stmt->typeName);
	return GetDomainAddressByName(domainName, missing_ok);
}


/*
 * DomainRenameConstraintStmtObjectAddress returns the ObjectAddress of the domain for
 * which the constraint is being renamed. When missing_ok this function will raise an
 * error if the domain cannot be found.
 */
ObjectAddress
DomainRenameConstraintStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);

	TypeName *domainName = makeTypeNameFromNameList(castNode(List, stmt->object));
	return GetDomainAddressByName(domainName, missing_ok);
}


/*
 * AlterDomainOwnerStmtObjectAddress returns the ObjectAddress for which the owner is
 * being changed. When missing_ok is false this function will raise an error if the domain
 * cannot be found.
 */
ObjectAddress
AlterDomainOwnerStmtObjectAddress(Node *node, bool missing_ok)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_DOMAIN);

	TypeName *domainName = makeTypeNameFromNameList(castNode(List, stmt->object));
	return GetDomainAddressByName(domainName, missing_ok);
}


/*
 * RenameDomainStmtObjectAddress returns the ObjectAddress of the domain being renamed.
 * When missing_ok is false this function will raise an error when the domain cannot be
 * found.
 */
ObjectAddress
RenameDomainStmtObjectAddress(Node *node, bool missing_ok)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_DOMAIN);

	TypeName *domainName = makeTypeNameFromNameList(castNode(List, stmt->object));
	return GetDomainAddressByName(domainName, missing_ok);
}


/*
 * get_constraint_typid returns the contypid of a constraint. This field is only set for
 * constraints on domain types. Returns InvalidOid if conoid is an invalid constraint, as
 * well as for constraints that are not on domain types.
 */
Oid
get_constraint_typid(Oid conoid)
{
	HeapTuple tp = SearchSysCache1(CONSTROID, ObjectIdGetDatum(conoid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_constraint contup = (Form_pg_constraint) GETSTRUCT(tp);
		Oid result = contup->contypid;
		ReleaseSysCache(tp);
		return result;
	}
	else
	{
		return InvalidOid;
	}
}
