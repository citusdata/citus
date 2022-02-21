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
static List * FilterNameListForDistributedDomains(List *domainNames, bool missing_ok,
												  List **distributedDomainAddresses);
static ObjectAddress GetDomainAddressByName(TypeName *domainName, bool missing_ok);

/*
 * PreprocessCreateDomainStmt handles the propagation of the create domain statements.
 */
List *
PreprocessCreateDomainStmt(Node *node, const char *queryString,
						   ProcessUtilityContext processUtilityContext)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_DOMAIN);

	QualifyTreeNode(node);
	const char *sql = DeparseTreeNode(node);
	sql = WrapCreateOrReplace(sql);

	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessCreateDomainStmt gets called after the domain has been created locally. When
 * the domain is decided to be propagated we make sure all the domains dependencies exist
 * on all workers.
 */
List *
PostprocessCreateDomainStmt(Node *node, const char *queryString)
{
	if (!ShouldPropagate())
	{
		return NIL;
	}

	/* check creation against multi-statement transaction policy */
	if (!ShouldPropagateCreateInCoordinatedTransction())
	{
		return NIL;
	}

	/*
	 * find object address of the just created object, because the domain has been created
	 * locally it can't be missing
	 */
	ObjectAddress typeAddress = GetObjectAddressFromParseTree(node, false);
	EnsureDependenciesExistOnAllNodes(&typeAddress);

	return NIL;
}


/*
 * PreprocessDropDomainStmt gets called before dropping the domain locally. For
 * distributed domains it will make sure the fully qualified statement is forwarded to all
 * the workers reflecting the drop of the domain.
 */
List *
PreprocessDropDomainStmt(Node *node, const char *queryString,
						 ProcessUtilityContext processUtilityContext)
{
	DropStmt *stmt = castNode(DropStmt, node);


	if (!ShouldPropagate())
	{
		return NIL;
	}

	QualifyTreeNode((Node *) stmt);

	List *oldDomains = stmt->objects;
	List *distributedDomainAddresses = NIL;
	List *distributedDomains = FilterNameListForDistributedDomains(
		oldDomains,
		stmt->missing_ok,
		&distributedDomainAddresses);
	if (list_length(distributedDomains) <= 0)
	{
		/* no distributed domains to drop */
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_DOMAIN);

	ObjectAddress *addressItem = NULL;
	foreach_ptr(addressItem, distributedDomainAddresses)
	{
		UnmarkObjectDistributed(addressItem);
	}

	/*
	 * temporary swap the lists of objects to delete with the distributed objects and
	 * deparse to an executable sql statement for the workers
	 */
	stmt->objects = distributedDomains;
	char *dropStmtSql = DeparseTreeNode((Node *) stmt);
	stmt->objects = oldDomains;

	/* to prevent recursion with mx we disable ddl propagation */
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) dropStmtSql,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessAlterDomainStmt gets called for all domain specific alter statements. When
 * the change happens on a distributed domain we reflect the changes on the workers.
 */
List *
PreprocessAlterDomainStmt(Node *node, const char *queryString,
						  ProcessUtilityContext processUtilityContext)
{
	AlterDomainStmt *stmt = castNode(AlterDomainStmt, node);

	ObjectAddress domainAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&domainAddress))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_DOMAIN);

	QualifyTreeNode((Node *) stmt);
	char *sqlStmt = DeparseTreeNode((Node *) stmt);
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sqlStmt,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterDomainStmt gets called after the domain has been altered locally. A
 * change on the constraints could cause new (undistributed) objects to be dependencies of
 * the domain. Here we recreate any new dependencies on the workers before we forward the
 * alter statement to the workers.
 */
List *
PostprocessAlterDomainStmt(Node *node, const char *queryString)
{
	AlterDomainStmt *stmt = castNode(AlterDomainStmt, node);

	ObjectAddress domainAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&domainAddress))
	{
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&domainAddress);
	return NIL;
}


/*
 * PreprocessDomainRenameConstraintStmt gets called locally when a constraint on a domain
 * is renamed. When the constraint is on a distributed domain we forward the statement
 * appropriately.
 */
List *
PreprocessDomainRenameConstraintStmt(Node *node, const char *queryString,
									 ProcessUtilityContext processUtilityContext)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_DOMCONSTRAINT);

	ObjectAddress domainAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&domainAddress))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_DOMAIN);

	QualifyTreeNode((Node *) stmt);
	char *sqlStmt = DeparseTreeNode((Node *) stmt);
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sqlStmt,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessAlterDomainOwnerStmt called locally when the owner of a constraint is
 * changed. For distributed domains the statement is forwarded to all the workers.
 */
List *
PreprocessAlterDomainOwnerStmt(Node *node, const char *queryString,
							   ProcessUtilityContext processUtilityContext)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);
	Assert(stmt->objectType == OBJECT_DOMAIN);

	ObjectAddress domainAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&domainAddress))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_DOMAIN);

	QualifyTreeNode((Node *) stmt);
	char *sqlStmt = DeparseTreeNode((Node *) stmt);
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sqlStmt,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterDomainOwnerStmt given the change of ownership could cause new
 * dependencies to exist for the domain we make sure all dependencies for the domain are
 * created before we forward the statement to the workers.
 */
List *
PostprocessAlterDomainOwnerStmt(Node *node, const char *queryString)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);

	ObjectAddress domainAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&domainAddress))
	{
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&domainAddress);
	return NIL;
}


/*
 * PreprocessRenameDomainStmt creates the statements to execute on the workers when the
 * domain being renamed is distributed.
 */
List *
PreprocessRenameDomainStmt(Node *node, const char *queryString,
						   ProcessUtilityContext processUtilityContext)
{
	RenameStmt *stmt = castNode(RenameStmt, node);
	Assert(stmt->renameType == OBJECT_DOMAIN);

	ObjectAddress domainAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&domainAddress))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_DOMAIN);

	QualifyTreeNode((Node *) stmt);
	char *sqlStmt = DeparseTreeNode((Node *) stmt);
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sqlStmt,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PreprocessAlterDomainSchemaStmt cretes the statements to execute on the workers when
 * the domain being moved to a new schema has been distributed.
 */
List *
PreprocessAlterDomainSchemaStmt(Node *node, const char *queryString,
								ProcessUtilityContext processUtilityContext)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);
	Assert(stmt->objectType == OBJECT_DOMAIN);

	ObjectAddress domainAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&domainAddress))
	{
		return NIL;
	}

	EnsureCoordinator();
	EnsureSequentialMode(OBJECT_DOMAIN);

	QualifyTreeNode((Node *) stmt);
	char *sqlStmt = DeparseTreeNode((Node *) stmt);
	List *commands = list_make3(DISABLE_DDL_PROPAGATION,
								(void *) sqlStmt,
								ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(NON_COORDINATOR_NODES, commands);
}


/*
 * PostprocessAlterDomainSchemaStmt makes sure any new dependencies (as result of the
 * schema move) are created on the workers before we forward the statement.
 */
List *
PostprocessAlterDomainSchemaStmt(Node *node, const char *queryString)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

	ObjectAddress domainAddress = GetObjectAddressFromParseTree((Node *) stmt, false);
	if (!ShouldPropagateObject(&domainAddress))
	{
		return NIL;
	}

	EnsureDependenciesExistOnAllNodes(&domainAddress);
	return NIL;
}


/*
 * FilterNameListForDistributedDomains given a liost of domain names we will return a list
 * filtered with only the names of distributed domains remaining. The pointer to the list
 * distributedDomainAddresses is populated with a list of ObjectAddresses of the domains
 * that are distributed. Indices between the returned list and the object addresses are
 * synchronizes.
 * Note: the pointer in distributedDomainAddresses should not be omitted
 *
 * When missing_ok is false this function will raise an error if a domain identified by a
 * domain name cannot be found.
 */
static List *
FilterNameListForDistributedDomains(List *domainNames, bool missing_ok,
									List **distributedDomainAddresses)
{
	Assert(distributedDomainAddresses != NULL);

	List *distributedDomainNames = NIL;
	TypeName *domainName = NULL;
	foreach_ptr(domainName, domainNames)
	{
		ObjectAddress objectAddress = GetDomainAddressByName(domainName, missing_ok);
		if (IsObjectDistributed(&objectAddress))
		{
			distributedDomainNames = lappend(distributedDomainNames, domainName);
			if (distributedDomainAddresses)
			{
				ObjectAddress *allocatedAddress = palloc0(sizeof(ObjectAddress));
				*allocatedAddress = objectAddress;
				*distributedDomainAddresses = lappend(*distributedDomainAddresses,
													  allocatedAddress);
			}
		}
	}

	return distributedDomainNames;
}


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
