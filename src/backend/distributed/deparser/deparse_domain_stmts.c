/*-------------------------------------------------------------------------
 *
 * deparse_domain_stmts.c
 *    Functions to turn all Statement structures related to domains back
 *    into sql.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_type.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_node.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"
#include "utils/syscache.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/deparser.h"
#include "distributed/listutils.h"
#include "distributed/namespace_utils.h"

static void AppendConstraint(StringInfo buf, Constraint *constraint, List *domainName,
							 TypeName *typeName);
static Node * replace_domain_constraint_value(ParseState *pstate, ColumnRef *cref);
static Node * TransformDefaultExpr(Node *expr, List *domainName, TypeName *typeName);
static Node * TransformConstraintExpr(Node *expr, TypeName *typeName);
static CoerceToDomainValue * GetCoerceDomainValue(TypeName *typeName);
static char * TypeNameAsIdentifier(TypeName *typeName);

static Oid DomainGetBaseTypeOid(List *names, int32 *baseTypeMod);

static void AppendAlterDomainStmtSetDefault(StringInfo buf, AlterDomainStmt *stmt);
static void AppendAlterDomainStmtAddConstraint(StringInfo buf, AlterDomainStmt *stmt);
static void AppendAlterDomainStmtDropConstraint(StringInfo buf, AlterDomainStmt *stmt);


/*
 * DeparseCreateDomainStmt returns the sql representation for the CREATE DOMAIN statement.
 */
char *
DeparseCreateDomainStmt(Node *node)
{
	CreateDomainStmt *stmt = castNode(CreateDomainStmt, node);
	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	const char *domainIdentifier = NameListToQuotedString(stmt->domainname);
	const char *typeIdentifier = TypeNameAsIdentifier(stmt->typeName);
	appendStringInfo(&buf, "CREATE DOMAIN %s AS %s", domainIdentifier, typeIdentifier);

	if (stmt->collClause)
	{
		const char *collateIdentifier =
			NameListToQuotedString(stmt->collClause->collname);
		appendStringInfo(&buf, " COLLATE %s", collateIdentifier);
	}

	Constraint *constraint = NULL;
	foreach_declared_ptr(constraint, stmt->constraints)
	{
		AppendConstraint(&buf, constraint, stmt->domainname, stmt->typeName);
	}

	appendStringInfoString(&buf, ";");

	return buf.data;
}


/*
 * TypeNameAsIdentifier returns the sql identifier of a TypeName. This is more complex
 * than concatenating the schema name and typename since certain types contain modifiers
 * that need to be correctly represented.
 */
static char *
TypeNameAsIdentifier(TypeName *typeName)
{
	int32 typmod = 0;
	Oid typeOid = InvalidOid;
	bits16 formatFlags = FORMAT_TYPE_TYPEMOD_GIVEN | FORMAT_TYPE_FORCE_QUALIFY;

	typenameTypeIdAndMod(NULL, typeName, &typeOid, &typmod);

	return format_type_extended(typeOid, typmod, formatFlags);
}


/*
 * DeparseDropDomainStmt returns the sql for teh DROP DOMAIN statement.
 */
char *
DeparseDropDomainStmt(Node *node)
{
	DropStmt *stmt = castNode(DropStmt, node);
	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfoString(&buf, "DROP DOMAIN ");
	if (stmt->missing_ok)
	{
		appendStringInfoString(&buf, "IF EXISTS ");
	}

	TypeName *domainName = NULL;
	bool first = true;
	foreach_declared_ptr(domainName, stmt->objects)
	{
		if (!first)
		{
			appendStringInfoString(&buf, ", ");
		}
		first = false;

		const char *identifier = NameListToQuotedString(domainName->names);
		appendStringInfoString(&buf, identifier);
	}

	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(&buf, " CASCADE");
	}

	appendStringInfoString(&buf, ";");

	return buf.data;
}


/*
 * DeparseAlterDomainStmt returns the sql representation of the DOMAIN specific ALTER
 * statements.
 */
char *
DeparseAlterDomainStmt(Node *node)
{
	AlterDomainStmt *stmt = castNode(AlterDomainStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	appendStringInfo(&buf, "ALTER DOMAIN %s ", NameListToQuotedString(stmt->typeName));
	switch (stmt->subtype)
	{
		case 'T': /* SET DEFAULT */
		{
			AppendAlterDomainStmtSetDefault(&buf, stmt);
			break;
		}

		case 'N': /* DROP NOT NULL */
		{
			appendStringInfoString(&buf, "DROP NOT NULL");
			break;
		}

		case 'O': /* SET NOT NULL */
		{
			appendStringInfoString(&buf, "SET NOT NULL");
			break;
		}

		case 'C': /* ADD [CONSTRAINT name] */
		{
			AppendAlterDomainStmtAddConstraint(&buf, stmt);
			break;
		}

		case 'X': /* DROP CONSTRAINT */
		{
			AppendAlterDomainStmtDropConstraint(&buf, stmt);
			break;
		}

		case 'V': /* VALIDATE CONSTRAINT */
		{
			appendStringInfo(&buf, "VALIDATE CONSTRAINT %s",
							 quote_identifier(stmt->name));
			break;
		}

		default:
		{
			elog(ERROR, "unsupported alter domain statement for distribution");
		}
	}

	appendStringInfoChar(&buf, ';');

	return buf.data;
}


/*
 * DeparseDomainRenameConstraintStmt returns the sql representation of the domain
 * constraint renaming.
 */
char *
DeparseDomainRenameConstraintStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	char *domainIdentifier = NameListToQuotedString(castNode(List, stmt->object));
	appendStringInfo(&buf, "ALTER DOMAIN %s RENAME CONSTRAINT %s TO %s;",
					 domainIdentifier,
					 quote_identifier(stmt->subname),
					 quote_identifier(stmt->newname));

	return buf.data;
}


/*
 * DeparseAlterDomainOwnerStmt returns the sql representation of the ALTER DOMAIN OWNER
 * statement.
 */
char *
DeparseAlterDomainOwnerStmt(Node *node)
{
	AlterOwnerStmt *stmt = castNode(AlterOwnerStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	List *domainName = castNode(List, stmt->object);
	char *domainIdentifier = NameListToQuotedString(domainName);
	appendStringInfo(&buf, "ALTER DOMAIN %s OWNER TO %s;",
					 domainIdentifier,
					 RoleSpecString(stmt->newowner, true));

	return buf.data;
}


/*
 * DeparseRenameDomainStmt returns the sql representation of the ALTER DOMAIN RENAME
 * statement.
 */
char *
DeparseRenameDomainStmt(Node *node)
{
	RenameStmt *stmt = castNode(RenameStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	List *domainName = castNode(List, stmt->object);
	char *domainIdentifier = NameListToQuotedString(domainName);
	appendStringInfo(&buf, "ALTER DOMAIN %s RENAME TO %s;",
					 domainIdentifier,
					 quote_identifier(stmt->newname));

	return buf.data;
}


/*
 * DeparseAlterDomainSchemaStmt returns the sql representation of the ALTER DOMAIN SET
 * SCHEMA statement.
 */
char *
DeparseAlterDomainSchemaStmt(Node *node)
{
	AlterObjectSchemaStmt *stmt = castNode(AlterObjectSchemaStmt, node);

	StringInfoData buf = { 0 };
	initStringInfo(&buf);

	List *domainName = castNode(List, stmt->object);
	char *domainIdentifier = NameListToQuotedString(domainName);
	appendStringInfo(&buf, "ALTER DOMAIN %s SET SCHEMA %s;",
					 domainIdentifier,
					 quote_identifier(stmt->newschema));

	return buf.data;
}


/*
 * DomainGetBaseTypeOid returns the type Oid and the type modifiers of the type underlying
 * a domain addresses by the namelist provided as the names argument. The type modifier is
 * only provided if the baseTypeMod pointer is a valid pointer on where to write the
 * modifier (not a NULL pointer).
 *
 * If the type cannot be found this function will raise a non-userfacing error. Care needs
 * to be taken by the caller that the domain is actually existing.
 */
static Oid
DomainGetBaseTypeOid(List *names, int32 *baseTypeMod)
{
	TypeName *domainName = makeTypeNameFromNameList(names);
	Oid domainoid = typenameTypeId(NULL, domainName);
	HeapTuple tup = SearchSysCache1(TYPEOID, ObjectIdGetDatum(domainoid));
	if (!HeapTupleIsValid(tup))
	{
		elog(ERROR, "cache lookup failed for type %u", domainoid);
	}
	Form_pg_type typTup = (Form_pg_type) GETSTRUCT(tup);
	Oid baseTypeOid = typTup->typbasetype;
	if (baseTypeMod)
	{
		*baseTypeMod = typTup->typtypmod;
	}
	ReleaseSysCache(tup);
	return baseTypeOid;
}


/*
 * AppendAlterDomainStmtSetDefault is a helper function that appends the default value
 * portion of an ALTER DOMAIN statement that is changing the default value of the domain.
 */
static void
AppendAlterDomainStmtSetDefault(StringInfo buf, AlterDomainStmt *stmt)
{
	if (stmt->def == NULL)
	{
		/* no default expression is a DROP DEFAULT statment */
		appendStringInfoString(buf, "DROP DEFAULT");
		return;
	}

	int32 baseTypMod = 0;
	Oid baseOid = DomainGetBaseTypeOid(stmt->typeName, &baseTypMod);
	TypeName *baseTypeName = makeTypeNameFromOid(baseOid, baseTypMod);

	/* cook the default expression, without cooking we can't deparse */
	Node *expr = stmt->def;
	expr = TransformDefaultExpr(expr, stmt->typeName, baseTypeName);

	/* deparse while the searchpath is cleared to force qualification of identifiers */
	int saveNestLevel = PushEmptySearchPath();
	char *exprSql = deparse_expression(expr, NIL, true, true);
	PopEmptySearchPath(saveNestLevel);

	appendStringInfo(buf, "SET DEFAULT %s", exprSql);
}


/*
 * AppendAlterDomainStmtAddConstraint is a helper function that appends the constraint
 * specification for an ALTER DOMAIN statement that adds a constraint to the domain.
 */
static void
AppendAlterDomainStmtAddConstraint(StringInfo buf, AlterDomainStmt *stmt)
{
	if (stmt->def == NULL || !IsA(stmt->def, Constraint))
	{
		ereport(ERROR, (errmsg("unable to deparse ALTER DOMAIN statement due to "
							   "unexpected contents")));
	}

	Constraint *constraint = castNode(Constraint, stmt->def);
	appendStringInfoString(buf, "ADD");

	int32 baseTypMod = 0;
	Oid baseOid = DomainGetBaseTypeOid(stmt->typeName, &baseTypMod);
	TypeName *baseTypeName = makeTypeNameFromOid(baseOid, baseTypMod);

	AppendConstraint(buf, constraint, stmt->typeName, baseTypeName);

	if (!constraint->initially_valid)
	{
		appendStringInfoString(buf, " NOT VALID");
	}
}


/*
 * AppendAlterDomainStmtDropConstraint is a helper function that appends the DROP
 * CONSTRAINT part of an ALTER DOMAIN statement for an alter statement that drops a
 * constraint.
 */
static void
AppendAlterDomainStmtDropConstraint(StringInfo buf, AlterDomainStmt *stmt)
{
	appendStringInfoString(buf, "DROP CONSTRAINT ");

	if (stmt->missing_ok)
	{
		appendStringInfoString(buf, "IF EXISTS ");
	}

	appendStringInfoString(buf, quote_identifier(stmt->name));

	if (stmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(buf, " CASCADE");
	}
}


/*
 * AppendConstraint is a helper function that appends a constraint specification to a sql
 * string that is adding a constraint.
 *
 * There are multiple places where a constraint specification is appended to sql strings.
 *
 * Given the complexities of serializing a constraint they all use this routine.
 */
static void
AppendConstraint(StringInfo buf, Constraint *constraint, List *domainName,
				 TypeName *typeName)
{
	if (constraint->conname)
	{
		appendStringInfo(buf, " CONSTRAINT %s", quote_identifier(constraint->conname));
	}

	switch (constraint->contype)
	{
		case CONSTR_CHECK:
		{
			Node *expr = NULL;
			if (constraint->raw_expr)
			{
				/* the expression was parsed from sql, still needs to transform */
				expr = TransformConstraintExpr(constraint->raw_expr, typeName);
			}
			else if (constraint->cooked_expr)
			{
				/* expression was read from the catalog, no cooking required just parse */
				expr = stringToNode(constraint->cooked_expr);
			}
			else
			{
				elog(ERROR, "missing expression for domain constraint");
			}

			int saveNestLevel = PushEmptySearchPath();
			char *exprSql = deparse_expression(expr, NIL, true, true);
			PopEmptySearchPath(saveNestLevel);

			appendStringInfo(buf, " CHECK (%s)", exprSql);
			return;
		}

		case CONSTR_DEFAULT:
		{
			Node *expr = NULL;
			if (constraint->raw_expr)
			{
				/* the expression was parsed from sql, still needs to transform */
				expr = TransformDefaultExpr(constraint->raw_expr, domainName, typeName);
			}
			else if (constraint->cooked_expr)
			{
				/* expression was read from the catalog, no cooking required just parse */
				expr = stringToNode(constraint->cooked_expr);
			}
			else
			{
				elog(ERROR, "missing expression for domain default");
			}

			int saveNestLevel = PushEmptySearchPath();
			char *exprSql = deparse_expression(expr, NIL, true, true);
			PopEmptySearchPath(saveNestLevel);

			appendStringInfo(buf, " DEFAULT %s", exprSql);
			return;
		}

		case CONSTR_NOTNULL:
		{
			appendStringInfoString(buf, " NOT NULL");
			return;
		}

		case CONSTR_NULL:
		{
			appendStringInfoString(buf, " NULL");
			return;
		}

		default:
		{
			ereport(ERROR, (errmsg("unsupported constraint for distributed domain")));
		}
	}
}


/*
 * TransformDefaultExpr transforms a default expression from the expression passed on the
 * AST to a cooked version that postgres uses internally.
 *
 * Only the cooked version can be easily turned back into a sql string, hence its use in
 * the deparser. This is only called for default expressions that don't have a cooked
 * variant stored.
 */
static Node *
TransformDefaultExpr(Node *expr, List *domainName, TypeName *typeName)
{
	const char *domainNameStr = NameListToQuotedString(domainName);
	int32 basetypeMod = 0; /* capture typeMod during lookup */
	Type tup = typenameType(NULL, typeName, &basetypeMod);
	Oid basetypeoid = typeTypeId(tup);

	ReleaseSysCache(tup);

	ParseState *pstate = make_parsestate(NULL);
	Node *defaultExpr = cookDefault(pstate, expr,
									basetypeoid,
									basetypeMod,
									domainNameStr,
									0);

	return defaultExpr;
}


/*
 * TransformConstraintExpr transforms a constraint expression from the expression passed
 * on the AST to a cooked version that postgres uses internally.
 *
 * Only the cooked version can be easily turned back into a sql string, hence its use in
 * the deparser. This is only called for default expressions that don't have a cooked
 * variant stored.
 */
static Node *
TransformConstraintExpr(Node *expr, TypeName *typeName)
{
	/*
	 * Convert the A_EXPR in raw_expr into an EXPR
	 */
	ParseState *pstate = make_parsestate(NULL);

	/*
	 * Set up a CoerceToDomainValue to represent the occurrence of VALUE in
	 * the expression.  Note that it will appear to have the type of the base
	 * type, not the domain.  This seems correct since within the check
	 * expression, we should not assume the input value can be considered a
	 * member of the domain.
	 */

	CoerceToDomainValue *domVal = GetCoerceDomainValue(typeName);

	pstate->p_pre_columnref_hook = replace_domain_constraint_value;
	pstate->p_ref_hook_state = (void *) domVal;

	expr = transformExpr(pstate, expr, EXPR_KIND_DOMAIN_CHECK);

	/*
	 * Make sure it yields a boolean result.
	 */
	expr = coerce_to_boolean(pstate, expr, "CHECK");

	/*
	 * Fix up collation information.
	 */
	assign_expr_collations(pstate, expr);

	return expr;
}


/*
 * GetCoerceDomainValue creates a stub CoerceToDomainValue struct representing the type
 * referenced by the typeName.
 */
static CoerceToDomainValue *
GetCoerceDomainValue(TypeName *typeName)
{
	int32 typMod = 0; /* capture typeMod during lookup */
	Type tup = LookupTypeName(NULL, typeName, &typMod, false);
	if (tup == NULL)
	{
		elog(ERROR, "unable to lookup type information for %s",
			 NameListToQuotedString(typeName->names));
	}

	CoerceToDomainValue *domVal = makeNode(CoerceToDomainValue);
	domVal->typeId = typeTypeId(tup);
	domVal->typeMod = typMod;
	domVal->collation = typeTypeCollation(tup);
	domVal->location = -1;

	ReleaseSysCache(tup);
	return domVal;
}


/* Parser pre_columnref_hook for domain CHECK constraint parsing */
static Node *
replace_domain_constraint_value(ParseState *pstate, ColumnRef *cref)
{
	/*
	 * Check for a reference to "value", and if that's what it is, replace
	 * with a CoerceToDomainValue as prepared for us by domainAddConstraint.
	 * (We handle VALUE as a name, not a keyword, to avoid breaking a lot of
	 * applications that have used VALUE as a column name in the past.)
	 */
	if (list_length(cref->fields) == 1)
	{
		Node *field1 = (Node *) linitial(cref->fields);
		Assert(IsA(field1, String));
		char *colname = strVal(field1);

		if (strcmp(colname, "value") == 0)
		{
			CoerceToDomainValue *domVal = copyObject(pstate->p_ref_hook_state);

			/* Propagate location knowledge, if any */
			domVal->location = cref->location;
			return (Node *) domVal;
		}
	}
	return NULL;
}
