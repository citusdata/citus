#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "postgres.h"

static void AppendWithGrantOption(StringInfo buf, GrantStmt *stmt);
static void AppendGrantOptionFor(StringInfo buf, GrantStmt *stmt);
static void AppendGrantRestrictAndCascade(StringInfo buf, GrantStmt *stmt);
static void AppendGrantedByInGrant(StringInfo buf, GrantStmt *stmt);

static void
AppendWithGrantOption(StringInfo buf, GrantStmt *stmt)
{
	if (stmt->grant_option)
	{
		appendStringInfo(buf, " WITH GRANT OPTION");
	}
}


static void
AppendGrantOptionFor(StringInfo buf, GrantStmt *stmt)
{
	if (!stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, "GRANT OPTION FOR ");
	}
}


static void
AppendGrantRestrictAndCascade(StringInfo buf, GrantStmt *stmt)
{
	if (!stmt->is_grant)
	{
		if (stmt->behavior == DROP_RESTRICT)
		{
			appendStringInfo(buf, " RESTRICT");
		}
		else if (stmt->behavior == DROP_CASCADE)
		{
			appendStringInfo(buf, " CASCADE");
		}
	}
}


static void
AppendGrantedByInGrant(StringInfo buf, GrantStmt *stmt)
{
	if (stmt->grantor)
	{
		appendStringInfo(buf, " GRANTED BY %s", RoleSpecString(stmt->grantor, true));
	}
}
