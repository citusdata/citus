#include "postgres.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "distributed/deparser.h"
#include "distributed/citus_ruleutils.h"



void
AppendWithGrantOption(StringInfo buf, GrantStmt *stmt)
{
	if (stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, " WITH GRANT OPTION");
	}
}

void
AppendGrantOptionFor(StringInfo buf, GrantStmt *stmt)
{
	if (!stmt->is_grant && stmt->grant_option)
	{
		appendStringInfo(buf, "GRANT OPTION FOR ");
	}
}


void
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


void
AppendGrantedByInGrant(StringInfo buf, GrantStmt *stmt)
{
	if (stmt->grantor)
	{
		appendStringInfo(buf, " GRANTED BY %s", RoleSpecString(stmt->grantor, true));
	}
}
