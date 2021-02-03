/*-------------------------------------------------------------------------
 *
 * index_pg_source.c
 *    Helper functions copy & pasted from PostgreSQL source code.
 *    All the functions in this file is copied from
 *    postgres/src/backend/commands/indexcmds.c
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/distributed/commands/index_pg_source.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/defrem.h"
#include "distributed/commands.h"
#include "distributed/listutils.h"
#include "mb/pg_wchar.h"


/* *INDENT-OFF* */

/*
 * This function is copy & paste from Postgres source code:
 * 		postgres/src/backend/commands/indexcmds.c
 *
 * Select the name to be used for an index.
 *
 * The argument list is pretty ad-hoc :-(
 */
char *
ChooseIndexName(const char *tabname, Oid namespaceId,
				List *colnames, List *exclusionOpNames,
				bool primary, bool isconstraint)
{
	char *indexname;

	if (primary)
	{
		/* the primary key's name does not depend on the specific column(s) */
		indexname = ChooseRelationName(tabname,
									   NULL,
									   "pkey",
									   namespaceId,
									   true);
	}
	else if (exclusionOpNames != NIL)
	{
		indexname = ChooseRelationName(tabname,
									   ChooseIndexNameAddition(colnames),
									   "excl",
									   namespaceId,
									   true);
	}
	else if (isconstraint)
	{
		indexname = ChooseRelationName(tabname,
									   ChooseIndexNameAddition(colnames),
									   "key",
									   namespaceId,
									   true);
	}
	else
	{
		indexname = ChooseRelationName(tabname,
									   ChooseIndexNameAddition(colnames),
									   "idx",
									   namespaceId,
									   false);
	}

	return indexname;
}


/*
 * This function is copy & paste from Postgres source code:
 * 		postgres/src/backend/commands/indexcmds.c
 *
 * Generate "name2" for a new index given the list of column names for it
 * (as produced by ChooseIndexColumnNames).  This will be passed to
 * ChooseRelationName along with the parent table name and a suitable label.
 *
 * We know that less than NAMEDATALEN characters will actually be used,
 * so we can truncate the result once we've generated that many.
 *
 * XXX See also ChooseForeignKeyConstraintNameAddition and
 * ChooseExtendedStatisticNameAddition.
 */
char *
ChooseIndexNameAddition(List *colnames)
{
	char buf[NAMEDATALEN * 2];
	int buflen = 0;
	ListCell *lc;

	buf[0] = '\0';
	foreach(lc, colnames)
	{
		const char *name = (const char *) lfirst(lc);

		if (buflen > 0)
		{
			buf[buflen++] = '_';    /* insert _ between names */
		}

		/*
		 * At this point we have buflen <= NAMEDATALEN.  name should be less
		 * than NAMEDATALEN already, but use strlcpy for paranoia.
		 */
		strlcpy(buf + buflen, name, NAMEDATALEN);
		buflen += strlen(buf + buflen);
		if (buflen >= NAMEDATALEN)
		{
			break;
		}
	}
	return pstrdup(buf);
}


/*
 *  * This function is copy & paste from Postgres source code:
 * 		postgres/src/backend/commands/indexcmds.c
 *
 * Select the actual names to be used for the columns of an index, given the
 * list of IndexElems for the columns.  This is mostly about ensuring the
 * names are unique so we don't get a conflicting-attribute-names error.
 *
 * Returns a List of plain strings (char *, not String nodes).
 */
List *
ChooseIndexColumnNames(List *indexElems)
{
	List *result = NIL;
	ListCell *lc;

	foreach(lc, indexElems)
	{
		IndexElem *ielem = (IndexElem *) lfirst(lc);
		const char *origname;
		const char *curname;
		int i;
		char buf[NAMEDATALEN];

		/* Get the preliminary name from the IndexElem */
		if (ielem->indexcolname)
		{
			origname = ielem->indexcolname; /* caller-specified name */
		}
		else if (ielem->name)
		{
			origname = ielem->name; /* simple column reference */
		}
		else
		{
			origname = "expr";  /* default name for expression */
		}

		/* If it conflicts with any previous column, tweak it */
		curname = origname;
		for (i = 1;; i++)
		{
			ListCell *lc2;
			char nbuf[32];
			int nlen;

			foreach(lc2, result)
			{
				if (strcmp(curname, (char *) lfirst(lc2)) == 0)
				{
					break;
				}
			}
			if (lc2 == NULL)
			{
				break;          /* found nonconflicting name */
			}
			sprintf(nbuf, "%d", i); /* lgtm[cpp/banned-api-usage-required-any] */

			/* Ensure generated names are shorter than NAMEDATALEN */
			nlen = pg_mbcliplen(origname, strlen(origname),
								NAMEDATALEN - 1 - strlen(nbuf));
			memcpy(buf, origname, nlen); /* lgtm[cpp/banned-api-usage-required-any] */
			strcpy(buf + nlen, nbuf); /* lgtm[cpp/banned-api-usage-required-any] */
			curname = buf;
		}

		/* And attach to the result list */
		result = lappend(result, pstrdup(curname));
	}
	return result;
}

/* *INDENT-ON* */
