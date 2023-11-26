/*-------------------------------------------------------------------------
 *
 * citus_deparseutils.c
 *
 *   This file contains common functions used for deparsing PostgreSQL
 *   statements to their equivalent SQL representation.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "commands/defrem.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "pg_version_constants.h"

#include "distributed/deparser.h"


/**
 * DefElemOptionToStatement converts a DefElem option to a SQL statement and
 * appends it to the given StringInfo buffer.
 *
 * @param buf The StringInfo buffer to append the SQL statement to.
 * @param option The DefElem option to convert to a SQL statement.
 * @param optionFormats The option format specification to use for the conversion.
 * @param optionFormatsLen The number of option formats in the opt_formats array.
 */
void
DefElemOptionToStatement(StringInfo buf, DefElem *option,
						 const DefElemOptionFormat *optionFormats,
						 int optionFormatsLen)
{
	const char *name = option->defname;
	int i;

	for (i = 0; i < optionFormatsLen; i++)
	{
		if (strcmp(name, optionFormats[i].name) == 0)
		{
			switch (optionFormats[i].type)
			{
				case OPTION_FORMAT_STRING:
				{
					char *value = defGetString(option);
					appendStringInfo(buf, optionFormats[i].format, quote_identifier(
										 value));
					break;
				}

				case OPTION_FORMAT_INTEGER:
				{
					int32 value = defGetInt32(option);
					appendStringInfo(buf, optionFormats[i].format, value);
					break;
				}

				case OPTION_FORMAT_BOOLEAN:
				{
					bool value = defGetBoolean(option);
					appendStringInfo(buf, optionFormats[i].format, value ? "true" :
									 "false");
					break;
				}

				case OPTION_FORMAT_LITERAL_CSTR:
				{
					char *value = defGetString(option);
					appendStringInfo(buf, optionFormats[i].format, quote_literal_cstr(
										 value));
					break;
				}

				default:
				{
					elog(ERROR, "unrecognized option type: %d", optionFormats[i].type);
					break;
				}
			}
		}
	}
}
