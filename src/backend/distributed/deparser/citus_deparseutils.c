#include "postgres.h"
#include "utils/builtins.h"
#include "commands/defrem.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "distributed/deparser.h"



void
handleOption(StringInfo buf, DefElem *option, const struct option_format *opt_formats, int
			 opt_formats_len)
{
	const char *name = option->defname;
	int i;

	for (i = 0; i < opt_formats_len; i++)
	{
		if (strcmp(name, opt_formats[i].name) == 0)
		{
			switch (opt_formats[i].type)
			{
				case T_String:
				{
					char *value = defGetString(option);
					appendStringInfo(buf, opt_formats[i].format, quote_identifier(value));
					break;
				}

				case T_Integer:
				{
					int32 value = defGetInt32(option);
					appendStringInfo(buf, opt_formats[i].format, value);
					break;
				}

				case T_Boolean:
				{
					bool value = defGetBoolean(option);
					appendStringInfo(buf, opt_formats[i].format, value ? "true" :
									 "false");
					break;
				}

				default:

					/* Should not happen */
					elog(ERROR, "unrecognized option type: %d", opt_formats[i].type);
			}
			return;
		}
	}
}
