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
			if (strcmp(opt_formats[i].type, "string") == 0)
			{
				char *value = defGetString(option);
				appendStringInfo(buf, opt_formats[i].format, quote_identifier(value));
			}
			else if (strcmp(opt_formats[i].type, "integer") == 0)
			{
				int32 value = defGetInt32(option);
				appendStringInfo(buf, opt_formats[i].format, value);
			}
			else if (strcmp(opt_formats[i].type, "boolean") == 0)
			{
				bool value = defGetBoolean(option);
				appendStringInfo(buf, opt_formats[i].format, value ? "true" : "false");
			}
			else
			{
				elog(ERROR, "unrecognized option type: %s", opt_formats[i].type);
			}
			break;
		}
	}
}
