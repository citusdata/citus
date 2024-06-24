/*-------------------------------------------------------------------------
 *
 * version_compat.h
 *	  Compatibility macros for writing code agnostic to PostgreSQL versions
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef VERSION_COMPAT_H
#define VERSION_COMPAT_H

#include "postgres.h"

#include "access/heapam.h"
#include "access/sdir.h"
#include "catalog/namespace.h"
#include "commands/explain.h"
#include "executor/tuptable.h"
#include "nodes/parsenodes.h"
#include "optimizer/optimizer.h"
#include "parser/parse_func.h"
#include "tcop/tcopprot.h"

#include "pg_version_compat.h"

#include "distributed/citus_ruleutils.h"
#include "distributed/citus_safe_lib.h"

typedef struct
{
	File fd;
	off_t offset;
} FileCompat;

static inline int
FileWriteCompat(FileCompat *file, char *buffer, int amount, uint32 wait_event_info)
{
	int count = FileWrite(file->fd, buffer, amount, file->offset, wait_event_info);
	if (count > 0)
	{
		file->offset += count;
	}
	return count;
}


static inline int
FileReadCompat(FileCompat *file, char *buffer, int amount, uint32 wait_event_info)
{
	int count = FileRead(file->fd, buffer, amount, file->offset, wait_event_info);
	if (count > 0)
	{
		file->offset += count;
	}
	return count;
}


static inline FileCompat
FileCompatFromFileStart(File fileDesc)
{
	FileCompat fc;

	/* ensure uninitialized padding doesn't escape the function */
	memset_struct_0(fc);
	fc.fd = fileDesc;
	fc.offset = 0;

	return fc;
}


#endif   /* VERSION_COMPAT_H */
