/*-------------------------------------------------------------------------
 *
 * cstore.c
 *
 * This file contains...
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "utils/rel.h"

#include "cstore.h"

static void CreateDirectory(StringInfo directoryName);
static bool DirectoryExists(StringInfo directoryName);

/* ParseCompressionType converts a string to a compression type. */
CompressionType
ParseCompressionType(const char *compressionTypeString)
{
	CompressionType compressionType = COMPRESSION_TYPE_INVALID;
	Assert(compressionTypeString != NULL);

	if (strncmp(compressionTypeString, COMPRESSION_STRING_NONE, NAMEDATALEN) == 0)
	{
		compressionType = COMPRESSION_NONE;
	}
	else if (strncmp(compressionTypeString, COMPRESSION_STRING_PG_LZ, NAMEDATALEN) == 0)
	{
		compressionType = COMPRESSION_PG_LZ;
	}

	return compressionType;
}

/* CreateDirectory creates a new directory with the given directory name. */
static void
CreateDirectory(StringInfo directoryName)
{
	int makeOK = mkdir(directoryName->data, S_IRWXU);
	if (makeOK != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create directory \"%s\": %m",
							   directoryName->data)));
	}
}

/* DirectoryExists checks if a directory exists for the given directory name. */
static bool
DirectoryExists(StringInfo directoryName)
{
	bool directoryExists = true;
	struct stat directoryStat;

	int statOK = stat(directoryName->data, &directoryStat);
	if (statOK == 0)
	{
		/* file already exists; check that it is a directory */
		if (!S_ISDIR(directoryStat.st_mode))
		{
			ereport(ERROR, (errmsg("\"%s\" is not a directory", directoryName->data),
							errhint("You need to remove or rename the file \"%s\".",
									directoryName->data)));
		}
	}
	else
	{
		if (errno == ENOENT)
		{
			directoryExists = false;
		}
		else
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not stat directory \"%s\": %m",
								   directoryName->data)));
		}
	}

	return directoryExists;
}

/*
 * RemoveCStoreDatabaseDirectory removes CStore directory previously
 * created for this database.
 * However it does not remove 'cstore_fdw' directory even if there
 * are no other databases left.
 */
void
RemoveCStoreDatabaseDirectory(Oid databaseOid)
{
	StringInfo cstoreDirectoryPath = makeStringInfo();
	StringInfo cstoreDatabaseDirectoryPath = makeStringInfo();

	appendStringInfo(cstoreDirectoryPath, "%s/%s", DataDir, CSTORE_FDW_NAME);

	appendStringInfo(cstoreDatabaseDirectoryPath, "%s/%s/%u", DataDir,
					 CSTORE_FDW_NAME, databaseOid);

	if (DirectoryExists(cstoreDatabaseDirectoryPath))
	{
		rmtree(cstoreDatabaseDirectoryPath->data, true);
	}
}


/*
 * InitializeCStoreTableFile creates data and footer file for a cstore table.
 * The function assumes data and footer files do not exist, therefore
 * it should be called on empty or non-existing table. Notice that the caller
 * is expected to acquire AccessExclusiveLock on the relation.
 */
void
InitializeCStoreTableFile(Oid relationId, Relation relation, CStoreOptions *cstoreOptions)
{
	TableWriteState *writeState = NULL;
	TupleDesc tupleDescriptor = RelationGetDescr(relation);

	/*
	 * Initialize state to write to the cstore file. This creates an
	 * empty data file and a valid footer file for the table.
	 */
	writeState = CStoreBeginWrite(relationId, cstoreOptions->filename,
			cstoreOptions->compressionType, cstoreOptions->stripeRowCount,
			cstoreOptions->blockRowCount, tupleDescriptor);
	CStoreEndWrite(writeState);
}


/*
 * CreateCStoreDatabaseDirectory creates the directory (and parent directories,
 * if needed) used to store automatically managed cstore_fdw files. The path to
 * the directory is $PGDATA/cstore_fdw/{databaseOid}.
 */
void
CreateCStoreDatabaseDirectory(Oid databaseOid)
{
	bool cstoreDirectoryExists = false;
	bool databaseDirectoryExists = false;
	StringInfo cstoreDatabaseDirectoryPath = NULL;

	StringInfo cstoreDirectoryPath = makeStringInfo();
	appendStringInfo(cstoreDirectoryPath, "%s/%s", DataDir, CSTORE_FDW_NAME);

	cstoreDirectoryExists = DirectoryExists(cstoreDirectoryPath);
	if (!cstoreDirectoryExists)
	{
		CreateDirectory(cstoreDirectoryPath);
	}

	cstoreDatabaseDirectoryPath = makeStringInfo();
	appendStringInfo(cstoreDatabaseDirectoryPath, "%s/%s/%u", DataDir,
					 CSTORE_FDW_NAME, databaseOid);

	databaseDirectoryExists = DirectoryExists(cstoreDatabaseDirectoryPath);
	if (!databaseDirectoryExists)
	{
		CreateDirectory(cstoreDatabaseDirectoryPath);
	}
}


/*
 * DeleteCStoreTableFiles deletes the data and footer files for a cstore table
 * whose data filename is given.
 */
void
DeleteCStoreTableFiles(char *filename)
{
	int dataFileRemoved = 0;
	int footerFileRemoved = 0;

	StringInfo tableFooterFilename = makeStringInfo();
	appendStringInfo(tableFooterFilename, "%s%s", filename, CSTORE_FOOTER_FILE_SUFFIX);

	/* delete the footer file */
	footerFileRemoved = unlink(tableFooterFilename->data);
	if (footerFileRemoved != 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
						  errmsg("could not delete file \"%s\": %m",
								 tableFooterFilename->data)));
	}

	/* delete the data file */
	dataFileRemoved = unlink(filename);
	if (dataFileRemoved != 0)
	{
		ereport(WARNING, (errcode_for_file_access(),
						  errmsg("could not delete file \"%s\": %m",
								 filename)));
	}
}
