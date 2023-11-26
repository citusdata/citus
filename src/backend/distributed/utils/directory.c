/*-------------------------------------------------------------------------
 *
 * directory.c
 *
 * Utility functions for dealing with directories.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"

#include "distributed/utils/directory.h"


/* Local functions forward declarations */
static bool FileIsLink(const char *filename, struct stat filestat);


/*
 * CitusCreateDirectory creates a new directory with the given directory name.
 */
void
CitusCreateDirectory(StringInfo directoryName)
{
	int makeOK = mkdir(directoryName->data, S_IRWXU);
	if (makeOK != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create directory \"%s\": %m",
							   directoryName->data)));
	}
}


/*
 * FileIsLink checks whether a file is a symbolic link.
 */
static bool
FileIsLink(const char *filename, struct stat filestat)
{
	return S_ISLNK(filestat.st_mode);
}


/*
 * CitusRemoveDirectory first checks if the given directory exists. If it does, the
 * function recursively deletes the contents of the given directory, and then
 * deletes the directory itself. This function is modeled on the Boost file
 * system library's remove_all() method.
 */
void
CitusRemoveDirectory(const char *filename)
{
	/* files may be added during execution, loop when that occurs */
	while (true)
	{
		struct stat fileStat;
		int removed = 0;

		int statOK = stat(filename, &fileStat);
		if (statOK < 0)
		{
			if (errno == ENOENT)
			{
				return;  /* if file does not exist, return */
			}
			else
			{
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not stat file \"%s\": %m", filename)));
			}
		}

		/*
		 * If this is a directory, iterate over all its contents and for each
		 * content, recurse into this function. Also, make sure that we do not
		 * recurse into symbolic links.
		 */
		if (S_ISDIR(fileStat.st_mode) && !FileIsLink(filename, fileStat))
		{
			const char *directoryName = filename;

			DIR *directory = AllocateDir(directoryName);
			if (directory == NULL)
			{
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not open directory \"%s\": %m",
									   directoryName)));
			}

			StringInfo fullFilename = makeStringInfo();
			struct dirent *directoryEntry = ReadDir(directory, directoryName);
			for (; directoryEntry != NULL; directoryEntry = ReadDir(directory,
																	directoryName))
			{
				const char *baseFilename = directoryEntry->d_name;

				/* if system file, skip it */
				if (strncmp(baseFilename, ".", MAXPGPATH) == 0 ||
					strncmp(baseFilename, "..", MAXPGPATH) == 0)
				{
					continue;
				}

				resetStringInfo(fullFilename);
				appendStringInfo(fullFilename, "%s/%s", directoryName, baseFilename);

				CitusRemoveDirectory(fullFilename->data);
			}

			pfree(fullFilename->data);
			pfree(fullFilename);
			FreeDir(directory);
		}

		/* we now have an empty directory or a regular file, remove it */
		if (S_ISDIR(fileStat.st_mode))
		{
			/*
			 * We ignore the TOCTUO race condition static analysis warning
			 * here, since we don't actually read the files or directories. We
			 * simply want to remove them.
			 */
			removed = rmdir(filename); /* lgtm[cpp/toctou-race-condition] */

			if (errno == ENOTEMPTY || errno == EEXIST)
			{
				continue;
			}
		}
		else
		{
			/*
			 * We ignore the TOCTUO race condition static analysis warning
			 * here, since we don't actually read the files or directories. We
			 * simply want to remove them.
			 */
			removed = unlink(filename); /* lgtm[cpp/toctou-race-condition] */
		}

		if (removed != 0 && errno != ENOENT)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not remove file \"%s\": %m", filename)));
		}

		return;
	}
}


/*
 * CleanupJobCacheDirectory cleans up all files in the job cache directory
 * as part of this process's start-up logic.
 */
void
CleanupJobCacheDirectory(void)
{
	/* use the default tablespace in {datadir}/base */
	StringInfo jobCacheDirectory = makeStringInfo();
	appendStringInfo(jobCacheDirectory, "base/%s", PG_JOB_CACHE_DIR);

	CitusRemoveDirectory(jobCacheDirectory->data);
	CitusCreateDirectory(jobCacheDirectory);

	pfree(jobCacheDirectory->data);
	pfree(jobCacheDirectory);
}
