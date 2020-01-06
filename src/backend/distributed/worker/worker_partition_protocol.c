/*-------------------------------------------------------------------------
 *
 * worker_partition_protocol.c
 *
 * Routines for partitioning table data into multiple files. Table partitioning
 * is one of the three distributed execution primitives that we apply on worker
 * nodes; and when partitioning data, we follow Hadoop's naming conventions as
 * much as possible.
 *
 * Copyright (c) Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/stat.h>
#include <math.h>
#include <unistd.h>
#include <stdint.h>

#include "access/hash.h"
#include "access/htup_details.h"
#include "access/nbtree.h"
#include "catalog/pg_am.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/copy.h"
#include "commands/defrem.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/multi_physical_planner.h"
#include "distributed/resource_lock.h"
#include "distributed/transmit.h"
#include "distributed/worker_protocol.h"
#include "distributed/version_compat.h"
#include "executor/spi.h"
#include "mb/pg_wchar.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


/* Config variables managed via guc.c */
bool BinaryWorkerCopyFormat = false;   /* binary format for copying between workers */
int PartitionBufferSize = 16384; /* total partitioning buffer size in KB */

/* Local variables */
static uint32 FileBufferSizeInBytes = 0; /* file buffer size to init later */


/* Local functions forward declarations */
static ShardInterval ** SyntheticShardIntervalArrayForShardMinValues(
	Datum *shardMinValues,
	int shardCount);
static StringInfo InitTaskAttemptDirectory(uint64 jobId, uint32 taskId);
static uint32 FileBufferSize(int partitionBufferSizeInKB, uint32 fileCount);
static FileOutputStream * OpenPartitionFiles(StringInfo directoryName, uint32 fileCount);
static void ClosePartitionFiles(FileOutputStream *partitionFileArray, uint32 fileCount);
static void RenameDirectory(StringInfo oldDirectoryName, StringInfo newDirectoryName);
static void FileOutputStreamWrite(FileOutputStream *file, StringInfo dataToWrite);
static void FileOutputStreamFlush(FileOutputStream *file);
static void FilterAndPartitionTable(const char *filterQuery,
									const char *columnName, Oid columnType,
									uint32 (*PartitionIdFunction)(Datum, Oid, const
																  void *),
									const void *partitionIdContext,
									FileOutputStream *partitionFileArray,
									uint32 fileCount);
static int ColumnIndex(TupleDesc rowDescriptor, const char *columnName);
static CopyOutState InitRowOutputState(void);
static void ClearRowOutputState(CopyOutState copyState);
static void OutputBinaryHeaders(FileOutputStream *partitionFileArray, uint32 fileCount);
static void OutputBinaryFooters(FileOutputStream *partitionFileArray, uint32 fileCount);
static uint32 RangePartitionId(Datum partitionValue, Oid partitionCollation, const
							   void *context);
static uint32 HashPartitionId(Datum partitionValue, Oid partitionCollation, const
							  void *context);
static StringInfo UserPartitionFilename(StringInfo directoryName, uint32 partitionId);
static bool FileIsLink(char *filename, struct stat filestat);


/* exports for SQL callable functions */
PG_FUNCTION_INFO_V1(worker_range_partition_table);
PG_FUNCTION_INFO_V1(worker_hash_partition_table);


/*
 * worker_range_partition_table executes the given filter query, repartitions
 * the filter query's results on a partitioning column, and writes the resulting
 * rows to a set of text files on local disk. The function then atomically
 * renames the directory in which the text files live to ensure deterministic
 * behavior.
 *
 * This function applies range partitioning through the use of a function
 * pointer and a range context object; for details, see RangePartitionId().
 */
Datum
worker_range_partition_table(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	uint32 taskId = PG_GETARG_UINT32(1);
	text *filterQueryText = PG_GETARG_TEXT_P(2);
	text *partitionColumnText = PG_GETARG_TEXT_P(3);
	Oid partitionColumnType = PG_GETARG_OID(4);
	ArrayType *splitPointObject = PG_GETARG_ARRAYTYPE_P(5);

	const char *filterQuery = text_to_cstring(filterQueryText);
	const char *partitionColumn = text_to_cstring(partitionColumnText);


	/* first check that array element's and partition column's types match */
	Oid splitPointType = ARR_ELEMTYPE(splitPointObject);

	CheckCitusVersion(ERROR);

	if (splitPointType != partitionColumnType)
	{
		ereport(ERROR, (errmsg("partition column type %u and split point type %u "
							   "do not match", partitionColumnType, splitPointType)));
	}

	/* use column's type information to get the comparison function */
	FmgrInfo *comparisonFunction = GetFunctionInfo(partitionColumnType,
												   BTREE_AM_OID, BTORDER_PROC);

	/* deserialize split points into their array representation */
	Datum *splitPointArray = DeconstructArrayObject(splitPointObject);
	int32 splitPointCount = ArrayObjectCount(splitPointObject);
	uint32 fileCount = splitPointCount + 1; /* range partitioning needs an extra bucket */

	/* create range partition context object */
	RangePartitionContext *partitionContext = palloc0(sizeof(RangePartitionContext));
	partitionContext->comparisonFunction = comparisonFunction;
	partitionContext->splitPointArray = splitPointArray;
	partitionContext->splitPointCount = splitPointCount;

	/* init directories and files to write the partitioned data to */
	StringInfo taskDirectory = InitTaskDirectory(jobId, taskId);
	StringInfo taskAttemptDirectory = InitTaskAttemptDirectory(jobId, taskId);

	FileOutputStream *partitionFileArray = OpenPartitionFiles(taskAttemptDirectory,
															  fileCount);
	FileBufferSizeInBytes = FileBufferSize(PartitionBufferSize, fileCount);

	/* call the partitioning function that does the actual work */
	FilterAndPartitionTable(filterQuery, partitionColumn, partitionColumnType,
							&RangePartitionId, (const void *) partitionContext,
							partitionFileArray, fileCount);

	/* close partition files and atomically rename (commit) them */
	ClosePartitionFiles(partitionFileArray, fileCount);
	CitusRemoveDirectory(taskDirectory);
	RenameDirectory(taskAttemptDirectory, taskDirectory);

	PG_RETURN_VOID();
}


/*
 * worker_hash_partition_table executes the given filter query, repartitions the
 * filter query's results on a partitioning column, and writes the resulting
 * rows to a set of text files on local disk. The function then atomically
 * renames the directory in which the text files live to ensure deterministic
 * behavior.
 *
 * This function applies hash partitioning through the use of a function pointer
 * and a hash context object; for details, see HashPartitionId().
 */
Datum
worker_hash_partition_table(PG_FUNCTION_ARGS)
{
	uint64 jobId = PG_GETARG_INT64(0);
	uint32 taskId = PG_GETARG_UINT32(1);
	text *filterQueryText = PG_GETARG_TEXT_P(2);
	text *partitionColumnText = PG_GETARG_TEXT_P(3);
	Oid partitionColumnType = PG_GETARG_OID(4);
	ArrayType *hashRangeObject = PG_GETARG_ARRAYTYPE_P(5);

	const char *filterQuery = text_to_cstring(filterQueryText);
	const char *partitionColumn = text_to_cstring(partitionColumnText);

	Datum *hashRangeArray = DeconstructArrayObject(hashRangeObject);
	int32 partitionCount = ArrayObjectCount(hashRangeObject);

	CheckCitusVersion(ERROR);

	HashPartitionContext *partitionContext = palloc0(sizeof(HashPartitionContext));
	partitionContext->syntheticShardIntervalArray =
		SyntheticShardIntervalArrayForShardMinValues(hashRangeArray, partitionCount);
	partitionContext->hasUniformHashDistribution =
		HasUniformHashDistribution(partitionContext->syntheticShardIntervalArray,
								   partitionCount);

	/* use column's type information to get the hashing function */
	FmgrInfo *hashFunction = GetFunctionInfo(partitionColumnType, HASH_AM_OID,
											 HASHSTANDARD_PROC);

	/* we create as many files as the number of split points */
	uint32 fileCount = partitionCount;

	partitionContext->hashFunction = hashFunction;
	partitionContext->partitionCount = partitionCount;

	/* we'll use binary search, we need the comparison function */
	if (!partitionContext->hasUniformHashDistribution)
	{
		partitionContext->comparisonFunction =
			GetFunctionInfo(partitionColumnType, BTREE_AM_OID, BTORDER_PROC);
	}

	/* init directories and files to write the partitioned data to */
	StringInfo taskDirectory = InitTaskDirectory(jobId, taskId);
	StringInfo taskAttemptDirectory = InitTaskAttemptDirectory(jobId, taskId);

	FileOutputStream *partitionFileArray = OpenPartitionFiles(taskAttemptDirectory,
															  fileCount);
	FileBufferSizeInBytes = FileBufferSize(PartitionBufferSize, fileCount);

	/* call the partitioning function that does the actual work */
	FilterAndPartitionTable(filterQuery, partitionColumn, partitionColumnType,
							&HashPartitionId, (const void *) partitionContext,
							partitionFileArray, fileCount);

	/* close partition files and atomically rename (commit) them */
	ClosePartitionFiles(partitionFileArray, fileCount);
	CitusRemoveDirectory(taskDirectory);
	RenameDirectory(taskAttemptDirectory, taskDirectory);

	PG_RETURN_VOID();
}


/*
 * SyntheticShardIntervalArrayForShardMinValues returns a shard interval pointer array
 * which gets the shardMinValues from the input shardMinValues array. Note that
 * we simply calculate shard max values by decrementing previous shard min values by one.
 *
 * The function only fills the min/max values of shard the intervals. Thus, should
 * not be used for general purpose operations.
 */
static ShardInterval **
SyntheticShardIntervalArrayForShardMinValues(Datum *shardMinValues, int shardCount)
{
	Datum nextShardMaxValue = Int32GetDatum(INT32_MAX);
	ShardInterval **syntheticShardIntervalArray =
		palloc(sizeof(ShardInterval *) * shardCount);

	for (int shardIndex = shardCount - 1; shardIndex >= 0; --shardIndex)
	{
		Datum currentShardMinValue = shardMinValues[shardIndex];
		ShardInterval *shardInterval = CitusMakeNode(ShardInterval);

		shardInterval->minValue = currentShardMinValue;
		shardInterval->maxValue = nextShardMaxValue;

		nextShardMaxValue = Int32GetDatum(DatumGetInt32(currentShardMinValue) - 1);

		syntheticShardIntervalArray[shardIndex] = shardInterval;
	}

	return syntheticShardIntervalArray;
}


/*
 * GetFunctionInfo first resolves the operator for the given data type, access
 * method, and support procedure. The function then uses the resolved operator's
 * identifier to fill in a function manager object, and returns this object.
 */
FmgrInfo *
GetFunctionInfo(Oid typeId, Oid accessMethodId, int16 procedureId)
{
	FmgrInfo *functionInfo = (FmgrInfo *) palloc0(sizeof(FmgrInfo));

	/* get default operator class from pg_opclass for datum type */
	Oid operatorClassId = GetDefaultOpClass(typeId, accessMethodId);

	Oid operatorFamilyId = get_opclass_family(operatorClassId);
	Oid operatorClassInputType = get_opclass_input_type(operatorClassId);

	Oid operatorId = get_opfamily_proc(operatorFamilyId, operatorClassInputType,
									   operatorClassInputType, procedureId);

	if (operatorId == InvalidOid)
	{
		ereport(ERROR, (errmsg("could not find function for data typeId %u", typeId)));
	}

	/* fill in the FmgrInfo struct using the operatorId */
	fmgr_info(operatorId, functionInfo);

	return functionInfo;
}


/*
 * DeconstructArrayObject takes in a single dimensional array, and deserializes
 * this array's members into an array of datum objects. The function then
 * returns this datum array.
 */
Datum *
DeconstructArrayObject(ArrayType *arrayObject)
{
	Datum *datumArray = NULL;
	bool *datumArrayNulls = NULL;
	int datumArrayLength = 0;

	bool typeByVal = false;
	char typeAlign = 0;
	int16 typeLength = 0;

	bool arrayHasNull = ARR_HASNULL(arrayObject);
	if (arrayHasNull)
	{
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						errmsg("worker array object cannot contain null values")));
	}

	Oid typeId = ARR_ELEMTYPE(arrayObject);
	get_typlenbyvalalign(typeId, &typeLength, &typeByVal, &typeAlign);

	deconstruct_array(arrayObject, typeId, typeLength, typeByVal, typeAlign,
					  &datumArray, &datumArrayNulls, &datumArrayLength);

	return datumArray;
}


/*
 * ArrayObjectCount takes in a single dimensional array, and returns the number
 * of elements in this array.
 */
int32
ArrayObjectCount(ArrayType *arrayObject)
{
	int32 dimensionCount = ARR_NDIM(arrayObject);
	int32 *dimensionLengthArray = ARR_DIMS(arrayObject);

	if (dimensionCount == 0)
	{
		return 0;
	}

	/* we currently allow split point arrays to have only one subarray */
	Assert(dimensionCount == 1);

	int32 arrayLength = ArrayGetNItems(dimensionCount, dimensionLengthArray);
	if (arrayLength <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR),
						errmsg("worker array object cannot be empty")));
	}

	return arrayLength;
}


/*
 * InitTaskDirectory creates a job and task directory using given identifiers,
 * if these directories do not already exist. The function then returns the task
 * directory's name.
 */
StringInfo
InitTaskDirectory(uint64 jobId, uint32 taskId)
{
	/*
	 * If the task tracker assigned this task (regular case), the tracker should
	 * have already created the job directory.
	 */
	StringInfo jobDirectoryName = JobDirectoryName(jobId);
	StringInfo taskDirectoryName = TaskDirectoryName(jobId, taskId);

	LockJobResource(jobId, AccessExclusiveLock);

	bool jobDirectoryExists = DirectoryExists(jobDirectoryName);
	if (!jobDirectoryExists)
	{
		CitusCreateDirectory(jobDirectoryName);
	}

	bool taskDirectoryExists = DirectoryExists(taskDirectoryName);
	if (!taskDirectoryExists)
	{
		CitusCreateDirectory(taskDirectoryName);
	}

	UnlockJobResource(jobId, AccessExclusiveLock);

	return taskDirectoryName;
}


/*
 * InitTaskAttemptDirectory finds a task attempt directory that is not taken,
 * and creates that directory. The function then returns the task attempt
 * directory's name.
 */
static StringInfo
InitTaskAttemptDirectory(uint64 jobId, uint32 taskId)
{
	StringInfo taskDirectoryName = TaskDirectoryName(jobId, taskId);
	uint32 randomId = (uint32) random();

	/*
	 * We should have only one process executing this task. Still, we append a
	 * random id just in case.
	 */
	StringInfo taskAttemptDirectoryName = makeStringInfo();
	appendStringInfo(taskAttemptDirectoryName, "%s_%0*u",
					 taskDirectoryName->data, MIN_TASK_FILENAME_WIDTH, randomId);

	/*
	 * If this task previously failed, and gets re-executed and improbably draws
	 * the same randomId, the task will fail to create the directory.
	 */
	CitusCreateDirectory(taskAttemptDirectoryName);

	return taskAttemptDirectoryName;
}


/* Calculates and returns the buffer size to use for each file. */
static uint32
FileBufferSize(int partitionBufferSizeInKB, uint32 fileCount)
{
	double partitionBufferSize = (double) partitionBufferSizeInKB * 1024.0;
	uint32 fileBufferSize = (uint32) rint(partitionBufferSize / fileCount);

	return fileBufferSize;
}


/*
 * OpenPartitionFiles takes in a directory name and file count, and opens new
 * partition files in this directory. The names for these new files are modeled
 * after Hadoop's naming conventions for map files. These file names, virtual
 * file descriptors, and file buffers are stored together in file output stream
 * objects. These objects are then returned in an array from this function.
 */
static FileOutputStream *
OpenPartitionFiles(StringInfo directoryName, uint32 fileCount)
{
	File fileDescriptor = 0;
	const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	const int fileMode = (S_IRUSR | S_IWUSR);

	FileOutputStream *partitionFileArray = palloc0(fileCount * sizeof(FileOutputStream));

	for (uint32 fileIndex = 0; fileIndex < fileCount; fileIndex++)
	{
		StringInfo filePath = UserPartitionFilename(directoryName, fileIndex);

		fileDescriptor = PathNameOpenFilePerm(filePath->data, fileFlags, fileMode);
		if (fileDescriptor < 0)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not open file \"%s\": %m", filePath->data)));
		}

		partitionFileArray[fileIndex].fileCompat = FileCompatFromFileStart(
			fileDescriptor);
		partitionFileArray[fileIndex].fileBuffer = makeStringInfo();
		partitionFileArray[fileIndex].filePath = filePath;
	}

	return partitionFileArray;
}


/*
 * ClosePartitionFiles walks over each file output stream object, and flushes
 * any remaining data in the file's buffer. The function then closes the file,
 * and deletes any allocated memory for the file stream object.
 */
static void
ClosePartitionFiles(FileOutputStream *partitionFileArray, uint32 fileCount)
{
	for (uint32 fileIndex = 0; fileIndex < fileCount; fileIndex++)
	{
		FileOutputStream *partitionFile = &partitionFileArray[fileIndex];

		FileOutputStreamFlush(partitionFile);

		FileClose(partitionFile->fileCompat.fd);
		FreeStringInfo(partitionFile->fileBuffer);
		FreeStringInfo(partitionFile->filePath);
	}

	pfree(partitionFileArray);
}


/*
 * MasterJobDirectoryName constructs a standardized job
 * directory path for the given job id on the master node.
 */
StringInfo
MasterJobDirectoryName(uint64 jobId)
{
	StringInfo jobDirectoryName = makeStringInfo();

	/*
	 * We use the default tablespace in {datadir}/base. Further, we need to
	 * apply padding on our 64-bit job id, and hence can't use UINT64_FORMAT.
	 */
	appendStringInfo(jobDirectoryName, "base/%s/%s%0*" INT64_MODIFIER "u",
					 PG_JOB_CACHE_DIR, MASTER_JOB_DIRECTORY_PREFIX,
					 MIN_JOB_DIRNAME_WIDTH, jobId);

	return jobDirectoryName;
}


/*
 * JobDirectoryName Constructs a standardized job
 * directory path for the given job id on the worker nodes.
 */
StringInfo
JobDirectoryName(uint64 jobId)
{
	/*
	 * We use the default tablespace in {datadir}/base.
	 */
	StringInfo jobDirectoryName = makeStringInfo();
	appendStringInfo(jobDirectoryName, "base/%s/%s%0*" INT64_MODIFIER "u",
					 PG_JOB_CACHE_DIR, JOB_DIRECTORY_PREFIX,
					 MIN_JOB_DIRNAME_WIDTH, jobId);

	return jobDirectoryName;
}


/* Constructs a standardized task directory path for given job and task ids. */
StringInfo
TaskDirectoryName(uint64 jobId, uint32 taskId)
{
	StringInfo jobDirectoryName = JobDirectoryName(jobId);

	StringInfo taskDirectoryName = makeStringInfo();
	appendStringInfo(taskDirectoryName, "%s/%s%0*u",
					 jobDirectoryName->data,
					 TASK_FILE_PREFIX, MIN_TASK_FILENAME_WIDTH, taskId);

	return taskDirectoryName;
}


/*
 * PartitionFilename returns a partition file path for given directory and id
 * which is suitable for use in worker_fetch_partition_file and tranmsit.
 *
 * It excludes the user ID part at the end of the filename, since that is
 * added by worker_fetch_partition_file itself based on the current user.
 * For the full path use UserPartitionFilename.
 */
StringInfo
PartitionFilename(StringInfo directoryName, uint32 partitionId)
{
	StringInfo partitionFilename = makeStringInfo();
	appendStringInfo(partitionFilename, "%s/%s%0*u",
					 directoryName->data,
					 PARTITION_FILE_PREFIX, MIN_PARTITION_FILENAME_WIDTH, partitionId);

	return partitionFilename;
}


/*
 * UserPartitionFilename returns the path of a partition file for the given
 * partition ID and the current user.
 */
static StringInfo
UserPartitionFilename(StringInfo directoryName, uint32 partitionId)
{
	StringInfo partitionFilename = PartitionFilename(directoryName, partitionId);

	appendStringInfo(partitionFilename, ".%u", GetUserId());

	return partitionFilename;
}


/*
 * JobDirectoryElement takes in a filename, and checks if this name lives in the
 * directory path that is used for task output files. Note that this function's
 * implementation is coupled with JobDirectoryName().
 */
bool
JobDirectoryElement(const char *filename)
{
	bool directoryElement = false;

	StringInfo directoryPath = makeStringInfo();
	appendStringInfo(directoryPath, "base/%s/%s", PG_JOB_CACHE_DIR, JOB_DIRECTORY_PREFIX);

	char *directoryPathFound = strstr(filename, directoryPath->data);
	if (directoryPathFound != NULL)
	{
		directoryElement = true;
	}

	pfree(directoryPath);

	return directoryElement;
}


/*
 * CacheDirectoryElement takes in a filename, and checks if this name lives in
 * the directory path that is used for job, task, table etc. files.
 */
bool
CacheDirectoryElement(const char *filename)
{
	bool directoryElement = false;

	StringInfo directoryPath = makeStringInfo();
	appendStringInfo(directoryPath, "base/%s/", PG_JOB_CACHE_DIR);

	char *directoryPathFound = strstr(filename, directoryPath->data);

	/*
	 * If directoryPath occurs at the beginning of the filename, then the
	 * pointers should now be equal.
	 */
	if (directoryPathFound == filename)
	{
		directoryElement = true;
	}

	pfree(directoryPath);

	return directoryElement;
}


/* Checks if a directory exists for the given directory name. */
bool
DirectoryExists(StringInfo directoryName)
{
	bool directoryExists = true;
	struct stat directoryStat;

	int statOK = stat(directoryName->data, &directoryStat);
	if (statOK == 0)
	{
		/* file already exists; just assert that it is a directory */
		Assert(S_ISDIR(directoryStat.st_mode));
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


/* Creates a new directory with the given directory name. */
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


#ifdef WIN32
static bool
FileIsLink(char *filename, struct stat filestat)
{
	return pgwin32_is_junction(filename);
}


#else
static bool
FileIsLink(char *filename, struct stat filestat)
{
	return S_ISLNK(filestat.st_mode);
}


#endif


/*
 * CitusRemoveDirectory first checks if the given directory exists. If it does, the
 * function recursively deletes the contents of the given directory, and then
 * deletes the directory itself. This function is modeled on the Boost file
 * system library's remove_all() method.
 */
void
CitusRemoveDirectory(StringInfo filename)
{
	struct stat fileStat;
	int removed = 0;

	int fileStated = stat(filename->data, &fileStat);
	if (fileStated < 0)
	{
		if (errno == ENOENT)
		{
			return;  /* if file does not exist, return */
		}
		else
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not stat file \"%s\": %m", filename->data)));
		}
	}

	/*
	 * If this is a directory, iterate over all its contents and for each
	 * content, recurse into this function. Also, make sure that we do not
	 * recurse into symbolic links.
	 */
	if (S_ISDIR(fileStat.st_mode) && !FileIsLink(filename->data, fileStat))
	{
		const char *directoryName = filename->data;

		DIR *directory = AllocateDir(directoryName);
		if (directory == NULL)
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not open directory \"%s\": %m",
								   directoryName)));
		}

		struct dirent *directoryEntry = ReadDir(directory, directoryName);
		for (; directoryEntry != NULL; directoryEntry = ReadDir(directory, directoryName))
		{
			const char *baseFilename = directoryEntry->d_name;

			/* if system file, skip it */
			if (strncmp(baseFilename, ".", MAXPGPATH) == 0 ||
				strncmp(baseFilename, "..", MAXPGPATH) == 0)
			{
				continue;
			}

			StringInfo fullFilename = makeStringInfo();
			appendStringInfo(fullFilename, "%s/%s", directoryName, baseFilename);

			CitusRemoveDirectory(fullFilename);

			FreeStringInfo(fullFilename);
		}

		FreeDir(directory);
	}

	/* we now have an empty directory or a regular file, remove it */
	if (S_ISDIR(fileStat.st_mode))
	{
		removed = rmdir(filename->data);
	}
	else
	{
		removed = unlink(filename->data);
	}

	if (removed != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not remove file \"%s\": %m", filename->data)));
	}
}


/* Moves directory from old path to the new one. */
static void
RenameDirectory(StringInfo oldDirectoryName, StringInfo newDirectoryName)
{
	int renamed = rename(oldDirectoryName->data, newDirectoryName->data);
	if (renamed != 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not rename directory \"%s\" to \"%s\": %m",
							   oldDirectoryName->data, newDirectoryName->data)));
	}
}


/*
 * FileOutputStreamWrite appends given data to file stream's internal buffers.
 * The function then checks if buffered data exceeds preconfigured buffer size;
 * if so, the function flushes the buffer to the underlying file.
 */
static void
FileOutputStreamWrite(FileOutputStream *file, StringInfo dataToWrite)
{
	StringInfo fileBuffer = file->fileBuffer;
	uint32 newBufferSize = fileBuffer->len + dataToWrite->len;

	appendBinaryStringInfo(fileBuffer, dataToWrite->data, dataToWrite->len);

	if (newBufferSize > FileBufferSizeInBytes)
	{
		FileOutputStreamFlush(file);

		resetStringInfo(fileBuffer);
	}
}


/* Flushes data buffered in the file stream object to the underlying file. */
static void
FileOutputStreamFlush(FileOutputStream *file)
{
	StringInfo fileBuffer = file->fileBuffer;

	errno = 0;
	int written = FileWriteCompat(&file->fileCompat, fileBuffer->data, fileBuffer->len,
								  PG_WAIT_IO);
	if (written != fileBuffer->len)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write %d bytes to partition file \"%s\"",
							   fileBuffer->len, file->filePath->data)));
	}
}


/*
 * FilterAndPartitionTable executes a given SQL query, and iterates over query
 * results in a read-only fashion. For each resulting row, the function applies
 * the partitioning function and determines the partition identifier. Then, the
 * function chooses the partition file corresponding to this identifier, and
 * serializes the row into this file using the copy command's text format.
 */
static void
FilterAndPartitionTable(const char *filterQuery,
						const char *partitionColumnName, Oid partitionColumnType,
						uint32 (*PartitionIdFunction)(Datum, Oid, const void *),
						const void *partitionIdContext,
						FileOutputStream *partitionFileArray,
						uint32 fileCount)
{
	FmgrInfo *columnOutputFunctions = NULL;
	int partitionColumnIndex = 0;
	Oid partitionColumnTypeId = InvalidOid;
	Oid partitionColumnCollation = InvalidOid;

	const char *noPortalName = NULL;
	const bool readOnly = true;
	const bool fetchForward = true;
	const int noCursorOptions = 0;
	const int prefetchCount = ROW_PREFETCH_COUNT;

	int connected = SPI_connect();
	if (connected != SPI_OK_CONNECT)
	{
		ereport(ERROR, (errmsg("could not connect to SPI manager")));
	}

	Portal queryPortal = SPI_cursor_open_with_args(noPortalName, filterQuery,
												   0, NULL, NULL, NULL, /* no arguments */
												   readOnly, noCursorOptions);
	if (queryPortal == NULL)
	{
		ereport(ERROR, (errmsg("could not open implicit cursor for query \"%s\"",
							   ApplyLogRedaction(filterQuery))));
	}

	CopyOutState rowOutputState = InitRowOutputState();

	SPI_cursor_fetch(queryPortal, fetchForward, prefetchCount);
	if (SPI_processed > 0)
	{
		TupleDesc rowDescriptor = SPI_tuptable->tupdesc;

		if (fileCount == 0)
		{
			ereport(ERROR, (errmsg("no partition to read into")));
		}

		partitionColumnIndex = ColumnIndex(rowDescriptor, partitionColumnName);
		partitionColumnTypeId = SPI_gettypeid(rowDescriptor, partitionColumnIndex);
		partitionColumnCollation = TupleDescAttr(rowDescriptor, partitionColumnIndex -
												 1)->attcollation;

		if (partitionColumnType != partitionColumnTypeId)
		{
			ereport(ERROR, (errmsg("partition column types %u and %u do not match",
								   partitionColumnTypeId, partitionColumnType)));
		}

		columnOutputFunctions = ColumnOutputFunctions(rowDescriptor,
													  rowOutputState->binary);
	}

	if (BinaryWorkerCopyFormat)
	{
		OutputBinaryHeaders(partitionFileArray, fileCount);
	}

	uint32 columnCount = (uint32) SPI_tuptable->tupdesc->natts;
	Datum *valueArray = (Datum *) palloc0(columnCount * sizeof(Datum));
	bool *isNullArray = (bool *) palloc0(columnCount * sizeof(bool));

	while (SPI_processed > 0)
	{
		for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
		{
			HeapTuple row = SPI_tuptable->vals[rowIndex];
			TupleDesc rowDescriptor = SPI_tuptable->tupdesc;
			bool partitionKeyNull = false;
			uint32 partitionId = 0;

			Datum partitionKey = SPI_getbinval(row, rowDescriptor,
											   partitionColumnIndex, &partitionKeyNull);

			/*
			 * If we have a partition key, we compute its bucket. Else if we have
			 * a null key, we then put this tuple into the 0th bucket. Note that
			 * the 0th bucket may hold other tuples as well, such as tuples whose
			 * partition keys hash to the value 0.
			 */
			if (!partitionKeyNull)
			{
				partitionId = (*PartitionIdFunction)(partitionKey,
													 partitionColumnCollation,
													 partitionIdContext);
				if (partitionId == INVALID_SHARD_INDEX)
				{
					ereport(ERROR, (errmsg("invalid distribution column value")));
				}
			}
			else
			{
				partitionId = 0;
			}

			/* deconstruct the tuple; this is faster than repeated heap_getattr */
			heap_deform_tuple(row, rowDescriptor, valueArray, isNullArray);

			AppendCopyRowData(valueArray, isNullArray, rowDescriptor,
							  rowOutputState, columnOutputFunctions, NULL);

			StringInfo rowText = rowOutputState->fe_msgbuf;

			FileOutputStream *partitionFile = &partitionFileArray[partitionId];
			FileOutputStreamWrite(partitionFile, rowText);

			resetStringInfo(rowText);
			MemoryContextReset(rowOutputState->rowcontext);
		}

		SPI_freetuptable(SPI_tuptable);

		SPI_cursor_fetch(queryPortal, fetchForward, prefetchCount);
	}

	pfree(valueArray);
	pfree(isNullArray);

	SPI_cursor_close(queryPortal);

	if (BinaryWorkerCopyFormat)
	{
		OutputBinaryFooters(partitionFileArray, fileCount);
	}

	/* delete row output memory context */
	ClearRowOutputState(rowOutputState);

	int finished = SPI_finish();
	if (finished != SPI_OK_FINISH)
	{
		ereport(ERROR, (errmsg("could not disconnect from SPI manager")));
	}
}


/*
 * Determines the column number for the given column name. The column number
 * count starts at 1.
 */
static int
ColumnIndex(TupleDesc rowDescriptor, const char *columnName)
{
	int columnIndex = SPI_fnumber(rowDescriptor, columnName);
	if (columnIndex == SPI_ERROR_NOATTRIBUTE)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
						errmsg("could not find column name \"%s\"", columnName)));
	}

	Assert(columnIndex >= 1);
	return columnIndex;
}


/*
 * InitRowOutputState creates and initializes a copy state object. This object
 * is internal to the copy command's implementation in Postgres; and we refactor
 * and refer to it here to avoid code duplication. We also only initialize the
 * fields needed for writing row data to text files, and skip the other fields.
 *
 * Note that the default field values used in commands/copy.c and this function
 * must match one another. Therefore, any changes to the default values in the
 * copy command must be propagated to this function.
 */
static CopyOutState
InitRowOutputState(void)
{
	CopyOutState rowOutputState = (CopyOutState) palloc0(sizeof(CopyOutStateData));

	int fileEncoding = pg_get_client_encoding();
	int databaseEncoding = GetDatabaseEncoding();
	int databaseEncodingMaxLength = pg_database_encoding_max_length();

	/* initialize defaults for printing null values */
	char *nullPrint = pstrdup("\\N");
	int nullPrintLen = strlen(nullPrint);
	char *nullPrintClient = pg_server_to_any(nullPrint, nullPrintLen, fileEncoding);

	/* set default text output characters */
	rowOutputState->null_print = nullPrint;
	rowOutputState->null_print_client = nullPrintClient;
	rowOutputState->delim = pstrdup("\t");

	rowOutputState->binary = BinaryWorkerCopyFormat;

	/* set encoding conversion information */
	rowOutputState->file_encoding = fileEncoding;

	if (PG_ENCODING_IS_CLIENT_ONLY(fileEncoding))
	{
		ereport(ERROR, (errmsg("cannot repartition into encoding caller cannot "
							   "receive")));
	}

	/* set up transcoding information and default text output characters */
	if ((fileEncoding != databaseEncoding) || (databaseEncodingMaxLength > 1))
	{
		rowOutputState->need_transcoding = true;
	}
	else
	{
		rowOutputState->need_transcoding = false;
	}

	/*
	 * Create a temporary memory context that we can reset once per row to
	 * recover palloc'd memory. This avoids any problems with leaks inside data
	 * type output routines, and should be faster than retail pfree's anyway.
	 */
	rowOutputState->rowcontext = AllocSetContextCreateExtended(CurrentMemoryContext,
															   "WorkerRowOutputContext",
															   ALLOCSET_DEFAULT_MINSIZE,
															   ALLOCSET_DEFAULT_INITSIZE,
															   ALLOCSET_DEFAULT_MAXSIZE);

	/* allocate the message buffer to use for serializing a row */
	rowOutputState->fe_msgbuf = makeStringInfo();

	return rowOutputState;
}


/* Clears copy state used for outputting row data. */
static void
ClearRowOutputState(CopyOutState rowOutputState)
{
	Assert(rowOutputState != NULL);

	MemoryContextDelete(rowOutputState->rowcontext);

	FreeStringInfo(rowOutputState->fe_msgbuf);

	pfree(rowOutputState->null_print_client);
	pfree(rowOutputState->delim);

	pfree(rowOutputState);
}


/*
 * Write the header of postgres' binary serialization format to each partition file.
 * This function is used when binary_worker_copy_format is enabled.
 */
static void
OutputBinaryHeaders(FileOutputStream *partitionFileArray, uint32 fileCount)
{
	for (uint32 fileIndex = 0; fileIndex < fileCount; fileIndex++)
	{
		/* Generate header for a binary copy */
		FileOutputStream partitionFile = { };
		CopyOutStateData headerOutputStateData;
		CopyOutState headerOutputState = (CopyOutState) & headerOutputStateData;

		memset(headerOutputState, 0, sizeof(CopyOutStateData));
		headerOutputState->fe_msgbuf = makeStringInfo();

		AppendCopyBinaryHeaders(headerOutputState);

		partitionFile = partitionFileArray[fileIndex];
		FileOutputStreamWrite(&partitionFile, headerOutputState->fe_msgbuf);
	}
}


/*
 * Write the footer of postgres' binary serialization format to each partition file.
 * This function is used when binary_worker_copy_format is enabled.
 */
static void
OutputBinaryFooters(FileOutputStream *partitionFileArray, uint32 fileCount)
{
	for (uint32 fileIndex = 0; fileIndex < fileCount; fileIndex++)
	{
		/* Generate footer for a binary copy */
		FileOutputStream partitionFile = { };
		CopyOutStateData footerOutputStateData;
		CopyOutState footerOutputState = (CopyOutState) & footerOutputStateData;

		memset(footerOutputState, 0, sizeof(CopyOutStateData));
		footerOutputState->fe_msgbuf = makeStringInfo();

		AppendCopyBinaryFooters(footerOutputState);

		partitionFile = partitionFileArray[fileIndex];
		FileOutputStreamWrite(&partitionFile, footerOutputState->fe_msgbuf);
	}
}


/*
 * RangePartitionId determines the partition number for the given data value
 * by applying range partitioning. More specifically, the function takes in a
 * data value and an array of sorted split points, and performs a binary search
 * within that array to determine the bucket the data value falls into. The
 * function then returns that bucket number.
 *
 * Note that we employ a version of binary search known as upper_bound; this
 * ensures that all null values fall into the zeroth bucket and that we maintain
 * full compatibility with the semantics of Hadoop's TotalOrderPartitioner.
 */
static uint32
RangePartitionId(Datum partitionValue, Oid partitionCollation, const void *context)
{
	RangePartitionContext *rangePartitionContext = (RangePartitionContext *) context;
	FmgrInfo *comparisonFunction = rangePartitionContext->comparisonFunction;
	Datum *pointArray = rangePartitionContext->splitPointArray;
	int32 currentLength = rangePartitionContext->splitPointCount;
	int32 halfLength = 0;
	uint32 firstIndex = 0;

	/*
	 * We implement a binary search variant known as upper_bound. This variant
	 * gives us the semantics we need for partitioned joins; and is also used by
	 * Hadoop's TotalOrderPartitioner. To implement this variant, we rely on SGI
	 * STL v3.3's source code for upper_bound(). Note that elements in the point
	 * array cannot be null.
	 */
	while (currentLength > 0)
	{
		halfLength = currentLength >> 1;
		uint32 middleIndex = firstIndex;
		middleIndex += halfLength;

		Datum middlePoint = pointArray[middleIndex];

		Datum comparisonDatum =
			FunctionCall2Coll(comparisonFunction, partitionCollation,
							  partitionValue, middlePoint);
		int comparisonResult = DatumGetInt32(comparisonDatum);

		/* if partition value is less than middle point */
		if (comparisonResult < 0)
		{
			currentLength = halfLength;
		}
		else
		{
			firstIndex = middleIndex;
			firstIndex++;
			currentLength = currentLength - halfLength - 1;
		}
	}

	return firstIndex;
}


/*
 * HashPartitionId determines the partition number for the given data value
 * using hash partitioning. More specifically, the function returns zero if the
 * given data value is null. If not, the function follows the exact same approach
 * as Citus distributed planner uses.
 *
 * partitionCollation is unused, as we do not support non deterministic collations
 * for hash distributed tables.
 */
static uint32
HashPartitionId(Datum partitionValue, Oid partitionCollation, const void *context)
{
	HashPartitionContext *hashPartitionContext = (HashPartitionContext *) context;
	FmgrInfo *hashFunction = hashPartitionContext->hashFunction;
	uint32 partitionCount = hashPartitionContext->partitionCount;
	ShardInterval **syntheticShardIntervalArray =
		hashPartitionContext->syntheticShardIntervalArray;
	FmgrInfo *comparisonFunction = hashPartitionContext->comparisonFunction;
	Datum hashDatum = FunctionCall1Coll(hashFunction, DEFAULT_COLLATION_OID,
										partitionValue);
	int32 hashResult = 0;
	uint32 hashPartitionId = 0;

	if (hashDatum == 0)
	{
		return hashPartitionId;
	}

	if (hashPartitionContext->hasUniformHashDistribution)
	{
		uint64 hashTokenIncrement = HASH_TOKEN_COUNT / partitionCount;

		hashResult = DatumGetInt32(hashDatum);
		hashPartitionId = (uint32) (hashResult - INT32_MIN) / hashTokenIncrement;
	}
	else
	{
		hashPartitionId =
			SearchCachedShardInterval(hashDatum, syntheticShardIntervalArray,
									  partitionCount, InvalidOid,
									  comparisonFunction);
	}


	return hashPartitionId;
}
