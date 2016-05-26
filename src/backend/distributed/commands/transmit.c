/*-------------------------------------------------------------------------
 *
 * transmit.c
 *	  Routines for transmitting regular files between two nodes.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "distributed/relay_utility.h"
#include "distributed/transmit.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "storage/fd.h"


/* Local functions forward declarations */
static File FileOpenForTransmit(const char *filename, int fileFlags, int fileMode);
static void SendCopyInStart(void);
static void SendCopyOutStart(void);
static void SendCopyDone(void);
static void SendCopyData(StringInfo fileBuffer);
static bool ReceiveCopyData(StringInfo copyData);


/*
 * RedirectCopyDataToRegularFile receives data from stdin using the standard copy
 * protocol. The function then creates or truncates a file with the given
 * filename, and appends received data to this file.
 */
void
RedirectCopyDataToRegularFile(const char *filename)
{
	StringInfo copyData = makeStringInfo();
	bool copyDone = false;
	File fileDesc = -1;
	const int fileFlags = (O_APPEND | O_CREAT | O_RDWR | O_TRUNC | PG_BINARY);
	const int fileMode = (S_IRUSR | S_IWUSR);

	fileDesc = FileOpenForTransmit(filename, fileFlags, fileMode);

	SendCopyInStart();

	copyDone = ReceiveCopyData(copyData);
	while (!copyDone)
	{
		/* if received data has contents, append to regular file */
		if (copyData->len > 0)
		{
			int appended = FileWrite(fileDesc, copyData->data, copyData->len);
			if (appended != copyData->len)
			{
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not append to received file: %m")));
			}
		}

		resetStringInfo(copyData);
		copyDone = ReceiveCopyData(copyData);
	}

	FreeStringInfo(copyData);
	FileClose(fileDesc);
}


/*
 * SendRegularFile reads data from the given file, and sends these data to
 * stdout using the standard copy protocol. After all file data are sent, the
 * function ends the copy protocol and closes the file.
 */
void
SendRegularFile(const char *filename)
{
	File fileDesc = -1;
	StringInfo fileBuffer = NULL;
	int readBytes = -1;
	const uint32 fileBufferSize = 32768; /* 32 KB */
	const int fileFlags = (O_RDONLY | PG_BINARY);
	const int fileMode = 0;

	/* we currently do not check if the caller has permissions for this file */
	fileDesc = FileOpenForTransmit(filename, fileFlags, fileMode);

	/*
	 * We read file's contents into buffers of 32 KB. This buffer size is twice
	 * as large as Hadoop's default buffer size, and may later be configurable.
	 */
	fileBuffer = makeStringInfo();
	enlargeStringInfo(fileBuffer, fileBufferSize);

	SendCopyOutStart();

	readBytes = FileRead(fileDesc, fileBuffer->data, fileBufferSize);
	while (readBytes > 0)
	{
		fileBuffer->len = readBytes;

		SendCopyData(fileBuffer);

		resetStringInfo(fileBuffer);
		readBytes = FileRead(fileDesc, fileBuffer->data, fileBufferSize);
	}

	SendCopyDone();

	FreeStringInfo(fileBuffer);
	FileClose(fileDesc);
}


/* Helper function that deallocates string info object. */
void
FreeStringInfo(StringInfo stringInfo)
{
	resetStringInfo(stringInfo);

	pfree(stringInfo->data);
	pfree(stringInfo);
}


/*
 * FileOpenForTransmit opens file with the given filename and flags. On success,
 * the function returns the internal file handle for the opened file. On failure
 * the function errors out.
 */
static File
FileOpenForTransmit(const char *filename, int fileFlags, int fileMode)
{
	File fileDesc = -1;
	int fileStated = -1;
	struct stat fileStat;

	fileStated = stat(filename, &fileStat);
	if (fileStated >= 0)
	{
		if (S_ISDIR(fileStat.st_mode))
		{
			ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
							errmsg("\"%s\" is a directory", filename)));
		}
	}

	fileDesc = PathNameOpenFile((char *) filename, fileFlags, fileMode);
	if (fileDesc < 0)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open file \"%s\": %m", filename)));
	}

	return fileDesc;
}


/*
 * SendCopyInStart sends the start copy in message to initiate receiving data
 * from stdin. The frontend should now send copy data.
 */
static void
SendCopyInStart(void)
{
	StringInfoData copyInStart = { NULL, 0, 0, 0 };
	const char copyFormat = 1; /* binary copy format */
	int flushed = 0;

	pq_beginmessage(&copyInStart, 'G');
	pq_sendbyte(&copyInStart, copyFormat);
	pq_sendint(&copyInStart, 0, 2);
	pq_endmessage(&copyInStart);

	/* flush here to ensure that FE knows it can send data */
	flushed = pq_flush();
	if (flushed != 0)
	{
		ereport(WARNING, (errmsg("could not flush copy start data")));
	}
}


/*
 * SendCopyOutStart sends the start copy out message to initiate sending data to
 * stdout. After this message, the backend will continue by sending copy data.
 */
static void
SendCopyOutStart(void)
{
	StringInfoData copyOutStart = { NULL, 0, 0, 0 };
	const char copyFormat = 1; /* binary copy format */

	pq_beginmessage(&copyOutStart, 'H');
	pq_sendbyte(&copyOutStart, copyFormat);
	pq_sendint(&copyOutStart, 0, 2);
	pq_endmessage(&copyOutStart);
}


/* Sends the copy-complete message. */
static void
SendCopyDone(void)
{
	StringInfoData copyDone = { NULL, 0, 0, 0 };
	int flushed = 0;

	pq_beginmessage(&copyDone, 'c');
	pq_endmessage(&copyDone);

	/* flush here to signal to FE that we are done */
	flushed = pq_flush();
	if (flushed != 0)
	{
		ereport(WARNING, (errmsg("could not flush copy start data")));
	}
}


/* Sends the copy data message to stdout. */
static void
SendCopyData(StringInfo fileBuffer)
{
	StringInfoData copyData = { NULL, 0, 0, 0 };

	pq_beginmessage(&copyData, 'd');
	pq_sendbytes(&copyData, fileBuffer->data, fileBuffer->len);
	pq_endmessage(&copyData);
}


/*
 * ReceiveCopyData receives one copy data message from stdin, and writes this
 * message's contents into the given argument. The function then checks if the
 * copy protocol has been completed, and if it has, the function returns true.
 * If not, the function returns false indicating there are more data to read.
 * If the received message does not conform to the copy protocol, the function
 * mirrors copy.c's error behavior.
 */
static bool
ReceiveCopyData(StringInfo copyData)
{
	int messageType = 0;
	int messageCopied = 0;
	bool copyDone = true;
	const int unlimitedSize = 0;

	HOLD_CANCEL_INTERRUPTS();
	pq_startmsgread();
	messageType = pq_getbyte();
	if (messageType == EOF)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("unexpected EOF on client connection")));
	}

	/* consume the rest of message before checking for message type */
	messageCopied = pq_getmessage(copyData, unlimitedSize);
	if (messageCopied == EOF)
	{
		ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
						errmsg("unexpected EOF on client connection")));
	}

	RESUME_CANCEL_INTERRUPTS();

	switch (messageType)
	{
		case 'd':       /* CopyData */
		{
			copyDone = false;
			break;
		}

		case 'c':       /* CopyDone */
		{
			copyDone = true;
			break;
		}

		case 'f':       /* CopyFail */
		{
			ereport(ERROR, (errcode(ERRCODE_QUERY_CANCELED),
							errmsg("COPY data failed: %s", pq_getmsgstring(copyData))));
			break;
		}

		case 'H':       /* Flush */
		case 'S':       /* Sync */
		{
			/*
			 * Ignore Flush/Sync for the convenience of client libraries (such
			 * as libpq) that may send those without noticing that the command
			 * they just sent was COPY.
			 */
			copyDone = false;
			break;
		}

		default:
		{
			ereport(ERROR, (errcode(ERRCODE_PROTOCOL_VIOLATION),
							errmsg("unexpected message type 0x%02X during COPY data",
								   messageType)));
			break;
		}
	}

	return copyDone;
}
