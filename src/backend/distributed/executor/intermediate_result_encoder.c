/*-------------------------------------------------------------------------
 *
 * intermediate_result_encoder.c
 *   Functions for encoding and decoding intermediate results.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */
#include <sys/stat.h>
#include <unistd.h>

#include "postgres.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "catalog/pg_enum.h"
#include "commands/copy.h"
#include "distributed/commands/multi_copy.h"
#include "distributed/connection_management.h"
#include "distributed/intermediate_results.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_client_executor.h"
#include "distributed/multi_executor.h"
#include "distributed/remote_commands.h"
#include "distributed/transmit.h"
#include "distributed/transaction_identifier.h"
#include "distributed/tuplestore.h"
#include "distributed/version_compat.h"
#include "distributed/worker_protocol.h"
#include "mb/pg_wchar.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


#define ENCODER_BUFFER_SIZE (4 * 1024 * 1024)

/* internal state of intermediate result file encoder */
struct IntermediateResultEncoder
{
	/*
	 * Result of encoding is accumulated in outputBuffer, until it is flushed.
	 * The flush is done when the data is returned as result of
	 * IntermediateResultEncoderReceive() and IntermediateResultEncoderDone()
	 * functions.
	 */
	StringInfo outputBuffer;

	/*
	 * Used for returning the flushed result of encoding, at which time move the
	 * data pointer from outputBuffer to flushBuffer before reseting length of
	 * outputBuffer.
	 *
	 * This is kept here to avoid allocating it everytime we need to flush some data.
	 */
	StringInfo flushBuffer;

	IntermediateResultFormat format;
	TupleDesc tupleDescriptor;

	/* used when format is *_COPY_FORMAT */
	CopyOutState copyOutState;
	FmgrInfo *columnOutputFunctions;
};

/* forward declaration of local functions */
static void ReadCopyFileIntoTupleStore(char *fileName, char *copyFormat,
									   TupleDesc tupleDescriptor,
									   Tuplestorestate *tupstore);
static Relation StubRelation(TupleDesc tupleDescriptor);


/*
 * IntermediateResultEncoderCreate returns an encoder which can encode tuples of
 * given tupleDesc form using the given format. Encoder possibly can add header
 * data at this stage.
 */
IntermediateResultEncoder *
IntermediateResultEncoderCreate(TupleDesc tupleDesc,
								IntermediateResultFormat format,
								MemoryContext tupleContext)
{
	IntermediateResultEncoder *encoder = palloc0(sizeof(IntermediateResultEncoder));
	encoder->format = format;
	encoder->tupleDescriptor = CreateTupleDescCopy(tupleDesc);
	encoder->outputBuffer = makeStringInfo();
	encoder->flushBuffer = makeStringInfo();

	if (format == TEXT_COPY_FORMAT || format == BINARY_COPY_FORMAT)
	{
		int fileEncoding = pg_get_client_encoding();
		int databaseEncoding = GetDatabaseEncoding();
		int databaseEncodingMaxLength = pg_database_encoding_max_length();

		char *nullPrint = "\\N";
		int nullPrintLen = strlen(nullPrint);
		char *nullPrintClient = pg_server_to_any(nullPrint, nullPrintLen, fileEncoding);

		CopyOutState copyOutState = palloc0(sizeof(CopyOutStateData));
		copyOutState->delim = "\t";
		copyOutState->null_print = nullPrint;
		copyOutState->null_print_client = nullPrintClient;
		copyOutState->binary = (format == BINARY_COPY_FORMAT);
		copyOutState->fe_msgbuf = encoder->outputBuffer;
		copyOutState->rowcontext = tupleContext;

		if (PG_ENCODING_IS_CLIENT_ONLY(fileEncoding))
		{
			ereport(ERROR, (errmsg("cannot repartition into encoding caller "
								   "cannot receive")));
		}

		/* set up transcoding information and default text output characters */
		copyOutState->need_transcoding = (fileEncoding != databaseEncoding) ||
										 (databaseEncodingMaxLength > 1);

		encoder->copyOutState = copyOutState;
		encoder->columnOutputFunctions =
			ColumnOutputFunctions(tupleDesc, copyOutState->binary);

		if (copyOutState->binary)
		{
			AppendCopyBinaryHeaders(copyOutState);
		}
	}

	return encoder;
}


/*
 * IntermediateResultEncoderReceive encodes the next row with the given encoder.
 */
StringInfo
IntermediateResultEncoderReceive(IntermediateResultEncoder *encoder,
								 Datum *values, bool *nulls)
{
	if (encoder->format == TEXT_COPY_FORMAT ||
		encoder->format == BINARY_COPY_FORMAT)
	{
		AppendCopyRowData(values, nulls, encoder->tupleDescriptor,
						  encoder->copyOutState, encoder->columnOutputFunctions, NULL);
	}

	if (encoder->outputBuffer->len > ENCODER_BUFFER_SIZE)
	{
		encoder->flushBuffer->data = encoder->outputBuffer->data;
		encoder->flushBuffer->len = encoder->outputBuffer->len;

		encoder->outputBuffer->len = 0;

		return encoder->flushBuffer;
	}

	return NULL;
}


/*
 * IntermediateResultEncoderDone tells the encoder that there is no more work
 * to do. Encoder possibly can add footer data at this stage.
 */
StringInfo
IntermediateResultEncoderDone(IntermediateResultEncoder *encoder)
{
	if (encoder->format == TEXT_COPY_FORMAT ||
		encoder->format == BINARY_COPY_FORMAT)
	{
		CopyOutState copyOutState = encoder->copyOutState;
		if (copyOutState->binary)
		{
			AppendCopyBinaryFooters(copyOutState);
		}
	}

	if (encoder->outputBuffer->len > 0)
	{
		encoder->flushBuffer->data = encoder->outputBuffer->data;
		encoder->flushBuffer->len = encoder->outputBuffer->len;

		encoder->outputBuffer->len = 0;

		return encoder->flushBuffer;
	}

	return NULL;
}


/*
 * IntermediateResultEncoderDestroy cleans up resources used by the encoder.
 */
void
IntermediateResultEncoderDestroy(IntermediateResultEncoder *encoder)
{
	if (encoder->copyOutState != NULL)
	{
		pfree(encoder->copyOutState);
	}

	if (encoder->columnOutputFunctions != NULL)
	{
		pfree(encoder->columnOutputFunctions);
	}
}


/*
 * ReadFileIntoTupleStore parses the records in the given file using the
 * given format and puts the rows in the given tuple store.
 */
void
ReadFileIntoTupleStore(char *fileName, IntermediateResultFormat format,
					   TupleDesc tupleDescriptor, Tuplestorestate *tupstore)
{
	if (format == TEXT_COPY_FORMAT || format == BINARY_COPY_FORMAT)
	{
		char *copyFormat = (format == TEXT_COPY_FORMAT) ? "text" : "binary";
		ReadCopyFileIntoTupleStore(fileName, copyFormat, tupleDescriptor, tupstore);
	}
}


/*
 * ReadCopyFileIntoTupleStore parses the records in a COPY-formatted file
 * according to the given tuple descriptor and stores the records in a tuple
 * store.
 */
static void
ReadCopyFileIntoTupleStore(char *fileName, char *copyFormat,
						   TupleDesc tupleDescriptor,
						   Tuplestorestate *tupstore)
{
	/*
	 * Trick BeginCopyFrom into using our tuple descriptor by pretending it belongs
	 * to a relation.
	 */
	Relation stubRelation = StubRelation(tupleDescriptor);

	EState *executorState = CreateExecutorState();
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);
	ExprContext *executorExpressionContext = GetPerTupleExprContext(executorState);

	int columnCount = tupleDescriptor->natts;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));

	List *copyOptions = NIL;

	int location = -1; /* "unknown" token location */
	DefElem *copyOption = makeDefElem("format", (Node *) makeString(copyFormat),
									  location);
	copyOptions = lappend(copyOptions, copyOption);

	CopyState copyState = BeginCopyFrom(NULL, stubRelation, fileName, false, NULL,
										NULL, copyOptions);

	while (true)
	{
		ResetPerTupleExprContext(executorState);
		MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

		bool nextRowFound = NextCopyFromCompat(copyState, executorExpressionContext,
											   columnValues, columnNulls);
		if (!nextRowFound)
		{
			MemoryContextSwitchTo(oldContext);
			break;
		}

		tuplestore_putvalues(tupstore, tupleDescriptor, columnValues, columnNulls);
		MemoryContextSwitchTo(oldContext);
	}

	EndCopyFrom(copyState);
	pfree(columnValues);
	pfree(columnNulls);
}


/*
 * StubRelation creates a stub Relation from the given tuple descriptor.
 * To be able to use copy.c, we need a Relation descriptor. As there is no
 * relation corresponding to the data loaded from workers, we need to fake one.
 * We just need the bare minimal set of fields accessed by BeginCopyFrom().
 */
static Relation
StubRelation(TupleDesc tupleDescriptor)
{
	Relation stubRelation = palloc0(sizeof(RelationData));
	stubRelation->rd_att = tupleDescriptor;
	stubRelation->rd_rel = palloc0(sizeof(FormData_pg_class));
	stubRelation->rd_rel->relkind = RELKIND_RELATION;

	return stubRelation;
}


/*
 * ResultFileFormatForTupleDesc returns the most suitable encoding format for
 * the given tuple descriptor.
 */
IntermediateResultFormat
ResultFileFormatForTupleDesc(TupleDesc tupleDesc)
{
	if (CanUseBinaryCopyFormat(tupleDesc))
	{
		return BINARY_COPY_FORMAT;
	}
	else
	{
		return TEXT_COPY_FORMAT;
	}
}
