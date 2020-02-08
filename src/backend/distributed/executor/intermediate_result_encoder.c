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
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "storage/fd.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


static void DatumFileEncoderReceive(IntermediateResultEncoder *encoder,
									Datum *values, bool *nulls);
static void SerializeSingleDatum(StringInfo datumBuffer, Datum datum,
								 bool datumTypeByValue, int datumTypeLength,
								 char datumTypeAlign);
static void ReadDatumFileIntoTupleStore(char *fileName, TupleDesc tupleDescriptor,
										Tuplestorestate *tupstore);
static void ReadCopyFileIntoTupleStore(char *fileName, char *copyFormat,
									   TupleDesc tupleDescriptor,
									   Tuplestorestate *tupstore);
static Relation StubRelation(TupleDesc tupleDescriptor);

IntermediateResultEncoder *
IntermediateResultEncoderCreate(TupleDesc tupleDesc, IntermediateResultFormat format)
{
	IntermediateResultEncoder *encoder = palloc0(sizeof(IntermediateResultEncoder));
	encoder->format = format;
	encoder->tupleDescriptor = tupleDesc;
	encoder->outputBuffer = makeStringInfo();

	if (format == TEXT_COPY_FORMAT || format == BINARY_COPY_FORMAT)
	{
		CopyOutState copyOutState = palloc0(sizeof(CopyOutStateData));
		copyOutState->delim = "\t";
		copyOutState->null_print = "\\N";
		copyOutState->null_print_client = "\\N";
		copyOutState->binary = (format == BINARY_COPY_FORMAT);
		copyOutState->fe_msgbuf = encoder->outputBuffer;
		copyOutState->rowcontext = CurrentMemoryContext; /* todo */

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


void
IntermediateResultEncoderReceive(IntermediateResultEncoder *encoder,
								 Datum *values, bool *nulls)
{
	if (encoder->format == TUPLE_DUMP_FORMAT)
	{
		DatumFileEncoderReceive(encoder, values, nulls);
	}
	else
	{
		AppendCopyRowData(values, nulls, encoder->tupleDescriptor,
						  encoder->copyOutState, encoder->columnOutputFunctions, NULL);
	}
}


static void
DatumFileEncoderReceive(IntermediateResultEncoder *encoder,
						Datum *values, bool *nulls)
{
	StringInfo buffer = encoder->outputBuffer;
	TupleDesc tupleDescriptor = encoder->tupleDescriptor;

	/* place holder for tuple size so we fill it later */
	int sizePos = buffer->len;
	int size = 0;
	appendBinaryStringInfo(buffer, (char *) &size, sizeof(size));

	for (int columnIndex = 0; columnIndex < tupleDescriptor->natts;)
	{
		unsigned char bitarray = 0;
		for (int bitIndex = 0; bitIndex < 8 && columnIndex < tupleDescriptor->natts;
			 bitIndex++, columnIndex++)
		{
			if (nulls[columnIndex])
			{
				bitarray |= (1 << bitIndex);
			}
		}

		appendBinaryStringInfo(buffer, (char *) &bitarray, 1);
	}

	/* serialize tuple ... */
	for (int columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
	{
		if (nulls[columnIndex])
		{
			continue;
		}

		Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, columnIndex);
		SerializeSingleDatum(buffer, values[columnIndex],
							 attributeForm->attbyval, attributeForm->attlen,
							 attributeForm->attalign);
	}

	/* fill in the correct size */
	size = buffer->len - sizePos - sizeof(size);
	memcpy(buffer->data + sizePos, &size, sizeof(size));
}


void
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
}


/*
 * SerializeSingleDatum serializes the given datum value and appends it to the
 * provided string info buffer.
 *
 * (taken from cstore_fdw)
 */
static void
SerializeSingleDatum(StringInfo datumBuffer, Datum datum, bool datumTypeByValue,
					 int datumTypeLength, char datumTypeAlign)
{
	uint32 datumLength = att_addlength_datum(0, datumTypeLength, datum);
	uint32 datumLengthAligned = att_align_nominal(datumLength, datumTypeAlign);

	enlargeStringInfo(datumBuffer, datumBuffer->len + datumLengthAligned + 1);

	char *currentDatumDataPointer = datumBuffer->data + datumBuffer->len;

	if (datumTypeLength > 0)
	{
		if (datumTypeByValue)
		{
			store_att_byval(currentDatumDataPointer, datum, datumTypeLength);
		}
		else
		{
			memcpy(currentDatumDataPointer, DatumGetPointer(datum), datumTypeLength);
		}
	}
	else
	{
		Assert(!datumTypeByValue);
		memcpy(currentDatumDataPointer, DatumGetPointer(datum), datumLength);
	}

	datumBuffer->len += datumLengthAligned;
}


void
ReadFileIntoTupleStore(char *fileName, IntermediateResultFormat format,
					   TupleDesc tupleDescriptor, Tuplestorestate *tupstore)
{
	if (format == TUPLE_DUMP_FORMAT)
	{
		ReadDatumFileIntoTupleStore(fileName, tupleDescriptor, tupstore);
	}
	else
	{
		char *copyFormat = (format == TEXT_COPY_FORMAT) ? "text" : "binary";
		ReadCopyFileIntoTupleStore(fileName, copyFormat, tupleDescriptor, tupstore);
	}
}


/*
 * ReadFileIntoTupleStore parses the records in a COPY-formatted file according
 * according to the given tuple descriptor and stores the records in a tuple
 * store.
 */
static void
ReadDatumFileIntoTupleStore(char *fileName, TupleDesc tupleDescriptor,
							Tuplestorestate *tupstore)
{
	EState *executorState = CreateExecutorState();
	MemoryContext executorTupleContext = GetPerTupleMemoryContext(executorState);

	int columnCount = tupleDescriptor->natts;
	Datum *columnValues = palloc0(columnCount * sizeof(Datum));
	bool *columnNulls = palloc0(columnCount * sizeof(bool));

	const int fileFlags = (O_RDONLY | PG_BINARY);
	const int fileMode = 0;
	StringInfo buffer = makeStringInfo();

	/* we currently do not check if the caller has permissions for this file */
	File fileDesc = FileOpenForTransmit(fileName, fileFlags, fileMode);
	FileCompat fileCompat = FileCompatFromFileStart(fileDesc);

	int tupleSize = 0;

	while (FileReadCompat(&fileCompat, (char *) &tupleSize, sizeof(tupleSize),
						  PG_WAIT_IO) == sizeof(tupleSize))
	{
		ResetPerTupleExprContext(executorState);
		MemoryContext oldContext = MemoryContextSwitchTo(executorTupleContext);

		enlargeStringInfo(buffer, tupleSize);
		FileReadCompat(&fileCompat, buffer->data, tupleSize, PG_WAIT_IO);
		uint32 currentDatumDataOffset = 0;

		for (int columnIndex = 0; columnIndex < tupleDescriptor->natts;)
		{
			unsigned char bitarray = buffer->data[currentDatumDataOffset++];
			for (int bitIndex = 0; bitIndex < 8 && columnIndex < tupleDescriptor->natts;
				 bitIndex++, columnIndex++)
			{
				if (bitarray & (1 << bitIndex))
				{
					columnNulls[columnIndex] = true;
				}
				else
				{
					columnNulls[columnIndex] = false;
				}
			}
		}

		for (int columnIndex = 0; columnIndex < tupleDescriptor->natts; columnIndex++)
		{
			if (columnNulls[columnIndex])
			{
				continue;
			}

			Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, columnIndex);
			char *currentDatumDataPointer = buffer->data + currentDatumDataOffset;

			columnValues[columnIndex] = fetch_att(currentDatumDataPointer,
												  attributeForm->attbyval,
												  attributeForm->attlen);

			currentDatumDataOffset = att_addlength_datum(currentDatumDataOffset,
														 attributeForm->attlen,
														 currentDatumDataPointer);
			currentDatumDataOffset = att_align_nominal(currentDatumDataOffset,
													   attributeForm->attalign);
		}

		tuplestore_putvalues(tupstore, tupleDescriptor, columnValues, columnNulls);
		MemoryContextSwitchTo(oldContext);
	}

	pfree(columnValues);
	pfree(columnNulls);
}


/*
 * ReadFileIntoTupleStore parses the records in a COPY-formatted file according
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
