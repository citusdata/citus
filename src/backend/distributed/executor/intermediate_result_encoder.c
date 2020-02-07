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


static void SerializeSingleDatum(StringInfo datumBuffer, Datum datum,
								 bool datumTypeByValue, int datumTypeLength,
								 char datumTypeAlign);

IntermediateResultEncoder *
IntermediateResultEncoderCreate(TupleDesc tupleDesc, IntermediateResultFormat format)
{
	IntermediateResultEncoder *encoder = palloc0(sizeof(IntermediateResultEncoder));
	encoder->format = format;
	encoder->tupleDescriptor = tupleDesc;
	encoder->outputBuffer = makeStringInfo();

	return encoder;
}


void
IntermediateResultEncoderReceive(IntermediateResultEncoder *encoder,
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
	/* todo */
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


/*
 * ReadFileIntoTupleStore parses the records in a COPY-formatted file according
 * according to the given tuple descriptor and stores the records in a tuple
 * store.
 */
void
ReadFileIntoTupleStore(char *fileName, IntermediateResultFormat format,
					   TupleDesc tupleDescriptor, Tuplestorestate *tupstore)
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
