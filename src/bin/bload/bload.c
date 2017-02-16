/*-------------------------------------------------------------------------
 *
 * bload.c
 *
 * This is the zeromq client of bulkload copy. It pulls data from zeromq server
 * and outputs the message to stdout.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "distributed/bload.h"
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <zmq.h>

/* constant used in binary protocol */
static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";

int
main(int argc, char *argv[])
{
	FILE *fp = NULL;
	uint64_t buffer_size = BatchSize + MaxRecordSize + 1;
	char buf[buffer_size];
	char connstr[64];
	int rc;
	const int zero = 0;
	const short negative = -1;
	bool binary = false;

	/* variables for zeromq */
	void *context = NULL;
	void *receiver = NULL;
	void *controller = NULL;
	int port = 0;
	int nbytes;

	fp = fopen("/tmp/bload.log", "a");
	if (!fp)
	{
		fp = stderr;
	}
	if (argc < 3)
	{
		fprintf(fp, "Usage: %s host port [binary]\n", argv[0]);
		fflush(fp);
		fclose(fp);
		return 1;
	}

	if (argc == 4 && strcmp(argv[3], "binary") == 0)
	{
		binary = true;
	}

	context = zmq_ctx_new();

	/*	Socket to receive messages on */
	receiver = zmq_socket(context, ZMQ_PULL);
	port = atoi(argv[2]);
	sprintf(connstr, "tcp://%s:%d", argv[1], port);
	rc = zmq_connect(receiver, connstr);
	if (rc != 0)
	{
		fprintf(fp, "zmq_connect() error(%d): %s\n", errno, strerror(errno));
		fflush(fp);
		fclose(fp);
		zmq_close(receiver);
		zmq_ctx_destroy(context);
		return 1;
	}

	/* Socket to receive control message */
	controller = zmq_socket(context, ZMQ_SUB);
	sprintf(connstr, "tcp://%s:%d", argv[1], port + 1);
	rc = zmq_connect(controller, connstr);
	if (rc != 0)
	{
		fprintf(fp, "zmq_connect() error(%d): %s\n", errno, strerror(errno));
		fflush(fp);
		fclose(fp);
		zmq_close(receiver);
		zmq_close(controller);
		zmq_ctx_destroy(context);
		return 1;
	}
	zmq_setsockopt(controller, ZMQ_SUBSCRIBE, "", 0);

	zmq_pollitem_t items[] = {
		{ receiver, 0, ZMQ_POLLIN, 0 },
		{ controller, 0, ZMQ_POLLIN, 0 }
	};

	if (binary)
	{
		/* Signature */
		fwrite(BinarySignature, 1, 11, stdout);

		/* Flags field(no OIDs) */
		fwrite((void *) &zero, 1, 4, stdout);

		/* No header extenstion */
		fwrite((void *) &zero, 1, 4, stdout);
		fflush(stdout);
	}
	while (true)
	{
		/* wait indefinitely for an event to occur */
		rc = zmq_poll(items, 2, -1);

		if (rc == -1) /* error occurs */
		{
			fprintf(fp, "zmq_poll() error(%d): %s\n", errno, strerror(errno));
			fflush(fp);
			break;
		}
		if (items[0].revents & ZMQ_POLLIN) /* receive a message */
		{
			nbytes = zmq_recv(receiver, buf, buffer_size - 1, 0);
			if (nbytes == -1)
			{
				fprintf(fp, "zmq_recv() error(%d): %s\n", errno, strerror(errno));
				fflush(fp);
				break;
			}
			fwrite(buf, 1, nbytes, stdout);
			fflush(stdout);
		}
		if (items[1].revents & ZMQ_POLLIN) /* receive signal kill */
		{
			fprintf(fp, "receive signal kill, wait for exhausting all messages\n");
			fflush(fp);

			/* consume all messages before exit */
			while (true)
			{
				/* wait 100 milliseconds for an event to occur */
				rc = zmq_poll(items, 1, 100);
				if (rc == 0) /* no more messages */
				{
					break;
				}
				else if (rc == -1) /* error occurs */
				{
					fprintf(fp, "zmq_poll() error(%d): %s\n", errno, strerror(errno));
					fflush(fp);
					break;
				}
				if (items[0].revents & ZMQ_POLLIN) /* receive a message */
				{
					nbytes = zmq_recv(receiver, buf, buffer_size - 1, 0);
					if (nbytes == -1)
					{
						fprintf(fp, "zmq_recv() error(%d): %s\n", errno, strerror(errno));
						fflush(fp);
						break;
					}
					fwrite(buf, 1, nbytes, stdout);
					fflush(stdout);
				}
			}
			fprintf(fp, "we have consume all messages, exit now\n");
			fflush(fp);
			if (binary)
			{
				/* Binary footers */
				fwrite((void *) &negative, 1, 2, stdout);
				fflush(stdout);
			}
			break;
		}
	}
	zmq_close(receiver);
	zmq_close(controller);
	zmq_ctx_destroy(context);
	fclose(fp);
	return 0;
}
