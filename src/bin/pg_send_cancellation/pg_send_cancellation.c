/*
 * pg_send_cancellation is a program for manually sending a cancellation
 * to a Postgres endpoint. It is effectively a command-line version of
 * PQcancel in libpq, but it can use any PID or cancellation key.
 *
 * Portions Copyright (c) Citus Data, Inc.
 *
 * For the internal_cancel function:
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Permission to use, copy, modify, and distribute this software and its
 * documentation for any purpose, without fee, and without a written agreement
 * is hereby granted, provided that the above copyright notice and this
 * paragraph and the following two paragraphs appear in all copies.
 *
 * IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
 * DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
 * LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS
 * DOCUMENTATION, EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY
 * AND FITNESS FOR A PARTICULAR PURPOSE.  THE SOFTWARE PROVIDED HEREUNDER IS
 * ON AN "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO
 * PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
 *
 */
#include "postgres_fe.h"

#include <sys/stat.h>
#include <fcntl.h>
#include <ctype.h>
#include <time.h>
#include <unistd.h>

#include "common/ip.h"
#include "common/link-canary.h"
#include "common/scram-common.h"
#include "common/string.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "mb/pg_wchar.h"
#include "port/pg_bswap.h"


#define ERROR_BUFFER_SIZE 256


static int internal_cancel(SockAddr *raddr, int be_pid, int be_key,
						   char *errbuf, int errbufsize);


/*
 * main entry point into the pg_send_cancellation program.
 */
int
main(int argc, char *argv[])
{
	if (argc == 2 && strcmp(argv[1], "-V") == 0)
	{
		pg_fprintf(stdout, "pg_send_cancellation (PostgreSQL) " PG_VERSION "\n");
		return 0;
	}

	if (argc < 4 || argc > 5)
	{
		char *program = argv[0];
		pg_fprintf(stderr, "%s requires 4 arguments\n\n", program);
		pg_fprintf(stderr, "Usage: %s <pid> <cancel key> <hostname> [port]\n", program);
		return 1;
	}

	char *pidString = argv[1];
	char *cancelKeyString = argv[2];
	char *host = argv[3];
	char *portString = "5432";

	if (argc >= 5)
	{
		portString = argv[4];
	}

	/* parse the PID and cancellation key */
	int pid = strtol(pidString, NULL, 10);
	int cancelAuthCode = strtol(cancelKeyString, NULL, 10);

	char errorBuffer[ERROR_BUFFER_SIZE] = { 0 };

	struct addrinfo *ipAddressList;
	struct addrinfo hint;
	int ipAddressListFamily = AF_UNSPEC;
	SockAddr socketAddress;

	memset(&hint, 0, sizeof(hint));
	hint.ai_socktype = SOCK_STREAM;
	hint.ai_family = ipAddressListFamily;

	/* resolve the hostname to an IP */
	int ret = pg_getaddrinfo_all(host, portString, &hint, &ipAddressList);
	if (ret || !ipAddressList)
	{
		pg_fprintf(stderr, "could not translate host name \"%s\" to address: %s\n",
				   host, gai_strerror(ret));
		return 1;
	}

	if (ipAddressList->ai_addrlen > sizeof(socketAddress.addr))
	{
		pg_fprintf(stderr, "invalid address length");
		return 1;
	}

	/*
	 * Explanation of IGNORE-BANNED:
	 * This is a common pattern when using getaddrinfo. The system guarantees
	 * that ai_addrlen < sizeof(socketAddress.addr). Out of an abundance of
	 * caution. We also check it above.
	 */
	memcpy(&socketAddress.addr, ipAddressList->ai_addr, ipAddressList->ai_addrlen); /* IGNORE-BANNED */
	socketAddress.salen = ipAddressList->ai_addrlen;

	/* send the cancellation */
	bool cancelSucceeded = internal_cancel(&socketAddress, pid, cancelAuthCode,
										   errorBuffer, sizeof(errorBuffer));
	if (!cancelSucceeded)
	{
		pg_fprintf(stderr, "sending cancellation to %s:%s failed: %s",
				   host, portString, errorBuffer);
		return 1;
	}

	pg_freeaddrinfo_all(ipAddressListFamily, ipAddressList);

	return 0;
}


/* *INDENT-OFF* */

/*
 * internal_cancel is copied from fe-connect.c
 *
 * The return value is true if the cancel request was successfully
 * dispatched, false if not (in which case an error message is available).
 * Note: successful dispatch is no guarantee that there will be any effect at
 * the backend.  The application must read the operation result as usual.
 *
 * CAUTION: we want this routine to be safely callable from a signal handler
 * (for example, an application might want to call it in a SIGINT handler).
 * This means we cannot use any C library routine that might be non-reentrant.
 * malloc/free are often non-reentrant, and anything that might call them is
 * just as dangerous.  We avoid sprintf here for that reason.  Building up
 * error messages with strcpy/strcat is tedious but should be quite safe.
 * We also save/restore errno in case the signal handler support doesn't.
 *
 * internal_cancel() is an internal helper function to make code-sharing
 * between the two versions of the cancel function possible.
 */
static int
internal_cancel(SockAddr *raddr, int be_pid, int be_key,
				char *errbuf, int errbufsize)
{
	int			save_errno = SOCK_ERRNO;
	pgsocket	tmpsock = PGINVALID_SOCKET;
	char		sebuf[PG_STRERROR_R_BUFLEN];
	int			maxlen;
	struct
	{
		uint32		packetlen;
		CancelRequestPacket cp;
	}			crp;

	/*
	 * We need to open a temporary connection to the postmaster. Do this with
	 * only kernel calls.
	 */
	if ((tmpsock = socket(raddr->addr.ss_family, SOCK_STREAM, 0)) == PGINVALID_SOCKET)
	{
		strlcpy(errbuf, "PQcancel() -- socket() failed: ", errbufsize);
		goto cancel_errReturn;
	}
retry3:
	if (connect(tmpsock, (struct sockaddr *) &raddr->addr, raddr->salen) < 0)
	{
		if (SOCK_ERRNO == EINTR)
			/* Interrupted system call - we'll just try again */
			goto retry3;
		strlcpy(errbuf, "PQcancel() -- connect() failed: ", errbufsize);
		goto cancel_errReturn;
	}

	/*
	 * We needn't set nonblocking I/O or NODELAY options here.
	 */

	/* Create and send the cancel request packet. */

	crp.packetlen = pg_hton32((uint32) sizeof(crp));
	crp.cp.cancelRequestCode = (MsgType) pg_hton32(CANCEL_REQUEST_CODE);
	crp.cp.backendPID = pg_hton32(be_pid);
	crp.cp.cancelAuthCode = pg_hton32(be_key);

retry4:
	if (send(tmpsock, (char *) &crp, sizeof(crp), 0) != (int) sizeof(crp))
	{
		if (SOCK_ERRNO == EINTR)
			/* Interrupted system call - we'll just try again */
			goto retry4;
		strlcpy(errbuf, "PQcancel() -- send() failed: ", errbufsize);
		goto cancel_errReturn;
	}

	/*
	 * Wait for the postmaster to close the connection, which indicates that
	 * it's processed the request.  Without this delay, we might issue another
	 * command only to find that our cancel zaps that command instead of the
	 * one we thought we were canceling.  Note we don't actually expect this
	 * read to obtain any data, we are just waiting for EOF to be signaled.
	 */
retry5:
	if (recv(tmpsock, (char *) &crp, 1, 0) < 0)
	{
		if (SOCK_ERRNO == EINTR)
			/* Interrupted system call - we'll just try again */
			goto retry5;
		/* we ignore other error conditions */
	}

	/* All done */
	closesocket(tmpsock);
	SOCK_ERRNO_SET(save_errno);
	return true;

cancel_errReturn:

	/*
	 * Make sure we don't overflow the error buffer. Leave space for the \n at
	 * the end, and for the terminating zero.
	 */
	maxlen = errbufsize - strlen(errbuf) - 2;
	if (maxlen >= 0)
	{
		/*
		 * Explanation of IGNORE-BANNED:
		 * This is well-tested libpq code that we would like to preserve in its
		 * original form. The appropriate length calculation is done above.
		 */
		strncat(errbuf, SOCK_STRERROR(SOCK_ERRNO, sebuf, sizeof(sebuf)), /* IGNORE-BANNED */
				maxlen);
		strcat(errbuf, "\n"); /* IGNORE-BANNED */
	}
	if (tmpsock != PGINVALID_SOCKET)
		closesocket(tmpsock);
	SOCK_ERRNO_SET(save_errno);
	return false;
}

/* *INDENT-ON* */
