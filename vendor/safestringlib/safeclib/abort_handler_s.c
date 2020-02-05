/*------------------------------------------------------------------
 * abort_handler_s.c
 *
 * 2012, Jonathan Toppins <jtoppins@users.sourceforge.net>
 *
 * Copyright (c) 2012 Cisco Systems
 * All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 *------------------------------------------------------------------
 */

#include "safeclib_private.h"

/**
 * NAME
 *    abort_handler_s
 *
 * SYNOPSIS
 *    #include "safe_lib.h"
 *    void abort_handler_s(const char *msg, void *ptr, errno_t error)
 *
 * DESCRIPTION
 *    This function writes a message on the standard error stream in
 *    an implementation-defined format. The message shall include the
 *    string pointed to by msg. The abort_handler_s function then calls
 *    the abort function.
 *
 * SPECIFIED IN
 *    ISO/IEC JTC1 SC22 WG14 N1172, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    msg       Pointer to the message describing the error
 *
 *    ptr       Pointer to aassociated data.  Can be NULL.
 *
 *    error     The error code encountered.
 *
 * RETURN VALUE
 *    Does not return to caller.
 *
 * ALSO SEE
 *    ignore_handler_s()
 *
 */

void abort_handler_s(const char *msg, void *ptr, errno_t error)
{
	slprintf("ABORT CONSTRAINT HANDLER: (%u) %s\n", error,
		 (msg) ? msg : "Null message");
	slabort();
}
EXPORT_SYMBOL(abort_handler_s)
