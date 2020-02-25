/*------------------------------------------------------------------
 * safe_lib.h -- Safe C Library
 *
 * October 2008, Bo Berry
 * Modified 2012, Jonathan Toppins <jtoppins@users.sourceforge.net>
 *
 * Copyright (c) 2008-2013 by Cisco Systems, Inc
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

#ifndef __SAFE_LIB_H__
#define __SAFE_LIB_H__

#ifdef __cplusplus
extern "C" {
#endif

#include "safe_types.h"
#include "safe_lib_errno.h"

/* C11 appendix K types - specific for bounds checking */
typedef size_t  rsize_t;

/*
 * We depart from the standard and allow memory and string operations to
 * have different max sizes. See the respective safe_mem_lib.h or
 * safe_str_lib.h files.
 */
/* #define RSIZE_MAX (~(rsize_t)0)  - leave here for completeness */

typedef void (*constraint_handler_t) (const char * /* msg */,
                                      void *       /* ptr */,
                                      errno_t      /* error */);

extern void abort_handler_s(const char *msg, void *ptr, errno_t error);
extern void ignore_handler_s(const char *msg, void *ptr, errno_t error);

#define sl_default_handler ignore_handler_s

#include "safe_mem_lib.h"
#include "safe_str_lib.h"

#ifdef __cplusplus
}
#endif
#endif /* __SAFE_LIB_H__ */
