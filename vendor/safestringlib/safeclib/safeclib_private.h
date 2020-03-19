/*------------------------------------------------------------------
 * safeclib_private.h - Internal library references
 *
 * 2012, Jonathan Toppins <jtoppins@users.sourceforge.net>
 *
 * Copyright (c) 2012, 2013 by Cisco Systems, Inc
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

#ifndef __SAFECLIB_PRIVATE_H__
#define __SAFECLIB_PRIVATE_H__

#ifdef __KERNEL__
/* linux kernel environment */

#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/ctype.h>

#define RCNEGATE(x)  ( -(x) )

#define slprintf(...) printk(KERN_EMERG __VA_ARGS__)
#define slabort()
#ifdef DEBUG
#define sldebug_printf(...) printk(KERN_DEBUG __VA_ARGS__)
#endif

#else  /* !__KERNEL__ */

#if HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#ifdef STDC_HEADERS
# include <ctype.h>
# include <stdlib.h>
# include <stddef.h>
#else
# ifdef HAVE_STDLIB_H
#  include <stdlib.h>
# endif
#endif
#ifdef HAVE_STRING_H
# if !defined STDC_HEADERS && defined HAVE_MEMORY_H
#  include <memory.h>
# endif
# include <string.h>
#endif
#ifdef HAVE_LIMITS_H
# include <limits.h>
#endif

#define EXPORT_SYMBOL(sym)
#define RCNEGATE(x)  (x)

#define slprintf(...) fprintf(stderr, __VA_ARGS__)
#define slabort()     abort()
#ifdef DEBUG
#define sldebug_printf(...) printf(__VA_ARGS__)
#endif

#endif /* __KERNEL__ */

#ifndef sldebug_printf
#define sldebug_printf(...)
#endif

#include "safe_lib.h"

#endif /* __SAFECLIB_PRIVATE_H__ */
