/*------------------------------------------------------------------
 * strprefix_s.c
 *
 * November 2008, Bo Berry
 *
 * Copyright (c) 2008-2011 by Cisco Systems, Inc
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
#include "safe_str_constraint.h"
#include "safe_str_lib.h"


/**
 * NAME
 *    strprefix_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    errno_t
 *    strprefix_s(const char *dest, rsize_t dmax, const char *src)
 *
 * DESCRIPTION
 *    Determines if the prefix pointed to by src is at the
 *    beginning of string pointed to by dest.  The prefix
 *    must be a complete match in dest.  Useful for command
 *    or user input parsing.  The scanning stops at the first
 *    null in dest or src, or after dmax characters.
 *
 * EXTENSION TO
 *    ISO/IEC TR 24731-1, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest       pointer to string to compare against
 *
 *    dmax       restricted maximum length of dest
 *
 *    src        pointer to the prefix
 *
 * OUTPUT PARAMETERS
 *    none
 *
 * RUNTIME CONSTRAINTS
 *    Neither dest nor src shall be a null pointer.
 *    dmax shall not equal zero.
 *    dmax shall not be greater than RSIZE_MAX_STR.
 *
 * RETURN VALUE
 *    EOK        successful operation, prefix present in dest
 *    ESNULLP    NULL pointer
 *    ESZEROL    zero length
 *    ESLEMAX    length exceeds max limit
 *    ESNOTFND   prefix not found in dest
 *
 * ALSO SEE
 *    strspn_s(), strcspn_s(), strpbrk_s(), strstr_s()
 *
 */
errno_t
strprefix_s (const char *dest, rsize_t dmax, const char *src)
{
    if (dest == NULL) {
        invoke_safe_str_constraint_handler("strprefix_s: dest is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }

    if (src == NULL) {
        invoke_safe_str_constraint_handler("strprefix_s: src is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }

    if (dmax == 0) {
        invoke_safe_str_constraint_handler("strprefix_s: dmax is 0",
                   NULL, ESZEROL);
        return (ESZEROL);
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strprefix_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return (ESLEMAX);
    }

    if (*src == '\0') {
        return (ESNOTFND);
    }

    while (*src && dmax) {

        if (*dest != *src) {
            return (ESNOTFND);
        }

        dmax--;
        dest++;
        src++;
    }

    return (EOK);
}
EXPORT_SYMBOL(strprefix_s)
