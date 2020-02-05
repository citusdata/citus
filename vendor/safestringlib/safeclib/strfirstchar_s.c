/*------------------------------------------------------------------
 * strfirstchar_s.c
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
 *    strfirstchar_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    errno_t
 *    strfirstchar_s(char *dest, rsize_t dmax, char c, char **first)
 *
 * DESCRIPTION
 *    This function returns a pointer to the first occurrence
 *    of character c in dest. The scanning stops at the first null
 *    or after dmax characters.
 *
 * EXTENSION TO
 *    ISO/IEC TR 24731, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest    pointer to string to compare against
 *
 *    dmax    restricted maximum length of string
 *
 *    c       character to locate
 *
 *    first   returned pointer to first occurrence of c
 *
 * OUTPUT PARAMETERS
 *    first   updated pointer to first occurrence of c
 *
 * RUNTIME CONSTRAINTS
 *    dest shall not be a null pointer.
 *    first shall not be a null pointer.
 *    dmax shall not be 0
 *    dmax shall not be greater than RSIZE_MAX_STR
 *
 * RETURN VALUE
 *    pointer to first occurence of c, NULL if not found
 *
 *    EOK         pointer to first occurrence is returned
 *    ESNULLP     NULL pointer
 *    ESZEROL     zero length
 *    ESLEMAX     length exceeds max limit
 *
 * ALSO SEE
 *    strlastchar_s(), strfirstdiff_s(), strfirstsame_s(),
 *    strlastdiff_s(), strlastsame_s(),
 *
 */
errno_t
strfirstchar_s (char *dest, rsize_t dmax, char c, char **first)
{

    if (first == NULL) {
        invoke_safe_str_constraint_handler("strfirstchar_s: index is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }
    *first = NULL;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("strfirstchar_s: dest is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }

    if (dmax == 0 ) {
        invoke_safe_str_constraint_handler("strfirstchar_s: dmax is 0",
                   NULL, ESZEROL);
        return (ESZEROL);
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strfirstchar_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return (ESLEMAX);
    }

    while (*dest && dmax) {

        if (*dest == c) {
            *first = dest;
            return (EOK);
        }
        dest++;
        dmax--;
    }

    return (ESNOTFND);
}
EXPORT_SYMBOL(strfirstchar_s)
