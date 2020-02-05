/*------------------------------------------------------------------
 * strcmpfld_s.c
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
 *    strcmpfld_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    errno_t
 *    strcmpfld_s(const char *dest, rsize_t dmax,
 *                const char *src, int *indicator)
 *
 * DESCRIPTION
 *    Compares the character array pointed to by src to the character array
 *    pointed to by dest for dmax characters.  The null terminator does not
 *    stop the comparison.
 *
 * EXTENSION TO
 *    ISO/IEC TR 24731, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest       pointer to character array to compare against
 *
 *    dmax       restricted maximum length of dest.  The length is
 *                used for the comparison of src against dest.
 *
 *    src        pointer to the character array to be compared to dest
 *
 *    indicator  pointer to result indicator, greater than, equal
 *               to or less than 0, if the character array pointed
 *                to by dest is greater than, equal to or less
 *                than the character array pointed to by src.
 * OUTPUT
 *    indicator  updated result indicator
 *
 * RUNTIME CONSTRAINTS
 *    Neither dest nor src shall be a null pointer.
 *    indicator shall not be a null pointer.
 *    dmax shall not be 0
 *    dmax shall not be greater than RSIZE_MAX_STR
 *
 * RETURN VALUE
 *    indicator, when the return code is OK
 *        >0   dest greater than src
 *         0   strings the same
 *        <0   dest less than src
 *
 *    EOK
 *    ESNULLP     pointer was null
 *    ESZEROL     length was zero
 *    ESLEMAX     length exceeded max
 *
 * ALSO SEE
 *    strcpyfld_s(), strcpyfldin_s(), strcpyfldout_s()
 *
 */
errno_t
strcmpfld_s (const char *dest, rsize_t dmax,
             const char *src, int *indicator)
{
    if (indicator == NULL) {
        invoke_safe_str_constraint_handler("strcmpfld_s: indicator is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }
    *indicator = 0;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("strcmpfld_s: dest is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }

    if (src == NULL) {
        invoke_safe_str_constraint_handler("strcmpfld_s: src is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }

    if (dmax == 0) {
        invoke_safe_str_constraint_handler("strcmpfld_s: dmax is 0",
                   NULL, ESZEROL);
        return (ESZEROL);
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strcmpfld_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return (ESLEMAX);
    }

    /* compare for dmax charactrers, not the null! */
    while (dmax) {

        if (*dest != *src) {
            break;
        }

        dest++;
        src++;
        dmax--;
    }

    *indicator = *dest - *src;
    return (EOK);
}
EXPORT_SYMBOL(strcmpfld_s)
