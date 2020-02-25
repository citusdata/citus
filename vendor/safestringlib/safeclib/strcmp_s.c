/*------------------------------------------------------------------
 * strcmp_s.c -- string compare
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
 *    strcmp_s
 *
 * Synpsos
 *    #include "safe_str_lib.h"
 *    errno_t
 *    strcmp_s(const char *dest, rsize_t dmax,
 *             const char *src, int *indicator)
 *
 * DESCRIPTION
 *    Compares string src to string dest.
 *
 * EXTENSION TO
 *    ISO/IEC JTC1 SC22 WG14 N1172, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest       pointer to string to compare against
 *
 *    dmax       restricted maximum length of string dest
 *
 *    src        pointer to the string to be compared to dest
 *
 *    indicator  pointer to result indicator, greater than,
 *               equal to or less than 0, if the string pointed
 *               to by dest is greater than, equal to or less
 *               than the string pointed to by src respectively.
 *
 * OUTPUT PARAMETERS
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
 *    strcasecmp_s()
 *
 */
errno_t
strcmp_s (const char *dest, rsize_t dmax,
          const char *src, int *indicator)
{
    if (indicator == NULL) {
        invoke_safe_str_constraint_handler("strcmp_s: indicator is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }
    *indicator = 0;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("strcmp_s: dest is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }

    if (src == NULL) {
        invoke_safe_str_constraint_handler("strcmp_s: src is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }

    if (dmax == 0) {
        invoke_safe_str_constraint_handler("strcmp_s: dmax is 0",
                   NULL, ESZEROL);
        return RCNEGATE(ESZEROL);
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strcmp_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return RCNEGATE(ESLEMAX);
    }

    while (*dest && *src && dmax) {

        if (*dest != *src) {
            break;
        }

        dest++;
        src++;
        dmax--;
    }

    *indicator = *dest - *src;
    return RCNEGATE(EOK);
}
EXPORT_SYMBOL(strcmp_s)
