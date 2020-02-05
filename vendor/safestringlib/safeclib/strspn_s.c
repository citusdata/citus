/*------------------------------------------------------------------
 * strspn_s.c
 *
 * November 2008, Bo Berry
 *
 * Copyright (c) 2008-2011, 2013 by Cisco Systems, Inc
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
 *    strspn_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    errno_t
 *    strspn_s(const char *dest, rsize_t dmax,
 *             const char *src,  rsize_t slen, rsize_t *count)
 *
 * DESCRIPTION
 *    This function computes the prefix length of the string
 *    pointed to by dest which consists entirely of characters
 *    that are included from the string pointed to by src.
 *
 * EXTENSION TO
 *    ISO/IEC TR 24731, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest     pointer to string to determine the prefix
 *
 *    dmax     restricted maximum length of string dest
 *
 *    src      pointer to exclusion string
 *
 *    slen     restricted maximum length of string src
 *
 *    count    pointer to a count variable that will be updated
 *              with the dest substring length
 *
 * OUTPUT PARAMETERS
 *    count    updated count
 *
 * RUNTIME CONSTRAINTS
 *    Neither dest nor src shall be a null pointer.
 *    count shall not be a null pointer.
 *    dmax shall not be 0
 *    dmax shall not be greater than RSIZE_MAX_STR
 *
 * RETURN VALUE
 *    EOK         count
 *    ESNULLP     NULL pointer
 *    ESZEROL     zero length
 *    ESLEMAX     length exceeds max limit
 *
 * ALSO SEE
 *    strcspn_s(), strpbrk_s(), strstr_s(), strprefix_s()
 *
 */
errno_t
strspn_s (const char *dest, rsize_t dmax,
          const char *src,  rsize_t slen, rsize_t *count)
{
    const char *scan2;
    rsize_t smax;
    bool match_found;

    if (count== NULL) {
        invoke_safe_str_constraint_handler("strspn_s: count is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }
    *count = 0;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("strspn_s: dest is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }

    if (src == NULL) {
        invoke_safe_str_constraint_handler("strspn_s: src is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }

    if (dmax == 0 ) {
        invoke_safe_str_constraint_handler("strspn_s: dmax is 0",
                   NULL, ESZEROL);
        return RCNEGATE(ESZEROL);
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strspn_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return RCNEGATE(ESLEMAX);
    }

    if (slen == 0 ) {
        invoke_safe_str_constraint_handler("strspn_s: slen is 0",
                   NULL, ESZEROL);
        return RCNEGATE(ESZEROL);
    }

    if (slen > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strspn_s: slen exceeds max",
                   NULL, ESLEMAX);
        return RCNEGATE(ESLEMAX);
    }

    while (*dest && dmax) {

        /*
         * Scan the entire src string for each dest character, counting
         * inclusions.
         */
        match_found = false;
        smax = slen;
        scan2 = src;
        while (*scan2 && smax) {

            if (*dest == *scan2) {
                match_found = true;
                break;
            }
            scan2++;
            smax--;
        }

        if (match_found) {
            (*count)++;
        } else {
            break;
        }

        dest++;
        dmax--;
    }

    return RCNEGATE(EOK);
}
EXPORT_SYMBOL(strspn_s)
