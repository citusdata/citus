/*------------------------------------------------------------------
 * strcasestr_s.c
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
 *    strcasestr_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    errno_t
 *    strcasestr_s(char *dest, rsize_t dmax,
 *                 const char *src, rsize_t slen, char **substring)
 *
 * DESCRIPTION
 *    The strcasestr_s() function locates the first occurrence of
 *    the substring pointed to by src which would be located in
 *    the string pointed to by dest.  The comparison is case
 *    insensitive.
 *
 * EXTENSION TO
 *    ISO/IEC TR 24731, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest      pointer to string to be searched for the substring
 *
 *    dmax      restricted maximum length of dest string
 *
 *    src       pointer to the sub string
 *
 *    slen      maximum length of src string
 *
 *    substring  returned pointer to the substring
 *
 * OUTPUT PARAMETERS
 *     substring  pointer to the substring
 *
 * RUNTIME CONSTRAINTS
 *    Neither dest nor src shall be a null pointer.
 *    Neither dmax nor slen shall equal zero.
 *    Neither dmax nor slen shall be greater than RSIZE_MAX_STR.
 *
 * RETURN VALUE
 *    EOK        successful operation, substring found.
 *    ESNULLP    NULL pointer
 *    ESZEROL    zero length
 *    ESLEMAX    length exceeds max limit
 *    ESNOTFND   substring not found
 *
 * ALSO SEE
 *    strstr_s(), strprefix_s()
 *
 */
errno_t
strcasestr_s (char *dest, rsize_t dmax,
              const char *src, rsize_t slen, char **substring)
{
    rsize_t len;
    rsize_t dlen;
    int i;

    if (substring == NULL) {
        invoke_safe_str_constraint_handler("strcasestr_s: substring is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }
    *substring = NULL;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("strcasestr_s: dest is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }

    if (dmax == 0) {
        invoke_safe_str_constraint_handler("strcasestr_s: dmax is 0",
                   NULL, ESZEROL);
        return (ESZEROL);
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strcasestr_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return (ESLEMAX);
    }

    if (src == NULL) {
        invoke_safe_str_constraint_handler("strcasestr_s: src is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }

    if (slen == 0) {
        invoke_safe_str_constraint_handler("strcasestr_s: slen is 0",
                   NULL, ESZEROL);
        return (ESZEROL);
    }

    if (slen > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strcasestr_s: slen exceeds max",
                   NULL, ESLEMAX);
        return (ESLEMAX);
    }

    /*
     * src points to a string with zero length, or
     * src equals dest, return dest
     */
    if (*src == '\0' || dest == src) {
        *substring = dest;
        return (EOK);
    }

    while (*dest && dmax) {
        i = 0;
        len = slen;
        dlen = dmax;

        while (dest[i] && dlen) {

            /* not a match, not a substring */
            if (toupper(dest[i]) != toupper(src[i])) {
                break;
            }

            /* move to the next char */
            i++;
            len--;
            dlen--;

            if (src[i] == '\0' || !len) {
                *substring = dest;
                return (EOK);
            }
        }
        dest++;
        dmax--;
    }

    /*
     * substring was not found, return NULL
     */
    *substring = NULL;
    return (ESNOTFND);
}
EXPORT_SYMBOL(strcasestr_s)
