/*------------------------------------------------------------------
 * strremovews_s.c
 *
 * November 2008, Bo Berry
 *
 * Copyright (c) 2008-2011 by Cisco Systems, Inc
 * All rights resevered.
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
 *    strremovews_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    errno_t
 *    strremovews_s(char *dest, rsize_t dmax)
 *
 * DESCRIPTION
 *    Removes beginning and trailing whitespace from the string pointed to by
 *    dest by shifting the text left over writting the beginning whitespace.
 *    The shifted-trimmed text is null terminated.
 *
 *    The text is shifted so the original pointer can continue to be used. This
 *    is useful when the memory was malloc'ed and will need to be freed.
 *
 * EXTENSION TO
 *    ISO/IEC TR 24731, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest    pointer to string to remove whitespace
 *
 *    dmax    restricted maximum length of string
 *
 * RUNTIME CONSTRAINTS
 *    dest shall not be a null pointer.
 *    dmax shall not be 0
 *    dmax shall not be greater than RSIZE_MAX_STR
 *    dest shall be null terminated
 *
 * RETURN VALUE
 *    EOK
 *    ESNULLP     NULL pointer
 *    ESZEROL     zero length
 *    ESLEMAX     length exceeds max limit
 *    ESUNTERM    dest was not null terminated
 *
 * SEE ALSO
 *    strljustify_s(),
 *
 */
errno_t
strremovews_s (char *dest, rsize_t dmax)
{
    char *orig_dest;
    char *orig_end;
    rsize_t orig_dmax;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("strremovews_s: dest is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }

    if (dmax == 0 ) {
        invoke_safe_str_constraint_handler("strremovews_s: dmax is 0",
                   NULL, ESZEROL);
        return (ESZEROL);
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strremovews_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return (ESLEMAX);
    }

    /*
     * corner case, a dmax of one requires a null
     */
    if (*dest == '\0' || dmax <= RSIZE_MIN_STR) {
        *dest = '\0';
        return (EOK);
    }

    orig_dest = dest;
    orig_dmax = dmax;

     /*
      * scan the string to be sure it is properly terminated
      */
     while (*dest) {
        if (dmax == 0) {
            while (orig_dmax) { *orig_dest++ = '\0';  orig_dmax--; }

            invoke_safe_str_constraint_handler(
                      "strremovews_s: dest is unterminated",
                       NULL, ESUNTERM);
            return (ESUNTERM);
        }
        dmax--;
        dest++;
    }

    /*
     * find first non-white space char
     */
    orig_end = dest-1;
    dest = orig_dest;
    while ((*dest == ' ') || (*dest == '\t')) {
        dest++;
    }

    /*
     * shift the text over the leading spaces
     */
    if (orig_dest != dest && *dest) {
        while (*dest) {
            *orig_dest++ = *dest;
            *dest++ = ' ';
        }
        *dest = '\0';
     }

    /*
     * strip trailing whitespace
     */
    dest = orig_end;
    while ((*dest == ' ') || (*dest == '\t')) {
        *dest = '\0';
        dest--;
    }

    return (EOK);
}
EXPORT_SYMBOL(strremovews_s)
