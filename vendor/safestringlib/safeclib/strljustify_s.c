/*------------------------------------------------------------------
 * strljustify_s.c
 *
 * November 2008, Bo Berry
 *
 * Copyright (c) 2008-2011 by Cisco Systems, Inc
 * All rights reseved.
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
 *    strljustify_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    errno_t
 *    strljustify_s(char *dest, rsize_t dmax)
 *
 * DESCRIPTION
 *    Removes beginning whitespace from the string pointed to by
 *    dest by shifting the text left over writting the beginning
 *    whitespace, left justifying the text.  The left justified
 *    text is null terminated.
 *
 *    The text is shifted so the original pointer can continue
 *    to be used.
 *
 * EXTENSION TO
 *    ISO/IEC JTC1 SC22 WG14 N1172, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest    pointer to string to left justify
 *
 *    dmax    restricted maximum length of string
 *
 * OUTPUT PARAMETERS
 *    dest    left justified
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
 * ALSO SEE
 *    strremovews_s(),
 *
 */
errno_t
strljustify_s (char *dest, rsize_t dmax)
{
    char *orig_dest;
    rsize_t orig_dmax;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("strljustify_s_s: "
                   "dest is null",
                   NULL, ESNULLP);
        return (ESNULLP);
    }

    if (dmax == 0 ) {
        invoke_safe_str_constraint_handler("strljustify_s_s: "
                   "dmax is 0",
                   NULL, ESZEROL);
        return (ESZEROL);
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strljustify_s_s: "
                   "dmax exceeds max",
                   NULL, ESLEMAX);
        return (ESLEMAX);
    }

    /*
     * corner case, a dmax of one allows only for a null
     */
    if (*dest == '\0' || dmax <= RSIZE_MIN_STR) {
        *dest = '\0';
        return (EOK);
    }

    orig_dmax = dmax;
    orig_dest = dest;

     /*
      * scan the string to be sure it is properly terminated
      */
     while (*dest) {
        if (dmax == 0) {
            while (orig_dmax) { *orig_dest++ = '\0';  orig_dmax--; }

            invoke_safe_str_constraint_handler(
                      "strljustify_s: dest is unterminated",
                       NULL, ESUNTERM);
            return (ESUNTERM);
        }
        dmax--;
        dest++;
    }

    /*
     * find first non-white space char
     */
    dest = orig_dest;
    while ((*dest == ' ') || (*dest == '\t')) {
        dest++;
    }

   /*
    * shift text, removing spaces, to left justify
    */
    if (orig_dest != dest && *dest) {
        while (*dest) {
            *orig_dest++ = *dest;
            *dest++ = ' ';
        }
        *orig_dest = '\0';
    }

    return (EOK);
}
EXPORT_SYMBOL(strljustify_s)
