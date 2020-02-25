/*------------------------------------------------------------------
 * strncpy_s.c
 *
 * October 2008, Bo Berry
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


/*
 * NAME
 *    strncpy_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    errno_t
 *    strncpy_s(char *dest, rsize_t dmax, const char *src, rsize_t slen)
 *
 * DESCRIPTION
 *    The strncpy_s function copies not more than slen successive characters
 *    (characters that follow a null character are not copied) from the
 *    array pointed to by src to the array pointed to by dest. If no null
 *    character was copied from src, then dest[n] is set to a null character.
 *
 *    All elements following the terminating null character (if any)
 *    written by strncpy_s in the array of dmax characters pointed to
 *    by dest take on the null value when strncpy_s returns.
 *
 * Specicified in:
 *    ISO/IEC TR 24731-1, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest      pointer to string that will be replaced by src.
 *              The resulting string is null terminated.
 *
 *    dmax      restricted maximum length of the resulting dest,
 *              including the null
 *
 *    src       pointer to the string that will be copied
 *              to string dest
 *
 *    slen      the maximum number of characters to copy from src
 *
 * OUTPUT PARAMETERS
 *    dest      updated with src string
 *
 * RUNTIME CONSTRAINTS
 *    Neither dmax nor slen shall be equal to zero.
 *    Neither dmax nor slen shall be equal zero.
 *    Neither dmax nor slen shall be greater than RSIZE_MAX_STR.
 *    If slen is either greater than or equal to dmax, then dmax
 *     should be more than strnlen_s(src,dmax)
 *    Copying shall not take place between objects that overlap.
 *    If there is a runtime-constraint violation, then if dest
 *       is not a null pointer and dmax greater than RSIZE_MAX_STR,
 *       then strncpy_s nulls dest.
 *
 * RETURN VALUE
 *    EOK        successful operation, the characters in src were copied
 *                  to dest and the result is null terminated.
 *    ESNULLP    NULL pointer
 *    ESZEROL    zero length
 *    ESLEMAX    length exceeds max limit
 *    ESOVRLP    strings overlap
 *    ESNOSPC    not enough space to copy src
 *
 * ALSO SEE
 *    strcat_s(), strncat_s(), strcpy_s()
 *-
 */
errno_t
strncpy_s (char *dest, rsize_t dmax, const char *src, rsize_t slen)
{
    rsize_t orig_dmax;
    char *orig_dest;
    const char *overlap_bumper;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("strncpy_s: dest is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }

    if (dmax == 0) {
        invoke_safe_str_constraint_handler("strncpy_s: dmax is 0",
                   NULL, ESZEROL);
        return RCNEGATE(ESZEROL);
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strncpy_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return RCNEGATE(ESLEMAX);
    }

    /* hold base in case src was not copied */
    orig_dmax = dmax;
    orig_dest = dest;

    if (src == NULL) {
        handle_error(orig_dest, orig_dmax, "strncpy_s: "
                     "src is null",
                     ESNULLP);
        return RCNEGATE(ESNULLP);
    }

    if (slen == 0) {
        handle_error(orig_dest, orig_dmax, "strncpy_s: "
                     "slen is zero",
                     ESZEROL);
        return RCNEGATE(ESZEROL);
    }

    if (slen > RSIZE_MAX_STR) {
        handle_error(orig_dest, orig_dmax, "strncpy_s: "
                     "slen exceeds max",
                     ESLEMAX);
        return RCNEGATE(ESLEMAX);
    }


   if (dest < src) {
       overlap_bumper = src;

        while (dmax > 0) {
            if (dest == overlap_bumper) {
                handle_error(orig_dest, orig_dmax, "strncpy_s: "
                        "overlapping objects",
                        ESOVRLP);
                return RCNEGATE(ESOVRLP);
            }

	    if (slen == 0) {
                /*
                 * Copying truncated to slen chars.  Note that the TR says to
                 * copy slen chars plus the null char.  We null the slack.
                 */
#ifdef SAFECLIB_STR_NULL_SLACK
                while (dmax) { *dest = '\0'; dmax--; dest++; }
#else
                *dest = '\0';
#endif
                return RCNEGATE(EOK);
            }

            *dest = *src;
            if (*dest == '\0') {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null slack */
                while (dmax) { *dest = '\0'; dmax--; dest++; }
#endif
                return RCNEGATE(EOK);
            }

            dmax--;
            slen--;
            dest++;
            src++;
        }

    } else {
        overlap_bumper = dest;

        while (dmax > 0) {
            if (src == overlap_bumper) {
                handle_error(orig_dest, orig_dmax, "strncpy_s: "
                        "overlapping objects",
                        ESOVRLP);
                return RCNEGATE(ESOVRLP);
            }

	    if (slen == 0) {
                /*
                 * Copying truncated to slen chars.  Note that the TR says to
                 * copy slen chars plus the null char.  We null the slack.
                 */
#ifdef SAFECLIB_STR_NULL_SLACK
                while (dmax) { *dest = '\0'; dmax--; dest++; }
#else
                *dest = '\0';
#endif
                return RCNEGATE(EOK);
            }

            *dest = *src;
            if (*dest == '\0') {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null slack */
                while (dmax) { *dest = '\0'; dmax--; dest++; }
#endif
                return RCNEGATE(EOK);
            }

            dmax--;
            slen--;
            dest++;
            src++;
        }
    }

    /*
     * the entire src was not copied, so zero the string
     */
    handle_error(orig_dest, orig_dmax, "strncpy_s: not enough "
                 "space for src",
                 ESNOSPC);
    return RCNEGATE(ESNOSPC);
}
EXPORT_SYMBOL(strncpy_s)
