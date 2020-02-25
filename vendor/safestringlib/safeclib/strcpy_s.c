/*------------------------------------------------------------------
 * strcpy_s.c
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


/**
 * NAME
 *    strcpy_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    errno_t
 *    strcpy_s(char *dest, rsize_t dmax, const char *src)
 *
 * DESCRIPTION
 *    The strcpy_s function copies the string pointed to by src
 *    (including the terminating null character) into the array
 *    pointed to by dest. All elements following the terminating
 *    null character (if any) written by strcpy_s in the array
 *    of dmax characters pointed to by dest are nulled when
 *    strcpy_s returns.
 *
 * SPECIFIED IN
 *    ISO/IEC TR 24731, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest      pointer to string that will be replaced by src.
 *
 *    dmax      restricted maximum length of dest
 *
 *    src       pointer to the string that will be copied
 *               to dest
 *
 * OUTPUT PARAMETERS
 *    dest      updated
 *
 * RUNTIME CONSTRAINTS
 *    Neither dest nor src shall be a null pointer.
 *    dmax shall not be greater than RSIZE_MAX_STR.
 *    dmax shall not equal zero.
 *    dmax shall be greater than strnlen_s(src, dmax).
 *    Copying shall not take place between objects that overlap.
 *    If there is a runtime-constraint violation, then if dest
 *       is not a null pointer and destmax is greater than zero and
 *       not greater than RSIZE_MAX_STR, then strcpy_s nulls dest.
 *
 * RETURN VALUE
 *    EOK        successful operation, the characters in src were
 *               copied into dest and the result is null terminated.
 *    ESNULLP    NULL pointer
 *    ESZEROL    zero length
 *    ESLEMAX    length exceeds max limit
 *    ESOVRLP    strings overlap
 *    ESNOSPC    not enough space to copy src
 *
 * ALSO SEE
 *    strcat_s(), strncat_s(), strncpy_s()
 *
 */
errno_t
strcpy_s (char *dest, rsize_t dmax, const char *src)
{
    rsize_t orig_dmax;
    char *orig_dest;
    const char *overlap_bumper;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("strcpy_s: dest is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }

    if (dmax == 0) {
        invoke_safe_str_constraint_handler("strcpy_s: dmax is 0",
                   NULL, ESZEROL);
        return RCNEGATE(ESZEROL);
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("strcpy_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return RCNEGATE(ESLEMAX);
    }

    if (src == NULL) {
#ifdef SAFECLIB_STR_NULL_SLACK
        /* null string to clear data */
        while (dmax) {  *dest = '\0'; dmax--; dest++; }
#else
        *dest = '\0';
#endif
        invoke_safe_str_constraint_handler("strcpy_s: src is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }

    if (dest == src) {
        return RCNEGATE(EOK);
    }

    /* hold base of dest in case src was not copied */
    orig_dmax = dmax;
    orig_dest = dest;

    if (dest < src) {
        overlap_bumper = src;

        while (dmax > 0) {
            if (dest == overlap_bumper) {
                handle_error(orig_dest, orig_dmax, "strcpy_s: "
                             "overlapping objects",
                             ESOVRLP);
                return RCNEGATE(ESOVRLP);
            }

            *dest = *src;
            if (*dest == '\0') {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null slack to clear any data */
                while (dmax) { *dest = '\0'; dmax--; dest++; }
#endif
                return RCNEGATE(EOK);
            }

            dmax--;
            dest++;
            src++;
        }

    } else {
        overlap_bumper = dest;

        while (dmax > 0) {
            if (src == overlap_bumper) {
                handle_error(orig_dest, orig_dmax, "strcpy_s: "
                      "overlapping objects",
                      ESOVRLP);
                return RCNEGATE(ESOVRLP);
            }

            *dest = *src;
            if (*dest == '\0') {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null slack to clear any data */
                while (dmax) { *dest = '\0'; dmax--; dest++; }
#endif
                return RCNEGATE(EOK);
            }

            dmax--;
            dest++;
            src++;
        }
    }

    /*
     * the entire src must have been copied, if not reset dest
     * to null the string.
     */
    handle_error(orig_dest, orig_dmax, "strcpy_s: not "
                 "enough space for src",
                 ESNOSPC);
    return RCNEGATE(ESNOSPC);
}
EXPORT_SYMBOL(strcpy_s)
