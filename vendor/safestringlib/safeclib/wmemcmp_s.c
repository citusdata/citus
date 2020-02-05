/*------------------------------------------------------------------
 * wmemcmp_s.c - Compares memory
 *
 * September 2014, D. Wheeler
 *
 * Copyright (c) 2014 Intel Corp
 * All rights reserved.
 *
 * Based on memcmp32_s.c, October 2008, by Bo Berry
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
#include "safe_mem_constraint.h"
#include "safe_mem_lib.h"

/**
 * NAME
 *    wmemcmp_s
 *
 * SYNOPSIS
 *    #include "safe_mem_lib.h"
 *    errno_t
 *    wmemcmp_s(const wchar_t *dest, rsize_t dmax,
 *               const wchar_t *src,  rsize_t smax, int *diff)
 *
 * DESCRIPTION
 *    Compares wide-character strings until they differ, and their difference is
 *    returned in diff.  If the wide character string is the same, diff=0.
 *
 * EXTENSION TO
 *    ISO/IEC JTC1 SC22 WG14 N1172, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 *  INPUT PARAMETERS
 *    dest      pointer to memory to compare against
 *
 *    dmax      maximum length of dest, in uint32_t
 *
 *    src       pointer to the source memory to compare with dest
 *
 *    smax      maximum length of src, in uint32_t
 *
 *    *diff     pointer to the diff which is an integer greater
 *              than, equal to or less than zero according to
 *              whether the object pointed to by dest is
 *              greater than, equal to or less than the object
 *              pointed to by src.
 *
 *  OUTPUT PARAMETERS
 *    none
 *
 * RUNTIME CONSTRAINTS
 *    Neither dest nor src shall be a null pointer.
 *    Neither dmax nor smax shall be zero.
 *    dmax shall not be greater than RSIZE_MAX_MEM.
 *    smax shall not be greater than dmax.
 *
 * RETURN VALUE
 *    EOK        successful operation
 *    ESNULLP    NULL pointer
 *    ESZEROL    zero length
 *    ESLEMAX    length exceeds max limit
 *
 * ALSO SEE
 *    memcmp_s(), memcmp16_s()
 *
 */
errno_t
wmemcmp_s (const wchar_t *dest, rsize_t dmax,
            const wchar_t *src,  rsize_t smax, int *diff)
{
    /*
     * must be able to return the diff
     */
    if (diff == NULL) {
        invoke_safe_mem_constraint_handler("wmemcmp_s: diff is null",
                   NULL, ESNULLP);
        return (RCNEGATE(ESNULLP));
    }
    *diff = -1;  /* default diff */


    if (dest == NULL) {
        invoke_safe_mem_constraint_handler("wmemcmp_s: dest is null",
                   NULL, ESNULLP);
        return (RCNEGATE(ESNULLP));
    }

    if (src == NULL) {
        invoke_safe_mem_constraint_handler("wmemcmp_s: src is null",
                   NULL, ESNULLP);
        return (RCNEGATE(ESNULLP));
    }

    if (dmax == 0) {
        invoke_safe_mem_constraint_handler("wmemcmp_s: dmax is 0",
                   NULL, ESZEROL);
        return (RCNEGATE(ESZEROL));
    }

    if (dmax > RSIZE_MAX_MEM32) {
        invoke_safe_mem_constraint_handler("wmemcmp_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return (RCNEGATE(ESLEMAX));
    }

    if (smax == 0) {
        invoke_safe_mem_constraint_handler("wmemcmp_s: smax is 0",
                   NULL, ESZEROL);
        return (RCNEGATE(ESZEROL));
    }

    if (smax > dmax) {
       invoke_safe_mem_constraint_handler("wmemcmp_s: smax exceeds dmax",
                  NULL, ESLEMAX);
       return (RCNEGATE(ESLEMAX));
    }

    /*
     * no need to compare the same memory
     */
    if (dest == src) {
        *diff = 0;
        return (RCNEGATE(EOK));
    }

    /*
     * now compare src to dest
     */
    *diff = 0;
    while (dmax != 0 && smax != 0) {
        if (*dest != *src) {
            *diff = *dest - *src;
            break;
        }

        dmax--;
        smax--;

        dest++;
        src++;
    }

    return (RCNEGATE(EOK));
}
EXPORT_SYMBOL(wmemcmp_s)
