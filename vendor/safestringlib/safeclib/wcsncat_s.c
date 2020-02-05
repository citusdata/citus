/*------------------------------------------------------------------
 * wcsncat_s.c
 *
 * August 2014, D Wheeler
 *
 * Copyright (c) 2014 by Intel Corp
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
 *    wcsncat_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    errno_t
 *    wcsncat_s(wchar_t *dest, rsize_t dmax, const wchar_t *src, rsize_t slen)
 *
 * DESCRIPTION
 *    The wcsncat_s function appends a copy of (at most) the
 *    first slen wide characters pointed to by src to the
 *    end of the string pointed to by dest and terminates the
 *    string with the null character. If less than slen wide
 *    characters are in the string src, the function stops
 *    copying after the null terminator is copied to dest.
 *    The initial character from src overwrites the null
 *    character at the end of dest.
 *
 *    All elements following the terminating null character (if
 *    any) written by strncat_s in the array of dmax characters
 *    pointed to by dest take unspecified values when strncat_s returns.
 *
 * SPECIFIED IN
 *    ISO/IEC TR 24731, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest      pointer to string that will be extended by src
 *              if dmax allows. The string is null terminated.
 *              If the resulting concatenated string is less
 *              than dmax, the remaining slack space is nulled.
 *
 *    dmax      restricted maximum length of the resulting dest,
 *              including the null
 *
 *    src       pointer to the string that will be concatenaed
 *              to string dest
 *
 *    slen      maximum characters to append
 *
 * OUTPUT PARAMETERS
 *    dest      updated string
 *
 * RUNTIME CONSTRAINTS
 *    Neither dest nor src shall be a null pointer
 *    dmax shall not equal zero
 *    dmax shall not be greater than RSIZE_STR_MAX
 *    dmax shall be greater than strnlen_s(src,m).
 *    Copying shall not takeplace between objects that overlap
 *    If there is a runtime-constraint violation, then if dest is
 *       not a null pointer and dmax is greater than zero and not
 *       greater thanRSIZE_MAX, then strncat_s sets dest[0] to the
 *       null character.
 *
 * RETURN VALUE
 *     EOK        successful operation, all the characters from src
 *                   were appended to dest and the result in dest is
 *                   null terminated.
 *     ESNULLP    NULL pointer
 *     ESZEROL    zero length
 *     ESLEMAX    length exceeds max limit
 *     ESUNTERM   dest not terminated
 *
 *
 */
errno_t
wcsncat_s (wchar_t *dest, rsize_t dmax, const wchar_t *src, rsize_t slen)
{
    rsize_t orig_dmax;
    wchar_t *orig_dest;
    const wchar_t *overlap_bumper;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("wcsncat_s: dest is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }

    if (src == NULL) {
        invoke_safe_str_constraint_handler("wcsncat_s: src is null",
                   NULL, ESNULLP);
        return RCNEGATE(ESNULLP);
    }

    if (slen*sizeof(wchar_t) > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("wcsncat_s: slen exceeds max",
                   NULL, ESLEMAX);
        return RCNEGATE(ESLEMAX);
    }

    if (dmax == 0) {
        invoke_safe_str_constraint_handler("wcsncat_s: dmax is 0",
                   NULL, ESZEROL);
        return RCNEGATE(ESZEROL);
    }

    if (dmax*sizeof(wchar_t) > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("wcsncat_s: dmax exceeds max",
                   NULL, ESLEMAX);
        return RCNEGATE(ESLEMAX);
    }

    /* hold base of dest in case src was not copied */
    orig_dmax = dmax;
    orig_dest = dest;

    if (dest < src) {
        overlap_bumper = src;

        /* Find the end of dest */
        while (*dest != L'\0') {

            if (dest == overlap_bumper) {
                handle_wc_error(orig_dest, orig_dmax, "wcsncat_s: "
                        "overlapping objects",
                        ESOVRLP);
                return RCNEGATE(ESOVRLP);
            }

            dest++;
            dmax--;
            if (dmax == 0) {
                handle_wc_error(orig_dest, orig_dmax, "wcsncat_s: "
                       "dest unterminated",
                       ESUNTERM);
                return RCNEGATE(ESUNTERM);
            }
        }

        while (dmax > 0) {
            if (dest == overlap_bumper) {
                handle_wc_error(orig_dest, orig_dmax, "wcsncat_s: "
                       "overlapping objects",
                       ESOVRLP);
                return RCNEGATE(ESOVRLP);
            }

            /*
             * Copying truncated before the source null is encountered
             */
            /* TODO: test if this copies at most slen characters including NULL */
            if (slen == 0) {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null remaining string */
                while (dmax) { *dest = L'\0'; dmax--; dest++; }
#else
                *dest = L'\0';
#endif
                return RCNEGATE(EOK);
            }

            *dest = *src;
            if (*dest == L'\0') {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null slack to clear data */
                while (dmax) { *dest = L'\0'; dmax--; dest++; }
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

        /* Find the end of dest */
        while (*dest != L'\0') {

            /*
             * NOTE: no need to check for overlap here since src comes first
             * in memory and we're not incrementing src here.
             */
            dest++;
            dmax--;
            if (dmax == 0) {
                handle_wc_error(orig_dest, orig_dmax, "wcsncat_s: "
                       "dest unterminated",
                       ESUNTERM);
                return RCNEGATE(ESUNTERM);
            }
        }

        while (dmax > 0) {
            if (src == overlap_bumper) {
                handle_wc_error(orig_dest, orig_dmax, "wcsncat_s: "
                        "overlapping objects",
                        ESOVRLP);
                return RCNEGATE(ESOVRLP);
            }

            /*
             * Copying truncated
             */
            if (slen == 0) {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null remaining string */
                while (dmax) { *dest = L'\0'; dmax--; dest++; }
#else
                *dest = L'\0';
#endif
                return RCNEGATE(EOK);
            }

            *dest = *src;
            if (*dest == L'\0') {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null slack to clear any data */
                while (dmax) { *dest = L'\0'; dmax--; dest++; }
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
     * the entire src was not copied, so the string will be nulled.
     */
    handle_wc_error(orig_dest, orig_dmax, "wcsncat_s: not enough space for src",
                 ESNOSPC);
    return RCNEGATE(ESNOSPC);
}
EXPORT_SYMBOL(wcsncat_s)
