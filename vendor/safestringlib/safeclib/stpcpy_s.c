/*------------------------------------------------------------------
 * stpcpy_s.c
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
 *    stpcpy_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    char *
 *    stpcpy_s(char *dest, rsize_t dmax, const char *src, errno_t *err);
 *
 * DESCRIPTION
 *    The stpcpy_s function copies the string pointed to by src
 *    (including the terminating null character) into the array
 *    pointed to by dest. All elements following the terminating
 *    null character (if any) written by stpcpy_s in the array
 *    of dmax characters pointed to by dest are nulled when
 *    strcpy_s returns. The function returns a pointer to the
 *    end of the string in dest - that is to the null terminator
 *    of dest - upon return. If an error occurs, NULL is returned
 *    and err is set to the error encountered.
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
 *    err      the error code upon error, or EOK if successful
 *
 * OUTPUT PARAMETERS
 *    dest      updated
 *    err       updated as follows:
 *    			  EOK        successful operation, the characters in src were
 *               		     copied into dest and the result is null terminated.
 *    			  ESNULLP    NULL pointer
 *    			  ESZEROL    zero length
 *    			  ESLEMAX    length exceeds max limit
 *    			  ESOVRLP    strings overlap
 *    			  ESNOSPC    not enough space to copy src
 *
 * RUNTIME CONSTRAINTS
 *    Neither dest nor src shall be a null pointer.
 *    dmax shall not be greater than RSIZE_MAX_STR.
 *    dmax shall not equal zero.
 *    dmax shall be greater than strnlen_s(src, dmax).
 *    Copying shall not take place between objects that overlap.
 *    If there is a runtime-constraint violation, then if dest
 *       is not a null pointer and destmax is greater than zero and
 *       not greater than RSIZE_MAX_STR, then stpcpy_s nulls dest.
 *
 * RETURN VALUE
 *   a char pointer to the terminating null at the end of dest
 *
 * ALSO SEE
 *    strcpy_s(), strcat_s(), strncat_s(), strncpy_s()
 *
 */
char *
stpcpy_s(char *dest, rsize_t dmax, const char *src, errno_t *err)
{
    rsize_t orig_dmax;
    char *orig_dest;
    const char *overlap_bumper;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("stpcpy_s: dest is null",
                   NULL, ESNULLP);
        *err = RCNEGATE(ESNULLP);
        return NULL;
    }

    if (dmax == 0) {
        invoke_safe_str_constraint_handler("stpcpy_s: dmax is 0",
                   NULL, ESZEROL);
        *err = RCNEGATE(ESZEROL);
        return NULL;
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("stpcpy_s: dmax exceeds max",
                   NULL, ESLEMAX);
        *err = RCNEGATE(ESLEMAX);
        return NULL;
    }

    if (src == NULL) {
#ifdef SAFECLIB_STR_NULL_SLACK
        /* null string to clear data */
        while (dmax) {  *dest = '\0'; dmax--; dest++; }
#else
        *dest = '\0';
#endif
        invoke_safe_str_constraint_handler("stpcpy_s: src is null",
                   NULL, ESNULLP);
        *err = RCNEGATE(ESNULLP);
        return NULL;
    }

    /* hold base of dest in case src was not copied */
    orig_dmax = dmax;
    orig_dest = dest;

    if (dest == src) {
    	/* look for the terminating null character, or return err if not found in dmax bytes */
    	while (dmax > 0) {
    		if (*dest == '\0') {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null slack to clear any data */
    		    while (dmax) { *dest = '\0'; dmax--; dest++; }
#endif
    		    *err = RCNEGATE(EOK);
    		    return dest;
    		}

    		dmax--;
    		dest++;
    	}
    	/* null terminator not found in src before end of dmax */
    	handle_error(orig_dest, orig_dmax, "stpcpy_s: not enough space for src",
    	                 ESNOSPC);
    	*err = RCNEGATE(ESNOSPC);
    	return NULL;
    }


    if (dest < src) {
        overlap_bumper = src;

        /* Check that the dest buffer does not overlap src buffer */
        while (dmax > 0) {
            if (dest == overlap_bumper) {
                handle_error(orig_dest, orig_dmax, "stpcpy_s: "
                             "overlapping objects",
                             ESOVRLP);
                *err = RCNEGATE(ESOVRLP);
                return NULL;
            }

            *dest = *src;
            if (*dest == '\0') {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null slack to clear any data */
                while (dmax) { *dest = '\0'; dmax--; dest++; }
#endif
                *err = RCNEGATE(EOK);
                return dest;
            }

            dmax--;
            dest++;
            src++;
        }

    } else {
        overlap_bumper = dest;

        while (dmax > 0) {
        	/* check that the src buffer does not run into the dest buffer - inifinite loop */
            if (src == overlap_bumper) {
            	/* NOTE (dmw) this condition guarantees that SRC has already been damaged! */
                handle_error(orig_dest, orig_dmax, "stpcpy_s: overlapping objects",
                      ESOVRLP);
                *err = RCNEGATE(ESOVRLP);
                return NULL;
            }

            *dest = *src;
            if (*dest == '\0') {
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null slack to clear any data */
                while (dmax) { *dest = '\0'; dmax--; dest++; }
#endif
                *err = RCNEGATE(EOK);
                return dest;
            }

            dmax--;
            dest++;
            src++;
        }
    }

    /*
     * Ran out of space in dest, and did not find the null terminator in src
     */
    handle_error(orig_dest, orig_dmax, "stpcpy_s: not "
                 "enough space for src",
                 ESNOSPC);
    *err = RCNEGATE(ESNOSPC);
    return NULL;
}
EXPORT_SYMBOL(stpcpy_s)
