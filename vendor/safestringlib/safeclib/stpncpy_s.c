/*------------------------------------------------------------------
 * stpncpy_s.c
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
 *    stpncpy_s
 *
 * SYNOPSIS
 *    #include "safe_str_lib.h"
 *    char *
 *    stpncpy_s(char *dest, rsize_t dmax, const char *src, rsize_t smax, errno_t *err);
 *
 * DESCRIPTION
 *    The stpncpy_s function copies at most smax characters from the string
 *    pointed to by src, including the terminating null byte ('\0'), to the
 *    array pointed to by dest. Exactly smax characters are written at dest.
 *    If the length strlen_s(src) is smaller than smax, the remaining smax
 *    characters in the array pointed to by dest are filled with null bytes.
 *    If the length strlen_s(src) is greater than or equal to smax, the string
 *    pointed to by dest will contain smax characters from src plus a null
 *    characters (dest will be null-terminated).
 *
 *    Therefore, dmax must be at least smax+1 in order to contain the terminator.
 *
 *    The function returns a pointer to the end of the string in dest -
 *    that is to the null terminator of dest. If an error occurs,
 *    NULL is returned and err is set to the error encountered.
 *
 * SPECIFIED IN
 *    ISO/IEC TR 24731, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest      pointer to string that will be replaced by src.
 *
 *    dmax      restricted maximum length of dest (must be at least smax+1)
 *
 *    src       pointer to the string that will be copied
 *               to dest
 *
 *    smax      the maximum number of characters from src to copy into dest
 *
 *    err       the error code upon error, or EOK if successful
 *
 * OUTPUT PARAMETERS
 *    dest      updated
 *    err       updated as follows:
 *    			  EOK        successful operation, the characters in src were
 *               		     copied into dest and the result is null terminated,
 *               		     and dest is returned to point to the first null at end of dest.
 *              On error, NULL is returned and err is set to one of hte following:
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
 *    dmax must be at least smax+1 to allow filling dest with smax characters plus NULL.
 *    If src and dest overlap, copying shall be stopped; destruction of src may have occurred.
 *    If there is a runtime-constraint violation, then:
 *       if dest is not a null pointer and dmax is greater than zero and
 *       not greater than RSIZE_MAX_STR, then stpncpy_s shall fill dest with nulls,
 *       if library was compiled with SAFECLIB_STR_NULL_SLACK.
 *
 * RETURN VALUE
 *   a char pointer to the terminating null at the end of dest
 *   or NULL pointer on error
 *
 * ALSO SEE
 *    stpcpy_s(), strcpy_s(), strcat_s(), strncat_s(), strncpy_s()
 *
 */
char *
stpncpy_s(char *dest, rsize_t dmax, const char *src, rsize_t smax, errno_t *err)
{
    rsize_t orig_dmax;
    char *orig_dest;

    if (dest == NULL) {
        invoke_safe_str_constraint_handler("stpncpy_s: dest is null",
                   NULL, ESNULLP);
        *err = RCNEGATE(ESNULLP);
        return NULL;
    }

    if (src == NULL) {
        invoke_safe_str_constraint_handler("stpncpy_s: src is null",
                   NULL, ESNULLP);
        *err = RCNEGATE(ESNULLP);
        dest[0] = '\0';
        return NULL;
    }

    if (dmax == 0) {
        invoke_safe_str_constraint_handler("stpncpy_s: dmax is 0",
                   NULL, ESZEROL);
        *err = RCNEGATE(ESZEROL);
        return NULL;
    }

    if (dmax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("stpncpy_s: dmax exceeds max",
                   NULL, ESLEMAX);
        *err = RCNEGATE(ESLEMAX);
        return NULL;
    }

    if (smax > RSIZE_MAX_STR) {
        invoke_safe_str_constraint_handler("stpncpy_s: smax exceeds max",
                   NULL, ESLEMAX);
        *err = RCNEGATE(ESLEMAX);
        return NULL;
    }

    if (dmax < (smax+1)) {
        invoke_safe_str_constraint_handler("stpncpy_s: dmax too short for smax",
                   NULL, ESNOSPC);
        *err = RCNEGATE(ESNOSPC);
        dest[0] = '\0';
        return NULL;
    }

    /* dmwheel1: Add check to prevent destruction of overlap into destination */
    if ((src < dest) && ((src+smax) >= dest)) {
        invoke_safe_str_constraint_handler("stpncpy_s: src+smax overlaps into dest",
                   NULL, ESOVRLP);
        *err = RCNEGATE(ESOVRLP);
        dest[0] = '\0';
        return NULL;
    }

    /* dmwheel1: Add check to prevent destruction of overlap into source */
    if ((dest < src) && ((dest+smax) >= src)) {
        invoke_safe_str_constraint_handler("stpncpy_s: dest+smax overlaps into src",
                   NULL, ESOVRLP);
        *err = RCNEGATE(ESOVRLP);
        dest[0] = '\0';
        return NULL;
    }

#ifdef SAFECLIB_STR_NULL_SLACK
    /* dmwheel1: Add check to prevent destruction of overlap into destination */
    if ((src < dest) && ((src+dmax) >= dest)) {
        invoke_safe_str_constraint_handler("stpncpy_s: src+dmax overlaps into dest",
                   NULL, ESOVRLP);
        *err = RCNEGATE(ESOVRLP);
        return NULL;
    }

    /* dmwheel1: Add check to prevent destruction of overlap into source */
    if ((dest < src) && ((dest+dmax) >= src)) {
        invoke_safe_str_constraint_handler("stpncpy_s: dest+dmax overlaps into src",
                   NULL, ESOVRLP);
        *err = RCNEGATE(ESOVRLP);
        return NULL;
    }
#endif


    if (src == NULL) {
#ifdef SAFECLIB_STR_NULL_SLACK
        /* null string to clear data */
        while (dmax) {  *dest = '\0'; dmax--; dest++; }
#else
        *dest = '\0';
#endif
        invoke_safe_str_constraint_handler("stpncpy_s: src is null",
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
    			/* add nulls to complete smax */
    			char *filler = dest; /* don't change dest, because we need to return it */
    			while (smax) { *filler = '\0'; dmax--; smax--; filler++; }
#ifdef SAFECLIB_STR_NULL_SLACK
                /* null dmax slack to clear any data */
    		    while (dmax) { *filler = '\0'; dmax--; filler++; }
#endif
    		    *err = RCNEGATE(EOK);
    		    return dest;
    		}
    		dmax--;
    		dest++;
    		if (--smax == 0) {
    			/* we have copied smax characters, add null terminator */
    			*dest = '\0';
    		}
    	}
    	/* null terminator not found in src before end of dmax */
    	handle_error(orig_dest, orig_dmax, "stpncpy_s: not enough space for src",
    	                 ESNOSPC);
    	*err = RCNEGATE(ESNOSPC);
    	return NULL;
    }


    /* All checks for buffer overlaps were made, just do the copies */
    while (dmax > 0) {

		*dest = *src; /* Copy the data into the destination */

		/* Check for maximum copy from source */
		if (smax == 0) {
			/* we have copied smax characters, add null terminator */
			*dest = '\0';
		}

		/* Check for end of copying */
		if (*dest == '\0') {
			/* add nulls to complete smax, if fewer than smax characters
			 * were in src when the NULL was encountered */
			char *filler = dest; /* don't change dest, because we need to return it */
			while (smax) { *filler = '\0'; dmax--; smax--; filler++; }
#ifdef SAFECLIB_STR_NULL_SLACK
			/* null dmax slack to clear any data */
			while (dmax) { *filler = '\0'; dmax--; filler++; }
#endif
			*err = RCNEGATE(EOK);
			return dest;
		}
		dmax--;
		smax--;
		dest++;
		src++;

	}
    /*
     * Ran out of space in dest, and did not find the null terminator in src
     */
    handle_error(orig_dest, orig_dmax, "stpncpy_s: not enough space for src",
                 ESNOSPC);
    *err = RCNEGATE(ESNOSPC);
    return NULL;
}
EXPORT_SYMBOL(stpncpy_s)
