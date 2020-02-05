/*------------------------------------------------------------------
 * strispassword_s.c
 *
 * October 2008, Bo Berry
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
 *    strispassword_s
 *
 * SYNOPSIS
 *    #include "strlib.h"
 *    bool
 *    strispassword_s(const char *dest, rsize_t dmax)
 *
 * DESCRIPTION
 *    This function validates the make-up of a password string.
 *    -SAFE_STR_PASSWORD_MIN_LENGTH character minimum
 *    -SAFE_STR_PASSWORD_MAX_LENGTH character maximum
 *    -at least SAFE_STR_MIN_LOWERCASE lower case characters
 *    -at least SAFE_STR_MIN_UPPERCASE upper case characters
 *    -at least SAFE_STR_MIN_NUMBERS number
 *    -at least SAFE_STR_MIN_SPECIALS special
 *
 * EXTENSION TO
 *    ISO/IEC TR 24731, Programming languages, environments
 *    and system software interfaces, Extensions to the C Library,
 *    Part I: Bounds-checking interfaces
 *
 * INPUT PARAMETERS
 *    dest       pointer to string
 *
 *    dmax       length of password string
 *
 * OUTPUT PARAMETERS
 *    none
 *
 * RUNTIME CONSTRAINTS
 *    dest shall not be a null pointer.
 *    length > SAFE_STR_PASSWORD_MIN_LENGTH
 *    length < SAFE_STR_PASSWORD_MAX_LENGTH
 *    dest shall not be unterminated
 *
 * RETURN VALUE
 *    true, string has valid password makeup
 *    false, string does not meet requirements or an error occurred
 *
 * ALSO SEE
 *    strzero_s()
 *
 */
bool
strispassword_s (const char *dest, rsize_t dmax)
{
    uint32_t cnt_all;
    uint32_t cnt_lowercase;
    uint32_t cnt_uppercase;
    uint32_t cnt_numbers;
    uint32_t cnt_specials;

    if (!dest) {
        invoke_safe_str_constraint_handler("strispassword_s: "
                   "dest is null",
                   NULL, ESNULLP);
        return (false);
    }

    if (dmax < SAFE_STR_PASSWORD_MIN_LENGTH) {
        invoke_safe_str_constraint_handler("strispassword_s: "
                   "dest is too short",
                   NULL, ESLEMIN);
        return (false);
    }

    if (dmax > SAFE_STR_PASSWORD_MAX_LENGTH) {
        invoke_safe_str_constraint_handler("strispassword_s: "
                   "dest exceeds max",
                   NULL, ESLEMAX);
        return (false);
    }

    if (*dest == '\0') {
        return (false);
    }

    cnt_all = cnt_lowercase = cnt_uppercase = 0;
    cnt_numbers = cnt_specials = 0;

    while (*dest) {

        if (dmax == 0) {
            invoke_safe_str_constraint_handler(
                      "strispassword_s: dest is unterminated",
                       NULL, ESUNTERM);
            return (false);
        }
        dmax--;

        cnt_all++;

        if ((*dest >= '0') && (*dest <= '9')) {
            cnt_numbers++;

        } else if ((*dest >= 'a') && (*dest <= 'z')) {
            cnt_lowercase++;

        } else if ((*dest >= 'A') && (*dest <= 'Z')) {
            cnt_uppercase++;

        /* allow all specials */
        } else if ((*dest >= 33) && (*dest <= 47)) {
            cnt_specials++;
        } else if ((*dest >= 58) && (*dest <= 64)) {
            cnt_specials++;
        } else if ((*dest >= 91) && (*dest <= 94)) {
            cnt_specials++;
        } else if ((*dest >= 95) && (*dest <= 96)) {
            cnt_specials++;
        } else if ((*dest >= 123) && (*dest <= 126)) {
            cnt_specials++;

        } else {
            /* illegal char in password string */
            return (false);
        }
        dest++;
    }

    if (cnt_all < SAFE_STR_PASSWORD_MAX_LENGTH &&
        cnt_numbers >=  SAFE_STR_MIN_NUMBERS &&
        cnt_lowercase >= SAFE_STR_MIN_LOWERCASE &&
        cnt_uppercase >= SAFE_STR_MIN_UPPERCASE &&
        cnt_specials >= SAFE_STR_MIN_SPECIALS ) {
        return (true);
    } else {
        return (false);
    }
}
EXPORT_SYMBOL(strispassword_s)
