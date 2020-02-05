/*------------------------------------------------------------------
 * sprintf_s.h -- Safe Sprintf Interfaces
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
#ifndef SPRINTF_S_H_
#define SPRINTF_S_H_

#include <safe_lib_errno.h>


#define SNPRFNEGATE(x) (-1*(x))



int snprintf_s_i(char *dest, rsize_t dmax, const char *format, int a);
int snprintf_s_si(char *dest, rsize_t dmax, const char *format, char *s, int a);
int snprintf_s_l(char *dest, rsize_t dmax, const char *format, long a);
int snprintf_s_sl(char *dest, rsize_t dmax, const char *format, char *s, long a);



#endif /* SPRINTF_S_H_ */
