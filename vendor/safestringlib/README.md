# safestringlib
The Secure Development Lifecycle (SDL) recommends banning certain C Library functions because they directly contribute 
to security vulnerabilities such as buffer overflows. However routines for the manipulation of strings and memory buffers 
are common in software and firmware, and are essential to accomplish certain programming tasks. Safer replacements for 
these functions that avoid or prevent serious security vulnerabilities (e.g. buffer overflows, string format attacks, 
conversion overflows/underflows, etc.) are available in the SafeString Library. 

This library includes routines for safe string operations (like strcpy) and memory routines (like memcpy) that are 
recommended for Linux/Android operating systems, and will also work for Windows. This library is especially useful for 
cross-platform situations where one library for these routines is preferred. 

The Safe String Library is based on the Safe C Library by Cisco, and includes replacement C Library functions for the SDL 
banned functions, as well as a number of additional useful routines that are also susceptible to buffer overflows. This
library continues to be made available under the MIT Open Source License.

Cisco's Safe C Library was extended by Intel's Security Center of Excellence (SeCoE) to add additional routines, and 
include additional unit tests.

LIST OF PRIMARY FUNCTIONS:
-----------------------------
* memcmp_s()
* memcpy_s()
* memmove_s()
* memset_s()
* memzero_s()

* stpcpy_s()
* stpncpy_s()
* strcat_s()
* strcpy_s()
* strcspn_s()
* strncat_s()
* strncpy_s()
* strnlen_s()
* strpbrk_s()
* strspn_s()
* strstr_s()
* strtok_s()

* wcpcpy_s()
* wcscat_s()
* wcscpy_s()
* wcsncat_s()
* wcsnlen_s()
* wmemcmp_s()
* wmemcpy_s()
* wmemmove_s()
* wmemset_s()


LIST OF ADDITIONAL STRING ROUTINES:
------------------------------------
* strcasecmp_s()
* strcasestr_s()
* strcmp_s()
* strcmpfld_s()
* strcpyfld_s()
* strcpyfldin_s()
* strcpyfldout_s()
* strfirstchar_s()
* strfirstdiff_s()
* strfirstsmae_s()
* strisalphanumeric_s()
* strisascii_s()
* strisdigit_s()
* strishes_s()
* strislowercase_s()
* strismixedcase_s()
* strispassword_s()
* strisuppercase_s()
* strlastchar_s()
* strlastdiff_s()
* strlastsame_s()
* strljustify_s()
* strnterminate_s()
* strprefix_s()
* stremovews_s()
* strtolowercase_s()
* strtouppercase_s()
* strzero_s()


PLANNED ENHANCEMENTS:
----------------------
- Add full sprintf_s() support
- Add full sscanf_s() support
