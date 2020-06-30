#!/bin/bash

# Checks for the APIs that are banned by microsoft. Since we compile for Linux
# we use the replacements from https://github.com/intel/safestringlib
# Not all replacement functions are available in safestringlib. If it doesn't
# exist and you cannot rewrite the code to not use the banned API, then you can
# add a comment containing "IGNORE-BANNED" to the line where the error is and
# this check will ignore that match.
#
# The replacement function that you should use are listed here:
# https://liquid.microsoft.com/Web/Object/Read/ms.security/Requirements/Microsoft.Security.SystemsADM.10082#guide

set -eu
# shellcheck disable=SC1091
source ci/ci_helpers.sh

files=$(find src -iname '*.[ch]' | git check-attr --stdin citus-style | grep -v ': unset$' | sed 's/: citus-style: set$//')

# grep is allowed to fail, that means no banned matches are found
set +e
# Required banned from banned.h. These functions are not allowed to be used at
# all.
# shellcheck disable=SC2086
grep -E '\b(strcpy|strcpyA|strcpyW|wcscpy|_tcscpy|_mbscpy|StrCpy|StrCpyA|StrCpyW|lstrcpy|lstrcpyA|lstrcpyW|_tccpy|_mbccpy|_ftcscpy|strcat|strcatA|strcatW|wcscat|_tcscat|_mbscat|StrCat|StrCatA|StrCatW|lstrcat|lstrcatA|lstrcatW|StrCatBuff|StrCatBuffA|StrCatBuffW|StrCatChainW|_tccat|_mbccat|_ftcscat|sprintfW|sprintfA|wsprintf|wsprintfW|wsprintfA|sprintf|swprintf|_stprintf|wvsprintf|wvsprintfA|wvsprintfW|vsprintf|_vstprintf|vswprintf|strncpy|wcsncpy|_tcsncpy|_mbsncpy|_mbsnbcpy|StrCpyN|StrCpyNA|StrCpyNW|StrNCpy|strcpynA|StrNCpyA|StrNCpyW|lstrcpyn|lstrcpynA|lstrcpynW|strncat|wcsncat|_tcsncat|_mbsncat|_mbsnbcat|StrCatN|StrCatNA|StrCatNW|StrNCat|StrNCatA|StrNCatW|lstrncat|lstrcatnA|lstrcatnW|lstrcatn|gets|_getts|_gettws|IsBadWritePtr|IsBadHugeWritePtr|IsBadReadPtr|IsBadHugeReadPtr|IsBadCodePtr|IsBadStringPtr|memcpy|RtlCopyMemory|CopyMemory|wmemcpy|lstrlen)\(' $files \
    | grep -v "IGNORE-BANNED" \
    && echo "ERROR: Required banned API usage detected" && exit 1

# Required banned from table on liquid. These functions are not allowed to be
# used at all.
# shellcheck disable=SC2086
grep -E  '\b(strcat|strcpy|strerror|strncat|strncpy|strtok|wcscat|wcscpy|wcsncat|wcsncpy|wcstok|fprintf|fwprintf|printf|snprintf|sprintf|swprintf|vfprintf|vprintf|vsnprintf|vsprintf|vswprintf|vwprintf|wprintf|fscanf|fwscanf|gets|scanf|sscanf|swscanf|vfscanf|vfwscanf|vscanf|vsscanf|vswscanf|vwscanf|wscanf|asctime|atof|atoi|atol|atoll|bsearch|ctime|fopen|freopen|getenv|gmtime|localtime|mbsrtowcs|mbstowcs|memcpy|memmove|qsort|rewind|setbuf|wmemcpy|wmemmove)\(' $files \
    | grep -v "IGNORE-BANNED" \
    && echo "ERROR: Required banned API usage from table detected" && exit 1

# Recommended banned from banned.h. If you can change the code not to use these
# that would be great. You can use IGNORE-BANNED if you need to use it anyway.
# You can also remove it from the regex, if you want to mark the API as allowed
# throughout the codebase (to not have to add IGNORED-BANNED everywhere). In
# that case note it in this comment that you did so.
# shellcheck disable=SC2086
grep -E '\b(wnsprintf|wnsprintfA|wnsprintfW|_snwprintf|_snprintf|_sntprintf|_vsnprintf|vsnprintf|_vsnwprintf|_vsntprintf|wvnsprintf|wvnsprintfA|wvnsprintfW|strtok|_tcstok|wcstok|_mbstok|makepath|_tmakepath| _makepath|_wmakepath|_splitpath|_tsplitpath|_wsplitpath|scanf|wscanf|_tscanf|sscanf|swscanf|_stscanf|snscanf|snwscanf|_sntscanf|_itoa|_itow|_i64toa|_i64tow|_ui64toa|_ui64tot|_ui64tow|_ultoa|_ultot|_ultow|CharToOem|CharToOemA|CharToOemW|OemToChar|OemToCharA|OemToCharW|CharToOemBuffA|CharToOemBuffW|alloca|_alloca|ChangeWindowMessageFilter)\(' $files  \
    | grep -v "IGNORE-BANNED" \
    && echo "ERROR: Recomended banned API usage detected" && exit 1

# Recommended banned from table on liquid. If you can change the code not to use these
# that would be great. You can use IGNORE-BANNED if you need to use it anyway.
# You can also remove it from the regex, if you want to mark the API as allowed
# throughout the codebase (to not have to add IGNORED-BANNED everywhere). In
# that case note it in this comment that you did so.
# Banned APIs ignored throughout the codebase:
# - strlen
# shellcheck disable=SC2086
grep -E '\b(alloca|getwd|mktemp|tmpnam|wcrtomb|wcrtombs|wcslen|wcsrtombs|wcstombs|wctomb|class_addMethod|class_replaceMethod)\(' $files  \
    | grep -v "IGNORE-BANNED" \
    && echo "ERROR: Recomended banned API usage detected" && exit 1
exit 0
