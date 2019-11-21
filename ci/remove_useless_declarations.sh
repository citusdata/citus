#!/bin/sh

set -eu

files=$(find src -iname '*.c' | git check-attr --stdin citus-style | grep -v ': unset$' | sed 's/: citus-style: set$//')
while true; do
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t)?(?=\b(?P=variable)\b))(?<=\n\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t$+{type}$+{variable} =/sg' $files
    # The following are simply the same regex, but repeated for different tab sizes
    # (this is needed because variable sized backtracking is not supported in perl)
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t\t)?(?=\b(?P=variable)\b))(?<=\n\t\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t\t$+{type}$+{variable} =/sg' $files
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t\t\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t\t\t)?(?=\b(?P=variable)\b))(?<=\n\t\t\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t\t\t$+{type}$+{variable} =/sg' $files
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t\t\t\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t\t\t\t)?(?=\b(?P=variable)\b))(?<=\n\t\t\t\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t\t\t\t$+{type}$+{variable} =/sg' $files
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t\t\t\t\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t\t\t\t\t)?(?=\b(?P=variable)\b))(?<=\n\t\t\t\t\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t\t\t\t\t$+{type}$+{variable} =/sg' $files
    # shellcheck disable=SC2086
    perl -i -p0e 's/\n\t\t\t\t\t\t(?!return )(?P<type>(\w+ )+\**)(?>(?P<variable>\w+)( = *[\w>\s\n-]*?)?;\n(?P<code_between>(?>(?P<comment_or_string_or_not_preprocessor>\/\*.*?\*\/|"(?>\\"|.)*?"|[^#]))*?)(\t\t\t\t\t\t)?(?=\b(?P=variable)\b))(?<=\n\t\t\t\t\t\t)(?P=variable) =(?![^;]*?[^>_]\b(?P=variable)\b[^_])/\n$+{code_between}\t\t\t\t\t\t$+{type}$+{variable} =/sg' $files
    git diff --quiet && break;
    git add .;
done
