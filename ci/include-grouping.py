#!/usr/bin/env python3
# easy command line to run against all citus-style checked files
# $ git ls-files | git check-attr --stdin citus-style | grep 'citus-style: set' | awk '{print $1}' | cut -d':' -f1 | xargs -n1 ./ci/include-grouping.py


import os
import sys


def main(args):
    if len(args) < 2:
        print("Usage: include-grouping.py <file>")
        return

    file = args[1]
    if not os.path.isfile(file):
        print("File does not exist", file=sys.stderr)
        return sys.exit(1)

    with open(file, "r") as f:
        with open(file + ".tmp", "w") as out_file:
            lines = f.readlines()
            includes = []
            skipped_lines = []
            for line in lines:
                if line.startswith("#include"):
                    includes.append(line)
                    skipped_lines = []
                else:
                    if len(includes) > 0:
                        # regroup all includes by skipping all empty lines while scanning
                        if len(line.strip()) == 0:
                            skipped_lines.append(line)
                            continue
                        print_sorted_includes(includes, file=out_file)
                        includes = []

                    # print skipped lines
                    for skipped_line in skipped_lines:
                        print(skipped_line, end="", file=out_file)
                    skipped_lines = []

                    print(line, end="", file=out_file)

    # move out_file to file
    os.rename(file + ".tmp", file)

    pass


def print_sorted_includes(includes, file=sys.stdout):
    default_group_key = 1
    groups = {}

    matches = [
        {
            "name": "system includes",
            "matcher": lambda x: x.startswith("<"),
            "group_key": -2,
            "priority": 0,
        },
        {
            "name": "naked postgres includes",
            "matcher": lambda x: not "/" in x,
            "group_key": 0,
            "priority": 9,
        },
        {
            "name": "postgres.h",
            "list": ['"postgres.h"'],
            "group_key": -1,
            "priority": -1,
        },
        {
            "name": "naked citus inlcudes",
            "list": ['"citus_version.h"', '"pg_version_compat.h"'],
            "group_key": 3,
            "priority": 0,
        },
        {
            "name": "positional citus includes",
            "list": ['"distributed/pg_version_constants.h"'],
            "group_key": 4,
            "priority": 0,
        },
        {
            "name": "columnar includes",
            "matcher": lambda x: x.startswith('"columnar/'),
            "group_key": 5,
            "priority": 1,
        },
        {
            "name": "distributed includes",
            "matcher": lambda x: x.startswith('"distributed/'),
            "group_key": 6,
            "priority": 1,
        },
    ]

    matches.sort(key=lambda x: x["priority"])

    common_system_include_error_prefixes = ["<nodes/", "<distributed/"]

    for include in includes:
        # extract the group key from the include
        include_content = include.split(" ")[1]

        # fix common system includes which are secretly postgres or citus includes
        for common_prefix in common_system_include_error_prefixes:
            if include_content.startswith(common_prefix):
                include_content = '"' + include_content.strip()[1:-1] + '"'
                include = include.split(" ")[0] + " " + include_content + "\n"
                break

        group_key = default_group_key
        for matcher in matches:
            if "list" in matcher:
                if include_content.strip() in matcher["list"]:
                    group_key = matcher["group_key"]
                    break

            if "matcher" in matcher:
                if matcher["matcher"](include_content):
                    group_key = matcher["group_key"]
                    break

        if group_key not in groups.keys():
            groups[group_key] = []
        groups[group_key].append(include)

    # iterate over all groups in the natural order of its keys
    first = True
    for group_key in sorted(groups.keys()):
        if not first:
            print(file=file)

        first = False
        includes = groups[group_key]
        includes.sort()

        prev = ""
        for include in includes:
            # remove duplicates
            if prev == include:
                continue
            print(include, end="", file=file)
            prev = include


if __name__ == "__main__":
    main(sys.argv)
