#!/usr/bin/env python3
"""
easy command line to run against all citus-style checked files:

$ git ls-files \
  | git check-attr --stdin citus-style \
  | grep 'citus-style: set' \
  | awk '{print $1}' \
  | cut -d':' -f1 \
  | xargs -n1 ./ci/include_grouping.py
"""

import collections
import os
import sys


def main(args):
    if len(args) < 2:
        print("Usage: include_grouping.py <file>")
        return

    file = args[1]
    if not os.path.isfile(file):
        sys.exit(f"File '{file}' does not exist")

    with open(file, "r") as in_file:
        with open(file + ".tmp", "w") as out_file:
            includes = []
            skipped_lines = []

            # This calls print_sorted_includes on a set of consecutive #include lines.
            # This implicitly keeps separation of any #include lines that are contained in
            # an #ifdef, because it will order the #include lines inside and after the
            # #ifdef completely separately.
            for line in in_file:
                # if a line starts with #include we don't want to print it yet, instead we
                # want to collect all consecutive #include lines
                if line.startswith("#include"):
                    includes.append(line)
                    skipped_lines = []
                    continue

                # if we have collected any #include lines, we want to print them sorted
                # before printing the current line. However, if the current line is empty
                # we want to perform a lookahead to see if the next line is an #include.
                # To maintain any separation between #include lines and their subsequent
                # lines we keep track of all lines we have skipped inbetween.
                if len(includes) > 0:
                    if len(line.strip()) == 0:
                        skipped_lines.append(line)
                        continue

                    # we have includes that need to be grouped before printing the current
                    # line.
                    print_sorted_includes(includes, file=out_file)
                    includes = []

                    # print any skipped lines
                    print("".join(skipped_lines), end="", file=out_file)
                    skipped_lines = []

                print(line, end="", file=out_file)

    # move out_file to file
    os.rename(file + ".tmp", file)


def print_sorted_includes(includes, file=sys.stdout):
    default_group_key = 1
    groups = collections.defaultdict(set)

    # define the groups that we separate correctly. The matchers are tested in the order
    # of their priority field. The first matcher that matches the include is used to
    # assign the include to a group.
    # The groups are printed in the order of their group_key.
    matchers = [
        {
            "name": "system includes",
            "matcher": lambda x: x.startswith("<"),
            "group_key": -2,
            "priority": 0,
        },
        {
            "name": "toplevel postgres includes",
            "matcher": lambda x: "/" not in x,
            "group_key": 0,
            "priority": 9,
        },
        {
            "name": "postgres.h",
            "matcher": lambda x: x.strip() in ['"postgres.h"'],
            "group_key": -1,
            "priority": -1,
        },
        {
            "name": "toplevel citus inlcudes",
            "matcher": lambda x: x.strip()
            in [
                '"citus_version.h"',
                '"pg_version_compat.h"',
                '"pg_version_constants.h"',
            ],
            "group_key": 3,
            "priority": 0,
        },
        {
            "name": "columnar includes",
            "matcher": lambda x: x.startswith('"columnar/'),
            "group_key": 4,
            "priority": 1,
        },
        {
            "name": "distributed includes",
            "matcher": lambda x: x.startswith('"distributed/'),
            "group_key": 5,
            "priority": 1,
        },
    ]
    matchers.sort(key=lambda x: x["priority"])

    # throughout our codebase we have some includes where either postgres or citus
    # includes are wrongfully included with the syntax for system includes. Before we
    # try to match those we will change the <> to "" to make them match our system. This
    # will also rewrite the include to the correct syntax.
    common_system_include_error_prefixes = ["<nodes/", "<distributed/"]

    # assign every include to a group
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
        for matcher in matchers:
            if matcher["matcher"](include_content):
                group_key = matcher["group_key"]
                break

        groups[group_key].add(include)

    # iterate over all groups in the natural order of its keys
    for i, group in enumerate(sorted(groups.items())):
        if i > 0:
            print(file=file)
        includes = group[1]
        print("".join(sorted(includes)), end="", file=file)


if __name__ == "__main__":
    main(sys.argv)
