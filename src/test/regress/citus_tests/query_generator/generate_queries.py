#!/usr/bin/env python3

"""generate_queries
Usage:
    generate_queries --seed=<seed>

Options:
    --seed=<seed>                          Seed number used by the query generator.(ex: 123)
"""

import os
import random
import signal
import sys

from data_gen import getTableData
from ddl_gen import getTableDDLs
from docopt import docopt
from query_gen import newQuery
from random_selections import currentMilliSecs

from config.config import getConfig, resetConfig


def _signal_handler(sig, frame):
    sys.exit(0)


def _interactiveMode(ddls, data):
    print(ddls)
    print(data)

    while True:
        res = input("Press x to exit or Enter to generate more")
        if res.lower() == "x":
            print("Exit from query generation mode!")
            sys.exit(0)

        query = newQuery()
        print(query)

        resetConfig()


def _fileMode(ddls, data):
    ddlFileName = (
        f"{os.path.dirname(os.path.abspath(__file__))}/out/{getConfig().ddlOutFile}"
    )
    with open(ddlFileName, "w") as ddlFile:
        ddlFile.writelines([ddls, data])

    queryCount = getConfig().queryCount
    fileName = (
        f"{os.path.dirname(os.path.abspath(__file__))}/out/{getConfig().queryOutFile}"
    )
    with open(fileName, "w") as f:
        # enable repartition joins due to https://github.com/citusdata/citus/issues/6865
        enableRepartitionJoinCommand = "SET citus.enable_repartition_joins TO on;\n"
        queryLines = [enableRepartitionJoinCommand]
        queryId = 1
        for _ in range(queryCount):
            query = newQuery()

            queryLine = "-- queryId: " + str(queryId) + "\n"
            queryLine += query + "\n\n"

            queryLines.append(queryLine)
            queryId += 1
        f.writelines(queryLines)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, _signal_handler)

    arguments = docopt(__doc__, version="generate_queries")
    seed = -1
    if "--seed" in arguments and arguments["--seed"] != "":
        seed = int(arguments["--seed"])
    else:
        seed = currentMilliSecs()
    assert seed > 0

    random.seed(seed)
    print(f"---SEED: {seed} ---")

    resetConfig()

    ddls = getTableDDLs()
    data = getTableData()

    if getConfig().interactiveMode:
        _interactiveMode(ddls, data)
    else:
        _fileMode(ddls, data)
