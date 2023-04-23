import signal
import sys

from data_gen import getTableData
from ddl_gen import getTableDDLs
from query_gen import newQuery

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
    ddlFileName = "out/" + getConfig().ddlOutFile
    with open(ddlFileName, "w") as ddlFile:
        ddlFile.writelines([ddls, data])

    queryCount = getConfig().queryCount
    fileName = "out/" + getConfig().queryOutFile
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

    resetConfig()

    ddls = getTableDDLs()
    data = getTableData()

    if getConfig().interactiveMode:
        _interactiveMode(ddls, data)
    else:
        _fileMode(ddls, data)
