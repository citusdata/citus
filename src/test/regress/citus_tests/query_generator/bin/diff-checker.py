import os
import re
import subprocess
import sys


def createPatternForFailedQueryBlock(acceptableErrors):
    totalAcceptableOrders = len(acceptableErrors)

    failedQueryBlockPattern = "-- queryId: [0-9]+\n.*\npsql:.*(?:"
    for acceptableErrorIdx in range(totalAcceptableOrders):
        failedQueryBlockPattern += acceptableErrors[acceptableErrorIdx]
        if acceptableErrorIdx < totalAcceptableOrders - 1:
            failedQueryBlockPattern += "|"
    failedQueryBlockPattern += ")"

    return failedQueryBlockPattern


def findFailedQueriesFromFile(queryOutFile, acceptableErrors):
    failedQueryIds = []
    outFileContent = ""
    failedQueryBlockPattern = createPatternForFailedQueryBlock(acceptableErrors)
    queryIdPattern = "queryId: ([0-9]+)"
    with open(queryOutFile, "r") as f:
        outFileContent = f.read()
        failedQueryContents = re.findall(failedQueryBlockPattern, outFileContent)
        failedQueryIds = [
            re.search(queryIdPattern, failedQueryContent)[1]
            for failedQueryContent in failedQueryContents
        ]

    return failedQueryIds


def removeFailedQueryOutputFromFile(outFile, failedQueryIds):
    if len(failedQueryIds) == 0:
        return

    distOutFileContentAsLines = []
    with open(outFile, "r") as f:
        distOutFileContentAsLines = f.readlines()

    with open(outFile, "w") as f:
        clear = False
        nextIdx = 0
        nextQueryIdToDelete = failedQueryIds[nextIdx]
        queryIdPattern = "queryId: ([0-9]+)"

        for line in distOutFileContentAsLines:
            matched = re.search(queryIdPattern, line)
            # founded line which contains query id
            if matched:
                # query id matches with the next failed query's id
                if nextQueryIdToDelete == matched[1]:
                    # clear lines until we find succesfull query
                    clear = True
                    nextIdx += 1
                    if nextIdx < len(failedQueryIds):
                        nextQueryIdToDelete = failedQueryIds[nextIdx]
                else:
                    # we found successfull query
                    clear = False

            if not clear:
                f.write(line)
    return


def removeFailedQueryOutputFromFiles(distQueryOutFile, localQueryOutFile):
    # remove the failed distributed query from both local and distributed query files to prevent diff for acceptable errors
    # some of generated queries fails with below errors due to https://github.com/citusdata/citus/issues/6653, we skip those until we support them
    acceptableErrors = [
        "ERROR:  complex joins are only supported when all distributed tables are co-located and joined on their distribution columns",
        "ERROR:  recursive complex joins are only supported when all distributed tables are co-located and joined on their distribution columns",
    ]
    failedDistQueryIds = findFailedQueriesFromFile(distQueryOutFile, acceptableErrors)
    removeFailedQueryOutputFromFile(distQueryOutFile, failedDistQueryIds)
    removeFailedQueryOutputFromFile(localQueryOutFile, failedDistQueryIds)
    return


def showDiffs(distQueryOutFile, localQueryOutFile, diffFile):
    exitCode = 1
    with open(diffFile, "w") as f:
        diffCommand = "diff -u {} {}".format(localQueryOutFile, distQueryOutFile)
        process = subprocess.Popen(diffCommand.split(), stdout=f, stderr=f, shell=False)
        process.wait()
        exitCode = process.returncode

    print("diff exit {}".format(exitCode))
    return exitCode


def exitIfAnyLocalQueryFailed(localQueryOutFile):
    allErrors = ["ERROR:"]
    failedLocalQueryIds = findFailedQueriesFromFile(localQueryOutFile, allErrors)
    assert (
        len(failedLocalQueryIds) == 0
    ), """There might be an internal error related to query generator or
                                              we might find a Postgres bug. Check local_queries.out to see the error."""
    return


if __name__ == "__main__":
    scriptDirPath = os.path.dirname(os.path.abspath(__file__))
    outFolderPath = scriptDirPath + "/../out"
    localQueryOutFile = "{}/local_queries.out".format(outFolderPath)
    distQueryOutFile = "{}/dist_queries.out".format(outFolderPath)
    diffFile = "{}/local_dist.diffs".format(outFolderPath)

    # exit if we have any error from local queries
    exitIfAnyLocalQueryFailed(localQueryOutFile)

    # find failed queries from distQueryOutFile and then remove failed queries and their results from
    # both distQueryOutFile and localQueryOutFile
    removeFailedQueryOutputFromFiles(distQueryOutFile, localQueryOutFile)

    # show diffs in unified format
    exitCode = showDiffs(distQueryOutFile, localQueryOutFile, diffFile)

    sys.exit(exitCode)
