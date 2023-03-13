from config.config import *


def getTableData():
    dataGenerationSql = ""

    tableIdx = 1
    (fromVal, toVal) = getConfig().dataRange
    tables = getConfig().targetTables
    for table in tables:
        # generate base rows
        if table.useRandom:
            dataGenerationSql += _genDataRandom(
                table.name, fromVal, toVal, table.rowCount
            )
        else:
            dataGenerationSql += _genData(table.name, fromVal, table.rowCount)
            dataGenerationSql += _genDifferentData(table.name, toVal, tableIdx)
        dataGenerationSql += "\n"

        # generate null rows
        if not table.citusType == CitusType.DISTRIBUTED:
            targetNullRows = int(table.rowCount * table.nullRate)
            dataGenerationSql += _genNullData(table.name, targetNullRows)
            dataGenerationSql += "\n"

        # generate duplicate rows
        targetDuplicateRows = int(table.rowCount * table.duplicateRate)
        dataGenerationSql += _genDupData(table.name, targetDuplicateRows)
        dataGenerationSql += "\n\n"
        tableIdx += 1
    return dataGenerationSql


def _genDataRandom(tableName, fromVal, toVal, rowCount):
    """returns string to fill table with random integers inside given range"""
    dataGenerationSql = ""
    dataGenerationSql += "INSERT INTO " + tableName

    dataRange = toVal - fromVal
    randomData = "({0} + {1} * random())::int".format(str(fromVal), str(dataRange))
    dataGenerationSql += (
        " SELECT " + randomData + " FROM generate_series(0," + str(rowCount) + ") i;"
    )
    return dataGenerationSql


def _genData(tableName, startVal, rowCount):
    """returns string to fill table with [startVal,startVal+rowCount] range of integers"""
    dataGenerationSql = ""
    dataGenerationSql += "INSERT INTO " + tableName
    dataGenerationSql += (
        " SELECT i FROM generate_series("
        + str(startVal)
        + ","
        + str(startVal + rowCount)
        + ") i;"
    )
    return dataGenerationSql


def _genNullData(tableName, nullCount):
    """returns string to fill table with NULLs"""
    dataGenerationSql = ""
    dataGenerationSql += "INSERT INTO " + tableName
    dataGenerationSql += (
        " SELECT NULL FROM generate_series(0," + str(nullCount) + ") i;"
    )
    return dataGenerationSql


def _genDupData(tableName, dupRowCount):
    """returns string to fill table with duplicate integers which are fetched from given table"""
    dataGenerationSql = ""
    dataGenerationSql += "INSERT INTO " + tableName
    dataGenerationSql += (
        " SELECT * FROM "
        + tableName
        + " ORDER BY "
        + getConfig().commonColName
        + " LIMIT "
        + str(dupRowCount)
        + ";"
    )
    return dataGenerationSql


def _genDifferentData(tableName, startVal, tableIdx):
    """returns string to fill table with different integers for given table"""
    startVal = startVal + tableIdx * 5
    endVal = startVal + 20
    dataGenerationSql = ""
    dataGenerationSql += "INSERT INTO " + tableName
    dataGenerationSql += (
        " SELECT i FROM generate_series(" + str(startVal) + "," + str(endVal) + ") i;"
    )
    return dataGenerationSql
