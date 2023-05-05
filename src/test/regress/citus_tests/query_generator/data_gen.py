from node_defs import CitusType

from config.config import getConfig


def getTableData():
    dataGenerationSql = ""

    tableIdx = 1
    (fromVal, toVal) = getConfig().dataRange
    tables = getConfig().targetTables
    for table in tables:
        # generate base rows
        dataGenerationSql += _genOverlappingData(table.name, fromVal, table.rowCount)
        dataGenerationSql += "\n"
        dataGenerationSql += _genNonOverlappingData(table.name, toVal, tableIdx)
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


def _genOverlappingData(tableName, startVal, rowCount):
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


def _genNonOverlappingData(tableName, startVal, tableIdx):
    """returns string to fill table with different integers for given table"""
    startVal = startVal + tableIdx * 20
    endVal = startVal + 20
    dataGenerationSql = ""
    dataGenerationSql += "INSERT INTO " + tableName
    dataGenerationSql += (
        " SELECT i FROM generate_series(" + str(startVal) + "," + str(endVal) + ") i;"
    )
    return dataGenerationSql
