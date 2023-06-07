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
        if table.citusType not in (
            CitusType.HASH_DISTRIBUTED,
            CitusType.SINGLE_SHARD_DISTRIBUTED,
        ):
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
    return f"INSERT INTO {tableName} SELECT i FROM generate_series({startVal}, {startVal + rowCount}) i;"


def _genNullData(tableName, nullCount):
    """returns string to fill table with NULLs"""
    return (
        f"INSERT INTO {tableName} SELECT NULL FROM generate_series(0, {nullCount}) i;"
    )


def _genDupData(tableName, dupRowCount):
    """returns string to fill table with duplicate integers which are fetched from given table"""
    return f"INSERT INTO {tableName} SELECT * FROM {tableName} ORDER BY {getConfig().commonColName} LIMIT {dupRowCount};"


def _genNonOverlappingData(tableName, startVal, tableIdx):
    """returns string to fill table with different integers for given table"""
    startVal = startVal + tableIdx * 20
    endVal = startVal + 20
    return f"INSERT INTO {tableName} SELECT i FROM generate_series({startVal}, {endVal}) i;"
