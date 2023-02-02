from node_defs import *

def parseJoinType(joinTypeText):
    return JoinType[joinTypeText]

def parseJoinTypeArray(joinTypeTexts):
    joinTypes = []
    for joinTypeText in joinTypeTexts:
        joinType = parseJoinType(joinTypeText)
        joinTypes.append(joinType)
    return joinTypes

def parseRteType(rteTypeText):
    return RTEType[rteTypeText]

def parseRteTypeArray(rteTypeTexts):
    rteTypes = []
    for rteTypeText in rteTypeTexts:
        rteType = parseRteType(rteTypeText)
        rteTypes.append(rteType)
    return rteTypes

def parseRestrictOp(restrictOpText):
        return RestrictOp[restrictOpText]

def parseRestrictOpArray(restrictOpTexts):
    restrictOps = []
    for restrictOpText in restrictOpTexts:
        restrictOp = parseRestrictOp(restrictOpText)
        restrictOps.append(restrictOp)
    return restrictOps

def parseTable(targetTableDict):
        name = targetTableDict['name']
        citusType = CitusType[targetTableDict['citusType']]
        maxCount = targetTableDict['maxCount']
        rowCount = targetTableDict['rowCount']
        nullRate = targetTableDict['nullRate']
        duplicateRate = targetTableDict['duplicateRate']
        useRandom = targetTableDict['useRandom']
        columns = []
        for columnDict in targetTableDict['columns']:
            col = parseColumn(columnDict)
            columns.append(col)
        dupCount = targetTableDict['dupCount']
        return Table(name, citusType, maxCount, rowCount,
                     nullRate, duplicateRate, useRandom,
                     columns, dupCount)

def parseTableArray(targetTableDicts):
    tables = []
    for targetTableDict in targetTableDicts:
        table = parseTable(targetTableDict['Table'])
        tables.append(table)
    return tables

def parseColumn(targetColumnDict):
    name = targetColumnDict['name']
    type = targetColumnDict['type']
    return Column(name, type)

def parseRange(rangeDict):
    fromVal = rangeDict['from']
    toVal = rangeDict['to']
    return (fromVal, toVal)
