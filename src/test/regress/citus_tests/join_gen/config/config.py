import copy

import yaml
from config.config_parser import *
from node_defs import *


class Config:
    def __init__(self):
        configObj = Config.parseConfigFile("config/config.yaml")

        self.targetTables = _dupTables(parseTableArray(configObj["targetTables"]))
        self.targetJoinTypes = parseJoinTypeArray(configObj["targetJoinTypes"])
        self.targetRteTypes = parseRteTypeArray(configObj["targetRteTypes"])
        self.targetRestrictOps = parseRestrictOpArray(configObj["targetRestrictOps"])
        self.commonColName = configObj["commonColName"]
        self.targetRteCount = configObj["targetRteCount"]
        self.targetCteCount = configObj["targetCteCount"]
        self.targetCteRteCount = configObj["targetCteRteCount"]
        self.targetAggregateFunctions = configObj["targetAggregateFunctions"]
        self.targetRteTableFunctions = configObj["targetRteTableFunctions"]
        self.semiAntiJoin = configObj["semiAntiJoin"]
        self.cartesianProduct = configObj["cartesianProduct"]
        self.limit = configObj["limit"]
        self.orderby = configObj["orderby"]
        self.forceOrderbyWithLimit = configObj["forceOrderbyWithLimit"]
        self.aggregate = configObj["aggregate"]
        self.useAvgAtTopLevelTarget = configObj["useAvgAtTopLevelTarget"]
        self.interactiveMode = configObj["interactiveMode"]
        self.queryOutFile = configObj["queryOutFile"]
        self.ddlOutFile = configObj["ddlOutFile"]
        self.queryCount = configObj["queryCount"]
        self.dataRange = parseRange(configObj["dataRange"])
        self.filterRange = parseRange(configObj["filterRange"])
        self.limitRange = parseRange(configObj["limitRange"])
        # print(self)

    def __repr__(self):
        rep = "targetRteCount: {}\n".format(self.targetRteCount)
        rep += "targetCteCount: {}\n".format(self.targetCteCount)
        rep += "targetCteRteCount: {}\n".format(self.targetCteRteCount)

        rep += "targetRteTypes:\n"
        for rteType in self.targetRteTypes:
            rep += "\t{}\n".format(rteType)

        rep += "targetJoinTypes:\n"
        for joinType in self.targetJoinTypes:
            rep += "\t{}\n".format(joinType)

        rep += "restrictOps:\n"
        for restrictOp in self.targetRestrictOps:
            rep += "\t{}\n".format(restrictOp)

        return rep

    @staticmethod
    def parseConfigFile(path):
        try:
            with open(path, "r") as configFile:
                return yaml.load(configFile, yaml.Loader)
        except:
            raise BaseException("cannot parse config.yaml")


_config = None


def resetConfig():
    global _config
    _config = Config()


def getConfig():
    return _config


def getAllTableNames():
    """returns table names from target tables given at config"""
    tables = getConfig().targetTables
    tableNames = [table.name for table in tables]
    return tableNames


def getMaxCountForTable(tableName):
    tables = getConfig().targetTables
    filtered = filter(lambda el: el.name == tableName, tables)
    filtered = list(filtered)
    assert len(filtered) == 1
    return filtered[0].maxCount


def isTableDistributed(table):
    return table.citusType == CitusType.DISTRIBUTED


def isTableReference(table):
    return table.citusType == CitusType.REFERENCE


def _dupTables(tables):
    dupTables = []
    for table in tables:
        distinctCopyCount = table.distinctCopyCount
        for dupIdx in range(1, distinctCopyCount):
            dupTable = copy.deepcopy(table)
            dupTable.name += str(dupIdx)
            dupTables.append(dupTable)
        table.name += "0"
    tables.extend(dupTables)
    return tables
