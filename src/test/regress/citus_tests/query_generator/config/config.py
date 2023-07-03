import copy
import os

import yaml
from config.config_parser import (
    parseJoinTypeArray,
    parseRange,
    parseRestrictOpArray,
    parseRteTypeArray,
    parseTableArray,
)
from node_defs import CitusType


class Config:
    def __init__(self):
        configObj = Config.parseConfigFile(
            f"{os.path.dirname(os.path.abspath(__file__))}/config.yaml"
        )

        self.targetTables = _distinctCopyTables(
            parseTableArray(configObj["targetTables"])
        )
        self.targetJoinTypes = parseJoinTypeArray(configObj["targetJoinTypes"])
        self.targetRteTypes = parseRteTypeArray(configObj["targetRteTypes"])
        self.targetRestrictOps = parseRestrictOpArray(configObj["targetRestrictOps"])
        self.commonColName = configObj["commonColName"]
        self.targetRteCount = configObj["targetRteCount"]
        self.targetCteCount = configObj["targetCteCount"]
        self.targetCteRteCount = configObj["targetCteRteCount"]
        self.semiAntiJoin = configObj["semiAntiJoin"]
        self.cartesianProduct = configObj["cartesianProduct"]
        self.limit = configObj["limit"]
        self.orderby = configObj["orderby"]
        self.forceOrderbyWithLimit = configObj["forceOrderbyWithLimit"]
        self.useAvgAtTopLevelTarget = configObj["useAvgAtTopLevelTarget"]
        self.interactiveMode = configObj["interactiveMode"]
        self.queryOutFile = configObj["queryOutFile"]
        self.ddlOutFile = configObj["ddlOutFile"]
        self.queryCount = configObj["queryCount"]
        self.dataRange = parseRange(configObj["dataRange"])
        self.filterRange = parseRange(configObj["filterRange"])
        self.limitRange = parseRange(configObj["limitRange"])
        self._ensureRteLimitsAreSane()
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

    def _ensureRteLimitsAreSane(self):
        totalAllowedUseForAllTables = 0
        for table in self.targetTables:
            totalAllowedUseForAllTables += table.maxAllowedUseOnQuery
        assert (
            totalAllowedUseForAllTables >= self.targetRteCount
        ), """targetRteCount cannot be greater than sum of maxAllowedUseOnQuery for all tables.
              Set targetRteCount to a lower value or increase maxAllowedUseOnQuery for tables at config.yml."""

    @staticmethod
    def parseConfigFile(path):
        try:
            with open(path, "r") as configFile:
                return yaml.load(configFile, yaml.Loader)
        except IOError as e:
            print(f"I/O error({0}): {1}".format(e.errno, e.strerror))
            raise BaseException("cannot parse config.yaml")
        except Exception:
            print("Unexpected error while parsing config.yml.")


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


def getMaxAllowedCountForTable(tableName):
    tables = getConfig().targetTables
    filtered = filter(lambda el: el.name == tableName, tables)
    filtered = list(filtered)
    assert len(filtered) == 1
    return filtered[0].maxAllowedUseOnQuery


def isTableDistributed(table):
    return table.citusType == CitusType.DISTRIBUTED


def isTableReference(table):
    return table.citusType == CitusType.REFERENCE


def _distinctCopyTables(tables):
    distinctCopyTables = []
    for table in tables:
        distinctCopyCount = table.distinctCopyCount
        for tblIdx in range(1, distinctCopyCount):
            distinctCopyTable = copy.deepcopy(table)
            distinctCopyTable.name += str(tblIdx)
            distinctCopyTables.append(distinctCopyTable)
        table.name += "0"
    tables.extend(distinctCopyTables)
    return tables
