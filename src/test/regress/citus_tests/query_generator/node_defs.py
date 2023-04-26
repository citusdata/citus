from enum import Enum


class JoinType(Enum):
    INNER = 1
    LEFT = 2
    RIGHT = 3
    FULL = 4


class RTEType(Enum):
    RELATION = 1
    SUBQUERY = 2
    CTE = 3
    VALUES = 4


class RestrictOp(Enum):
    LT = 1
    GT = 2
    EQ = 3


class CitusType(Enum):
    DISTRIBUTED = 1
    REFERENCE = 2
    POSTGRES = 3


class Table:
    def __init__(
        self,
        name,
        citusType,
        maxAllowedUseOnQuery,
        rowCount,
        nullRate,
        duplicateRate,
        columns,
        distinctCopyCount,
    ):
        self.name = name
        self.citusType = citusType
        self.maxAllowedUseOnQuery = maxAllowedUseOnQuery
        self.rowCount = rowCount
        self.nullRate = nullRate
        self.duplicateRate = duplicateRate
        self.columns = columns
        self.distinctCopyCount = distinctCopyCount


class Column:
    def __init__(self, name, type):
        self.name = name
        self.type = type
