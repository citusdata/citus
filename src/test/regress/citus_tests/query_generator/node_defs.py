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
    HASH_DISTRIBUTED = 1
    SINGLE_SHARD_DISTRIBUTED = 2
    REFERENCE = 3
    POSTGRES = 4


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
        colocateWith,
    ):
        self.name = name
        self.citusType = citusType
        self.maxAllowedUseOnQuery = maxAllowedUseOnQuery
        self.rowCount = rowCount
        self.nullRate = nullRate
        self.duplicateRate = duplicateRate
        self.columns = columns
        self.distinctCopyCount = distinctCopyCount
        self.colocateWith = colocateWith


class Column:
    def __init__(self, name, type):
        self.name = name
        self.type = type
