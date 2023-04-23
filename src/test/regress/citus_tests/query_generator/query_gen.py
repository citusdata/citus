import random

from node_defs import RTEType
from random_selections import (
    randomJoinOp,
    randomRestrictOp,
    randomRteType,
    shouldSelectThatBranch,
)

from config.config import getAllTableNames, getConfig, getMaxAllowedCountForTable

# query_gen.py generates a new query from grammar rules below. It randomly chooses allowed rules
# to generate a query. Here are some important notes about the query generation:
#
# - We enforce the max allowed # of usage for each table. It also enforces global total # of tables.
# - Table names, restriction types and any other selections are chosen between the values provided by
#   configuration file (config.yml).
# - Entry point for the generator is newQuery and all other methods are internal methods. We pass a context
#   object named GeneratorContext to all internal methods as parameter to perform checks and generations
#   on the query via the context.
# - shouldSelectThatBranch() is useful utility method to randomly chose a grammar rule. Some of the rules
#   are only selected if we allowed them in configuration file.
# - We enforce table limits separately if we are inside cte part of the query(see targetCteRteCount).
#   We also enforce max # of ctes for a query.
# - commonColName from the config is used at select and where clauses.
# - useAvgAtTopLevelTarget is useful to return single row as query result. It is also useful to track Citus
#   query bugs via run_query_compare_test.py.
# - '=' restriction is removed from the config by default to return values different than null most of the time.
# - 'RTE.VALUES' is also removed from the config for the same reason as above.
# - Filter range is selected as same with data range for the same reason as above.
# - aliasStack at GeneratorContext is useful to put correct table names into where clause.

# grammar syntax
#
# ======Assumptions======
# 1. Tables has common dist col
# 2. All operations execute on common column for all tables.
#
# TODO: RTE_FUNCTION, RTE_TABLEFUNC
#
# ====SYNTAX====
# ===Nonterminals===
#   Query
#   SelectExpr
#   FromExpr
#   RestrictExpr
#   RteList
#   Rte
#   SubqueryRte
#   RelationRte
#   JoinList
#   JoinOp
#   Using
#   RestrictList
#   Restrict
#   CteRte
#   CteList
#   Cte
#   ValuesRte
#   Limit
#   OrderBy
#
# ===Terminals===
#   e 'SELECT' 'FROM' 'INNER JOIN' 'LEFT JOIN' 'RIGHT JOIN' 'FULL JOIN' 'WHERE' 'LIMIT' 'USING' 'WITH'
#     'ORDER BY' 'VALUES' 'IN' 'NOT' 'AS' '<' '>' '=' '*' ',' ';' '(' ')'
#
# ===Rules===
# Start -> Query ';' || 'WITH' CteList Query ';'
# Query -> SelectExpr FromExpr [OrderBy] [Limit] || 'SELECT' 'avg(avgsub.DistColName)' 'FROM' SubqueryRte 'AS avgsub'
# SelectExpr -> 'SELECT' 'curAlias()' '.' DistColName
# FromExpr -> 'FROM' (Rte JoinList JoinOp Rte Using || RteList) ['WHERE' 'nextRandomAlias()' '.' DistColName RestrictExpr]
# RestrictExpr -> ('<' || '>' || '=') Int || ['NOT'] 'IN' SubqueryRte
# JoinList ->  JoinOp Rte Using JoinList || e
# Using -> 'USING' '(' DistColName ')'
# RteList -> Rte [, RteList] || Rte
# Rte -> SubqueryRte 'AS' 'nextRandomAlias()' || RelationRte 'AS' 'nextRandomAlias()' ||
#        CteRte 'AS' 'nextRandomAlias()' || ValuesRte 'AS' 'nextRandomAlias()'
# SubqueryRte -> '(' Query ')'
# RelationRte -> 'nextRandomTableName()'
# CteRte -> 'randomCteName()'
# CteList -> Cte [',' CteList] || Cte
# Cte -> 'nextRandomAlias()' 'AS' '(' Query ')'
# ValuesRte -> '(' 'VALUES' '(' 'random()' ')' ')'
# JoinOp -> 'INNER JOIN' || 'LEFT JOIN' || 'RIGHT JOIN' || 'FULL JOIN'
# Limit -> 'LIMIT' 'random()'
# OrderBy -> 'ORDER BY' DistColName
# DistColName -> 'hardwired(get from config)'


class GeneratorContext:
    """context to store some variables which should be available during generation"""

    def __init__(self):
        # each level's last table is used in WHERE clause for the level
        self.aliasStack = []
        # tracks if we are inside cte as we should not refer cte inside cte
        self.insideCte = False
        # total rtes in cte + non-cte parts
        self.totalRteCount = 0
        # rte count in non-cte part to enforce non-cte rte limit
        self.currentRteCount = 0
        # cte count to enforce cte limit
        self.currentCteCount = 0
        # rte count in cte part to enforce rte limit in cte
        self.currentCteRteCount = 0
        # rte counts per table to enforce rte limit per table
        self.perTableRtes = {}
        # tables which hit count limit
        self.disallowedTables = set()
        # useful to track usage avg only at first select
        self.usedAvg = False

    def randomCteName(self):
        """returns a randomly selected cte name"""
        randCteRef = random.randint(0, self.currentCteCount - 1)
        return " cte_" + str(randCteRef)

    def curAlias(self):
        """returns current alias name to be used for the current table"""
        return " table_" + str(self.totalRteCount)

    def curCteAlias(self):
        """returns current alias name to be used for the current cte"""
        return " cte_" + str(self.currentCteCount)

    def hasAnyCte(self):
        """returns if context has any cte"""
        return self.currentCteCount > 0

    def canGenerateNewRte(self):
        """checks if context exceeds allowed rte count"""
        return self.currentRteCount < getConfig().targetRteCount

    def canGenerateNewCte(self):
        """checks if context exceeds allowed cte count"""
        return self.currentCteCount < getConfig().targetCteCount

    def canGenerateNewRteInsideCte(self):
        """checks if context exceeds allowed rte count inside a cte"""
        return self.currentCteRteCount < getConfig().targetCteRteCount

    def addAlias(self, alias):
        """adds given alias to the stack"""
        self.aliasStack.append(alias)

    def removeLastAlias(self):
        """removes the latest added alias from the stack"""
        return self.aliasStack.pop()

    def getRteNameEnforcingRteLimits(self):
        """returns rteName by enforcing rte count limits for tables"""
        # do not enforce per table rte limit if we are inside cte
        if self.insideCte:
            rteName = random.choice(getAllTableNames())
            return " " + rteName + " "

        while True:
            # keep trying to find random table by eliminating the ones which hit table limit
            allowedNames = set(getAllTableNames()) - self.disallowedTables
            assert len(allowedNames) > 0
            rteName = random.choice(list(allowedNames))

            # not yet added to rte count map, so we can allow the name
            if rteName not in self.perTableRtes:
                self.perTableRtes[rteName] = 0
                break
            # limit is not exceeded, so we can allow the name
            if self.perTableRtes[rteName] < getMaxAllowedCountForTable(rteName):
                break
            else:
                self.disallowedTables.add(rteName)

        # increment rte count for the table name
        self.perTableRtes[rteName] += 1
        return " " + rteName + " "


def newQuery():
    """returns generated query"""
    genCtx = GeneratorContext()
    return _start(genCtx)


def _start(genCtx):
    # Query ';' || 'WITH' CteList Query ';'
    query = ""
    if not genCtx.canGenerateNewCte() or shouldSelectThatBranch():
        query += _genQuery(genCtx)
    else:
        genCtx.insideCte = True
        query += " WITH "
        query += _genCteList(genCtx)
        genCtx.insideCte = False
        query += _genQuery(genCtx)
    query += ";"
    return query


def _genQuery(genCtx):
    # SelectExpr FromExpr [OrderBy] [Limit] || 'SELECT' 'avg(avgsub.DistColName)' 'FROM' SubqueryRte 'AS avgsub'
    query = ""
    if (
        getConfig().useAvgAtTopLevelTarget
        and not genCtx.insideCte
        and not genCtx.usedAvg
    ):
        genCtx.usedAvg = True
        query += "SELECT "
        query += "count(*), avg(avgsub." + getConfig().commonColName + ") FROM "
        query += _genSubqueryRte(genCtx)
        query += " AS avgsub"
    else:
        query += _genSelectExpr(genCtx)
        query += _genFromExpr(genCtx)
        choseOrderby = False
        if getConfig().orderby and shouldSelectThatBranch():
            query += _genOrderBy(genCtx)
            choseOrderby = True
        if getConfig().limit and shouldSelectThatBranch():
            if not choseOrderby and getConfig().forceOrderbyWithLimit:
                query += _genOrderBy(genCtx)
            query += _genLimit(genCtx)
    return query


def _genOrderBy(genCtx):
    # 'ORDER BY' DistColName
    query = ""
    query += " ORDER BY "
    query += getConfig().commonColName + " "
    return query


def _genLimit(genCtx):
    # 'LIMIT' 'random()'
    query = ""
    query += " LIMIT "
    (fromVal, toVal) = getConfig().limitRange
    query += str(random.randint(fromVal, toVal))
    return query


def _genSelectExpr(genCtx):
    # 'SELECT' 'curAlias()' '.' DistColName
    query = ""
    query += " SELECT "
    commonColName = getConfig().commonColName
    query += genCtx.curAlias() + "." + commonColName + " "

    return query


def _genFromExpr(genCtx):
    # 'FROM' (Rte JoinList JoinOp Rte Using || RteList) ['WHERE' 'nextRandomAlias()' '.' DistColName RestrictExpr]
    query = ""
    query += " FROM "

    if shouldSelectThatBranch():
        query += _genRte(genCtx)
        query += _genJoinList(genCtx)
        query += randomJoinOp()
        query += _genRte(genCtx)
        query += _genUsing(genCtx)
    else:
        query += _genRteList(genCtx)

    alias = genCtx.removeLastAlias()
    if shouldSelectThatBranch():
        query += " WHERE "
        query += alias + "." + getConfig().commonColName
        query += _genRestrictExpr(genCtx)
    return query


def _genRestrictExpr(genCtx):
    # ('<' || '>' || '=') Int || ['NOT'] 'IN' '(' SubqueryRte ')'
    query = ""
    if (
        not getConfig().semiAntiJoin
        or not genCtx.canGenerateNewRte()
        or shouldSelectThatBranch()
    ):
        query += randomRestrictOp()
        (fromVal, toVal) = getConfig().filterRange
        query += str(random.randint(fromVal, toVal))
    else:
        if shouldSelectThatBranch():
            query += " NOT"
        query += " IN "
        query += _genSubqueryRte(genCtx)
    return query


def _genCteList(genCtx):
    # Cte [',' CteList] || Cte
    query = ""

    if shouldSelectThatBranch():
        query += _genCte(genCtx)
        if not genCtx.canGenerateNewCte():
            return query
        query += ","
        query += _genCteList(genCtx)
    else:
        query += _genCte(genCtx)
    return query


def _genCte(genCtx):
    # 'nextRandomAlias()' 'AS' '(' Query ')'
    query = ""
    query += genCtx.curCteAlias()
    genCtx.currentCteCount += 1
    query += " AS "
    query += " ( "
    query += _genQuery(genCtx)
    query += " ) "
    return query


def _genRteList(genCtx):
    # RteList -> Rte [, RteList] || Rte
    query = ""
    if getConfig().cartesianProduct and shouldSelectThatBranch():
        query += _genRte(genCtx)
        if not genCtx.canGenerateNewRte():
            return query
        if genCtx.insideCte and not genCtx.canGenerateNewRteInsideCte():
            return query
        query += ","
        query += _genRteList(genCtx)
    else:
        query += _genRte(genCtx)
    return query


def _genJoinList(genCtx):
    # JoinOp Rte Using JoinList || e
    query = ""

    if shouldSelectThatBranch():
        if not genCtx.canGenerateNewRte():
            return query
        if genCtx.insideCte and not genCtx.canGenerateNewRteInsideCte():
            return query
        query += randomJoinOp()
        query += _genRte(genCtx)
        query += _genUsing(genCtx)
        query += _genJoinList(genCtx)
    return query


def _genUsing(genCtx):
    # 'USING' '(' DistColName ')'
    query = ""
    query += " USING (" + getConfig().commonColName + " ) "
    return query


def _genRte(genCtx):
    # SubqueryRte as 'nextRandomAlias()' || RelationRte as 'curAlias()' ||
    # CteRte as 'curAlias()' || ValuesRte 'AS' 'nextRandomAlias()'
    alias = genCtx.curAlias()
    modifiedAlias = None

    if genCtx.insideCte:
        genCtx.currentCteRteCount += 1
    else:
        genCtx.currentRteCount += 1
    genCtx.totalRteCount += 1

    # donot dive into recursive subquery further if we hit into rte limit, replace it with relation rte
    rteType = randomRteType()
    if not genCtx.canGenerateNewRte():
        rteType = RTEType.RELATION

    # donot dive into recursive subquery further if we hit into rte in cte limit, replace it with relation rte
    if genCtx.insideCte and not genCtx.canGenerateNewRteInsideCte():
        rteType = RTEType.RELATION

    # we cannot refer to cte if we are inside it or we donot have any cte
    if (genCtx.insideCte or not genCtx.hasAnyCte()) and rteType == RTEType.CTE:
        rteType = RTEType.RELATION

    query = ""
    if rteType == RTEType.SUBQUERY:
        query += _genSubqueryRte(genCtx)
    elif rteType == RTEType.RELATION:
        query += _genRelationRte(genCtx)
    elif rteType == RTEType.CTE:
        query += _genCteRte(genCtx)
    elif rteType == RTEType.VALUES:
        query += _genValuesRte(genCtx)
        modifiedAlias = alias + "(" + getConfig().commonColName + ") "
    else:
        raise BaseException("unknown RTE type")

    query += " AS "
    query += alias if not modifiedAlias else modifiedAlias
    genCtx.addAlias(alias)

    return query


def _genSubqueryRte(genCtx):
    # '(' Query ')'
    query = ""
    query += " ( "
    query += _genQuery(genCtx)
    query += " ) "
    return query


def _genRelationRte(genCtx):
    # 'randomAllowedTableName()'
    query = ""
    query += genCtx.getRteNameEnforcingRteLimits()
    return query


def _genCteRte(genCtx):
    # 'randomCteName()'
    query = ""
    query += genCtx.randomCteName()
    return query


def _genValuesRte(genCtx):
    # '( VALUES(random()) )'
    query = ""
    (fromVal, toVal) = getConfig().dataRange
    query += " ( VALUES(" + str(random.randint(fromVal, toVal)) + " ) ) "
    return query
