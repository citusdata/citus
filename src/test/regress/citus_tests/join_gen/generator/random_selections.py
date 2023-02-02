from config.config import *
from node_defs import *

import random

def shouldSelectThatBranch():
    '''returns 0 or 1 randomly'''
    return random.randint(0, 1)

def randomRteType():
    '''returns a randomly selected RteType given at config'''
    rtes = getConfig().targetRteTypes
    return random.choice(rtes)

def randomJoinOp():
    '''returns a randomly selected JoinOp given at config'''
    joinTypes = getConfig().targetJoinTypes
    return ' ' + random.choice(joinTypes).name + ' JOIN'

def randomRestrictOp():
    '''returns a randomly selected RestrictOp given at config'''
    restrictOps = getConfig().targetRestrictOps
    restrictOp = random.choice(restrictOps)
    opText = ''
    if restrictOp == RestrictOp.EQ:
        opText = '='
    elif restrictOp == RestrictOp.LT:
        opText = '<'
    elif restrictOp == RestrictOp.GT:
        opText = '>'
    else:
        raise BaseException('Unknown restrict op')

    return ' ' + opText + ' '

def randomAggregateFunc():
    '''returns a randomly selected aggregate function name given at config'''
    targetAggregateFunctions = getConfig().targetAggregateFunctions
    return random.choice(targetAggregateFunctions)

def randomTableFunc():
    '''returns a randomly selected tablefunc definition given at config'''
    targetTableFuncs = getConfig().targetRteTableFunctions
    return ' ' + random.choice(targetTableFuncs) + ' '

