import random
import time

from node_defs import RestrictOp

from config.config import getConfig


def shouldSelectThatBranch():
    """returns 0 or 1 randomly"""
    return random.randint(0, 1)


def currentMilliSecs():
    """returns total milliseconds since epoch"""
    return round(time.time() * 1000)


def randomRteType():
    """returns a randomly selected RteType given at config"""
    rtes = getConfig().targetRteTypes
    return random.choice(rtes)


def randomJoinOp():
    """returns a randomly selected JoinOp given at config"""
    joinTypes = getConfig().targetJoinTypes
    return " " + random.choice(joinTypes).name + " JOIN"


def randomRestrictOp():
    """returns a randomly selected RestrictOp given at config"""
    restrictOps = getConfig().targetRestrictOps
    restrictOp = random.choice(restrictOps)
    opText = ""
    if restrictOp == RestrictOp.LT:
        opText = "<"
    elif restrictOp == RestrictOp.GT:
        opText = ">"
    elif restrictOp == RestrictOp.EQ:
        opText = "="
    else:
        raise BaseException("Unknown restrict op")

    return " " + opText + " "
