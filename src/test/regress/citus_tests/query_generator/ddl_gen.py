from config.config import getConfig, isTableDistributed, isTableReference


def getTableDDLs():
    ddls = ""
    tables = getConfig().targetTables
    for table in tables:
        ddls += _genTableDDL(table)
        ddls += "\n"
    return ddls


def _genTableDDL(table):
    ddl = ""
    ddl += "DROP TABLE IF EXISTS " + table.name + ";"
    ddl += "\n"

    ddl += "CREATE TABLE " + table.name + "("
    for column in table.columns[:-1]:
        ddl += _genColumnDDL(column)
        ddl += ",\n"
    if len(table.columns) > 0:
        ddl += _genColumnDDL(table.columns[-1])
    ddl += ");\n"

    if isTableDistributed(table):
        ddl += (
            "SELECT create_distributed_table("
            + "'"
            + table.name
            + "','"
            + getConfig().commonColName
            + "'"
            + ");"
        )
        ddl += "\n"
    elif isTableReference(table):
        ddl += "SELECT create_reference_table(" + "'" + table.name + "'" + ");"
        ddl += "\n"
    return ddl


def _genColumnDDL(column):
    ddl = ""
    ddl += column.name
    ddl += " "
    ddl += column.type
    return ddl
