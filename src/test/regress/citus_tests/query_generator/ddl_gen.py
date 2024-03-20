from config.config import (
    getConfig,
    isTableHashDistributed,
    isTableReference,
    isTableSingleShardDistributed,
)


def getTableDDLs():
    ddls = ""
    tables = getConfig().targetTables
    for table in tables:
        ddls += _genTableDDL(table)
        ddls += "\n"
    return ddls


def _genTableDDL(table):
    ddl = ""
    ddl += f"DROP TABLE IF EXISTS {table.name};\n"
    ddl += f"CREATE TABLE {table.name}("
    for column in table.columns[:-1]:
        ddl += _genColumnDDL(column)
        ddl += ",\n"
    if len(table.columns) > 0:
        ddl += _genColumnDDL(table.columns[-1])
    ddl += ");\n"

    if isTableHashDistributed(table):
        ddl += f"SELECT create_distributed_table('{table.name}','{getConfig().commonColName}', colocate_with=>'{table.colocateWith}');\n"
    if isTableSingleShardDistributed(table):
        ddl += f"SELECT create_distributed_table('{table.name}', NULL, colocate_with=>'{table.colocateWith}');\n"
    elif isTableReference(table):
        ddl += f"SELECT create_reference_table('{table.name}');\n"
    return ddl


def _genColumnDDL(column):
    return f"{column.name} {column.type}"
