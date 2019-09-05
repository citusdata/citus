OLD_BINDIR = 'OLD_BINDIR'
NEW_BINDIR = 'NEW_BINDIR'
TEMP_DIR = 'TEMP_DIR'
NEW_DATADIR = 'NEW_DATADIR'
OLD_DATADIR = 'OLD_DATADIR'
PG_SRCDIR = 'PG_SRCDIR'
BEFORE_UPGRADE_SCHEDULE = './before_upgrade_schedule'
AFTER_UPGRADE_SCHEDULE = './after_upgrade_schedule'

config = {
    OLD_BINDIR : '',
    NEW_BINDIR : '',
    PG_SRCDIR : '',
    TEMP_DIR : '',
    NEW_DATADIR : '',
    OLD_DATADIR : '',
}

def init_config(arguments):
    config[OLD_BINDIR] = arguments['--old-bindir'] 
    config[NEW_BINDIR] = arguments['--new-bindir']  
    config[PG_SRCDIR] = arguments['--postgres-srcdir'] 
    config[TEMP_DIR] = './tmp_upgrade'
    config[NEW_DATADIR] = config[TEMP_DIR] + '/newData'
    config[OLD_DATADIR] = config[TEMP_DIR] + '/oldData'    



USER = 'postgres'
DBNAME = 'postgres'

COORDINATOR_NAME = 'coordinator'
NODE_NAMES = [COORDINATOR_NAME, 'worker1', 'worker2']

WORKER_PORTS = [9701, 9702]
NODE_PORTS = {
    COORDINATOR_NAME: 9700,
    'worker1': 9701,
    'worker2': 9702,
}

