import os

fileDir = os.path.dirname(os.path.realpath('__file__'))
filename = os.path.join(fileDir, 'src/backend/distributed/shared_library_init.c')
shared_library_init = open(filename, 'r')
lines = shared_library_init.readlines()
getnextline = False
previous_guc = ''
for line in lines:
    if getnextline:
        if line < previous_guc:
            exit(1)
        previous_guc = line
        getnextline = False
    if 'DefineCustom' in line:
        getnextline = True
