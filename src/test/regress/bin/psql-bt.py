#!/usr/bin/env python

import subprocess, sys, os

args = " ".join(map(lambda s: '"%s"' % s if ' ' in s else s, sys.argv[1:]))

psql = subprocess.Popen([args + " 2>&1"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=os.environ, shell=True)

psql.stdin.write("SELECT pg_backend_pid();\n")
psql.stdout.readline() # SELECT pg_backend_pid();
psql.stdout.readline() # pg_backend_pid
psql.stdout.readline() # -----------------------
pgpid = int(psql.stdout.readline().strip())
psql.stdout.readline() # (1 row)
psql.stdout.readline() # (empty line)

gdb = subprocess.Popen(['sudo', 'gdb', '-batch', '-ex', 'c', '-ex', 'bt', '--pid', str(pgpid)], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

gdb.stdout.readline()

for line in sys.stdin.readlines():
    psql.stdin.write(line)
psql.stdin.close()

while psql.returncode is None:
    psql.poll()

sys.stdout.write(''.join(psql.stdout.readlines()))

ret = psql.returncode

if ret:
    sys.stdout.write(''.join(gdb.stdout.readlines()))
    sys.stdout.write(''.join(gdb.stderr.readlines()))

exit(ret)
