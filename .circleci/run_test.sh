#!/bin/bash

set -euxo pipefail
IFS=$'\n\t'

status=0

export PGPORT=${PGPORT:-55432}

function cleanup {
  pg_ctl -D /tmp/postgres stop
  rm -rf /tmp/postgres
}
trap cleanup EXIT

rm -rf /tmp/postgres
initdb -E unicode /tmp/postgres
echo "shared_preload_libraries = 'cstore_fdw'" >> /tmp/postgres/postgresql.conf
pg_ctl -D /tmp/postgres -o "-p ${PGPORT}" -l /tmp/postgres_logfile start || status=$?
if [ -z $status ]; then cat /tmp/postgres_logfile; fi

make "${@}" || status=$?
diffs="regression.diffs"

if test -f "${diffs}"; then cat "${diffs}"; fi

exit $status
