set -euo pipefail
grep -o -E "(\.*\"citus.\w+\")," src/backend/distributed/shared_library_init.c > gucs.out
sort -c gucs.out
