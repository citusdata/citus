#!/bin/bash

# make bash behave
set -euo pipefail
IFS=$'\n\t'

# shellcheck disable=SC1091
source ci/ci_helpers.sh

# read pg major version, error if not provided
PG_MAJOR=${PG_MAJOR:?please provide the postgres major version}

# get codename from release file
. /etc/os-release
codename=${VERSION#*(}
codename=${codename%)*}

# we'll do everything with absolute paths
basedir="$(pwd)"

# get the project and clear out the git repo (reduce workspace size
rm -rf "${basedir}/.git"

build_ext() {
  pg_major="$1"
  echo "--- Temporarily moving static lib---" >&2
  mv /usr/lib/x86_64-linux-gnu/libpq.a /usr/lib/x86_64-linux-gnu/libpq.a.bak
  ls -la /usr/lib/x86_64-linux-gnu | grep libpq  >&2
  echo "---- ENV----" >&2
  env  >&2
  echo "------ pg_config output -----" >&2
  /usr/lib/postgresql/${pg_major}/bin/pg_config >&2

  pg_config --libs >&2
  file /usr/lib/x86_64-linux-gnu/libpq.so.5.15 >&2
  file /usr/lib/x86_64-linux-gnu/libpq.so >&2

  echo "---- Try libpq link----" >&2
  echo 'int main() { return 0; }' > test.c
  gcc test.c -L/usr/lib/x86_64-linux-gnu -lpq -o test.out -Wl,-v

  builddir="${basedir}/build-${pg_major}"
  echo "Beginning build for PostgreSQL ${pg_major}..." >&2

  # do everything in a subdirectory to avoid clutter in current directory
  mkdir -p "${builddir}" && cd "${builddir}"

  CFLAGS=-Werror LDFLAGS="-Wl,-v" "${basedir}/configure" PG_CONFIG="/usr/lib/postgresql/${pg_major}/bin/pg_config" --enable-coverage --with-security-flags

  installdir="${builddir}/install"
  make V=1 -j$(nproc) && mkdir -p "${installdir}" && { make DESTDIR="${installdir}" install-all || make DESTDIR="${installdir}" install ; }

  cd "${installdir}" && find . -type f -print > "${builddir}/files.lst"
  tar cvf "${basedir}/install-${pg_major}.tar" `cat ${builddir}/files.lst`

  cd "${builddir}" && rm -rf install files.lst && make clean
}

build_ext "${PG_MAJOR}"
