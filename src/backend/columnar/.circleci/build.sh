#!/bin/bash

set -euxo pipefail
IFS=$'\n\t'

status=0

basedir="$(pwd)"
installdir="${basedir}/install-${PG_MAJOR}"

make install DESTDIR="${installdir}"
pushd "${installdir}"
find . -type f -print > "${basedir}/files.lst"
cat "${basedir}/files.lst"
tar cvf "${basedir}/install-${PG_MAJOR}.tar" $(cat "${basedir}/files.lst")
popd
