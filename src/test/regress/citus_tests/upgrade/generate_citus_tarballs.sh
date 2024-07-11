#!/bin/bash

set -euxo pipefail

pg_version=$1
citus_version=$2

base="$(pwd)"

# If tarball already exsists we're good
if [ -f "${base}/install-pg${pg_version}-citus${citus_version}.tar" ]; then
    exit 0
fi

basedir="${base}/${citus_version}"

rm -rf "${basedir}"
mkdir -p "${basedir}"
cd "${basedir}"
citus_dir=${basedir}/citus_$citus_version
git clone --branch "$citus_version" https://github.com/citusdata/citus.git --depth 1 citus_"$citus_version"
builddir="${basedir}/build"

# do everything in a subdirectory to avoid clutter in current directory
mkdir -p "${builddir}"
cd "${builddir}"

"${citus_dir}/configure" --without-libcurl

installdir="${builddir}/install"
make "-j$(nproc)"
mkdir -p "${installdir}"
make DESTDIR="${installdir}" install

cd "${installdir}"

find . -type f -print >"${builddir}/files.lst"

tar cvf "${basedir}/install-pg${pg_version}-citus${citus_version}.tar" $(cat "${builddir}"/files.lst)
mv "${basedir}/install-pg${pg_version}-citus${citus_version}.tar" "${base}/install-pg${pg_version}-citus${citus_version}.tar"

cd "${builddir}"
rm -rf install files.lst
make clean

