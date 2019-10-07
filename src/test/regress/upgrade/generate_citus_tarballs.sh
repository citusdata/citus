#!/bin/bash


citus_versions=( $1 master )

base="$(pwd)"

build_ext() {
  citus_version="$1"
  basedir="${base}/${citus_version}"
  
  mkdir -p ${basedir}
  cd ${basedir}
  citus_dir=${basedir}/citus_$citus_version
  git clone --branch $citus_version https://github.com/citusdata/citus.git --depth 1 citus_$citus_version  
  builddir="${basedir}/build"

  # do everything in a subdirectory to avoid clutter in current directory
  mkdir -p "${builddir}" && cd "${builddir}"

  "${citus_dir}/configure" --without-libcurl

  installdir="${builddir}/install"
  make && mkdir -p "${installdir}" && make DESTDIR="${installdir}" install

  cd "${installdir}" && find . -type f -print > "${builddir}/files.lst"
  tar cvf "${basedir}/install-citus${citus_version}.tar" `cat ${builddir}/files.lst`
  mv "${basedir}/install-citus${citus_version}.tar" "${base}/install-citus${citus_version}.tar"

  cd "${builddir}" && rm -rf install files.lst && make clean
}
for citus_version in "${citus_versions[@]}"
do 
  build_ext "${citus_version}"
done