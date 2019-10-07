#!/bin/bash


citus_old_version=$1

base="$(pwd)"


install_citus_and_tar() {
  # do everything in a subdirectory to avoid clutter in current directory
  mkdir -p "${builddir}" && cd "${builddir}" || exit

  "${citus_dir}/configure" --without-libcurl

  installdir="${builddir}/install"
  make "-j$(nproc)" && mkdir -p "${installdir}" && make DESTDIR="${installdir}" install

  cd "${installdir}" && find . -type f -print > "${builddir}/files.lst"
  tar cvf "${basedir}/install-citus${citus_version}.tar" `cat ${builddir}/files.lst`
  mv "${basedir}/install-citus${citus_version}.tar" "${base}/install-citus${citus_version}.tar"

  cd "${builddir}" && rm -rf install files.lst && make clean
}

build_current() {
  citus_version="$1"
  basedir="${base}/${citus_version}"
  
  mkdir -p "${basedir}"
  cd "${basedir}" || exit
  citus_dir="${base}/../../../.."  
  builddir="${basedir}/build"

  make -C "${citus_dir}" clean

  install_citus_and_tar

}

build_ext() {
  citus_version="$1"
  basedir="${base}/${citus_version}"
  
  mkdir -p "${basedir}"
  cd "${basedir}" || exit
  citus_dir=${basedir}/citus_$citus_version
  git clone --branch "$citus_version" https://github.com/citusdata/citus.git --depth 1 citus_"$citus_version"  
  builddir="${basedir}/build"

  install_citus_and_tar
}

build_current "master"
build_ext "${citus_old_version}"
