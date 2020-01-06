#!/bin/bash

set -euxo pipefail

citus_old_version=$1

base="$(pwd)"


install_citus_and_tar() {
  # do everything in a subdirectory to avoid clutter in current directory
  mkdir -p "${builddir}" && cd "${builddir}"

  "${citus_dir}/configure" --without-libcurl

  installdir="${builddir}/install"
  make "-j$(nproc)" && mkdir -p "${installdir}" && make DESTDIR="${installdir}" install

  cd "${installdir}" && find . -type f -print > "${builddir}/files.lst"

  tar cvf "${basedir}/install-citus${citus_version}.tar" $(cat "${builddir}"/files.lst)
  mv "${basedir}/install-citus${citus_version}.tar" "${base}/install-citus${citus_version}.tar"

  cd "${builddir}" && rm -rf install files.lst && make clean
}

build_current() {
  citus_version="$1"
  basedir="${base}/${citus_version}"

  mkdir -p "${basedir}"
  citus_repo="${base}/../../../.."

  cd "$citus_repo" && cp -R . /tmp/citus_copy
  mv /tmp/citus_copy "${basedir}/citus_${citus_version}"
  builddir="${basedir}/build"
  cd "${basedir}"

  citus_dir=${basedir}/citus_$citus_version

  make -C "${citus_dir}" clean
  cd "${citus_dir}"
  ./configure --without-libcurl

  install_citus_and_tar
}

build_ext() {
  citus_version="$1"
  basedir="${base}/${citus_version}"

  if test -f "${base}/install-citus${citus_version}.tar"; then
    return
  fi

  mkdir -p "${basedir}"
  cd "${basedir}"
  citus_dir=${basedir}/citus_$citus_version
  git clone --branch "$citus_version" https://github.com/citusdata/citus.git --depth 1 citus_"$citus_version"
  builddir="${basedir}/build"

  install_citus_and_tar
}

build_current "master"
build_ext "${citus_old_version}"
