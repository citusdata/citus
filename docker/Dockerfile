FROM ubuntu:16.04 AS pgdebbuilder

RUN set -ex; \
    apt-get update; \
    apt-get install -y curl dpkg-dev lsb-release devscripts; \
    curl -sf https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -;

RUN set -ex; \
	echo "deb-src http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main 11" >> /etc/apt/sources.list.d/pgdg.list; \
	apt-get update; \
    apt-get build-dep -y postgresql-11;

RUN useradd -ms /bin/bash builder

USER builder

COPY build_citus_debs install-isolation.mk /home/builder/

RUN /home/builder/build_citus_debs

# vim:set ft=dockerfile:
FROM ubuntu:16.04

COPY --from=pgdebbuilder /home/builder/debs /debs

RUN set -ex; \
	if ! command -v gpg > /dev/null; then \
		apt-get update; \
		apt-get install -y --no-install-recommends \
			gnupg \
			dirmngr \
		; \
		rm -rf /var/lib/apt/lists/*; \
	fi

# explicitly set user/group IDs
RUN set -eux; \
	groupadd -r postgres --gid=999; \
# https://salsa.debian.org/postgresql/postgresql-common/blob/997d842ee744687d99a2b2d95c1083a2615c79e8/debian/postgresql-common.postinst#L32-35
	useradd -r -g postgres --uid=999 --home-dir=/var/lib/postgresql --shell=/bin/bash postgres; \
# also create the postgres user's home directory with appropriate permissions
# see https://github.com/docker-library/postgres/issues/274
	mkdir -p /var/lib/postgresql; \
	chown -R postgres:postgres /var/lib/postgresql

# make the "en_US.UTF-8" locale so postgres will be utf-8 enabled by default
RUN set -eux; \
	if [ -f /etc/dpkg/dpkg.cfg.d/docker ]; then \
# if this file exists, we're likely in "debian:xxx-slim", and locales are thus being excluded so we need to remove that exclusion (since we need locales)
		grep -q '/usr/share/locale' /etc/dpkg/dpkg.cfg.d/docker; \
		sed -ri '/\/usr\/share\/locale/d' /etc/dpkg/dpkg.cfg.d/docker; \
		! grep -q '/usr/share/locale' /etc/dpkg/dpkg.cfg.d/docker; \
	fi; \
	apt-get update; apt-get install -y locales; rm -rf /var/lib/apt/lists/*; \
	localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG en_US.utf8

# install "nss_wrapper" in case we need to fake "/etc/passwd" and "/etc/group" (especially for OpenShift)
# https://github.com/docker-library/postgres/issues/359
# https://cwrap.org/nss_wrapper.html
RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends libnss-wrapper; \
	rm -rf /var/lib/apt/lists/*

RUN mkdir /docker-entrypoint-initdb.d

RUN set -ex; \
# pub   4096R/ACCC4CF8 2011-10-13 [expires: 2019-07-02]
#       Key fingerprint = B97B 0AFC AA1A 47F0 44F2  44A0 7FCC 7D46 ACCC 4CF8
# uid                  PostgreSQL Debian Repository
	apt-get update; \
	apt-get install -y --no-install-recommends ca-certificates curl; \
	export GNUPGHOME="$(mktemp -d)"; \
    curl -sf https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -; \
	rm -rf "$GNUPGHOME"; \
	apt-get purge -y --auto-remove ca-certificates curl; \
	apt-key list

ENV PG_MAJOR 11
ENV PG_VERSION 11.1-1.pgdg16.04+1

RUN set -ex; \
	\
# see note below about "*.pyc" files
	export PYTHONDONTWRITEBYTECODE=1; \
	\
	dpkgArch="$(dpkg --print-architecture)"; \
	case "$dpkgArch" in \
		amd64|i386|ppc64el) \
# arches officialy built by upstream
			echo "deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main $PG_MAJOR" > /etc/apt/sources.list.d/pgdg.list; \
			apt-get update; \
			;; \
		*) \
# we're on an architecture upstream doesn't officially build for
# let's build binaries from their published source packages
			echo "deb-src http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main $PG_MAJOR" > /etc/apt/sources.list.d/pgdg.list; \
			\
			case "$PG_MAJOR" in \
				9.* | 10 ) ;; \
				*) \
# https://github.com/docker-library/postgres/issues/484 (clang-6.0 required, only available in stretch-backports)
# TODO remove this once we hit buster+
					echo 'deb http://deb.debian.org/debian stretch-backports main' >> /etc/apt/sources.list.d/pgdg.list; \
					;; \
			esac; \
			\
			tempDir="$(mktemp -d)"; \
			cd "$tempDir"; \
			\
			savedAptMark="$(apt-mark showmanual)"; \
			\
# build .deb files from upstream's source packages (which are verified by apt-get)
			apt-get update; \
			apt-get build-dep -y \
				postgresql-common pgdg-keyring \
				"postgresql-$PG_MAJOR=$PG_VERSION" \
			; \
			DEB_BUILD_OPTIONS="nocheck parallel=$(nproc)" \
				apt-get source --compile \
					postgresql-common pgdg-keyring \
					"postgresql-$PG_MAJOR=$PG_VERSION" \
			; \
# we don't remove APT lists here because they get re-downloaded and removed later
			\
# reset apt-mark's "manual" list so that "purge --auto-remove" will remove all build dependencies
# (which is done after we install the built packages so we don't have to redownload any overlapping dependencies)
			apt-mark showmanual | xargs apt-mark auto > /dev/null; \
			apt-mark manual $savedAptMark; \
			\
# create a temporary local APT repo to install from (so that dependency resolution can be handled by APT, as it should be)
			ls -lAFh; \
			dpkg-scanpackages . > Packages; \
			grep '^Package: ' Packages; \
			echo "deb [ trusted=yes ] file://$tempDir ./" > /etc/apt/sources.list.d/temp.list; \
# work around the following APT issue by using "Acquire::GzipIndexes=false" (overriding "/etc/apt/apt.conf.d/docker-gzip-indexes")
#   Could not open file /var/lib/apt/lists/partial/_tmp_tmp.ODWljpQfkE_._Packages - open (13: Permission denied)
#   ...
#   E: Failed to fetch store:/var/lib/apt/lists/partial/_tmp_tmp.ODWljpQfkE_._Packages  Could not open file /var/lib/apt/lists/partial/_tmp_tmp.ODWljpQfkE_._Packages - open (13: Permission denied)
			apt-get -o Acquire::GzipIndexes=false update; \
			;; \
	esac; \
	\
	apt-get install -y postgresql-common; \
	sed -ri 's/#(create_main_cluster) .*$/\1 = false/' /etc/postgresql-common/createcluster.conf; \
	apt-get install -y \
		"postgresql-$PG_MAJOR=$PG_VERSION" \
	; \
	savedAptMark="$(apt-mark showmanual)"; \
	apt-get install -y --no-install-recommends gdebi-core; \
# reset apt-mark's "manual" list so that "purge --auto-remove" will remove all gdebi dependencies
# (which is done after we install the built packages so we don't have to redownload any overlapping dependencies)
	apt-mark showmanual | xargs apt-mark auto > /dev/null; \
	apt-mark manual $savedAptMark; \
    gdebi --non-interactive /debs/postgresql-server-dev-$PG_MAJOR*.deb; \
	apt-get install -y --no-install-recommends \
		autotools-dev \
		build-essential \
		ca-certificates \
		git \
		libssl-dev \
		libkrb5-dev \
		libicu-dev \
		libcurl4-openssl-dev \
		ssh; \
	\
	rm -rf /var/lib/apt/lists/*; \
	\
# we have leftovers from building, let's purge them (including extra, unnecessary build deps)
	apt-get purge -y --auto-remove; \
	rm -rf "$tempDir" /etc/apt/sources.list.d/temp.list; \
	\
# some of the steps above generate a lot of "*.pyc" files (and setting "PYTHONDONTWRITEBYTECODE" beforehand doesn't propagate properly for some reason), so we clean them up manually (as long as they aren't owned by a package)
	find /usr -name '*.pyc' -type f -exec bash -c 'for pyc; do dpkg -S "$pyc" &> /dev/null || rm -vf "$pyc"; done' -- '{}' +

# make the sample config easier to munge (and "correct by default")
RUN set -eux; \
	dpkg-divert --add --rename --divert "/usr/share/postgresql/postgresql.conf.sample.dpkg" "/usr/share/postgresql/$PG_MAJOR/postgresql.conf.sample"; \
	cp -v /usr/share/postgresql/postgresql.conf.sample.dpkg /usr/share/postgresql/postgresql.conf.sample; \
	ln -sv ../postgresql.conf.sample "/usr/share/postgresql/$PG_MAJOR/"; \
	sed -ri "s!^#?(listen_addresses)\s*=\s*\S+.*!\1 = '*'!" /usr/share/postgresql/postgresql.conf.sample; \
	grep -F "listen_addresses = '*'" /usr/share/postgresql/postgresql.conf.sample

RUN mkdir -p /var/run/postgresql && chown -R postgres:postgres /var/run/postgresql && chmod 2777 /var/run/postgresql

ENV PATH $PATH:/usr/lib/postgresql/$PG_MAJOR/bin

RUN apt-get update; \
	apt-get install -y --no-install-recommends sudo; \
	rm -rf /var/lib/apt/lists/*; \
	echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers; \
	usermod -aG sudo postgres

USER postgres
