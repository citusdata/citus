ARG PG_MAJOR

FROM postgres:$PG_MAJOR

COPY files /tmp

RUN set -ex; \
	ln -s /tmp/sbin/* /usr/local/sbin/; \
	ln -s /tmp/bin/* /usr/local/bin; \
	install-testdeps

WORKDIR /home/circleci
