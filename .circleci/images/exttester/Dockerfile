ARG PG_MAJOR

FROM postgres:$PG_MAJOR

COPY files /tmp

RUN set -ex; \
	ln -s /tmp/sbin/* /usr/local/sbin/; \
	install-testdeps

WORKDIR /home/circleci

ENTRYPOINT ["/usr/local/sbin/test-ext"]
