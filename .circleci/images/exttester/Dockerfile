ARG PG_MAJOR

FROM postgres:$PG_MAJOR

COPY files /tmp

RUN /tmp/install-testdeps

COPY --chown=builder:builder workspace /home/builder/workspace

ENTRYPOINT ["/tmp/test-ext"]
