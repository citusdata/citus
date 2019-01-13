ARG PG_MAJOR

FROM postgres:$PG_MAJOR

COPY files /tmp

RUN /tmp/install-testdeps

COPY --chown=circleci:circleci workspace /home/circleci/workspace

ENTRYPOINT ["/tmp/test-ext"]
