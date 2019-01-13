FROM buildpack-deps:stretch

ARG PG_MAJORS

COPY files /tmp

RUN /tmp/install-extdeps $PG_MAJORS && rm /tmp/install-extdeps

USER builder

RUN /tmp/build-ext $PG_MAJORS
