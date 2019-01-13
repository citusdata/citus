FROM buildpack-deps:stretch

ARG PG_MAJORS

COPY files /tmp

RUN /tmp/install-builddeps $PG_MAJORS && rm /tmp/install-builddeps

USER builder

RUN /tmp/build-pgdebs $PG_MAJORS

SHELL [ "/bin/bash", "-c"]

RUN cd $HOME && mkdir debs && cp postgresql-*/postgresql-server-dev-*.deb debs
