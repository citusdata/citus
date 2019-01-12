FROM buildpack-deps:stretch

COPY files /tmp

RUN /tmp/install-builddeps

USER builder

WORKDIR /home/builder

RUN /tmp/build-pgdebs
