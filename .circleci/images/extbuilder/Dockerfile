FROM buildpack-deps:stretch

COPY files/ /tmp

RUN /tmp/install-extdeps

USER builder

WORKDIR /home/builder
