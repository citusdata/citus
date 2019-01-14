# buildpack-deps:stretch
FROM buildpack-deps@sha256:f73faa32ec26ab3679b6eaca60df6b02367e04d6cec3a381342ea9345968f0de

COPY files /tmp

RUN set -ex; \
	ln -s /tmp/sbin/* /usr/local/sbin/; \
	ln -s /tmp/bin/* /usr/local/bin/; \
	ln -s /tmp/etc/* /etc/dpkg/dpkg.cfg.d/; \
	install-extdeps

USER circleci
WORKDIR /home/circleci
