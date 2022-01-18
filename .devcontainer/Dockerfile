FROM ubuntu:20.04 AS base

# environment is to make python pass an interactive shell, probably not the best timezone given a wide variety of colleagues
ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN apt update && apt install -y \
    autoconf \
    bzip2 \
    curl \
    flex \
    gcc \
    gdb \
    git \
    htop \
    libcurl4-gnutls-dev \
    libicu-dev \
    libkrb5-dev \
    liblz4-dev \
    liblz4-dev \
    libpam0g-dev \
    libreadline-dev \
    libreadline-dev \
    libselinux1-dev \
    libssl-dev \
    libxslt-dev \
    libzstd-dev \
    locales \
    make \
    pspg \
    python3 \
    python3-pip \
    sudo \
    uncrustify \
    uuid-dev \
    vim \
    zlib1g-dev \
 && apt clean

RUN locale-gen en_US.UTF-8

# allow all sudoers to login without a password prompt
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN useradd -ms /bin/bash citus
RUN usermod -aG sudo citus

WORKDIR /home/citus
USER citus

RUN git clone --branch feature/configure https://github.com/thanodnl/pgenv.git .pgenv
ENV PATH="/home/citus/.pgenv/bin:${PATH}"
ENV PATH="/home/citus/.pgenv/pgsql/bin:${PATH}"

COPY --chown=citus:citus configure.flags .pgenv/
# COPY --chown=citus:citus *.tar.bz2 .pgenv/src/

USER citus

# build postgres versions separately for effective parrallelism and caching of already built versions when changing only certain versions
FROM base AS pg12
RUN pgenv build 12.9
RUN rm .pgenv/src/*.tar*
# TODO remove excessive artifacts for smaller images
RUN make -C .pgenv/src/postgresql-*/ clean

FROM base AS pg13
RUN pgenv build 13.5
RUN rm .pgenv/src/*.tar*
# TODO remove excessive artifacts for smaller images
RUN make -C .pgenv/src/postgresql-*/ clean

FROM base AS pg14
RUN pgenv build 14.1
RUN rm .pgenv/src/*.tar*
# TODO remove excessive artifacts for smaller images
RUN make -C .pgenv/src/postgresql-*/ clean

# assemble the final container by copying over the artifacts from separately build containers
FROM base AS devcontainer
COPY --from=pg12 /home/citus/.pgenv/src /home/citus/.pgenv/src
COPY --from=pg12 /home/citus/.pgenv/pgsql-12.9 /home/citus/.pgenv/pgsql-12.9

COPY --from=pg13 /home/citus/.pgenv/src /home/citus/.pgenv/src
COPY --from=pg13 /home/citus/.pgenv/pgsql-13.5 /home/citus/.pgenv/pgsql-13.5

COPY --from=pg14 /home/citus/.pgenv/src /home/citus/.pgenv/src
COPY --from=pg14 /home/citus/.pgenv/pgsql-14.1 /home/citus/.pgenv/pgsql-14.1

# add some common tools to the final container
# bin directory for user tools
RUN mkdir .bin
ENV PATH="/home/citus/.bin:${PATH}"

# install citus-dev
RUN git clone https://github.com/citusdata/tools.git citus-tools
RUN pip3 install -r citus-tools/citus_dev/requirements.txt
RUN ln -s /home/citus/citus-tools/citus_dev/citus_dev .bin/
RUN sudo make -C citus-tools/uncrustify install bindir=/usr/local/bin pkgsysconfdir=/usr/local/etc/

# TODO some LC_ALL errors, possibly solved by locale-gen
RUN git clone https://github.com/so-fancy/diff-so-fancy.git
RUN ln -s /home/citus/diff-so-fancy/diff-so-fancy .bin/

# place to run your cluster with citus_dev
VOLUME /data
RUN sudo mkdir /data
RUN sudo chown citus:citus /data

COPY --chown=citus:citus .psqlrc .

# sets default pg version
RUN pgenv switch 14.1
