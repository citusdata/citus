FROM ubuntu:22.04 AS base

# environment is to make python pass an interactive shell, probably not the best timezone given a wide variety of colleagues
ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN yes | unminimize

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
    man\
    pkg-config \
    pspg \
    python3 \
    python3-pip \
    sudo \
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

RUN git clone --branch feature/switch https://github.com/thanodnl/pgenv.git .pgenv
COPY --chown=citus:citus pgenv/config/ .pgenv/config/
ENV PATH="/home/citus/.pgenv/bin:${PATH}"
ENV PATH="/home/citus/.pgenv/pgsql/bin:${PATH}"

USER citus

# build postgres versions separately for effective parrallelism and caching of already built versions when changing only certain versions
FROM base AS pg12
RUN pgenv build 12.10
RUN rm .pgenv/src/*.tar*
# TODO remove excessive artifacts for smaller images
RUN find .pgenv/src/ -name "*.o" | xargs rm
#RUN #make -C .pgenv/src/postgresql-*/ clean

# create a staging directory with all files we want to copy from our pgenv build
# we will copy the contents of the staged folder into the final image at once
RUN mkdir .pgenv-staging/
RUN cp -r .pgenv/src .pgenv/pgsql-* .pgenv/config .pgenv-staging/
RUN rm .pgenv-staging/config/default.conf

FROM base AS pg13
RUN pgenv build 13.6
RUN rm .pgenv/src/*.tar*
# TODO remove excessive artifacts for smaller images
RUN find .pgenv/src/ -name "*.o" | xargs rm
#RUN make -C .pgenv/src/postgresql-*/ clean

# create a staging directory with all files we want to copy from our pgenv build
# we will copy the contents of the staged folder into the final image at once
RUN mkdir .pgenv-staging/
RUN cp -r .pgenv/src .pgenv/pgsql-* .pgenv/config .pgenv-staging/
RUN rm .pgenv-staging/config/default.conf

FROM base AS pg14
RUN pgenv build 14.2
RUN rm .pgenv/src/*.tar*
RUN find .pgenv/src/ -name "*.o" | xargs rm
# TODO remove excessive artifacts for smaller images
#RUN make -C .pgenv/src/postgresql-*/ clean

# create a staging directory with all files we want to copy from our pgenv build
# we will copy the contents of the staged folder into the final image at once
RUN mkdir .pgenv-staging/
RUN cp -r .pgenv/src .pgenv/pgsql-* .pgenv/config .pgenv-staging/
RUN rm .pgenv-staging/config/default.conf

FROM base AS uncrustify-builder

RUN sudo apt update && sudo apt install -y cmake tree

WORKDIR /uncrustify
RUN curl -L https://github.com/uncrustify/uncrustify/archive/uncrustify-0.68.1.tar.gz | tar xz
WORKDIR /uncrustify/uncrustify-uncrustify-0.68.1/
RUN mkdir build
WORKDIR /uncrustify/uncrustify-uncrustify-0.68.1/build/
RUN cmake ..
RUN make -sj8

RUN make install DESTDIR=/uncrustify
RUN echo "cachebuster"
RUN tree /uncrustify/usr

# assemble the final container by copying over the artifacts from separately build containers
FROM base AS devcontainer

COPY --from=pg12 /home/citus/.pgenv-staging/ /home/citus/.pgenv/
COPY --from=pg13 /home/citus/.pgenv-staging/ /home/citus/.pgenv/
COPY --from=pg14 /home/citus/.pgenv-staging/ /home/citus/.pgenv/

COPY --from=uncrustify-builder /uncrustify/usr/ /usr/

# add some common tools to the final container
# bin directory for user tools
RUN mkdir .bin
ENV PATH="/home/citus/.bin:${PATH}"

# install citus-dev
RUN git clone https://github.com/citusdata/tools.git citus-tools \
 && pip3 install -r citus-tools/citus_dev/requirements.txt \
 && ln -s /home/citus/citus-tools/citus_dev/citus_dev .bin/ \
 && sudo make -C citus-tools/uncrustify install bindir=/usr/local/bin pkgsysconfdir=/usr/local/etc/

# TODO some LC_ALL errors, possibly solved by locale-gen
RUN git clone https://github.com/so-fancy/diff-so-fancy.git \
 && ln -s /home/citus/diff-so-fancy/diff-so-fancy .bin/

# place to run your cluster with citus_dev
VOLUME /data
RUN sudo mkdir /data \
 && sudo chown citus:citus /data

COPY --chown=citus:citus .psqlrc .

# sets default pg version
RUN pgenv switch latest 14
