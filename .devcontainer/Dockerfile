FROM ubuntu:22.04 AS base

# environment is to make python pass an interactive shell, probably not the best timezone given a wide variety of colleagues
ENV TZ=Europe/Amsterdam
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# install build tools
RUN apt update && apt install -y \
    bzip2 \
    curl \
    flex \
    gcc \
    git \
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
    pkg-config \
    python3 \
    python3-pip \
    sudo \
    uuid-dev \
    zlib1g-dev \
 && apt clean

RUN locale-gen en_US.UTF-8

# add the citus user to sudoers and allow all sudoers to login without a password prompt
RUN useradd -ms /bin/bash citus \
 && usermod -aG sudo citus \
 && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

WORKDIR /home/citus
USER citus

RUN git clone --branch feature/switch https://github.com/thanodnl/pgenv.git .pgenv
COPY --chown=citus:citus pgenv/config/ .pgenv/config/
ENV PATH="/home/citus/.pgenv/bin:${PATH}"
ENV PATH="/home/citus/.pgenv/pgsql/bin:${PATH}"

USER citus

# build postgres versions separately for effective parrallelism and caching of already built versions when changing only certain versions
FROM base AS pg14
RUN pgenv build 14.8
RUN rm .pgenv/src/*.tar*
RUN make -C .pgenv/src/postgresql-*/ clean
RUN make -C .pgenv/src/postgresql-*/src/include install

# create a staging directory with all files we want to copy from our pgenv build
# we will copy the contents of the staged folder into the final image at once
RUN mkdir .pgenv-staging/
RUN cp -r .pgenv/src .pgenv/pgsql-* .pgenv/config .pgenv-staging/
RUN rm .pgenv-staging/config/default.conf

FROM base AS pg15
RUN pgenv build 15.3
RUN rm .pgenv/src/*.tar*
RUN make -C .pgenv/src/postgresql-*/ clean
RUN make -C .pgenv/src/postgresql-*/src/include install

# create a staging directory with all files we want to copy from our pgenv build
# we will copy the contents of the staged folder into the final image at once
RUN mkdir .pgenv-staging/
RUN cp -r .pgenv/src .pgenv/pgsql-* .pgenv/config .pgenv-staging/
RUN rm .pgenv-staging/config/default.conf

FROM base AS pg16
RUN pgenv build 16beta2
RUN rm .pgenv/src/*.tar*
RUN make -C .pgenv/src/postgresql-*/ clean
RUN make -C .pgenv/src/postgresql-*/src/include install

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

FROM base AS pipenv

RUN sudo pip3 install pipenv

WORKDIR /workspaces/citus/
COPY src/ src/
COPY .vscode/Pipfile .vscode/Pipfile.lock .devcontainer/.vscode/
RUN ( cd src/test/regress && pipenv install )
RUN ( cd .devcontainer/.vscode && pipenv install )

# assemble the final container by copying over the artifacts from separately build containers
FROM base AS devcontainer

LABEL org.opencontainers.image.source=https://github.com/citusdata/citus
LABEL org.opencontainers.image.description="Development container for the Citus project"
LABEL org.opencontainers.image.licenses=AGPL-3.0-only

RUN yes | sudo unminimize

# install developer productivity tools
RUN sudo apt update \
 && sudo apt install -y \
    autoconf2.69 \
    bash-completion \
    gdb \
    htop \
    lsof \
    man \
    net-tools \
    pspg \
    vim \
 && sudo apt clean

RUN sudo pip3 install pipenv pipenv-shebang

# add some common tools to the final container
# bin directory for user tools
RUN mkdir .bin
ENV PATH="/home/citus/.bin:${PATH}"

# install citus-dev
RUN git clone --branch feature/citus-dev-pipenv-shebang https://github.com/citusdata/tools.git citus-tools \
 && ( cd citus-tools/citus_dev && pipenv install ) \
 && ln -s /home/citus/citus-tools/citus_dev/citus_dev .bin/ \
 && sudo make -C citus-tools/uncrustify install bindir=/usr/local/bin pkgsysconfdir=/usr/local/etc/

# TODO some LC_ALL errors, possibly solved by locale-gen
RUN git clone https://github.com/so-fancy/diff-so-fancy.git \
 && ln -s /home/citus/diff-so-fancy/diff-so-fancy .bin/

COPY --link --from=uncrustify-builder /uncrustify/usr/ /usr/

COPY --link --from=pg14 /home/citus/.pgenv-staging/ /home/citus/.pgenv/
COPY --link --from=pg15 /home/citus/.pgenv-staging/ /home/citus/.pgenv/
COPY --link --from=pg16 /home/citus/.pgenv-staging/ /home/citus/.pgenv/

COPY --link --from=pipenv /home/citus/.local/share/virtualenvs/ /home/citus/.local/share/virtualenvs/

# place to run your cluster with citus_dev
VOLUME /data
RUN sudo mkdir /data \
 && sudo chown citus:citus /data

COPY --chown=citus:citus .psqlrc .

# sets default pg version
RUN pgenv switch latest 15
