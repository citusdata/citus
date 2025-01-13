FROM ubuntu:22.04 AS base

# environment is to make python pass an interactive shell, probably not the best timezone given a wide variety of colleagues
ENV TZ=UTC
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# install build tools
RUN apt update && apt install -y \
    bzip2 \
    cpanminus \
    curl \
    flex \
    gcc \
    git \
    libcurl4-gnutls-dev \
    libicu-dev \
    libkrb5-dev \
    liblz4-dev \
    libpam0g-dev \
    libreadline-dev \
    libselinux1-dev \
    libssl-dev \
    libxslt-dev \
    libzstd-dev \
    locales \
    make \
    perl \
    pkg-config \
    python3 \
    python3-pip \
    software-properties-common \
    sudo \
    uuid-dev \
    valgrind \
    zlib1g-dev \
 && add-apt-repository ppa:deadsnakes/ppa -y \
 && apt install -y \
    python3.9-full \
 # software properties pulls in pkexec, which makes the debugger unusable in vscode
 && apt purge -y \
    software-properties-common \
 && apt autoremove -y \
 && apt clean

RUN sudo pip3 install pipenv pipenv-shebang

RUN cpanm install IPC::Run

RUN locale-gen en_US.UTF-8

# add the citus user to sudoers and allow all sudoers to login without a password prompt
RUN useradd -ms /bin/bash citus \
 && usermod -aG sudo citus \
 && echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

WORKDIR /home/citus
USER citus

# run all make commands with the number of cores available
RUN echo "export MAKEFLAGS=\"-j \$(nproc)\"" >> "/home/citus/.bashrc"

RUN git clone --branch v1.3.2 --depth 1 https://github.com/theory/pgenv.git .pgenv
COPY --chown=citus:citus pgenv/config/ .pgenv/config/
ENV PATH="/home/citus/.pgenv/bin:${PATH}"
ENV PATH="/home/citus/.pgenv/pgsql/bin:${PATH}"

USER citus

# build postgres versions separately for effective parrallelism and caching of already built versions when changing only certain versions
FROM base AS pg14
RUN MAKEFLAGS="-j $(nproc)" pgenv build 14.15
RUN rm .pgenv/src/*.tar*
RUN make -C .pgenv/src/postgresql-*/ clean
RUN make -C .pgenv/src/postgresql-*/src/include install

# create a staging directory with all files we want to copy from our pgenv build
# we will copy the contents of the staged folder into the final image at once
RUN mkdir .pgenv-staging/
RUN cp -r .pgenv/src .pgenv/pgsql-* .pgenv/config .pgenv-staging/
RUN rm .pgenv-staging/config/default.conf

FROM base AS pg15
RUN MAKEFLAGS="-j $(nproc)" pgenv build 15.10
RUN rm .pgenv/src/*.tar*
RUN make -C .pgenv/src/postgresql-*/ clean
RUN make -C .pgenv/src/postgresql-*/src/include install

# create a staging directory with all files we want to copy from our pgenv build
# we will copy the contents of the staged folder into the final image at once
RUN mkdir .pgenv-staging/
RUN cp -r .pgenv/src .pgenv/pgsql-* .pgenv/config .pgenv-staging/
RUN rm .pgenv-staging/config/default.conf

FROM base AS pg16
RUN MAKEFLAGS="-j $(nproc)" pgenv build 16.6
RUN rm .pgenv/src/*.tar*
RUN make -C .pgenv/src/postgresql-*/ clean
RUN make -C .pgenv/src/postgresql-*/src/include install

# create a staging directory with all files we want to copy from our pgenv build
# we will copy the contents of the staged folder into the final image at once
RUN mkdir .pgenv-staging/
RUN cp -r .pgenv/src .pgenv/pgsql-* .pgenv/config .pgenv-staging/
RUN rm .pgenv-staging/config/default.conf

FROM base AS pg17
RUN MAKEFLAGS="-j $(nproc)" pgenv build 17.2
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
RUN MAKEFLAGS="-j $(nproc)" make -s

RUN make install DESTDIR=/uncrustify

# builder for all pipenv's to get them contained in a single layer
FROM base AS pipenv

WORKDIR /workspaces/citus/

# tools to sync pgenv with vscode
COPY --chown=citus:citus .vscode/Pipfile .vscode/Pipfile.lock .devcontainer/.vscode/
RUN ( cd .devcontainer/.vscode && pipenv install )

# environment to run our failure tests
COPY --chown=citus:citus src/ src/
RUN ( cd src/test/regress && pipenv install )

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
    fswatch \
    gdb \
    htop \
    libdbd-pg-perl \
    libdbi-perl \
    lsof \
    man \
    net-tools \
    psmisc \
    pspg \
    tree \
    vim \
 && sudo apt clean

# Since gdb will run in the context of the root user when debugging citus we will need to both
# download the gdbpg.py script as the root user, into their home directory, as well as add .gdbinit
# as a file owned by root
# This will make that as soon as the debugger attaches to a postgres backend (or frankly any other process)
# the gdbpg.py script will be sourced and the developer can direcly use it.
RUN sudo curl -o /root/gdbpg.py https://raw.githubusercontent.com/tvesely/gdbpg/6065eee7872457785f830925eac665aa535caf62/gdbpg.py
COPY --chown=root:root .gdbinit /root/

# install developer dependencies in the global environment
RUN --mount=type=bind,source=requirements.txt,target=requirements.txt pip install -r requirements.txt

# for persistent bash history across devcontainers we need to have
# a) a directory to store the history in
# b) a prompt command to append the history to the file
# c) specify the history file to store the history in
# b and c are done in the .bashrc to make it persistent across shells only
RUN sudo install -d -o citus -g citus /commandhistory \
 && echo "export PROMPT_COMMAND='history -a' && export HISTFILE=/commandhistory/.bash_history" >> "/home/citus/.bashrc"

# install citus-dev
RUN git clone --branch develop https://github.com/citusdata/tools.git citus-tools \
 && ( cd citus-tools/citus_dev && pipenv install ) \
 && mkdir -p ~/.local/bin \
 && ln -s /home/citus/citus-tools/citus_dev/citus_dev-pipenv .local/bin/citus_dev \
 && sudo make -C citus-tools/uncrustify install bindir=/usr/local/bin pkgsysconfdir=/usr/local/etc/ \
 && mkdir -p ~/.local/share/bash-completion/completions/ \
 && ln -s ~/citus-tools/citus_dev/bash_completion ~/.local/share/bash-completion/completions/citus_dev

# TODO some LC_ALL errors, possibly solved by locale-gen
RUN git clone https://github.com/so-fancy/diff-so-fancy.git \
 && mkdir -p ~/.local/bin \
 && ln -s /home/citus/diff-so-fancy/diff-so-fancy .local/bin/

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

# with the copy linking of layers github actions seem to misbehave with the ownership of the
# directories leading upto the link, hence a small patch layer to have to right ownerships set
RUN sudo chown --from=root:root citus:citus -R ~

# sets default pg version
RUN pgenv switch 17.2

# make connecting to the coordinator easy
ENV PGPORT=9700
