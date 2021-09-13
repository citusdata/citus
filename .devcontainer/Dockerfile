FROM ubuntu:21.10

RUN apt update && apt install -y \
    autoconf \
    bzip2 \
    curl \
    flex \
    gcc \
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
    make \
    sudo \
    uuid-dev \
    zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*
# TODO remove caches with `apt clean` instead

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
COPY --chown=citus:citus *.tar.bz2 .pgenv/src/

USER citus

# TODO add more postgres versions
RUN pgenv build 13.4

# sets default pg version
RUN pgenv switch 13.4

# probably fold into install block earlier
#environment is to make python pass an interactive shell, probably not the best timezone given a wide variety of colleagues
ENV TZ=Europe/Amsterdam
RUN sudo ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ | sudo tee /etc/timezone

RUN sudo apt update && sudo apt install -y \
    python3 \
    python3-pip \
    locales \
 && sudo apt clean

RUN sudo locale-gen en_US.UTF-8

RUN sudo apt update && sudo apt install -y \
    pspg \
    uncrustify \
 && sudo apt clean

# bin directory for user tools
RUN mkdir .bin
ENV PATH="/home/citus/.bin:${PATH}"

# install citus-dev
RUN git clone https://github.com/citusdata/tools.git citus-tools
RUN pip3 install -r citus-tools/citus_dev/requirements.txt
RUN ln -s /home/citus/citus-tools/citus_dev/citus_dev .bin/
RUN sudo make -C citus-tools/uncrustify install bindir=/usr/local/bin pkgsysconfdir=/usr/local/etc/

# TODO add citus_indent

# TODO some LC_ALL errors, possibly solved by locale-gen
RUN git clone https://github.com/so-fancy/diff-so-fancy.git
RUN ln -s /home/citus/diff-so-fancy/diff-so-fancy .bin/

# place to run your cluster with citus_dev
VOLUME /data
RUN sudo mkdir /data
RUN sudo chown citus:citus /data

COPY --chown=citus:citus .psqlrc .

RUN sudo apt update && sudo apt install -y \
    gdb \
 && sudo apt clean
 