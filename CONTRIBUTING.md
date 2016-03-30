# Contributing to Citus

We're happy you want to contribute! You can help us in different ways:

* Open an [issue](https://github.com/citusdata/citus/issues) with
  suggestions for improvements
* Fork this repository and submit a pull request

Before accepting any code contributions we ask that Citus contributors
sign a Contributor License Agreement (CLA). For an explanation of
why we ask this as well as instructions for how to proceed, see the
[Citus CLA](https://cla.citusdata.com).

### Getting and building

#### Mac

1. Install Xcode
2. Install packages with Homebrew

  ```bash
  brew update
  brew install git postgresql
  ```

3. Get, build, and test the code

  ```bash
  git clone https://github.com/citusdata/citus.git

  cd citus
  ./configure
  make
  make install
  cd src/test/regress
  make check
  ```

#### Debian-based Linux (Ubuntu, Debian)

1. Install build dependencies

  ```bash
  echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" | \
       sudo tee /etc/apt/sources.list.d/pgdg.list
  wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
       sudo apt-key add -
  sudo apt-get update

  sudo apt-get install -y postgresql-server-dev-9.5 postgresql-9.5 \
                          libedit-dev libselinux1-dev libxslt-dev  \
                          libpam0g-dev git flex make
  ```

2. Get, build, and test the code

  ```bash
  git clone https://github.com/citusdata/citus.git
  cd citus
  ./configure
  make
  sudo make install
  cd src/test/regress
  make check
  ```

#### Red Hat-based Linux (RHEL, CentOS, Fedora)

1. Find the PostgreSQL 9.5 RPM URL for your repo at [yum.postgresql.org](http://yum.postgresql.org/repopackages.php#pg95)
2. Register its contents with Yum:

  ```bash
  sudo yum install -y <url>
  ```

3. Install build dependencies

  ```bash
  sudo yum update -y
  sudo yum groupinstall -y 'Development Tools'
  sudo yum install -y postgresql95-devel postgresql95-server    \
                      libxml2-devel libxslt-devel openssl-devel \
                      pam-devel readline-devel git

  git clone https://github.com/citusdata/citus.git
  cd citus
  PG_CONFIG=/usr/pgsql-9.5/bin/pg_config ./configure
  make
  sudo make install
  cd src/test/regress
  make check
  ```
