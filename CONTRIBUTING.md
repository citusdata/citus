# Contributing to Citus

We're happy you want to contribute! You can help us in different ways:

* Open an [issue](https://github.com/citusdata/citus/issues) with
  suggestions for improvements
* Fork this repository and submit a pull request

### Getting and building

#### Mac

1. Install XCode
2. Install packages with homebrew

   ```bash
   brew update
   brew install git openssl postgresql
   brew link openssl --force
   ```

3. Get the code

   ```bash
   git clone https://github.com/citusdata/citus.git
   ```

4. Build and test

   ```bash
   cd citus
   ./configure
   make
   sudo make install
   cd src/test/regress
   make check-multi
   ```

#### Linux

1. Install a C compiler and Git 1.8+
2. Install packages

   ```bash
   # Using APT
   apt-get update
   apt-get install -y make git wget libreadline-dev libxslt1-dev libxml2-dev libselinux1-dev libpam-ocaml-dev

   # Using YUM
   yum install -y openssl-devel pam-devel libxml2-devel libxslt-devel readline-devel zlib-devel postgresql95-devel postgresql95-server
   ```

3. Install PostgreSQL 9.5 ([instructions](http://www.postgresql.org/download/linux/))
4. Get the code

   ```bash
   git clone https://github.com/citusdata/citus.git
   ```

5. Build and test

   ```bash
   cd citus
   ./configure
   make
   sudo make install
   cd src/test/regress
   make check-multi
   ```
