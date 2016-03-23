# Contributing to Citus

We're happy you want to contribute! You can help us in different ways:

* Open an [issue](https://github.com/citusdata/citus/issues) with
  suggestions for improvements
* Fork this repository and submit a pull request

### Getting and building

#### Mac

1. Install XCode
3. Install packages with homebrew

   ```bash
   brew install git postgresql-9.5`
   brew link openssl --force`
   ```

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
   make check
   ```

#### Linux

1. Install a C compiler and Git 1.8+
2. Install PostgreSQL 9.5 ([instructions](http://www.postgresql.org/download/))
3. Install packages

   ```bash
   # Ubuntu
   apt-get install postgresql-server-dev-9.5 libreadline-dev
   ```

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
   make check
   ```
