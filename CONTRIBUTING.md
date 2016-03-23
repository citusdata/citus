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
