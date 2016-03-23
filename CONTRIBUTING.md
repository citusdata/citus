# Contributing to Citus

We're happy you want to contribute! You can help us in different ways:

* Open an [issue](https://github.com/citusdata/citus/issues) with
  suggestions for improvements
* Fork this repository and submit a pull request

As with the majority of major open source projects, we ask that
Citus contributors sign a Contributor License Agreement (CLA). We
know that this creates a bit of red tape, but also believe that it
is the best way to create total transparency around your rights as
a contributor and the most effective way of protecting you as well
as Citus Data as the company behind the open source project.

If you plan on contributing to Citus, please follow these [instructions
for signing our CLA](https://www.citusdata.com/community/CLA)

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
