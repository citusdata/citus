# Contributing to Citus

We're happy you want to contribute! You can help us in different ways:

* Open an [issue](https://github.com/citusdata/citus/issues) with
  suggestions for improvements
* Fork this repository and submit a pull request

### Getting and building

1.  Install the following prerequisites, as necessary:
  - A C compiler. On Mac OS X, Xcode should suffice.
  - PostgreSQL development libraries

    ```bash
    # on mac
    brew install postgresql

    # on linux
    apt-get install libpq-dev
    ```

  - Git 1.8+

2.  Get the code

   ```bash
   git clone https://github.com/citusdata/citus.git
   ```

3.  Build and test

   ```bash
   ./configure
   make
   make install
   make check
   ```
