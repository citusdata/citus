# Contributing to Citus

We're happy you want to contribute! You can help us in different ways:

* Open an [issue](https://github.com/citusdata/citus/issues) with
  suggestions for improvements
* Fork this repository and submit a pull request

Before accepting any code contributions we ask that contributors
sign a Contributor License Agreement (CLA). For an explanation of
why we ask this as well as instructions for how to proceed, see the
[Microsoft CLA](https://cla.opensource.microsoft.com/).

### Getting and building

[PostgreSQL documentation](https://www.postgresql.org/support/versioning/) has a
section on upgrade policy.

	We always recommend that all users run the latest available minor release [for PostgreSQL] for whatever major version is in use.

We expect Citus users to honor this recommendation and use latest available
PostgreSQL minor release. Failure to do so may result in failures in our test
suite. There are some known improvements in PG test architecture such as
[this commit](https://github.com/postgres/postgres/commit/3f323956128ff8589ce4d3a14e8b950837831803)
that are missing in earlier minor versions.

#### Mac

1. Install Xcode
2. Install packages with Homebrew

  ```bash
  brew update
  brew install git postgresql python
  ```

3. Get, build, and test the code

  ```bash
  git clone https://github.com/citusdata/citus.git

  cd citus
  ./configure
  make
  make install
  # Optionally, you might instead want to use `make install-all`
  # since `multi_extension` regression test would fail due to missing downgrade scripts.
  cd src/test/regress

  pip install pipenv
  pipenv --rm
  pipenv install
  pipenv shell

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

  sudo apt-get install -y postgresql-server-dev-14 postgresql-14 \
                          autoconf flex git libcurl4-gnutls-dev libicu-dev \
                          libkrb5-dev liblz4-dev libpam0g-dev libreadline-dev \
                          libselinux1-dev libssl-dev libxslt1-dev libzstd-dev \
                          make uuid-dev
  ```

2. Get, build, and test the code

  ```bash
  git clone https://github.com/citusdata/citus.git
  cd citus
  ./configure
  make
  sudo make install
  # Optionally, you might instead want to use `sudo make install-all`
  # since `multi_extension` regression test would fail due to missing downgrade scripts.
  cd src/test/regress

  pip install pipenv
  pipenv --rm
  pipenv install
  pipenv shell

  make check
  ```

#### Red Hat-based Linux (RHEL, CentOS, Fedora)

1. Find the RPM URL for your repo at [yum.postgresql.org](http://yum.postgresql.org/repopackages.php)
2. Register its contents with Yum:

  ```bash
  sudo yum install -y <url>
  ```

3. Register EPEL and SCL repositories for your distro.

  On CentOS:

  ```bash
  yum install -y centos-release-scl-rh epel-release
  ```

  On RHEL, see [this RedHat blog post](https://developers.redhat.com/blog/2018/07/07/yum-install-gcc7-clang/) to install set-up SCL first. Then run:

  ```bash
  yum install -y epel-release
  ```

4. Install build dependencies

  ```bash
  sudo yum update -y
  sudo yum groupinstall -y 'Development Tools'
  sudo yum install -y postgresql14-devel postgresql14-server     \
                      git libcurl-devel libxml2-devel libxslt-devel \
                      libzstd-devel llvm-toolset-7-clang llvm5.0 lz4-devel \
                      openssl-devel pam-devel readline-devel

  git clone https://github.com/citusdata/citus.git
  cd citus
  PG_CONFIG=/usr/pgsql-14/bin/pg_config ./configure
  make
  sudo make install
  # Optionally, you might instead want to use `sudo make install-all`
  # since `multi_extension` regression test would fail due to missing downgrade scripts.
  cd src/test/regress

  pip install pipenv
  pipenv --rm
  pipenv install
  pipenv shell

  make check
  ```

### Following our coding conventions

CircleCI will automatically reject any PRs which do not follow our coding
conventions. The easiest way to ensure your PR adheres to those conventions is
to use the [citus_indent](https://github.com/citusdata/tools/tree/develop/uncrustify)
tool. This tool uses `uncrustify` under the hood.

```bash
# Uncrustify changes the way it formats code every release a bit. To make sure
# everyone formats consistently we use version 0.68.1:
curl -L https://github.com/uncrustify/uncrustify/archive/uncrustify-0.68.1.tar.gz | tar xz
cd uncrustify-uncrustify-0.68.1/
mkdir build
cd build
cmake ..
make -j5
sudo make install
cd ../..

git clone https://github.com/citusdata/tools.git
cd tools
make uncrustify/.install
```

Once you've done that, you can run the `make reindent` command from the top
directory to recursively check and correct the style of any source files in the
current directory. Under the hood, `make reindent` will run `citus_indent` and
some other style corrections for you.

You can also run the following in the directory of this repository to
automatically format all the files that you have changed before committing:

```bash
cat > .git/hooks/pre-commit << __EOF__
#!/bin/bash
citus_indent --check --diff || { citus_indent --diff; exit 1; }
__EOF__
chmod +x .git/hooks/pre-commit
```

### Making SQL changes

Sometimes you need to make change to the SQL that the citus extension runs upon
creations. The way this is done is by changing the last file in
`src/backend/distributed/sql`, or creating it if the last file is from a
published release. If you needed to create a new file, also change the
`default_version` field in `src/backend/distributed/citus.control` to match your
new version. All the files in this directory are run in order based on
their name. See [this page in the Postgres
docs](https://www.postgresql.org/docs/current/extend-extensions.html) for more
information on how Postgres runs these files.

#### Changing or creating functions

If you need to change any functions defined by Citus. You should check inside
`src/backend/distributed/sql/udfs` to see if there is already a directory for
this function, if not create one. Then change or create the file called
`latest.sql` in that directory to match how it should create the function. This
should be including any DROP (IF EXISTS), COMMENT and REVOKE statements for this
function.

Then copy the `latest.sql` file to `{version}.sql`, where `{version}` is the
version for which this sql change is, e.g. `{9.0-1.sql}`. Now that you've
created this stable snapshot of the function definition for your version you
should use it in your actual sql file, e.g.
`src/backend/distributed/sql/citus--8.3-1--9.0-1.sql`. You do this by using C
style `#include` statements like this:
```
#include "udfs/myudf/9.0-1.sql"
```

#### Other SQL

Any other SQL you can put directly in the main sql file, e.g.
`src/backend/distributed/sql/citus--8.3-1--9.0-1.sql`.

### Backporting a commit to a release branch

1. Check out the release branch that you want to backport to `git checkout release-11.3`
2. Make sure you have the latest changes `git pull`
3. Create a new release branch with a unique name `git checkout -b release-11.3-<yourname>`
4. Cherry-pick the commit that you want to backport `git cherry-pick -x <sha>` (the `-x` is important)
5. Push the branch `git push`
6. Wait for tests to pass
7. If the cherry-pick required non-trivial merge conflicts, create a PR and ask
   for a review.
8. After the tests pass on CI, fast-forward the release branch `git push origin release-11.3-<yourname>:release-11.3`

### Running tests

See [`src/test/regress/README.md`](https://github.com/citusdata/citus/blob/master/src/test/regress/README.md)
