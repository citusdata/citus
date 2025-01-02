# Contributing to Citus

We're happy you want to contribute! You can help us in different ways:

* Open an [issue](https://github.com/citusdata/citus/issues) with
  suggestions for improvements
* Fork this repository and submit a pull request

Before accepting any code contributions we ask that contributors
sign a Contributor License Agreement (CLA). For an explanation of
why we ask this as well as instructions for how to proceed, see the
[Microsoft CLA](https://cla.opensource.microsoft.com/).

### Devcontainer / Github Codespaces

The easiest way to start contributing is via our devcontainer. This container works both locally in visual studio code with docker-desktop/docker-for-mac as well as [Github Codespaces](https://github.com/features/codespaces). To open the project in vscode you will need the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers). For codespaces you will need to [create a new codespace](https://codespace.new/citusdata/citus).

With the extension installed you can run the following from the command pallet to get started

```
> Dev Containers: Clone Repository in Container Volume...
```

In the subsequent popup paste the url to the repo and hit enter.

```
https://github.com/citusdata/citus
```

This will create an isolated Workspace in vscode, complete with all tools required to build, test and run the Citus extension. We keep this container up to date with the supported postgres versions as well as the exact versions of tooling we use.

To quickly start we suggest splitting your terminal once to have two shells. The left one in the `/workspaces/citus`, the second one changed to `/data`. The left terminal will be used to interact with the project, the right one with a testing cluster.

To get citus installed from source we run `make install -s` in the first terminal. Once installed you can start a Citus cluster in the second terminal via `citus_dev make citus`. The cluster will run in the background, and can be interacted with via `citus_dev`. To get an overview of the available commands.

With the Citus cluster running you can connect to the coordinator in the first terminal via `psql -p9700`. Because the coordinator is the most common entrypoint the `PGPORT` environment is set accordingly, so a simple `psql` will connect directly to the coordinator.

### Debugging in the VS code

1. Start Debugging: Press F5 in VS Code to start debugging. When prompted, you'll need to attach the debugger to the appropriate PostgreSQL process.

2. Identify the Process: If you're running a psql command, take note of the PID that appears in your psql prompt. For example:
```
[local] citus@citus:9700 (PID: 5436)=#
```
This PID (5436 in this case) indicates the process that you should attach the debugger to.
If you are uncertain about which process to attach, you can list all running PostgreSQL processes using the following command:
```
ps aux | grep postgres
```

Look for the process associated with the PID you noted. For example:
```
citus      5436  0.0  0.0  0  0 ?        S    14:00   0:00 postgres: citus citus
```
4. Attach the Debugger: Once you've identified the correct PID, select that process when prompted in VS Code to attach the debugger. You should now be able to debug the PostgreSQL session tied to the psql command.

5. Set Breakpoints and Debug: With the debugger attached, you can set breakpoints within the code. This allows you to step through the code execution, inspect variables, and fully debug the PostgreSQL instance running in your container.

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
  # If you have already installed the project, you need to clean it first
  make clean
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
  # If you have already installed the project previously, you need to clean it first
  make clean
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
  # If you have already installed the project previously, you need to clean it first
  make clean
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

Our coding conventions are documented in [STYLEGUIDE.md](STYLEGUIDE.md).

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

### Documentation

User-facing documentation is published on [docs.citusdata.com](https://docs.citusdata.com/). When adding a new feature, function, or setting, you can open a pull request or issue against the [Citus docs repo](https://github.com/citusdata/citus_docs/).

Detailed descriptions of the implementation for Citus developers are provided in the [Citus Technical Documentation](src/backend/distributed/README.md). It is currently a single file for ease of searching. Please update the documentation if you make any changes that affect the design or add major new features.

# Making a pull request ready for reviews

Asking for help and asking for reviews are two different things. When you're asking for help, you're asking for someone to help you with something that you're not expected to know.

But when you're asking for a review, you're asking for someone to review your work and provide feedback. So, when you're asking for a review, you're expected to make sure that:

* Your changes don't perform **unnecessary line addition / deletions / style changes on unrelated files / lines**.

* All CI jobs are **passing**, including **style checks** and **flaky test detection jobs**. Note that if you're an external contributor, you don't have to wait CI jobs to run (and finish) because they don't get automatically triggered for external contributors.

* Your PR has necessary amount of **tests** and that they're passing.

* You separated as much as possible work into **separate PRs**, e.g., a prerequisite bugfix, a refactoring etc..

* Your PR doesn't introduce a typo or something that you can easily fix yourself.

* After all CI jobs pass, code-coverage measurement job (CodeCov as of today) then kicks in. That's why it's important to make the **tests passing** first. At that point, you're expected to check **CodeCov annotations** that can be seen in the **Files Changed** tab and expected to make sure that it doesn't complain about any lines that are not covered. For example, it's ok if CodeCov complains about an `ereport()` call that you put for an "unexpected-but-better-than-crashing" case, but it's not ok if it complains about an uncovered `if` branch that you added.

* And finally, perform a **self-review** to make sure that:
  * Code and code-comments reflects the idea **without requiring an extra explanation** via a chat message / email / PR comment.
    This is important because we don't expect developers to reach out to author / read about the whole discussion in the PR to understand the idea behind a commit merged into `main` branch.
  * PR description is clear enough.
  * If-and-only-if you're **introducing a user facing change / bugfix**, your PR has a line that starts with `DESCRIPTION: <Present simple tense word that starts with a capital letter, e.g., Adds support for / Fixes / Disallows>`.
  * **Commit messages** are clear enough if the commits are doing logically different things.
