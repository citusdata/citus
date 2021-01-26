---
name: Release Checklist Template
about: Release Checklist Items
title: ''
labels: ''
assignees: ''

---

## Several things to note here

These instructions assume you have `$VERSION`, `$PROJECT`, and `$REPO` environment variables set in your shell (e.g. `9.5.0`, `citus`, and `citus`). With those set, code from most steps can be copy-pasted.

**After this checklist, you're still not done: open a release checklist in Enterprise and release there, too!**

# Prepare CHANGELOG
- [ ] Run `prepare_changelog.pl $PROJECT $VERSION <earliest_date>` on `$REPO` directory, and check the following:
  - [ ] All items are listed in `CHANGELOG`
  - [ ] The new entries are in both `master` and `release-x.y`
  - [ ] There are no missing entries
  - [ ] The lengths of items do not exceed 78 characters
  - [ ] The items are in ordered in terms of their importance
  - [ ] All the items are present simple tense
  

# Prepare Release Branch
- [ ] Run `prepare_release.pl $PROJECT $VERSION` on `$REPO` directory, and check the following:
  - [ ] `configure.in` `configure` and `multi_extension.out` files are updated with the latest version
  - [ ] Ensure all needed changes are in the relevant `release-x.y` branch. `git log --cherry-pick --no-merges release-x.y...master` can be helpful.
- [ ] Get _complete_ approval for the commit you're tagging before creating or pushing any tags. Tags should be immutable, so do **not** proceed to the next step until you're sure you have _everything_ you want in your release branch
- [ ] Use `git checkout release-$VERSION && git pull && git tag -a -s v$VERSION` to create an annotated, signed tag for the release. Summarize the release in the one-line tag annotation (beneath 52 characters).
- [ ]  Ensure that tag points to the release branch by using the following
```bash
$ git rev-parse tags/vX.Y.Z
<commitId>
$ git branch --contains <commitId>
release-X.Y
```

You might also want to use `git log --oneline --decorate --graph --all`.

- [ ] Push the tag with `git push origin v$VERSION`

- [ ] Visit the project's releases page (e.g. `open https://github.com/citusdata/$REPO/releases`)
  - [ ] Create a new release object for your git tag (i.e. `v$VERSION`). Leave the description blank (it will auto-fill with the tag description)


# Update OS Packages
## Debian and RedHat
- Change your directory to `packaging` repository directory & checkout `all-$PROJECT` branch.
- [ ] Run `update_os_package.pl all $PROJECT $VERSION`.
  - Then check the following (needed for both debian & redhat):
    - [ ] Updated `pkglatest` variable in the `pkgvars` file to `$VERSION.citus-1`
  - Then check the following (needed for debian):
    - [ ] A new entry (`$VERSION.citus-1`, `stable`) is added to the `debian/changelog` file
  - Then check the following (needed for redhat):
    - [ ] `$PROJECT.spec` file is updated:
      - [ ] `Version:` field
      - [ ] `Source0:` field
      - [ ] A new entry (`$VERSION.citus-1`) in the `%changelog` section
- [ ] Optional: Test the Debian release build locally: `citus_package -p=debian/jessie -p=debian/stretch -p=debian/buster -p=ubuntu/xenial -p=ubuntu/trusty -p=ubuntu/bionic local release 2>&1 | tee -a citus_package.log`
  - [ ] Ensure no warnings or errors are present: `grep -Ei '(warning|\bi|\be|\bw):' citus_package.log | sort | uniq -c`. Ignore any warnings about _using a gain-root-command while being root_ or _Recognised distributions_
- [ ] Optional: Test the Debian nightly build locally: `citus_package -p=debian/jessie -p=debian/stretch -p=debian/buster -p=ubuntu/xenial -p=ubuntu/trusty -p=ubuntu/bionic local nightly 2>&1 | tee -a citus_package.log`
  - [ ] Ensure no warnings or errors are present: `grep -Ei '(warning|\bi|\be|\bw):' citus_package.log | sort | uniq -c`. Ignore any warnings about _using a gain-root-command while being root_ or _Recognised distributions_
- [ ] Optional: Test the Red Hat release build locally: `citus_package -p=el/8 -p=el/7 -p=el/6 -p=ol/7 -p=ol/6 local release 2>&1 | tee -a citus_package.log`
  - [ ] Ensure no warnings or errors are present: `grep -Ei '(warning|\bi|\be|\bw):' citus_package.log | sort | uniq -c`. Ignore any errors about `--disable-dependency-tracking`
- [ ] Optional: Test the Red Hat nightly build locally: `citus_package -p=el/8 -p=el/7 -p=el/6 -p=ol/7 -p=ol/6 local nightly 2>&1 | tee -a citus_package.log`
  - [ ] Ensure no warnings or errors are present: `grep -Ei '(warning|\bi|\be|\bw):' citus_package.log | sort | uniq -c`. Ignore any errors about `--disable-dependency-tracking`
- [ ] Check the CI outputs for the PR on packaging repo thoroughly.
- [ ] After confirming that the packages do not have any warnings or errors, merge the PR. Ignore any warnings about _using a gain-root-command while being root_ or _Recognised distributions_ or _--disable-dependency-tracking_
- [ ] Get changes reviewed; merge the PR
- [ ] Ensure Travis builds completed successfully (new releases should be in packagecloud, *you should expect 36 items for citus<9.5 and 50 items for citus=> 9.5 when searching for new version*)

# Update Docker
Note that we create docker images for only the latest version of Citus. So, you don’t need to update it if you are releasing a point version of an older major version.

You need to provide <new-postgresql-version> if you want to update the  PG version on docker templates.

- [ ] Run `update_docker.pl $VERSION [<new-postgresql-version>]` on docker repo, and check the following:
  - [ ] `release-$VERSION` branch is created in [docker repository checkout](https://github.com/citusdata/docker), based on `master`
  - [ ] Version of the base PostgreSQL image is bumped in the `FROM` instruction of the `Dockerfile` and `Dockerfile-alpine`, if you supplied the optional parameter
  - [ ] Citus version is bumped in the `Dockerfile`, `Dockerfile-alpine` and `docker-compose.yml` files
  - [ ] A new entry in the `CHANGELOG` noting that the Citus version has been bumped (and the PostgreSQL one, if applicable)
  - [ ] Optional: Locally build your image and test it standalone and in a cluster: `docker build -t citusdata/citus:$VERSION .`
- [ ] Check CI job outputs of the pull request and ask for review, then merge the pr.
- [ ] Tag the latest `master` as `v$VERSION`: `git fetch && git tag -a -s v$VERSION origin/master && git push origin v$VERSION`
- [ ] Ensure the Docker Hub builds (e.g. https://hub.docker.com/r/citusdata/citus/builds) complete successfully
(You should expect 9 tags to be updated/created on [dockerhub](https://hub.docker.com/r/citusdata/citus/tags?page=1&ordering=last_updated))
- latest
- nightly
- x.y.x
- x.y
- x
- x.y.x-alpine
- x.y-alpine
- x-alpine
- alpine



# Update PGXN
- [ ] Run `update_pgxn.pl $VERSION <old-version>` on packaging repo, and check the following:
  - [ ] Check out the `pgxn-$PROJECT` branch of the [packaging repository](https://github.com/citusdata/packaging)
  - [ ] All version occurrences are bumped in `META.json`
  - [ ] All version occurrences are bumped in `pkgvars`
- [ ] Optional: Test locally with `citus_package -p=pgxn local release`
- [ ] After merging, ensure the Travis build completed successfully (a new release should appear in PGXN eventually)

# Update PGDG

PGDG has separate teams for Red Hat and Debian builds.

## Red Hat

  - [ ] Create a new feature request in [the RPM Redmine](https://redmine.postgresql.org/projects/pgrpms/issues/new) asking their team to update the Citus version in their spec file
  - [ ] Wait for the issue to be closed and verify that the package is available in the PGDG RPM repo
