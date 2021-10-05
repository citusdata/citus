---
name: Packaging Request Checklist
about: If you need packaging, please use this template
title: Bump to Citus X.Y.Z
labels: release checklist
assignees: gurkanindibay

---

## Several things to note here

These instructions assume you have `$VERSION`, `$PROJECT`, and `$REPO` environment variables set in your shell (e.g. `10.0.2`, `citus`, and `citus`). With those set, code from most steps can be copy-pasted.

**After this checklist, you're still not done: open a release checklist in Enterprise and release there, too!**

# Prepare CHANGELOG
- [ ] Run `prepare_changelog.pl $PROJECT $VERSION <earliest_date>` on `$REPO` directory, and check the following:
  - [ ] All items are listed in `CHANGELOG`
  - [ ] There are no missing entries
  - [ ] The lengths of items do not exceed 78 characters
  - [ ] The items are in ordered in terms of their importance
  - [ ] All the items are present simple tense
  - [ ] The new entries are in both `master` and `release-x.y`


# Prepare Release Branch
- [ ] Run `python -m tools.packaging_automation.prepare_release --gh_token <gh_token> --prj_name [citus|citus_enterprise] --prj_ver <prj_ver> --schema_version <schema_version> [--cherry_pick_enabled --earliest_pr_date <Y.m.d>]` on `$REPO` directory, and check the following:
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
- [x] Run the pipeline using branch name as all-citus https://github.com/citusdata/packaging/actions/workflows/update_package_properties.yml. Input tag name and if version is fancy, input the fancy version_no. Other parameters could be kept as is if you want
  - Then check the following (needed for both debian & redhat):
    - [x] Updated `pkglatest` variable in the `pkgvars` file to `$VERSION.citus-1`
  - Then check the following (needed for debian):
    - [x] A new entry (`$VERSION.citus-1`, `stable`) is added to the `debian/changelog` file
  - Then check the following (needed for redhat):
    - [x] `$PROJECT.spec` file is updated:
      - [x] `Version:` field
      - [ ] `Source0:` field
      - [ ] A new entry (`$VERSION.citus-1`) in the `%changelog` section
- [ ] <s>Check the CI outputs for the PR on packaging repo thoroughly.</s>
- [ ] Get changes reviewed; merge the PR
- [ ] Ensure Github Actions builds completed successfully and package count for each os is as below table and packages in postgres versions is compliant with postgres-matrix.yml in the all-citus branch

# Update Docker
Note that we create docker images for only the latest version of Citus. So, you don’t need to update it if you are releasing a point version of an older major version.

- [ ] Run `https://github.com/citusdata/docker/actions/workflows/update_version.yml` on docker repo, and check the following:
  - [ ] A new PR created with `release-XXX` pattern in [docker repository checkout](https://github.com/citusdata/docker), based on `master`
  - [ ] Citus version is bumped in the `Dockerfile`, `Dockerfile-alpine` and `docker-compose.yml` files
  - [ ] A new entry in the `CHANGELOG` noting that the Citus version has been bumped (and the PostgreSQL one, if applicable)
  - [ ] Optional: Locally build your image and test it standalone and in a cluster: `docker build -t citusdata/citus:$VERSION .`
- [ ] Check CI job outputs of the pull request and ask for review, then merge the pr.
- [ ] Tag the latest `master` as `v$VERSION`: `git fetch && git tag -a -s v$VERSION origin/master && git push origin v$VERSION`
- [ ] Ensure the Github Actions builds complete successfully
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
- [ ] Run https://github.com/citusdata/packaging/actions/workflows/update-pgxn-version.yml` on packaging repo, and check the following:
  - [ ] Check out the `pgxn-$PROJECT` branch of the [packaging repository](https://github.com/citusdata/packaging)
  - [ ] All version occurrences are bumped in `META.json`
  - [ ] All version occurrences are bumped in `pkgvars`
- [ ] After merging, ensure the Github Actions build completed successfully (a new release should appear in PGXN eventually)

# Update PGDG

PGDG has separate teams for Red Hat and Debian builds.

## Red Hat

  - [ ] Create a new feature request in [the RPM Redmine](https://redmine.postgresql.org/projects/pgrpms/issues/new) asking their team to update the Citus version in their spec file
  - [ ] Wait for the issue to be closed and verify that the package is available in the PGDG RPM repo
