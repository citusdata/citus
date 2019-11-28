---
name: Release checklist template
about: Template issue for release checklist
title: Release Checklist x.y[.z]
labels: ''
assignees: ''

---

### CHECKLIST


**Prepare Project**
 

- [ ] Ensure all needed changes are in the relevant release-x.y branch. git log --cherry-pick --no-merges release-x.y...master can be helpful. Be sure to cherry-pick changes in the same order they were merged to the main branch (but do not cherry-pick merge commits themselves)
- [ ] Add a CHANGELOG entry in the master branch summarizing meaningful changes
- [ ] Use git cherry-pick to add the new CHANGELOG entry to the release-x.y branch
- [ ] Directly in the release-x.y branch, modify configure.in to reflect the new version
- [ ] Run autoconf to generate a new configure file and update the SHOW citus.version output in the multi_extension test
- [ ] Get complete approval for the commit you're tagging before creating or pushing any tags. Tags should be immutable, so do not proceed to the next step until you're sure you have everything you want in your release branch
- [ ] Use git tag -a -s v$VERSION to create an annotated, signed tag for the release. Summarize the release in the one-line tag annotation (beneath 52 characters). Push the tag with git push origin v$VERSION
- [ ] Visit the project's releases page (e.g. open https://github.com/citusdata/$REPO/releases) 
- [ ] Create a new release object for your git tag (i.e. v$VERSION). Leave the description blank (it will auto-fill with the tag description)
    - [ ] Click the Source code (tar.gz) link for the new version to download a source tarball
    - [ ] Sign the tarball using gpg -a -u 3F95D6C6 -b $PROJECT-$VERSION.tar.gz
    - [ ]  Back on the releases page, click the Edit button for your release. Attach the signature ($PROJECT-$VERSION.tar.gz.asc) and click the Update release button


**Update OS Packages**
 

- [ ] Check out the debian-$PROJECT branch of the packaging repository; create a new branch for your changes 
    - [ ] Add a new entry ($VERSION.citus-1, stable) to the debian/changelog file
    - [ ] Update the pkglatest variable in the pkgvars file to $VERSION.citus-1
    - [ ] Test the Debian release build locally: citus_package -p=debian/jessie -p=debian/wheezy -p=debian/stretch -p=ubuntu/xenial -p=ubuntu/trusty local release 2>&1 | tee -a citus_package.log
    - [ ] Test the Debian nightly build locally: citus_package -p=debian/jessie -p=debian/wheezy -p=debian/stretch -p=ubuntu/xenial -p=ubuntu/trusty local nightly 2>&1 | tee -a citus_package.log
    - [ ] Ensure no warnings or errors are present: grep -Ei '(warning|\bi|\be|\bw):' citus_package.log | sort | uniq -c. Ignore any warnings about using a gain-root-command while being root or Recognised distributions
    - [ ] Push these changes to GitHub and open a pull request, noting any errors you found
- [ ] Check out the redhat-$PROJECT branch of the packaging repository; create a new branch for your changes 
    - [ ] Update the pkglatest variable in the pkgvars file to $VERSION.citus-1
    - [ ] Edit the $PROJECT.spec file, being sure to: 
    - [ ] Update the Version: field
    - [ ] Update the Source0: field
    - [ ] Add a new entry ($VERSION.citus-1) to the %changelog section
    - [ ] Test the Red Hat release build locally: citus_package -p=el/7 -p=el/6 -p=fedora/26 -p=fedora/25 -p=ol/7 -p=ol/6 local release 2>&1 | tee -a citus_package.log
    - [ ] Test the Red Hat nightly build locally: citus_package -p=el/7 -p=el/6 -p=fedora/26 -p=fedora/25 -p=ol/7 -p=ol/6 local nightly 2>&1 | tee -a citus_package.log
    - [ ] Ensure no warnings or errors are present: grep -Ei '(warning|\bi|\be|\bw):' citus_package.log | sort | uniq -c. Ignore any errors about --disable-dependency-tracking
    - [ ] Push these changes to GitHub and open a pull request
- [ ] Get changes reviewed; use the "squash" strategy to close the PR
- [ ] Ensure both Travis builds completed successfully (new releases should be in packagecloud)

**Update Docker**
 

- [ ] Create a release-$VERSION branch in your docker repository checkout, based on develop
- [ ]  If needed, bump the version of the base PostgreSQL image in the FROM instruction of the main Dockerfile
- [ ]  Bump the Citus version number in the Dockerfile and docker-compose.yml files
- [ ]  Add a new entry in the CHANGELOG noting that the Citus version has been bumped (and the PostgreSQL one, if applicable)
- [ ]  Locally build your image and test it standalone and in a cluster: docker build -t citusdata/citus:$VERSION .
- [ ]  Open two pull request: one against develop, and one against master. Get one or the other reviewed, and merge both. You can do it by copying your branch with another name and opening a PR to the remaining base.
- [ ]  Tag the latest master as v$VERSION: git fetch && git tag -a -s v$VERSION origin/master && git push origin v$VERSION
- [ ]  Ensure the Docker Hub builds (e.g. https://hub.docker.com/r/citusdata/citus/builds) complete successfully

**Update PGXN**
 

- [ ] Check out the pgxn-$PROJECT branch of the packaging repository
- [ ] Bump all version occurrences in META.json
- [ ] Bump all version occurrences in pkgvars
- [ ] Test locally with citus_package -p=pgxn local release
- [ ] Push these changes to GitHub and open a pull request
- [ ] After merging, ensure the Travis build completed successfully (a new release should appear in PGXN eventually)

**Update PGDG**
- [ ] PGDG has separate teams for Red Hat and Debian builds.

**Red Hat**
 
- [ ] Create a new feature request in the RPM Redmine asking their team to update the Citus version in their spec file
- [ ]  Wait for the issue to be closed and verify that the package is available in the PGDG RPM repo
