![Citus Banner](/github-banner.png)

[![Build Status](https://travis-ci.com/citusdata/citus.svg?token=bSq3ym67qubHCGNs1iYZ&branch=master)](https://travis-ci.com/citusdata/citus)
[![Citus IRC](https://img.shields.io/badge/irc-%23citus-blue.svg)](https://webchat.freenode.net/?channels=citus)
[![Latest Docs](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://www.citusdata.com/documentation/citus-documentation/index.html)

### What is Citus?

* **Open-source** PostgreSQL extension (not a fork)
* **Scalable** across multiple hosts through sharding and replication
* **Distributed** engine for query parallelization
* **Highly available** in the face of host failures

Citus horizontally scales PostgreSQL across commodity servers using
sharding and replication. Its query engine parallelizes incoming
SQL queries across these servers to enable real-time responses on
large datasets.

Citus extends the underlying database rather than forking it, which
gives developers and enterprises the power and familiarity of a
traditional relational database. As an extension, Citus supports
new PostgreSQL releases, allowing users to benefit from new features
while maintaining compatibility with existing PostgreSQL tools.

Common Use-Cases:
* Powering real-time analytic dashboards
* Exploratory queries on events as they happen
* Large dataset archival and reporting
* Session analytics (funnels, segmentation, and cohorts)

To learn more, visit [citusdata.com](https://www.citusdata.com).

### Quickstart

#### Local Citus Cluster

* Install docker-compose: [Mac](https://www.docker.com/products/docker-toolbox) | [Linux](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-14-04)
* (Mac only) connect to Docker VM
   ```bash
   eval $(docker-machine env default)
   ```

* Pull and start the docker images
   ```bash
   wget https://raw.githubusercontent.com/citusdata/docker/master/docker-compose.yml
   docker-compose -p citus up -d
   ```

* Connect to the master database
   ```bash
   docker exec -it citus_master psql -U postgres -d postgres
   ```

* Follow the [first tutorial](https://www.citusdata.com/documentation/citus-documentation/tutorials/tut-real-time.html) instructions
* To shut the cluster down, run

   ```bash
   docker-compose -p citus down
   ```

### Learn More

The project [documentation](https://www.citusdata.com/documentation/citus-documentation/index.html) and
[tutorials](https://www.citusdata.com/documentation/citus-documentation/tutorials/tut-real-time.html) are good places to start.
We’re responsive on Github, so you can use the [issue
tracker](https://github.com/citusdata/citus/issues) to check for or
submit bug reports and feature requests. For more immediate help
or general discussion we’re on IRC at `#citus` on Freenode and
[@citusdata](https://twitter.com/citusdata) on Twitter.

We also offer training and dedicated support options. More information
is available on our [support
page](https://www.citusdata.com/citus-products/citus-data-pricing).

### Contributing

Citus is built on and of open source. We welcome your contributions,
and have added a
[helpwanted](https://github.com/citusdata/citus/labels/helpwanted) label
to issues which are accessible to new contributors. The CONTRIBUTING.md
file explains how to get started developing the Citus extension
itself and our code quality guidelines.
