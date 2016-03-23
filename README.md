![Citus Banner](/github-banner.png)

[![Build Status](https://travis-ci.com/citusdata/citus.svg?token=bSq3ym67qubHCGNs1iYZ&branch=master)](https://travis-ci.com/citusdata/citus)
[![Citus IRC](https://img.shields.io/badge/irc-%23citus-blue.svg)](https://webchat.freenode.net/?channels=citus)
[![Latest Docs](https://img.shields.io/badge/docs-latest-brightgreen.svg)][docs]

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
Note that Citus supports many (but not all) SQL commands; see the
[FAQ][faq] for more details.

Common Use-Cases:
* Powering real-time analytic dashboards
* Exploratory queries on events as they happen
* Large dataset archival and reporting
* Session analytics (funnels, segmentation, and cohorts)

To learn more, visit [citusdata.com](https://www.citusdata.com).

### Quickstart

#### Local Citus Cluster

* Install docker-compose: [Mac][mac_install] | [Linux][linux_install]
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

* Follow the [first tutorial][tutorial] instructions
* To shut the cluster down, run

   ```bash
   docker-compose -p citus down
   ```

### Learn More

The project [documentation][docs] and [tutorials][tutorial] are
good places to start.  We’re responsive on Github, so you can use
the [issue tracker][issues]  to check for or submit bug reports and
feature requests. For more immediate help or general discussion
we’re on IRC at `#citus` on Freenode and [@citusdata][twitter] on
Twitter.

We also offer training and dedicated support options. More information
is available on our [support page][support].

### Contributing

Citus is built on and of open source. We welcome your contributions,
and have added a
[helpwanted](https://github.com/citusdata/citus/labels/helpwanted) label
to issues which are accessible to new contributors. The
[CONTRIBUTING.md](CONTRIBUTING.md) file explains how to get started
developing the Citus extension itself and our code quality guidelines.

### Who is Using Citus?

Citus is deployed in production by many customers, ranging from
technology start-ups to large enterprises. Here are some examples:

* [CloudFlare](https://www.cloudflare.com/) uses Citus to provide
real-time analytics on 100 TBs of data from over 4 million customer
websites. [Case
Study](https://blog.cloudflare.com/scaling-out-postgresql-for-cloudflare-analytics-using-citusdb/)
* [MixRank](https://mixrank.com/) uses Citus to efficiently collect
and analyze vast amounts of data to allow inside B2B sales teams
to find new customers. [Case
Study](https://www.citusdata.com/solutions/case-studies/mixrank-case-study)
* [Neustar](https://www.neustar.biz/) builds and maintains scalable
ad-tech infrastructure that counts billions of events per day using
Citus and HyperLogLog.
* [Agari](https://www.agari.com/) uses Citus to secure more than
85 percent of U.S. consumer emails on two 6-8 TB clusters. [Case
Study](https://www.citusdata.com/solutions/case-studies/agari-case-study)
* [Heap](https://heapanalytics.com/) uses Citus to run dynamic
funnel, segmentation, and cohort queries across billions of users
and tens of billions of events. [Watch
Video](https://www.youtube.com/watch?v=NVl9_6J1G60&list=PLixnExCn6lRpP10ZlpJwx6AuU3XIgNWpL)

### License

Copyright © 2012–2016 Citus Data, Inc.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the [GNU Affero General Public License][license] for more details.

[docs]: https://www.citusdata.com/docs/citus/5.0
[faq]: https://www.citusdata.com/frequently-asked-questions
[issues]: https://github.com/citusdata/citus/issues
[linux_install]: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-14-04
[mac_install]: https://www.docker.com/products/docker-toolbox
[support]: https://www.citusdata.com/citus-products/citus-data-pricing
[tutorial]: https://www.citusdata.com/docs/citus/5.0/tutorials/tut-real-time.html
[twitter]: https://twitter.com/citusdata
[license]: LICENSE
