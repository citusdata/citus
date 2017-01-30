![Citus Banner](/github-banner.png)

[![Build Status](https://travis-ci.org/citusdata/citus.svg?branch=master)](https://travis-ci.org/citusdata/citus)
[![Slack Status](http://slack.citusdata.com/badge.svg)](https://slack.citusdata.com)
[![Latest Docs](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://docs.citusdata.com/)

### What is Citus?

* **Open-source** PostgreSQL extension (not a fork)
* **Scalable** across multiple machines through sharding and replication
* **Distributed** engine for query parallelization
* **Database** designed to scale multi-tenant applications

Citus is a distributed database that scales across commodity servers using transparent
sharding and replication. Citus extends the underlying database rather than forking it,
giving developers and enterprises the power and familiarity of a relational database. As
an extension, Citus supports new PostgreSQL releases, and allows you to benefit from new
features while maintaining compatibility with existing PostgreSQL tools.

Citus serves many use cases. Two common ones are:

1. [Multi-tenant database](https://www.citusdata.com/blog/2016/10/03/designing-your-saas-database-for-high-scalability):
Most B2B applications already have the notion of a tenant /
customer / account built into their data model. Citus allows you to scale out your
transactional relational database to 100K+ tenants with minimal changes to your
application.

2. [Real-time analytics](https://www.citusdata.com/blog/2017/01/27/getting-started-with-github-events-data):
Citus enables ingesting large volumes of data and running
analytical queries on that data in human real-time. Example applications include analytic
dashboards with subsecond response times and exploratory queries on unfolding events.

To learn more, visit [citusdata.com](https://www.citusdata.com) and join
the [mailing list](https://groups.google.com/forum/#!forum/citus-users) to
stay on top of the latest developments.

### Getting started with Citus

The fastest way to get up and running is to create a Citus Cloud account. You can also setup a local Citus cluster with Docker.

#### Citus Cloud

Citus Cloud runs on top of AWS as a fully managed database as a service and has development plans available for getting started. You can provision a Citus Cloud account at [https://console.citusdata.com](https://console.citusdata.com/users/sign_up) and get started with just a few clicks.

#### Local Citus Cluster

If you're looking to get started locally, you can follow the following steps to get up and running.

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

### Talk to Contributors and Learn More

<table class="tg">
<col width="45%">
<col width="65%">
<tr>
  <td>Documentation</td>
  <td>Try the <a
  href="https://docs.citusdata.com/en/v6.0/tutorials/tut-hash-distribution.html">Citus
  tutorial</a> for a hands-on introduction or <br/>the <a
  href="https://docs.citusdata.com">documentation</a> for
  a more comprehensive reference.</td>
</tr>
<tr>
  <td>Google Groups</td>
  <td>The <a
  href="https://groups.google.com/forum/#!forum/citus-users">Citus Google
  Group</a> is our place for detailed questions and discussions.</td>
</tr>
<tr>
  <td>Slack</td>
  <td>Chat with us in our community <a
  href="https://slack.citusdata.com">Slack channel</a>.</td>
</tr>
<tr>
  <td>Github Issues</td>
  <td>We track specific bug reports and feature requests on our <a
  href="https://github.com/citusdata/citus/issues">project
  issues</a>.</td>
</tr>
<tr>
  <td>Twitter</td>
  <td>Follow <a href="https://twitter.com/citusdata">@citusdata</a>
  for general updates and PostgreSQL scaling tips.</td>
</tr>
</table>

### Contributing

Citus is built on and of open source, and we welcome your contributions.
The [CONTRIBUTING.md](CONTRIBUTING.md) file explains how to get started
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

___

Copyright © 2012–2017 Citus Data, Inc.

[faq]: https://www.citusdata.com/frequently-asked-questions
[linux_install]: https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-compose-on-ubuntu-14-04
[mac_install]: https://www.docker.com/products/docker-toolbox
[tutorial]: https://docs.citusdata.com/en/v6.0/tutorials/tut-hash-distribution.html
