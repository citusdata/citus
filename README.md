![Citus Banner](/github-banner.png)

[![Slack Status](http://slack.citusdata.com/badge.svg)](https://slack.citusdata.com)
[![Latest Docs](https://img.shields.io/badge/docs-latest-brightgreen.svg)](https://docs.citusdata.com/)
[![Circleci Status](https://circleci.com/gh/citusdata/citus.svg?style=svg)](https://circleci.com/gh/citusdata/citus.svg?style=svg)
[![Code Coverage](https://codecov.io/gh/citusdata/citus/branch/master/graph/badge.svg)](https://codecov.io/gh/citusdata/citus/branch/master/graph/badge.svg)

### What is Citus?

* **Open-source** PostgreSQL extension (not a fork)
* **Built to scale out** across multiple nodes
* **Distributed** engine for query parallelization
* **Database** designed to scale out multi-tenant applications, real-time analytics dashboards, and high-throughput transactional workloads

Citus is an open source extension to Postgres that distributes your data and your queries across multiple nodes. Because Citus is an extension to Postgres, and not a fork, Citus gives developers and enterprises a scale-out database while keeping the power and familiarity of a relational database. As an extension, Citus supports new PostgreSQL releases, and allows you to benefit from new features while maintaining compatibility with existing PostgreSQL tools.

Citus serves many use cases. Three common ones are:

1. [Multi-tenant & SaaS applications](https://www.citusdata.com/blog/2016/10/03/designing-your-saas-database-for-high-scalability):
Most B2B applications already have the notion of a tenant /
customer / account built into their data model. Citus allows you to scale out your
transactional relational database to 100K+ tenants with minimal changes to your
application.

2. [Real-time analytics](https://www.citusdata.com/blog/2017/12/27/real-time-analytics-dashboards-with-citus/):
Citus enables ingesting large volumes of data and running
analytical queries on that data in human real-time. Example applications include analytic
dashboards with sub-second response times and exploratory queries on unfolding events.

3. High-throughput transactional workloads:
By distributing your workload across a database cluster, Citus ensures low latency and high performance even with a large number of concurrent users and high volumes of transactions.

To learn more, visit [citusdata.com](https://www.citusdata.com) and join
the [Citus slack](https://slack.citusdata.com/) to
stay on top of the latest developments.

### Getting started with Citus

The fastest way to get up and running is to deploy Citus in the cloud. You can also setup a local Citus database cluster with Docker.

#### Hyperscale (Citus) on Azure Database for PostgreSQL

Hyperscale (Citus) is a deployment option on Azure Database for PostgreSQL, a fully-managed database as a service. Hyperscale (Citus) employs the Citus open source extension so you can scale out across multiple nodes. To get started with Hyperscale (Citus), [learn more](https://www.citusdata.com/product/hyperscale-citus/) on the Citus website or use the [Hyperscale (Citus) Quickstart](https://docs.microsoft.com/en-us/azure/postgresql/quickstart-create-hyperscale-portal) in the Azure docs.

#### Citus Cloud

Citus Cloud runs on top of AWS as a fully managed database as a service. You can provision a Citus Cloud account at [https://console.citusdata.com](https://console.citusdata.com/users/sign_up) and get started with just a few clicks.

#### Local Citus Cluster

If you're looking to get started locally, you can follow the following steps to get up and running.

1. Install Docker Community Edition and Docker Compose
  * Mac:
    1. [Download](https://www.docker.com/community-edition#/download) and install Docker.
    2. Start Docker by clicking on the application’s icon.
  * Linux:
    ```bash
    curl -sSL https://get.docker.com/ | sh
    sudo usermod -aG docker $USER && exec sg docker newgrp `id -gn`
    sudo systemctl start docker

    sudo curl -sSL https://github.com/docker/compose/releases/download/1.11.2/docker-compose-`uname -s`-`uname -m` -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
    ```
    The above version of Docker Compose is sufficient for running Citus, or you can install the [latest version](https://github.com/docker/compose/releases/latest).

2. Pull and start the Docker images
  ```bash
  curl -sSLO https://raw.githubusercontent.com/citusdata/docker/master/docker-compose.yml
  docker-compose -p citus up -d
  ```

3. Connect to the master database
  ```bash
  docker exec -it citus_master psql -U postgres
  ```

4. Follow the [first tutorial][tutorial] instructions
5. To shut the cluster down, run

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
  href="https://docs.citusdata.com/en/stable/tutorials/multi-tenant-tutorial.html">Citus
  tutorial</a> for a hands-on introduction or <br/>the <a
  href="https://docs.citusdata.com">documentation</a> for
  a more comprehensive reference.</td>
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
<tr>
  <td>Citus Blog</td>
  <td>Read our <a href="https://www.citusdata.com/blog/">Citus Data Blog</a>
  for posts on Postgres, Citus, and scaling your database.</td>
</tr>
</table>

### Contributing

Citus is built on and of open source, and we welcome your contributions.
The [CONTRIBUTING.md](CONTRIBUTING.md) file explains how to get started
developing the Citus extension itself and our code quality guidelines.

### Who is Using Citus?

Citus is deployed in production by many customers, ranging from
technology start-ups to large enterprises. Here are some examples:

* [Algolia](https://www.algolia.com/) uses Citus to provide real-time analytics for over 1B searches per day. For faster insights, they also use TopN and HLL extensions. [User Story](https://www.citusdata.com/customers/algolia)
* [Heap](https://heapanalytics.com/) uses Citus to run dynamic
funnel, segmentation, and cohort queries across billions of users
and has more than 700B events in their Citus database cluster. [Watch Video](https://www.youtube.com/watch?v=NVl9_6J1G60&list=PLixnExCn6lRpP10ZlpJwx6AuU3XIgNWpL)
* [Pex](https://pex.com/) uses Citus to ingest 80B data points per day and analyze that data in real-time. They use a 20+ node cluster on Google Cloud. [User Story](https://www.citusdata.com/customers/pex)
* [MixRank](https://mixrank.com/) uses Citus to efficiently collect
and analyze vast amounts of data to allow inside B2B sales teams
to find new customers. [User Story](https://www.citusdata.com/customers/mixrank)
* [Agari](https://www.agari.com/) uses Citus to secure more than
85 percent of U.S. consumer emails on two 6-8 TB clusters. [User
Story](https://www.citusdata.com/customers/agari)
* [Copper (formerly ProsperWorks)](https://copper.com/) powers a cloud CRM service with Citus. [User Story](https://www.citusdata.com/customers/copper)


You can read more user stories about how they employ Citus to scale Postgres for both multi-tenant SaaS applications as well as real-time analytics dashboards [here](https://www.citusdata.com/customers/).

___

Copyright © Citus Data, Inc.

[faq]: https://www.citusdata.com/frequently-asked-questions
[tutorial]: https://docs.citusdata.com/en/stable/tutorials/multi-tenant-tutorial.html

teste
