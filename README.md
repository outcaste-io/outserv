# Outserv: GraphQL Indexing and Serving System

Outserv is a GraphQL Indexing and Serving System, built to make it easier to
deploy a fast, scalable, production grade system to serve data via GraphQL.

## What is the motivation behind Outserv?

A typical GraphQL production tech stack today contains:

1. A source of truth database (like Blockchain or Postgres)
1. A search system (like Elastic Search)
1. A cache system (like Redis)
1. A GraphQL layer (like Hasura / Apollo)
1. Business Logic

Outserv cuts down this stack significantly. A tech stack with Outserv looks like
this:

1. A source of truth database (like Blockchain or Postgres)
1. Outserv providing search, caching and GraphQL APIs
1. Business Logic

Outserv aims to make it trivial for anyone to bring up a production grade
GraphQL tech stack -- which we consider to be an important step to web3
decentralization.

## Install from Source

If you want to install from source, install Go 1.13+ or later and the following dependencies:

### Ubuntu

```bash
sudo apt-get update
sudo apt-get install gcc make
```

### Build and Install

Then clone the Outserv repository and use `make install` to install the Dgraph binary to `$GOPATH/bin`.

```bash
git clone https://github.com/outcaste-io/outserv.git
cd ./outserv
make install
```

### Run

```bash
$ outserv graphql # To run the server
$ outserv graphql --help # To see the various customization options.
```

## Get Started
**To get started with Outserv, follow the Dgraph tutorials (for now):**

- Installation to queries in 3 steps via [dgraph.io/docs/](https://dgraph.io/docs/get-started/).
- A longer interactive tutorial via [dgraph.io/tour/](https://dgraph.io/tour/).
- Tutorial and
presentation videos on [YouTube channel](https://www.youtube.com/channel/UCghE41LR8nkKFlR3IFTRO4w/featured).

## Bugs / Feature Requests

To report bugs or request features, please use GitHub issues. Please do answer these
following questions:

1. What is the problem you are trying to solve for?
2. What did you do?
3. What did you expect?
4. What did you see instead?

## License

This project, as a whole, is licensed under the terms of the Smart License v1.0.
A copy of the Smart license is available in [LICENSE.md](LICENSE.md) file.
Frequently asked questions about the license are addressed in
[LICENSE_FAQ.md](LICENSE_FAQ.md).

Certain portions of this project are licensed by contributors or others
under the terms of open source licenses such as the Apache 2.0 license.
