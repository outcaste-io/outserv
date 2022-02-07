Outserv: GraphQL Indexing and Serving System

Outserv is a fork of [Dgraph](https://github.com/dgraph-io/dgraph), focusing on
serving data via GraphQL. This fork is being maintained by Manish Jain,
ex-founder of Dgraph Labs and the Outcaste team consisting of ex-Dgraph
employees.

Outserv's goal is to provide [Google](https://www.google.com) production level scale and throughput,
while serving real-time user queries over terabytes of structured data.
Dgraph supports GraphQL, and responds in JSON.

**Outserv's main focus is on serving structured data via GraphQL.** While we
would still maintain DQL query language, we might remove certain features of DQL
to simplify the system and improve storage and performance.

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

This project, as a whole, is licensed under the terms of the Smart License.
Certain portions of this project are licensed by contributors or others
under the terms of open source licenses such as the Apache 2.0 license.
