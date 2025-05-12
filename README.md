# SCDM: Structured Common Data Model

A tool to index and query performance metrics that come from [Crucible](https://github.com/perftool-incubator/crucible) runs.
This is an extension of the [CDM](https://github.com/perftool-incubator/CommonDataModel), that
attempts to use as many of the prior concepts and structures as possible, while providing
a normalized relational model.


```
SCDM: Structured Common Data Model - A tool to index and query performance metrics that come from Crucible runs

Usage: scdm [OPTIONS] <COMMAND>

Commands:
  parse   Parse the results of a crucible iteration and import into DB
  query   Query the the CDM DB
  import  Import run from OpenSearch CDM DB
  init    Init the SCDM tables if they don't exist
  help    Print this message or the help of the given subcommand(s)
```

Note that the `parse` command relies on parsing the `.ndjson` docs that the new `rickshaw-gen-docs` phase of a Crucible run will produce.
These docs are suitable to be uploaded to OpenSearch using the bulk API, but in this case we parse them into a relation format.
If you don't have these `.ndjson` files present. It is recommended to use the `import` command to pull them directly from a local
OpenSearch instance.

## Relational Model

![Relational Model](model.svg)
