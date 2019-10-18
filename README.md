# R utilities for pushing Spark RDD's to MariaDB distributed tables

## Introduction

The repo contains a set of R utilities for pushing Spark
RDD's to MariaDB distributed tables.


## Installation

Download/clone the repo and source the R-scripts from within your R script.

## Overview of utilities

The repo contains the following primary R functions:

- `pushAdminToMDB`: sets up connections, access credentials and
  rights among the frontend and backend MariaDB db instances. The
  function is only useful/needed if pushing to a distributed db.
  
- `pushSchemaToMDB`: pushes the schema of the RDD to the db. The
  function is designed to work with distributed db's but can also be
  used with non-distributed instances of MariaDB. The advantage of
  using it in the latter case is that it sets up an auto-increment
  field `id` which can be used as a partitioning variable for a read
  into Spark.
  
The repo also offers several utility functions:
 
- `parseArguments`: a parser of the arguments passed to a SparkR script

- `partitionByListColumn`: a string writer corresponding to
partitioning by
[LIST COLUMNS](https://mariadb.com/kb/en/library/range-columns-and-list-columns-partitioning-types/).
At this time, *partitioning_expression* can only be a variable name, i.e.
expressions are not supported.

- `partitionByHash`: a string writer corresponding to partitioning by
[HASH](https://mariadb.com/kb/en/library/spider-use-cases/), see *Use
case 2*.

- `partitionByRangeColumn`: a string writer corresponding to
partitioning by
[RANGE COLUMNS](https://mariadb.com/kb/en/library/range-columns-and-list-columns-partitioning-types/).


`pushSchemaToMDB` accepts user-supplied partitioning strings and
therefore users are not confined to the three types of
partitioning logic offered here.


## Detailed description

A detailed description of all utilities will be provied in due course.

## Examples

Please, see `spark-test.R` for a use case.

