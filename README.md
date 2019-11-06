#  Utilities for pushing Spark RDD's to MariaDB distributed databases

## Introduction

The repo contains a set of Python and R utilities for pushing Spark
RDD's to MariaDB distributed databases. The need for such utilities
arises from the fact that table schemas in MariaDB are not propagated
automatically from the frontend instance to the backend instances. The
utilities in this repo implement the propagation.


## Installation

### R scripts

Clone/download the repo and source the R-scripts from your script. It
is also possible to source them directly from the repo using:

``` 
myURL <- c(
    "https://raw.githubusercontent.com/goshevs/sparkDBUtilities/master/R/sparkArgsParser.R",
    "https://raw.githubusercontent.com/goshevs/sparkDBUtilities/master/R/sparkToDistMDB.R")
eval(parse(text = getURL(myURL[1], ssl.verifypeer = FALSE)))
eval(parse(text = getURL(myURL[2], ssl.verifypeer = FALSE)))
```

### Python modules

Clone/download the repo and import the modules in your script. It may
be necessary to update the `PYTHONPATH` environment variable with the
location of the scripts to make them discoverable by Python.


## Overview of utilities

All R and Python functions have identical syntax which should
facilitate transitions between the software packages.

### sparkToDistMDB

This script/module contains two primary functions:

- `pushAdminToMDB`: sets up connections, access credentials and
  rights among the frontend and backend MariaDB db instances. The
  function is only useful/needed if pushing to a distributed db.
  
- `pushSchemaToMDB`: pushes the schema of an RDD to the db. The
  function is designed to work with distributed db's but can also be
  used with non-distributed instances of MariaDB. The advantage of
  using it in the latter case is that it sets up an auto-increment
  field `id` which can be used as a partitioning variable for reading
  a db table into Spark.

The script/module also contains four utility functions:

- `getSchema`: retrieves the RDD schema. 

- `partitionByListColumn`: a string writer corresponding to
partitioning by
[LIST COLUMNS](https://mariadb.com/kb/en/library/range-columns-and-list-columns-partitioning-types/).
At this time, *partitioning_expression* can only be a variable name, i.e.
expressions are not supported.

- `partitionByHash`: a string writer corresponding to partitioning by
[HASH](https://mariadb.com/kb/en/library/spider-use-cases/)(see *Use
case 2*).

- `partitionByRangeColumn`: a string writer corresponding to
partitioning by
[RANGE COLUMNS](https://mariadb.com/kb/en/library/range-columns-and-list-columns-partitioning-types/).


`pushSchemaToMDB` accepts user-supplied partitioning strings and
therefore users are not confined to these three types of
partitioning logic.


### sparkArgsParser

This script/module contains function `parseArguments` which parses
the arguments passed to `$MY_SPARK_JOBSCRIPT` as defined in the
SparkHPC setup scripts. 

In R, `parseArguments` outputs a list, while in Python it outputs a
dictionary, with the following keys:

- `dataSet`:  /path/to/dataset/file  
- `dbName`: name of the database to write to  
- `dbNode`: **master** node of the database  
- `dbPort`: port of the **frontend** db node  
- `dbUser`: user name for logging in to the **frontend** db node  
- `dbPass`: password for logging in to the **frontend** db node  
- `dbUrl`: jdbc database connection string   
- `dbNodes`: list of all nodes on the db cluster  
- `dbBEUser`: user name for logging in to the **backend** db servers  
- `dbBEPass`: passowrd for `dbBEUser`  
- `dbBENodes`: list of db backend nodes  


## Syntax

### `pushAdminToMDB`

Utility `pushAdminToMDB` has the following syntax:

```
pushAdminToMDB(dbNodes, dbBENodes, dbPort,
               dbUser, dbPass, dbName, 
               groupSuffix, debug)
```

Where:

- `dbNodes`: list of all nodes on the db cluster  
- `dbBENodes`: list of db backend nodes  
- `dbPort`: port of the **backend** nodes (default: 3306)  
- `dbUser`: user name for logging in to the **backend** nodes  
- `dbPass`: password for logging in to the **backend** nodes  
- `dbName`: name of the database to write to  
- `groupSuffix`: the tag in `.my.cnf` file to refer for login
  informaiton when logging to the db  
- `debug`: if TRUE/True, prints out all commands instead of execturing
  them (default: FALSE/False)

<br>

### `pushSchemaToMDB`

Utility `pushSchemaToMDB` has the following syntax:

```
pushSchemaToMDB(dbNodes, dbName, dbTableName, tableSchema,
                partColumn, partitionString, groupSuffix,
                debug)
```

Where:

- `dbNodes`: list of all nodes on the db cluster  
- `dbName`: name of the database to write to  
- `dbTableName`: name of the table to create  
- `tableSchema`: the schema of the RDD to be written to the db  
- `partColumn`: list of columns for partitioning  
- `partitionString`: string with db partitioning commands  
- `groupSuffix`: the tag in `.my.cnf` file to refer to for db login
  information  
- `debug`: if TRUE/True, prints out all commands instead of executing
  them (default: FALSE/False)  

If pushing to a non-distributed database instance or to the frontend 
of a distributed database instance:  
- `dbNodes` would be the string of the name of the node to push to  
- arguments `partColumn`, `partitionString`, and `changeType` should
  be omitted.

<br>

### `getSchema`

Utility `getSchema` has the following syntax:

```
getSchema(RDD, key, changeType)
```

Where:

- `RDD`: a Spark RDD  
- `key`: the key that matches Spark SQL column types to MariaDB column
  types. Currently, *DecimalType* is not supported. A default key is
  provided by functions `makeJdbcKey` which are included in the
  respective R or Python `sparkToDistMDB` file.
- `changeType`: optional list (in R) or dictionary (in Python) containing
  key-value pairs of column name and column type with the
  changes to Spark-created/inferred schema the user wishes to implement  

<br>

### `partitionByListColumn`

Utility `pushAdminToMDB` has the following syntax:

``` 
partitionByListColumn(partitionRules, beNodes, tableSchema, defaultAdd)
```

Where:

- `partitionRules`: list (in R) or dictionary (in Python) with
  partitioning rules. The admissible structures/containers are:  
  - R: a list of named lists (where names are RDD column names)  
  - Python: a default dictionary where every value is a list  
- `beNodes`: list of db backend nodes  
- `tableSchema`: the schema of the RDD to be written to the db  
- `defaultAdd`: add the `DEFAULT` partitioning provision to the partitioning
  rules (default: TRUE/True). This feature is supported on MariaDB 10.2 and
  higher  
  
<br>

### `partitionByHash`

Utility `partitionByHash` has the following syntax:

```
 partitionByHash(partColumn, beNodes)
 ```
 
 Where: 
 
 - `partColumn`: an RDD column name  
 - `beNodes`: list of db backend nodes  
 
<br> 

### `partitionByRangeColumn`

Utility `partitionByRangeColumn` has the following syntax:

```
partitionByRangeColumn(partitionRules, beNodes, tableSchema,
                       maxValAdd, sortVal)
```

Where:

- `partitionRules`: list (in R) or dictionary (in Python) with
  partitioning rules. The admissible structures/containers are:  
  - R: a named list of vectors (where names are RDD column names)  
  - Python: a default dictionary where every value is a list  
- `beNodes`: list of db backend nodes  
- `tableSchema`: the schema of the RDD to be written to the db  
- `maxValAdd`: add `maxvalue` clauses to the partitioning rules
  (default: TRUE/True)  
- `sortVal`: Sorts the partitioning values in an increasing order
  (default: TRUE/True). Applicable if partitioning by a single column. 


## Limitations

Users should consider the following constraints when using this
software:
- The number of partitions **must equal** the number of backend db
  instances.
- As mentioned above, columns of *DecimalType* are not supported at
  this time.
- `partitionByListColumn` accepts columns names only as expressions
  are not supported at this time.
  

## Examples

Please, see directory `examples` for a use case in Python and R.

