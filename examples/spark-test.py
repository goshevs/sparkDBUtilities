################################################################################
### Pushing a RDD to a distributed instance of MDB
##
##
##
##
## Simo Goshev
## Oct 30, 2019

import sys
import sparkArgsParser as sap
import sparkToDistMDB as stdb
from pyspark.sql import SparkSession
from collections import defaultdict


## Debug Flag
debugFlag = True

## Collect and parse the arguments passed to the script
myArgs = sap.parseArguments(sys.argv)

## Initialize a spark session
spark = SparkSession.builder.getOrCreate()

## Load dataset
print('PRINTX: Loading data')
myData = spark.read.csv(myArgs['dataSet'], 
                        header=True, 
                        sep = '*')
                        ## inferSchema = True)



################################################################################
### SET UP CONNECTIONS AND CREDENTIALS

## One time call per session (if writing to the same DB) to set up connectivity
print('PRINTX: Set up connectivity and credentials')
stdb.pushAdminToMDB(dbNodes = myArgs['dbNodes'],
                    dbBENodes = myArgs['dbBENodes'],
                    dbPort = myArgs['dbPort'],
                    dbUser = myArgs['dbUser'],
                    dbPass = myArgs['dbPass'],
                    dbName = myArgs['dbName'],
                    groupSuffix = myArgs['dbName'],
                    debug = debugFlag)
 

################################################################################
### PARTITION BY LIST COLUMNS

myTableName =  'myTableListColumns'
myPartition = defaultdict(list)
myPartition['ORGID'] = [['1', '2']]

## Testing changing of the column type
## myData = myData.withColumn('ORGID', myData.year.cast('string'))
myNewType = dict({'ORGID': 'VARCHAR(30)'})

## Change the table schema
myTableSchema = stdb.getSchema(myData, changeType = myNewType)

## Table-specific call that sets up the distributed table
print('PRINTX: Push the schema to the distributed db (LIST COLUMNS)')
stdb.pushSchemaToMDB(dbNodes = myArgs['dbNodes'],
                     dbName = myArgs['dbName'],
                     dbTableName = myTableName,
                     tableSchema = myTableSchema,
                     partColumn = [key for key in myPartition],
                     partitionString = stdb.partitionByListColumn(
                         myPartition,
                         myTableSchema,
                         myArgs['dbBENodes']),
                     groupSuffix = myArgs['dbName'],
                     debug = debugFlag
                     )

if not debugFlag:
    print('PRINTX: Push the data to the distributed db (LIST COLUMNS)')
    myData.write \
      .jdbc(myArgs['dbUrl'], myTableName, mode = 'append', 
            properties={'user': myArgs['dbUser'],
                        'password': myArgs['dbPass']})


################################################################################
### PARTITION BY RANGE COLUMNS

myTableName =  'myTableRangeColumns'
myPartition = defaultdict(list)
myPartition['year'] = ['2011', '2013']
myPartition['ORGID'] = ['1', '3']

## Type change from TEXT to VARCHAR(30)
myNewType = dict({'year': 'INTEGER', 'ORGID': 'VARCHAR(30)'})

## Change the table schema
myTableSchema = stdb.getSchema(myData, changeType = myNewType)

## Table-specific call that sets up the distributed table
print('PRINTX: Push the schema to the distributed db (RANGE COLUMNS)')
stdb.pushSchemaToMDB(dbNodes = myArgs['dbNodes'],
                     dbName = myArgs['dbName'],
                     dbTableName = myTableName,
                     tableSchema = myTableSchema,
                     partColumn = [key for key in myPartition],
                     partitionString = stdb.partitionByRangeColumn(
                         myPartition,
                         myTableSchema,
                         myArgs['dbBENodes'],
                         maxValAdd = False),
                     groupSuffix = myArgs['dbName'],
                     debug = debugFlag
                     )
if not debugFlag:
    print('PRINTX: Push the data to the distributed db (RANGE COLUMNS)')
    myData.write \
      .jdbc(myArgs['dbUrl'], myTableName, mode = 'append',
            properties={'user': myArgs['dbUser'],
                        'password': myArgs['dbPass']})


################################################################################
### PARTITION BY HASH

myTableName = 'myTableHash'
partColumn = 'ORGID'

## Type change from TEXT to VARCHAR(30)
myNewType = dict({'ORGID': 'VARCHAR(30)'})

## Change the table schema
myTableSchema = stdb.getSchema(myData, changeType = myNewType)


## Table-specific call that sets up the distributed table
print('PRINTX: Push the schema to the distributed db (HASH)')
stdb.pushSchemaToMDB(dbNodes = myArgs['dbNodes'],
                     dbName = myArgs['dbName'],
                     dbTableName = myTableName,
                     tableSchema = myTableSchema,
                     partColumn = partColumn,
                     partitionString = stdb.partitionByHash(
                         partColumn,
                         myArgs['dbBENodes']),
                     groupSuffix = myArgs['dbName'],
                     debug = debugFlag
                     )

if not debugFlag:
    print('PRINTX: Push the data to the distributed db (HASH)')
    myData.write \
      .jdbc(myArgs['dbUrl'], myTableName, mode = 'append',
            properties={'user': myArgs['dbUser'],
                        'password': myArgs['dbPass']})


################################################################################
### NO PARTITIONING

myTableName = 'myTableNonDist'

## Table-specific call that sets up the distributed table
print('PRINTX: Push the schema to the frontend db (NON-DISTRIBUTED)')
stdb.pushSchemaToMDB(dbNodes = myArgs['dbNode'],
                     dbName = myArgs['dbName'],
                     dbTableName = myTableName,
                     tableSchema = stdb.getSchema(myData),
                     groupSuffix = myArgs['dbName'],
                     debug = debugFlag
                     )

if not debugFlag:
    print('PRINTX: Push the data to frontend db (NON-DISTRIBUTED)')
    myData.write \
      .jdbc(myArgs['dbUrl'], myTableName, mode = 'append',
            properties={'user': myArgs['dbUser'],
                        'password': myArgs['dbPass']})


spark.stop()
