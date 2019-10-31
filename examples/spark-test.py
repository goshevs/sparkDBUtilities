################################################################################
### Pushing a RDD to a distributed instance of MDB
##
##
##
##
## Simo Goshev
## Oct 30, 2019

import sys, sparkArgsParser, sparkToDistMDB
from pyspark.sql import SparkSession
from collections import defaultdict


    
## Collect and parse the arguments passed to the script
myArgs = parseArguments(sys.argv)

## Initialize a spark session
spark = SparkSession.builder.getOrCreate()

## Load dataset
print("PRINTX: Loading data")
myData = spark.read.csv(myArgs['dataSet'], header=True, sep = '*')


################################################################################
### SET UP CONNECTIONS AND CREDENTIALS

## One time call per session (if writing to the same DB) to set up connectivity
print("PRINTX: Set up connectivity and credentials")
pushAdminToMDB(dbNodes = myArgs['dbNodes'],
               dbBENodes = myArgs['dbBENodes'],
               dbPort = myArgs['dbPort'],
               dbUser = myArgs['dbUser'],
               dbPass = myArgs['dbPass'],
               dbName = myArgs['dbName'],
               groupSuffix = myArgs['dbName'])
 

################################################################################
### PARTITION BY LIST COLUMNS

myTableName =  "myTableListColumns"
myD = defaultdict(list)
myD['ORGID'] = [['1', '2'], ['3', '4', '5']]

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the distributed db")
pushSchemaToMDB(dbNodes = myArgs['dbNodes'],
                dbName = myArgs['dbName'],
                dbTableName = myTableName,
                tableSchema = getSchema(myData, makeJdbcKey()),
                groupSuffix = myArgs['dbName'],
                partColumn = [key for key in myD],
                partitionString = partitionByListColumn(myD,
                                                        myArgs['dbBENodes']) )

print("PRINTX: Push the data to the distributed db (LIST COLUMNS)")
myData.write \
  .mode("append") \
  .jdbc(myArgs['dbUrl'], myTableName,
        properties={"user": myArgs['dbUser'],
                    "password": myArgs['dbPass']})


################################################################################
### PARTITION BY RANGE COLUMNS

myTableName =  "myTableRangeColumns"
myD = defaultdict(list)
myD['year'] = ['2013']
myD['ORGID'] = ['3']

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the distributed db")
pushSchemaToMDB(dbNodes = myArgs['dbNodes'],
                dbName = myArgs['dbName'],
                dbTableName = myTableName,
                tableSchema = getSchema(myData, makeJdbcKey()),
                groupSuffix = myArgs['dbName'],
                partColumn = [key for key in myD],
                partitionString = partitionByRangeColumn(myD,
                                                         myArgs['dbBENodes']) )

print("PRINTX: Push the data to the distributed db (RANGE COLUMNS)")
myData.write \
  .mode("append") \
  .jdbc(myArgs['dbUrl'], myTableName,
        properties={"user": myArgs['dbUser'],
                    "password": myArgs['dbPass']})


################################################################################
### PARTITION BY HASH

myTableName = "myTableHash"
partColumn = "ORGID"

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the distributed db")
pushSchemaToMDB(dbNodes = myArgs['dbNodes'],
                dbName = myArgs['dbName'],
                dbTableName = myTableName,
                tableSchema = getSchema(myData, makeJdbcKey()),
                groupSuffix = myArgs['dbName'],
                partColumn = [key for key in myD],
                partitionString = partitionByHash(partColumn,
                                                  myArgs['dbBENodes']) )
   
print("PRINTX: Push the data to the distributed db (HASH)")
myData.write \
  .mode("append") \
  .jdbc(myArgs['dbUrl'], myTableName,
        properties={"user": myArgs['dbUser'],
                    "password": myArgs['dbPass']})


################################################################################
### NO PARTITIONING

myTableName = "myTableNonDist"

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the frontend db")
   pushSchemaToMDB(dbNodes = myArgs['dbNodes'],
                    dbName = myArgs['dbName'],
                    dbTableName = myTableName,
                    tableSchema = getSchema(myData, makeJdbcKey()),
                    groupSuffix = myArgs['dbName'])

print("PRINTX: Push the data to frontend db (NON-DISTRIBUTED)")
myData.write \
  .mode("append") \
  .jdbc(myArgs['dbUrl'], myTableName,
        properties={"user": myArgs['dbUser'],
                    "password": myArgs['dbPass']})


spark.stop()
