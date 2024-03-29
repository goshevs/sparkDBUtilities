################################################################################
### Pushing a RDD to a distributed instance of MDB
##
##
##
##
## Simo Goshev
## Nov 04, 2019


rm(list=ls())

## Load packages
require(SparkR)
require(stringr)
require(RCurl)
require(digest)


## Load scripts
## myPath = '/Users/goshev/Desktop/gitProjects/sparkDBUtilities/R/'
myPath = '/data/goshev/projects/sparktest/scripts/'
source(paste0(myPath, 'sparkArgsParser.R'))
source(paste0(myPath, 'sparkToDistMDB.R'))
       
## myURL <- c("https://raw.githubusercontent.com/goshevs/sparkDBUtilities/master/R/sparkArgsParser.R",
##            "https://raw.githubusercontent.com/goshevs/sparkDBUtilities/master/R/sparkToDistMDB.R")
## eval(parse(text = getURL(myURL[1], ssl.verifypeer = FALSE)))
## eval(parse(text = getURL(myURL[2], ssl.verifypeer = FALSE)))


## Debug Flag
debugFlag = FALSE


## Collect the arguments passed to the script
myArgs = commandArgs(trailingOnly=TRUE)

## Parse the arguments
userConfig <- parseArguments(myArgs)

## Initialize a spark session
sparkR.session()

## Load dataset
print("PRINTX: Loading data")

myData <- read.df(userConfig$dataSet,
                  header = T,
                  na.strings = "",
                  source = "csv",
                  sep = "*")


################################################################################
### SET UP CONNECTIONS AND CREDENTIALS

## One time call per session (if writing to the same DB) to set up connectivity
print("PRINTX: Set up connectivity and credentials ")
pushAdminToMDB(dbNodes = userConfig$dbNodes,
               dbBENodes = userConfig$dbBENodes, 
               dbPort = userConfig$dbPort,
               dbUser = userConfig$dbBEUser,
               dbPass = userConfig$dbBEPass,
               dbName = userConfig$dbName,
               groupSuffix = userConfig$dbName,
               debug = debugFlag
              )


################################################################################
### PARTITION BY LIST COLUMNS

myTableName <- 'myTableListColumns'
myPartition <- list('ORGID' = list(c('1', '2'), c('3', '4', '5')))

## Type change from TEXT to VARCHAR(30)
myNewType <- list('ORGID' = 'VARCHAR(30)')

## Change the table schema
myTableSchema <- getSchema(myData, changeType = myNewType)

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the distributed db (LIST COLUMNS)")
pushSchemaToMDB(dbNodes = userConfig$dbNodes,
                dbName = userConfig$dbName,
                dbTableName = myTableName,
                tableSchema = myTableSchema,
                partColumn = names(myPartition),
                partitionString = partitionByListColumn(
                    myPartition,
                    userConfig$dbBENodes,
                    myTableSchema,
                    defaultAdd = FALSE),
                groupSuffix = userConfig$dbName,
                debug = debugFlag
                )


if (!debugFlag) {
    print("PRINTX: Push the data to the distributed db (LIST COLUMNS)")
    write.jdbc(myData, userConfig$dbUrl, myTableName, mode = "append",
               user = userConfig$dbUser, password = userConfig$dbPass)
}


################################################################################
### PARTITION BY RANGE COLUMNS

myTableName <- "myTableRangeColumns"
myPartition <- list('year' = c('2013'),
                    'ORGID' = c('3'))

## Type change from TEXT to VARCHAR(30)
myNewType <- list('year' = 'INTEGER',
                  'ORGID' = 'VARCHAR(30)')

## Change the table schema
myTableSchema <- getSchema(myData, changeType = myNewType)

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the distributed db (RANGE COLUMNS)")
pushSchemaToMDB(dbNodes = userConfig$dbNodes,
                dbName = userConfig$dbName,
                dbTableName = myTableName,
                tableSchema = myTableSchema,
                partColumn = names(myPartition),
                partitionString = partitionByRangeColumn(
                    myPartition,
                    userConfig$dbBENodes,
                    myTableSchema,
                    maxValAdd = TRUE),
                groupSuffix = userConfig$dbName,
                debug = debugFlag
                )

if (!debugFlag) {
    print("PRINTX: Push the data to the distributed db (RANGE COLUMNS)")
    write.jdbc(myData, userConfig$dbUrl, myTableName, mode = "append",
               user = userConfig$dbUser, password = userConfig$dbPass)
}

################################################################################
### PARTITION BY HASH


myTableName <- "myTableHash"
partColumn <- "ORGID"

## Type change from TEXT to INTEGER
myNewType <- list('ORGID' = 'MEDIUMINT')

## Change the table schema
myTableSchema <- getSchema(myData, changeType = myNewType)


## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the distributed db (HASH)")
pushSchemaToMDB(dbNodes = userConfig$dbNodes,
                dbName = userConfig$dbName,
                dbTableName = myTableName,
                tableSchema = myTableSchema,
                partColumn = partColumn,
                partitionString = partitionByHash(
                    partColumn,
                    userConfig$dbBENodes),
                groupSuffix = userConfig$dbName,
                debug = debugFlag
                )

if (!debugFlag) {
    print("PRINTX: Push the data to the distributed db (HASH)")
    write.jdbc(myData, userConfig$dbUrl, myTableName, mode = "append",
               user = userConfig$dbUser, password = userConfig$dbPass)
}

################################################################################
### NO PARTITIONING

myTableName <- "myTableNonDist"

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the frontend db (NON-DISTRIBUTED)")
pushSchemaToMDB(dbNodes = userConfig$dbNode,
                dbName = userConfig$dbName,
                dbTableName = myTableName,
                tableSchema = getSchema(myData),
                groupSuffix = userConfig$dbName,
                debug = debugFlag
                )

if (!debugFlag) {
    print("PRINTX: Push the data to frontend db (NON-DISTRIBUTED)")
    write.jdbc(myData, userConfig$dbUrl, myTableName, mode = "append",
               user = userConfig$dbUser, password = userConfig$dbPass)
}

sparkR.stop()
