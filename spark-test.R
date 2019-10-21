################################################################################
### Pushing a RDD to a distributed instance of MDB
##
##
##
##
## Simo Goshev
## Oct 18, 2019


rm(list=ls())

## Load packages
library(SparkR)
library(stringr)
library(RCurl)


## Load scripts
myURL <- c("https://raw.githubusercontent.com/goshevs/sparkDBUtilities/devel/sparkArgsParser.R",
           "https://raw.githubusercontent.com/goshevs/sparkDBUtilities/devel/sparkToDistMDB.R")
eval(parse(text = getURL(myURL[1], ssl.verifypeer = FALSE)))
eval(parse(text = getURL(myURL[2], ssl.verifypeer = FALSE)))


## Collect the arguments passed to the script
myArgs = commandArgs(trailingOnly=TRUE)

## Parse the arguments
userConfig <- parseArguments(myArgs)

## Initialize a spark session
sparkR.session()


## Load dataset
print("PRINTX: Loading data")

print(userConfig$dataSet)

myData <- read.df(userConfig$dataSet,
                  header = T,
                  na.strings = "",
                  source = "csv",
                  sep = "*",
                  inferSchema = TRUE)
    
## Define parameters for a MDB table push
myTableName <- "myTableListColumns"
partColumn <- "ORGID"
myPartitions <- list(c('1', '2'),
                     c('3', '4', '5'))

## One time call per session (if writing to the same DB) to set up connectivity
print("PRINTX: Set up connectivity and credentials ")
pushAdminToMDB(dbNodes = userConfig$dbNodes,
               dbBENodes = userConfig$dbBENodes, 
               dbPort = userConfig$dbPort,
               dbUser = userConfig$dbBEUser,
               dbPass = userConfig$dbBEPass,
               dbName = userConfig$dbName,
               groupSuffix = userConfig$dbName)

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the distributed db")
pushSchemaToMDB(dbNodes = userConfig$dbNodes,
                dbName = userConfig$dbName,
                dbTableName = myTableName,
                tableSchema = getSchema(myData),
                partColumn = partColumn,
                partitionString = partitionByListColumn(partColumn, myPartitions),
                groupSuffix = userConfig$dbName)

print("PRINTX: Push the data to the distributed db (LIST COLUMNS)")
write.jdbc(myData, userConfig$dbUrl, myTableName, mode = "append",
           user = userConfig$dbUser, password = userConfig$dbPass)


##########################


myTableName <- "myTableRangeColumns"
partColumn <- c("year", "ORGID")
myPartitions <- list(c('2013', '3'))

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the distributed db")
pushSchemaToMDB(dbNodes = userConfig$dbNodes,
                dbName = userConfig$dbName,
                dbTableName = myTableName,
                tableSchema = getSchema(myData),
                partColumn = partColumn,
                partitionString = partitionByRangeColumn(partColumn, myPartitions),
                groupSuffix = userConfig$dbName)

print("PRINTX: Push the data to the distributed db (RANGE COLUMNS)")
write.jdbc(myData, userConfig$dbUrl, myTableName, mode = "append",
           user = userConfig$dbUser, password = userConfig$dbPass)

############################

myTableName <- "myTableHash"
partColumn <- "ORGID"

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the distributed db")
pushSchemaToMDB(dbNodes = userConfig$dbNodes,
                dbName = userConfig$dbName,
                dbTableName = myTableName,
                tableSchema = getSchema(myData),
                partColumn = partColumn,
                partitionString = partitionByHash(partColumn,userConfig$dbBENodes),
                groupSuffix = userConfig$dbName)

print("PRINTX: Push the data to the distributed db (HASH)")
write.jdbc(myData, userConfig$dbUrl, myTableName, mode = "append",
           user = userConfig$dbUser, password = userConfig$dbPass)


##############################


myTableName <- "myTableNonDist"

## Table-specific call that sets up the distributed table
print("PRINTX: Push the schema to the frontend db")
pushSchemaToMDB(dbNodes = userConfig$dbNode,
                dbName = userConfig$dbName,
                dbTableName = myTableName,
                tableSchema = getSchema(myData),
                groupSuffix = userConfig$dbName)

print("PRINTX: Push the data to the distributed db (NON-DISTRIBUTED)")
write.jdbc(myData, userConfig$dbUrl, myTableName, mode = "append",
           user = userConfig$dbUser, password = userConfig$dbPass)

