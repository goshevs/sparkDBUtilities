################################################################################
### Pushing a RDD to a distributed instance of MDB
##
##
##
##
## Simo Goshev
## Oct 17, 2019


rm(list=ls())

## Load packages
library(SparkR)
library(stringr)


## Load scripts
source("~/scripts/r-utilities/sparkArgsParser.R") 
source("~/scripts/r-utilities/sparkToDistMDB.R")

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
                  sep = "*",
                  inferSchema = TRUE)

    
## Define parameters for a MDB table push
myTableName <- "myTable"
partColumn <- "ORGID"
myPartitions <- list(c('1', '2'),
                     c('3', '4', '5'))


## One time call per session which sets up connectivity
print("Set up connectivity and credentials ")
pushAdminToMDB(dbNodes = userConfig$dbNodes,
               dbPort = userConfig$dbPort,
               dbUser = userConfig$dbBEUser,
               dbPass = userConfig$dbBEPass,
               dbName = userConfig$dbName,
               dbTableName = myTableName,
               groupSuffix = myDbName)

## Table-specific call that sets up the distributed table
print("Push the schema to the distributed db")
pushSchemaToMDB(dbNodes = userConfig$dbNodes,
                dbName = userConfig$dbName,
                dbTableName = myTableName,
                tableSchema = getSchema(myData),
                partColumn = partColumn,
                partitionString = partitionByListColumn(partColumn, myPartitions),
                groupSuffix = myDbName)

print("Push the data to the distributed db")
write.jdbc(myData, userConfig$dbUrl, myTableName, mode = "append",
           user = userConfig$dbUser, password = userConfig$dbPass)



