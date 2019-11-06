#!/usr/local/bin/python

################################################################################
###  Functions for pushing RDD's from Spark to a distibuted instance of MariaDB
##
##
##
##
##
##
## Simo Goshev
## Nov 04, 2019


import copy, subprocess, re
from collections import defaultdict
from datetime import datetime

    
################################################################################
### UTILITY FUNCTIONS

## Repeat
def repeat(x,y):
    ''' Repeating vales of list x the number of times in list y '''

    return([i for i,j in zip(x, y) for k in range(j)])


## Replace integer with string partitioning values
def partColVal2String(x, tableSchema):
    ''' Write out string partitioning values'''

    quotedTypeList = ['CHAR', 'VARCHAR', 'BINARY',
                      'CHAR BYTE', 'VARBINARY']
    reString = '|'.join(quotedTypeList)

    partColNames = [k for k in x if
                    len(re.findall(reString, tableSchema[k][2])) > 0]

    myNewD = defaultdict(list)
    if len(partColNames) > 0:
        ## Replace values
        for k in partColNames:
            for l in x[k]:
                if type(l).__name__ != "list":
                    mylist = "\'" + str(l) + "\'"
                else:
                    mylist = [("\'" + str(m) + "\'") for m in l]
                myNewD[k].append(mylist)
            x[k] = myNewD[k]
    return(x)

    
################################################################################
### CREATE CONVERSION KEY

def makeJdbcKey():
    ''' Update this functions if definitions change '''

    ## Create the key
    colType = ["IntegerType", "LongType", "DoubleType", "FloatType", "ShortType",
               "ByteType", "BooleanType", "StringType",  "BinaryType",  "TimestampType",
               "DateType"]
    mysqlType = ["INTEGER", "BIGINT", "DOUBLE PRECISION", "REAL", "INTEGER", "BYTE",
                "BIT(1)", "TEXT", "BLOB", "TIMESTAMP", "DATE"]
    myKey = dict(zip(colType, mysqlType))
    
    return(myKey)


################################################################################
### RETRIEVING TABLE SCHEMA

def getSchema(myData, key = makeJdbcKey(), changeType = None ):
    ''' Get the schema of the data and add mySQL data types '''     

    ## Schema
    mySchema = myData.schema

    ## Initialize a container
    mySchemaDict = defaultdict(list)

    ## Collect fields
    for i in mySchema.fields:
        mySchemaDict[i.name].extend([str(i.dataType), i.nullable])     

    ## Attach the SQL types to the variables
    for sKey in mySchemaDict:
        if str(mySchemaDict[sKey][0]) == "DecimalType":
            raise TypeError("DecimalType is present in data. Possible errors. Please, address accordingly")
        else:
            if changeType is None or sKey not in changeType:
                mySchemaDict[sKey].append(key[mySchemaDict[sKey][0]])
            else:
                mySchemaDict[sKey].append(changeType[sKey].upper())  
    
    return(mySchemaDict)




################################################################################
### SHARDING RULES

## ====== >>> SHARDING BY LIST COLUMNS

def partitionByListColumn(partitionRules, tableSchema, beNodes,
                          defaultAdd = True):
    ''' Partitioning by LIST COLUMNS '''

    ## Copy the dict to leave the original unaffected
    partDict = copy.deepcopy(partitionRules)

    ## Cast values of partitioning character type columns to character type 
    partDict = partColVal2String(partDict, tableSchema)

    myKey = [key for key in partDict][0]

    ## If defaultAdd, then add default partition provisions
    myDefValue = str(hash('STDB-DEFAULT-PARTITION' + str(hash(datetime.now()))))
    if defaultAdd:
        partDict[myKey].append(myDefValue)

    ## Check for matching element numbers of partValueList and beNodes
    numEl = [len(partDict[key]) for key in partDict][0]
    
    if numEl != len(beNodes):
        raise RuntimeError("The number of partitions must equal the number of backend servers.")
    
    header = "PARTITION BY LIST COLUMNS (" + myKey  + ")"
    
    myPartCommand = []
    for el in partDict[myKey]:
        i = str(partDict[myKey].index(el) + 1)
        if el != myDefValue:
            myString = ("PARTITION pt" + i +  " VALUES IN (" +
                        ', '.join(el) + ") COMMENT = 'srv \\\"backend" + i + "\\\"'")
        else:
            myString = ("PARTITION pt" + i + " DEFAULT COMMENT = 'srv \\\"backend" + i + "\\\"'")
        myPartCommand.append(myString)

    return(header + '(' + ', '.join(myPartCommand) +  ')')



## ====== >>> SHARDING BY HASH

def partitionByHash(partColumn, beNodes):
    ''' Partitioning by HASH '''

    header = "PARTITION BY HASH (" + partColumn + ")"
    myPartCommand = []
    for el in beNodes:
        i = str(beNodes.index(el) + 1)
        myPartCommand.append(("PARTITION pt" + i + " COMMENT = 'srv \\\"backend" + i + "\\\"'"))

    return(header + '(' + ', '.join(myPartCommand) + ')')


    
## ====== >>> SHARDING BY RANGE COLUMNS

def partitionByRangeColumn(partitionRules, tableSchema, beNodes,
                           maxValAdd = True, sortVal = True):
    ''' Partitioning by RANGE COLUMNS '''

    ## Copy the dict to leave the original unaffected
    partDict = copy.deepcopy(partitionRules)

    ## Cast values of partitioning character type columns to character type 
    partDict = partColVal2String(partDict, tableSchema)

    ## Sort partValues; only sort if partColumn < 2!
    if sortVal and len(partDict) < 2:
        [partDict[key].sort() for key in partDict]
            
    ## Add maxvalue to partValuesList
    if maxValAdd:
        [partDict[key].append("maxvalue") for key in partDict]

    ## Check for matching element numbers of partValueList and beNodes
    myCount = [len(partDict[key]) for key in partDict]
    myCountUnique = set(myCount)
    if len(myCountUnique) != 1:
        raise RuntimeError("The number of values among partitioning variables is not identical.")
    if len(beNodes) != myCount[0]:
        raise RuntimeError("The number of partitions must equal the number of backend servers.")
      
    header = "PARTITION BY RANGE COLUMNS (" + ', '.join(partDict.keys()) + ")"

    myPartCommand = []
    for i in range(myCount[0]):
        myVals = []
        for key in partDict:
            myVals.append(partDict[key][i])
        
        myPartCommand.append(("PARTITION pt" +  str(i + 1) + " VALUES LESS THAN (" +
                              ', '.join(myVals) + ") COMMENT = 'srv \\\"backend" + str(i + 1) + "\\\"'"))
        
    return(header + '(' + ', '.join(myPartCommand) + ')')


################################################################################
### COMPOSING MDB CALLS

def pushAdminToMDBString(dbBENodes, dbPort, dbUser, dbPass, dbName, frontEnd = True):
    ''' Write out the string of the command for pushing the admin info to the db '''

    if frontEnd:
        myCommand = []
        for i in range(len(dbBENodes)):
            j = str(i + 1)
            myCommand.append(("DROP SERVER IF EXISTS backend" + j + "; " +
                       "CREATE SERVER backend" + j + " FOREIGN DATA WRAPPER mysql " +
                       "OPTIONS(HOST '" + dbBENodes[i] + "', DATABASE '" + dbName +
                       "', USER '" + dbUser + "', PASSWORD '" + dbPass + "', PORT " + dbPort + ");"))
        myCall = ' '.join(myCommand)
    else:
        myCall = ("CREATE OR REPLACE USER " + dbUser + "; " +
                  "SET PASSWORD FOR " + dbUser + " = PASSWORD('" + dbPass + "'); " +
                  "GRANT ALL ON " + dbName + ".* TO " + dbUser + ";")
        
    return(myCall)


    
def pushSchemaToMDBString(dbTableName, tableSchema, partColumn = None,
                          partitionString = None, frontEnd = True):
    ''' Write out the string of the command for pushing the schema of the table '''

    ## Check for BLOB/TEXT types in partColumn; use user input if provided
    if partitionString is not None and partColumn is not None:  # dist MDB
        if type(partColumn).__name__ == "str":
            partColumn = [partColumn]

        for key in partColumn:
            if len(re.findall('BLOB|TEXT', tableSchema[key][2])) > 0:
                raise RuntimeError(key + ": Partitioning columns cannot be of type BLOB/TEXT")                      
    
    ## Common component
    commonPart = ("DROP TABLE IF EXISTS " + dbTableName + "; " +
                  "CREATE TABLE " + dbTableName + " (" +
                  "id INT NOT NULL AUTO_INCREMENT, " +
                  ', '.join([' '.join([key, tableSchema[key][2]]) + " DEFAULT NULL" for key in tableSchema]))

    ## Default engine
    dbEngine = "InnoDB"

    ## Default key and engine
    dbKeyEngineDefault = ", PRIMARY KEY(id)) ENGINE = " + dbEngine + ";"

    if partitionString is not None and partColumn is not None:  # dist MDB
        ## Write out the schema
        if frontEnd:
            partColumns = partColumn[:]
            ## Add "id" to partColumn (if not in it) for form primaryKeys
            try:
                partColumns.index('id')
            except:
                print("Adding 'id' to the list of columns")
                partColumns.insert(0,'id')

            dbEngine = "SPIDER"
            myCall = (commonPart + 
                      ", PRIMARY KEY(" + ', '.join(partColumns) + ")" +
                      ") ENGINE = " + dbEngine +
                      " COMMENT='wrapper \\\"mysql\\\", table \\\"" + dbTableName + "\\\"' " +
                      partitionString)
        else:
            myCall = commonPart + dbKeyEngineDefault

    elif partitionString is None and partColumn is None:  # non-dist MDB
        myCall = commonPart + dbKeyEngineDefault
        
    else:
        raise RuntimeError("Arguments partColumn and partitionString incorrectly specified")

    return(myCall)


    
def pushToMDB(callList, dbNodes, dbName, groupSuffix, debug):
    ''' Make calls to the db '''

    mySystemCalls = [("mysql --defaults-group-suffix=" + groupSuffix +
                    " -h " + myNode + " -D " + dbName +
                    " -e \"" + myCall + "\"") for myNode, myCall in zip(dbNodes, callList)]    
    ## Push to MDB
    for myCall in mySystemCalls:
        if not debug:
            subprocess.call(myCall, shell = True)
        else:
            print(myCall)

        
################################################################################
### EXECUTING THE MDB CALLS


####### ===>> SETUP CALL

def pushAdminToMDB(dbNodes, dbBENodes, dbPort, dbUser, dbPass, dbName,
                   groupSuffix, debug = False):
    ''' Composing and making the db admin call '''
    ## Number of frontend and backend nodes
    nodeNumVector = [1, len(dbBENodes)]

    ## Write out frontend and backend calls
    myAdminCalls = [pushAdminToMDBString(dbBENodes = dbBENodes,
                                         dbPort = dbPort,
                                         dbUser = dbUser,
                                         dbPass = dbPass,
                                         dbName = dbName,
                                         frontEnd = i)
                    for i in [True, False]]
    
    ## Add backend calls
    myAdminCalls = repeat(myAdminCalls, nodeNumVector)
    
    ## Submit the call
    pushToMDB(myAdminCalls, dbNodes, dbName, groupSuffix, debug)
    

####### ===>> TABLE SCHEMA CALL

def pushSchemaToMDB(dbNodes, dbName, dbTableName, tableSchema, groupSuffix,
                    partColumn = None, partitionString = None,
                    debug = False):
    ''' Composing and making the table schema call to the db '''

    ## Check the tyoe if dbNodes
    if type(dbNodes).__name__ == 'list':
        
        if len(dbNodes) > 1:  # dist DB
            ## Number of frontend and backend nodes
            nodeNumVector = [1, len(dbNodes) - 1]

            ## Write out frontend and backend calls
            mySchemaCall = [pushSchemaToMDBString(dbTableName = dbTableName,
                                                  tableSchema = tableSchema,
                                                  partColumn = partColumn,
                                                  partitionString = partitionString,
                                                  frontEnd = i)
                            for i in [True, False]]
        
            ## Add backend calls
            mySchemaCall = repeat(mySchemaCall, nodeNumVector)

        else:
            raise RuntimeError(("If you wish to write to the frontend only, please" +
                                " provide the name of the node as a string"))
        
    elif type(dbNodes).__name__ == 'str':  #non-dist DB
        mySchemaCall = [pushSchemaToMDBString(dbTableName = dbTableName,
                                              tableSchema = tableSchema)]
        dbNodes = [dbNodes]

    else:
        raise RuntimeError("Incorrect node specification")
    
    ## Submit the call
    pushToMDB(mySchemaCall, dbNodes, dbName, groupSuffix, debug)


    
    




    




