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
## Oct 30, 2019


from collections import defaultdict
import copy, subprocess


def repeat(x,y):
    ''' Repeating vales of list x the number of times in list y '''

    return([i for i,j in zip(x, y) for k in range(j)])

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

def getSchema(myData, myKey):
    ''' Get the schema of the data and add mySQL data types '''     

    ## Schema
    mySchema = myData.schema

    ## Initialize a container
    mySchemaDict = defaultdict(list)

    ## Collect fields
    for i in mySchema.fields:
        mySchemaDict[i.name].extend([str(i.dataType), i.nullable])     

    ## Attach the SQL types to the variables
    for key in mySchemaDict:
        if str(key) == "DecimalType":
            raise TypeError("DecimalType is present in data. Possible errors. Please, address accordingly")
        else:
            mySchemaDict[key].append(myKey[mySchemaDict[key][0]])

    return(mySchemaDict)


################################################################################
### SHARDING RULES

def partitionByListColumn(partitionRules, beNodes):
    ''' Partitioning by LIST COLUMNS '''

    ## Copy the dict to leave the original unaffected
    partDict = copy.deepcopy(partitionRules)

    ## Check for matching element numbers of partValueList and beNodes
    numEl = [len(partDict[key]) for key in partDict][0]
    myKey = [key for key in partDict][0]

    if numEl != len(beNodes):
        raise RuntimeError("The number of partitions must equal the number of backend servers.")
    
    header = "PARTITION BY LIST COLUMNS (" + myKey  + ")"
    
    myPartCommand = []
    for el in partDict[myKey]:
        i = str(partDict[myKey].index(el) + 1)
        myPartCommand.append(("PARTITION pt" + i +  " VALUES IN (" +
                              ', '.join(el) + ") COMMENT = 'srv \\\"backend" + i + "\\\"'"))

    return(header + '(' + ', '.join(myPartCommand) +  ')')


def partitionByHash(partColumn, beNodes):
    ''' Partitioning by HASH '''

    header = "PARTITION BY HASH (" + partColumn + ")"
    myPartCommand = []
    for el in beNodes:
        i = str(beNodes.index(el) + 1)
        myPartCommand.append(("PARTITION pt" + i + " COMMENT = 'srv \\\"backend" + i + "\\\"'"))

    return(header + '(' + ', '.join(myPartCommand) + ')')
    


def partitionByRangeColumn(partitionRules, beNodes, maxValAdd = True, sortVal = True):
    ''' Partitioning by RANGE COLUMNS '''

    ## Copy the dict to leave the original unaffected
    partDict = copy.deepcopy(partitionRules)

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
        
        ## Add "id" to partColumn (if not in it) for form primaryKeys
        try:
            partColumn.index('id')
        except:
            print("Adding 'id' to the list of columns")
            partColumn.append('id')

        ## Write out the schema
        if frontEnd:
            dbEngine = "SPIDER"
            myCall = (commonPart + 
                      ", PRIMARY KEY(" + ', '.join(partColumn) + ")" +
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


    
def pushToMDB(callList, dbNodes, dbName, groupSuffix):
    ''' Make calls to the db '''

    mySystemCalls = [("mysql --defaults-group-suffix=" + groupSuffix +
                    " -h " + myNode + " -D " + dbName +
                    " -e \"" + myCall + "\"") for myNode, myCall in zip(dbNodes, callList)]    
    ## Push to MDB
    for myCall in mySystemCalls:
        ## subprocess.run(myCall, shell = True)
        print(myCall)

        
################################################################################
### EXECUTING THE MDB CALLS


####### ===>> SETUP CALL

def pushAdminToMDB(dbNodes, dbBENodes, dbPort, dbUser, dbPass, dbName, groupSuffix):
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
    pushToMDB(myAdminCalls, dbNodes, dbName, groupSuffix)
    

####### ===>> TABLE SCHEMA CALL

def pushSchemaToMDB(dbNodes, dbName, dbTableName, tableSchema, groupSuffix,
                    partColumn = None, partitionString = None):
    ''' Composing and making the table schema call to the db '''
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

    else:  #non-dist DB
        mySchemaCall = [pushSchemaToMDBString(dbTableName = dbTableName,
                                              tableSchema = tableSchema)]
           
    ## Submit the call
    pushToMDB(mySchemaCall, dbNodes, dbName, groupSuffix)



if __name__ == "__main__":
    
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
      .master('local') \
      .getOrCreate()

    ## Retrieve arguments passed to the script
    myArgs = parseArguments(sys.argv)
    
    myDataFile =  "/Users/goshev/Desktop/spark/spark-test-data.txt"
    myData = spark.read.csv(myDataFile, header=True, sep = '*')

    ## Recast year to integer
    myData = myData.withColumn("year", myData.year.cast("integer"))

    ## Get the schema
    mySchema = getSchema(myData, makeJdbcKey())

    ## Print the schema
    print(mySchema)

    ## Partition by list columns
    myD = defaultdict(list)
    myD['ORGID'] = [['1', '3', '5'], ['4', '7', '9']]
    mybeNodes =  ['compute004', 'compute005']
    print(partitionByListColumn(myD, mybeNodes))

    ## Partition by hash
    myColumn = "ORGID"
    print(partitionByHash(myColumn, mybeNodes))

    ## Partition by range columns
    myD = defaultdict(list)
    myD['year'] = ['2012', '2014', '2018']
    myD['ORGID'] = ['1', '3', '5']
    mybeNodes =  ['compute004', 'compute005', 'compute006', 'compute007']
    print(partitionByRangeColumn(myD, mybeNodes))
    
    
    ## Push admin info to db
    pushAdminToMDB(dbNodes = myArgs['dbNodes'],
                   dbBENodes = myArgs['dbBENodes'],
                   dbPort = myArgs['dbPort'],
                   dbUser = myArgs['dbUser'],
                   dbPass = myArgs['dbPass'],
                   dbName = myArgs['dbName'],
                   groupSuffix = myArgs['dbName'])

    ## Push schema to db
    pushSchemaToMDB(dbNodes = myArgs['dbNodes'],
                    dbName = myArgs['dbName'],
                    dbTableName = "testTable",
                    tableSchema = getSchema(myData, makeJdbcKey()),
                    groupSuffix = "testData",
                    partColumn = [key for key in myD],
                    partitionString = partitionByRangeColumn(myD, myArgs['dbBENodes']) )
                       
           
    spark.stop()


    
    




    




