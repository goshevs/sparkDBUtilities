#!/usr/local/bin R

################################################################################
### Functions for pushing RDD's from Spark to a distibuted instance of MariaDB
##
##
##
##
##
## Simo Goshev
## Nov 04, 2019


################################################################################
### Get the schema and cast it to a dataframe, then merge with key 

## Have to keep the key updated.
## https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala

##       case IntegerType => Option(JdbcType("INTEGER", java.sql.Types.INTEGER))
##       case LongType => Option(JdbcType("BIGINT", java.sql.Types.BIGINT))
##       case DoubleType => Option(JdbcType("DOUBLE PRECISION", java.sql.Types.DOUBLE))
##       case FloatType => Option(JdbcType("REAL", java.sql.Types.FLOAT))
##       case ShortType => Option(JdbcType("INTEGER", java.sql.Types.SMALLINT))
##       case ByteType => Option(JdbcType("BYTE", java.sql.Types.TINYINT))
##       case BooleanType => Option(JdbcType("BIT(1)", java.sql.Types.BIT))
##       case StringType => Option(JdbcType("TEXT", java.sql.Types.CLOB))
##       case BinaryType => Option(JdbcType("BLOB", java.sql.Types.BLOB))
##       case TimestampType => Option(JdbcType("TIMESTAMP", java.sql.Types.TIMESTAMP))
##       case DateType => Option(JdbcType("DATE", java.sql.Types.DATE))

################################################################################
### CREATE CONVERSION KEY

makeJdbcKey <- function() {
    ## Update this function if definitions change
    as.data.frame(
        list(colType = c("IntegerType", "LongType", "DoubleType", "FloatType", "ShortType",
                         "ByteType", "BooleanType", "StringType",  "BinaryType",  "TimestampType",
                         "DateType"), 
             mysqlType = c("INTEGER", "BIGINT", "DOUBLE PRECISION", "REAL", "INTEGER", "BYTE",
                           "BIT(1)", "TEXT", "BLOB", "TIMESTAMP", "DATE")),
        stringsAsFactors = FALSE)
    
}
## Note: currently ignoring DecimalType ==> DECIMAL(precision, scale) ##


################################################################################
### RETRIEVING TABLE SCHEMA

getSchema <- function(x, key = makeJdbcKey(), changeType = NULL) {
    
    mySchema <- schema(x)
    mySchemaFields <- lapply(sparkR.callJMethod(mySchema$jobj, "fields"), structField)

    x.schema <- as.data.frame(
        list(colName = unlist(
                 lapply(mySchemaFields,
                        function(x) sparkR.callJMethod(x$jobj, "name"))),
             colType = unlist(
                 lapply(
                     mySchemaFields, function(x) sparkR.callJMethod(
                                                     sparkR.callJMethod(x$jobj, "dataType"), 
                                                     "toString"))),
             Nullable = unlist(
                 lapply(mySchemaFields,
                        function(x) sparkR.callJMethod(x$jobj, "nullable")))
             ),
        stringsAsFactors = FALSE
    )

    ## Check for DecimalType and if true, fail
    if ("DecimalType" %in% unique(x.schema$colType)) {
        stop("getSchema: DecimalType is present in data. Possible errors. Please, address accordingly", call.=FALSE)
    }
    
    mySchemaFinal <- merge(x.schema, key, by = "colType", sort = FALSE)
    mySchemaFinal <- mySchemaFinal[, c("colName", "colType", "mysqlType", "Nullable")]

    ## Change the type of columns per changeType user instructions
    if (!missing(changeType)) {
        for (var in names(changeType)) {
            mySchemaFinal[mySchemaFinal$colName == var, "mysqlType"] <- toupper(changeType[[var]])
        }
    }
    return(mySchemaFinal)
}



################################################################################
### UTILITY FUNCTION

## Replace integer with string partitioning values
partColVal2String <- function(x, tableSchema) {

    ## Define list of admissible character types
    ## https://mariadb.com/kb/en/library/string-data-types/
    quotedTypeList <- c('CHAR', 'VARCHAR', 'BINARY', 'CHAR BYTE', 'VARBINARY')
    grepString <- paste0(quotedTypeList, collapse = "|")

    charVar <- tableSchema[tableSchema$colName %in% names(x) & 
                           grepl(grepString, tableSchema$mysqlType), 'colName']
    
    if (length(charVar) > 0) {
        ## Replace values
        repList <- lapply(x[charVar], function(i) {sapply(i, function(j) {paste0("'", j ,"'")}, USE.NAMES = FALSE)})
        for (var in charVar) { x[[var]] <- repList[[var]]}
    }
    
    return(x)
}

################################################################################
### SHARDING RULES


## FRONTEND: Write out partition rules string
## https://mariadb.com/kb/en/library/spider-use-cases/
## https://mariadb.com/kb/en/library/partitioning-types/
## https://dev.mysql.com/doc/refman/5.5/en/partitioning-columns-range.html

## ====== >>> SHARDING BY LIST COLUMNS

## PARTITION BY LIST COLUMNS (owner)
## (
##  PARTITION pt1 VALUES IN ('Bill', 'Bob', 'Chris') COMMENT = 'srv "backend1"',
##  PARTITION pt2 VALUES IN ('Maria', 'Olivier') COMMENT = 'srv "backend2"'
## ) ;

## --------------------------

partitionByListColumn <- function(partitionRules, beNodes, tableSchema, defaultAdd = TRUE) {

    partColumn <- names(partitionRules)

    ## Cast values of partitioning character type columns to character type 
    partitionRules <- partColVal2String(partitionRules, tableSchema)
   
    ## If defaultAdd, then add default partition provisions
    myDefValue <- as.character(digest(paste0('STDB-DEFAULT-PARTITION', date())))

    if (defaultAdd) {
        partNumber <- length(partitionRules[[partColumn]])
        partitionRules[[partColumn]][partNumber + 1] <- myDefValue
    }
        
    ## Check for matching element numbers of partValueList and beNodes
    if (length(beNodes) != length(partitionRules[[partColumn]])) {
        stop("The number of partitions must equal the number of backend servers.", call.=FALSE)
    }
        
    header <- paste0("PARTITION BY LIST COLUMNS (", partColumn, ")")
    body <- unlist(lapply(
        seq_along(partitionRules[[partColumn]]),
        function(i) {
            if (!myDefValue %in% partitionRules[[partColumn]][[i]]) {
                paste0("PARTITION pt", i, " VALUES IN (",
                       paste0(partitionRules[[partColumn]][[i]], collapse = ',') ,
                       ") COMMENT = 'srv \\\"backend", i, "\\\"'")
            } else {
                paste0("PARTITION pt", i, " DEFAULT COMMENT = 'srv \\\"backend", i, "\\\"'")
            }
        }))
    paste(header, "(", paste(body, collapse = ","), ")")
}



## ====== >>> SHARDING BY HASH

## PARTITION BY HASH (id)
## (
##  PARTITION pt1 COMMENT = 'srv "backend1"',
##  PARTITION pt2 COMMENT = 'srv "backend2"'
## ) ;

## --------------------------
## Input:
## partColumn: a string with a column name
## beNodes: a vector of backend nodes

partitionByHash <- function(partColumn, beNodes) {

    ## Writing out partitoning schema   
    header <- paste0("PARTITION BY HASH (", partColumn, ")")
    body <- unlist(lapply(
        seq_along(beNodes),
        function(i) {                                   
            paste0("PARTITION pt", i, " COMMENT = 'srv \\\"backend", i, "\\\"'")
        }))
    paste(header, "(", paste(body, collapse = ","), ")")
}


## ====== >>> SHARDING BY RANGE COLUMNS

##  PARTITION BY RANGE COLUMNS (accountName)
## (
##  PARTITION pt1 VALUES LESS THAN ('M') COMMENT = 'srv "backend1"',
##  PARTITION pt2 VALUES LESS THAN (MAXVALUE) COMMENT = 'srv "backend2"'
## ) ;

## --------------------------

partitionByRangeColumn <- function(partitionRules, beNodes, tableSchema,
                                   maxValAdd = TRUE, sortVal = TRUE) {

    partColumn <- names(partitionRules)

    ## Cast values of partitioning character type columns to character type 
    partitionRules <- partColVal2String(partitionRules, tableSchema)
       
    ## Accommodate multiple columns
    partColumnCollapsed <- ifelse(length(partColumn) > 1,
                                 paste(partColumn, collapse = ","),
                                 partColumn)

    ## Sort partValues
    if (sortVal & length(partColumn) < 2) { # only sort if partColumn < 2!
        myVals <- partitionRules[[partColumn]][1]
        partitionRules[[partColumn]][1] <- sort(myVals)
    }
            
    ## Add maxvalue to partValuesList
    if (maxValAdd) {
        partitionRules <- lapply(partitionRules, c, "maxvalue")
    }

    ## Check for matching element numbers of partValueList and beNodes
    myCount <- lapply(partitionRules, length)
    myCountUnique <- unique(myCount)
    if (length(myCountUnique) != 1) {
        stop("The number of values among partitioning variables is not identical.", call.=FALSE)
    }   
    if (length(beNodes) != myCountUnique) {
        stop("The number of partitions must equal the number of backend servers.", call.=FALSE)
    }

    ## Write out partitioning
    header <- paste0("PARTITION BY RANGE COLUMNS (", partColumnCollapsed, ")")
    body <- unlist(lapply(
        1:myCount[[1]],
        function(i) {
            innerString <- paste(
                unlist(lapply(partitionRules, '[', i)), collapse = ',')

            paste0("PARTITION pt", i, " VALUES LESS THAN (",
                   innerString,
                   ") COMMENT = 'srv \\\"backend", i, "\\\"'")
        }))
    paste(header, "(", paste(body, collapse = ","), ")")
}



################################################################################
### COMPOSING MDB CALLS


## ===>>> ONE-TIME SETUP CALL

pushAdminToMDBString <- function(dbBENodes, dbPort, dbUser, dbPass,
                                 dbName, frontEnd = TRUE) {
    if (frontEnd) {

        commandString <- unlist(lapply(
            seq_along(dbBENodes),
            function(i) {
                paste0("DROP SERVER IF EXISTS backend", i, "; ",
                       "CREATE SERVER backend" , i, " FOREIGN DATA WRAPPER mysql ",
                       "OPTIONS(HOST '", dbBENodes[i], "', DATABASE '", dbName,
                       "', USER '", dbUser, "', PASSWORD '", dbPass, "', PORT ", dbPort, ");"
                       )}))
        myCall <- paste(commandString, collapse = "")
    } else {
        myCall <- paste0("CREATE OR REPLACE USER ", dbUser, "; ",
                         "SET PASSWORD FOR ", dbUser, " = PASSWORD('", dbPass, "'); ",
                         "GRANT ALL ON ", dbName, ".* TO ", dbUser,";")
    }
    return(myCall)
}



## ===>>> TABLE CALLS                 

pushSchemaToMDBString <- function(dbTableName, tableSchema, partColumn = NULL,
                                  partitionString = NULL, frontEnd = TRUE) {

    ## Check for BLOB/TEXT types in partColumn
    if (!missing(partitionString) & !missing(partColumn)) { # distributed MDB

        myProbVars <- tableSchema[tableSchema$colName %in% partColumn &
                                  grepl("BLOB|TEXT", tableSchema$mysqlType), "colName"]
        
        if (length(myProbVars) > 0 ) {
            stop(paste(paste(myProbVars, collapse = ","), ": Partitioning columns cannot be of type BLOB/TEXT"), call.=FALSE)
        }
    }
    
    ## Common component
    commonPart <- paste("DROP TABLE IF EXISTS", dbTableName, ";",
                        "CREATE TABLE", dbTableName, "(" ,
                        "id INT NOT NULL AUTO_INCREMENT, ",
                        paste(tableSchema$colName,
                              tableSchema$mysqlType,
                              "DEFAULT NULL",
                              collapse = ", "))

    ## Default engine
    dbEngine <- "InnoDB"

    ## Default key and engine
    dbKeyEngineDefault <- paste0(", PRIMARY KEY(id))", " ENGINE = ", dbEngine, ";")

    ## Write out the schame
    if (!missing(partitionString) & !missing(partColumn)) { # distributed MDB
        if (frontEnd) {
            ## Add "id" to partColumn (if not in it) for form primaryKeys
            if (!'id' %in% partColumn) {
                partColumn <- c("id", partColumn)
            }

            ## Accommodate multiple primary keys
            primaryKeys <- ifelse(length(partColumn) > 1,
                                  paste(partColumn, collapse = ","),
                                  partColumn)

            dbEngine <- "SPIDER"
            myCall <- paste0(commonPart,
                             paste0(", PRIMARY KEY(", primaryKeys, ")"),
                             ") ENGINE = ", dbEngine,
                             " COMMENT='wrapper \\\"mysql\\\", table \\\"", dbTableName, "\\\"' ",
                             partitionString)
        } else {

            myCall <- paste(commonPart, dbKeyEngineDefault)

        }
    } else if (missing(partitionString) & missing(partColumn)) { # non-distributed MDB

        myCall <- paste(commonPart, dbKeyEngineDefault)
        
    } else {

        stop("Arguments partColumn and partitionString incorrectly specified", call.=FALSE)

    }
    
    return(myCall)
}


## ===>>> SYSTEM CALL

pushToMDB <- function(callVector, dbNodes, dbName, groupSuffix, debug) {
    
    mySystemCalls <- paste0("mysql --defaults-group-suffix=", groupSuffix,
                           " -h ", dbNodes, " -D ", dbName,
                           " -e \"", callVector, "\"")
    
    ## Push to MDB
    if (!debug) {
        lapply(mySystemCalls, system, intern = TRUE)
    } else {
        ## Print the call
        lapply(mySystemCalls, print)
    }
}

################################################################################
### EXECUTING THE MDB CALLS


####### ===>> SETUP CALL

pushAdminToMDB <- function(dbNodes, dbBENodes, dbPort, dbUser ,dbPass,
                           dbName, groupSuffix, debug = FALSE) {

    ## Number of frontend and backend nodes
    nodeNumVector <- c(1, length(dbBENodes))

    ## Write out frontend and backend calls
    myAdminCalls <- unlist(
        lapply(c(TRUE, FALSE), 
               function(i) {
                   pushAdminToMDBString(
                       dbBENodes = dbBENodes,
                       dbPort = dbPort,
                       dbUser = dbUser,
                       dbPass = dbPass,
                       dbName = dbName,
                       frontEnd = i)
               }))
    
    ## Add backend calls
    myAdminCalls <- rep(myAdminCalls, nodeNumVector)
    
    ## Submit the call
    pushToMDB(myAdminCalls, dbNodes, dbName, groupSuffix, debug)
}


####### ===>> TABLE SCHEMA CALL

pushSchemaToMDB <- function(dbNodes, dbName, dbTableName, tableSchema, groupSuffix,
                            partColumn = NULL, partitionString = NULL,
                            debug = FALSE) {

    if (length(dbNodes) > 1 ) { # dist DB
        ## Number of frontend and backend nodes
        nodeNumVector <- c(1, length(dbNodes) - 1)

        ## Write out frontend and backend calls
        mySchemaCall <- unlist(
            lapply(c(TRUE, FALSE), 
                   function(i) {
                       pushSchemaToMDBString(
                           dbTableName = dbTableName,
                           tableSchema = tableSchema,
                           partColumn = partColumn,
                           partitionString = partitionString,
                           frontEnd = i)
                   }))
        
        ## Add backend calls
        mySchemaCall <- rep(mySchemaCall, nodeNumVector)

    } else { #non-dist DB
        
        mySchemaCall <- pushSchemaToMDBString(dbTableName = dbTableName,
                                              tableSchema = tableSchema)
    }
    
    ## Submit the call
    pushToMDB(mySchemaCall, dbNodes, dbName, groupSuffix, debug)
}
