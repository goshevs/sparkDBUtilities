#!/usr/local/bin R

################################################################################
### Functions for pushing RDD's from Spark to a distibuted instance of MariaDB
##
##
##
##
##
## Simo Goshev
## Oct 16, 2019




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


my.spark.jdbc.conversion.key <- as.data.frame(
    list(colType = c("IntegerType", "LongType", "DoubleType", "FloatType", "ShortType",
                     "ByteType", "BooleanType", "StringType",  "BinaryType",  "TimestampType",
                     "DateType"), 
         mysqlType = c("INTEGER", "BIGINT", "DOUBLE PRECISION", "REAL", "INTEGER", "BYTE",
                       "BIT(1)", "TEXT", "BLOB", "TIMESTAMP", "DATE"))
)
## Note: currently ignoring DecimalType ==> DECIMAL(precision, scale) ##


getSchema <- function(x, key = my.spark.jdbc.conversion.key) {
    
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
             )
    )

    ## Check for DecimalType and if true, fail
    if ("DecimalType" %in% unique(x.schema$colType)) {
        stop("getSchema: DecimalType is present in data. Possible errors. Please, address accordingly")
    }
    
    mySchemaFinal <- merge(x.schema, key, by = "colType", sort = FALSE)
    mySchemaFinal <- mySchemaFinal[, c("colName", "colType", "mysqlType", "Nullable")]
    
    return(mySchemaFinal)
}



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
## Input:
## partColumn: a string with a column name
## partValueList: a list with values to split partColumn by

partitionByListColumn <- function(partColumn, partValueList) {
    header <- paste0("PARTITION BY LIST COLUMNS (", partColumn, ")")
    body <- unlist(lapply(
        seq_along(partValueList),
        function(i) {                                   
            paste0("PARTITION pt", i, " VALUES IN (",
                   paste0(partValueList[[i]], collapse = ',') ,
                   ") COMMENT = 'srv \\\"backend", i, "\\\"'")
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

partitionByHash <- function(partColumn) {
    header <- paste0("PARTITION BY HASH (", partColumn, ")")
    body <- unlist(lapply(
        seq_along(partValueList),
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
## Input:
## partColumn: a vector of strings, names of columns
## partValuesList: a list of values corresponding to every column in partColumns
### Corner case: one column --> partValuesList: a vector of values of partColumns

partitionByRangeColumn <- function(partColumn, partValueList, maxValAdd = TRUE, sortVal = TRUE) {

    ## Accommodate multiple columns
    partColumn <- ifelse(length(partColumn) > 1,
                         paste(partColumn, collapse = ","),
                         partColumn)

    ## Sort partValues
    if (sortVal) {
        if (is.list(partValueList)) {
            partValueList <-  lapply(partValueList, sort)
        } else {
            partValueList <- sort(partValueList)
        }
    }
            
    ## Add maxvalue to partValuesList
    if (maxValAdd) {
        if (is.list(partValueList)) {
            partValueList <- lapply(partValueList, c, "maxvalue")
        } else {
            partValueList <- c(partValueList, "maxvalue")
        }
    }
    
    header <- paste0("PARTITION BY RANGE COLUMNS (", partColumn, ")")
    body <- unlist(lapply(
        seq_along(partValueList),
        function(i) {                                   
            paste0("PARTITION pt", i, " VALUES LESS THAN (",
                   paste0(partValueList[[i]], collapse = ',') ,
                   ") COMMENT = 'srv \\\"backend", i, "\\\"'")
        }))
    paste(header, "(", paste(body, collapse = ","), ")")
}



################################################################################
### Write out MDB calls

## ===>>> One-time call
pushAdminToMDBString <- function(dbNodes, dbPort, dbUser ,dbPass,
                                 dbName, dbTableName, frontEnd = TRUE) {
    if (frontEnd) {
        ## Remove frontend node from the list of nodes
        commandString <- unlist(lapply(
            seq_along(dbNodes[-1]),
            function(i) {
                paste0("DROP SERVER IF EXISTS backend", i, "; ",
                       "CREATE SERVER backend" , i, " FOREIGN DATA WRAPPER mysql ",
                       "OPTIONS(HOST '", dbNodes[i], "', DATABASE '", dbName,
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



## ===>>> Repeat call for every table created            
pushSchemaToMDBString <- function(dbTableName, tableSchema, partColumn, partValueList,
                                  partitionString, frontEnd = TRUE) {

    ## Check partition string
    if (length(partitionString == 0)) {
        stop("Partition string is a required argument.")
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


    ## Add "id" to partColumn (if not in it) for form primaryKeys
    myTest <- intersect("id", partColumn)
    if (length(myTest) == 0) {
        primaryKeys <- c("id", partColumn)
    }

    ## Accommodate multiple primary keys
    primaryKeys <- ifelse(length(primaryKeys) > 1,
                         paste(primaryKeys, collapse = ","),
                         primaryKeys)
    
    
    ## Write out the schema
    if (frontEnd) {
        dbEngine <- "SPIDER"
        myCall <- paste0(commonPart,
                         paste0("PRIMARY KEY(", primaryKeys, ")"),
                         ") ENGINE =", dbEngine,
                         " COMMENT='wrapper \\\"mysql\\\", table \\\"", dbTableName, "\\\"' ",
                         partitionString)
    } else {
        myCall <- paste(commonPart, 
                        ", PRIMARY KEY(id))", "ENGINE =", dbEngine,  ";")
    }

    return(myCall)
}


## ===>>> System call

pushToMDB <- function(callVector, groupSuffix) {
    
    mySytemCalls <- paste0("mysql --defaults-group-suffix=", groupSuffix,
                           " -h ", dbNodes, " -D ", dbName,
                           " -e \"", callVector, "\"")
    
    ## Push to MDB    
    lapply(mySytemCalls, system, intern = TRUE)

    ## Print the call
    ## lapply(mySytemCalls, print)
}


################################################################################
### Submitting the MDB calls

####### ===>> ADMIN CALL
pushAdminToMDB <- function(dbNodes, dbPort, dbUser ,dbPass,
                           dbName, dbTableName, groupSuffix) {

    ## Number of frontend and backend nodes
    nodeNumVector <- c(1, length(dbNodes)-1)

    ## Write out frontend and backend calls
    myAdminCalls <- unlist(
        lapply(c(TRUE, FALSE), 
               function(i) {
                   pushAdminToMDBString(
                       dbNodes = dbNodes,
                       dbPort = dbPort,
                       dbUser = dbUser,
                       dbPass = dbPass,
                       dbName = dbName,
                       dbTableName = dbTableName,
                       frontEnd = i)
               }))
    
    ## Add backend calls
    myAdminCalls <- rep(myAdminCalls, nodeNumVector)
    
    ## Submit the call
    pushToMDB(myAdminCalls, groupSuffix)
}


####### ===>> TABLE SCHEMA CALL

pushSchemaToMDB <- function(dbNodes, dbName,
                            dbTableName, tableSchema,
                            partitionString, groupSuffix) {

    ## Number of frontend and backend nodes
    nodeNumVector <- c(1, length(dbNodes)-1)

    ## Write out frontend and backend calls
    mySchemaCalls <- unlist(
        lapply(c(TRUE, FALSE), 
               function(i) {
                   pushSchemaToMDBString(
                       dbTableName = dbTableName,
                       tableSchema = tableSchema,
                       partitionString = partitionString,
                       frontEnd = i)
               }))
    
    ## Add backend calls
    mySchemaCalls <- rep(mySchemaCalls, nodeNumVector)
    
    ## Submit the call
    pushToMDB(mySchemaCalls, groupSuffix)
}


## mysql --defaults-group-suffix=testData -h compute002 -D testData -e "DROP TABLE IF EXISTS myTable ; CREATE TABLE myTable (id INT NOT NULL AUTO_INCREMENT,  year INTEGER DEFAULT NULL, ORGID INTEGER DEFAULT NULL, CHIAPAYERCLAIMCONTROLNUMBER INTEGER DEFAULT NULL, PROCEDURECODECLEANED TEXT DEFAULT NULL , PRIMARY KEY(id, ORGID)) ENGINE = SPIDER COMMENT='wrapper \"mysql\", table \"myTable\"' PARTITION BY list columns (ORGID) ( PARTITION pt1 values in (1,2) COMMENT ='srv \"backend1\"', PARTITION pt2 values in (3,4,5) COMMENT = 'srv \"backend2\"');"

