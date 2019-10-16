#!/usr/local/bin R

################################################################################
### Parsing the arguments passed to a spark script
##
##
##
##
##
## Simo Goshev
## Oct 11, 2019


## Arguments passed to the script:
## "$MY_SPARK_DATASET"
## "$MDB_MASTER_NODE"
## "$MDB_NODES"
## "$MDB_USER_CREDENTIALS"
## "$MDB_BACKEND_CREDENTIALS"
## "$MDB_DATABASE_NAME"

## Collect the arguments
myArgs = commandArgs(trailingOnly=TRUE)

## Name the arguments
myArgNames <- c("myDataSetFile", "myDbNodePortFile", "myDbNodesFile",
                "myUserCredFile", "myBECredFile", "myDbNameFile")
myArgs <- as.list(myArgs)
names(myArgs) <- myArgNames


## Extracting info from arguments
if (myArgs$myDataSetFile != "") {
    userConfig <- list(dataSet = readLines(myArgs$myDataSetFile)
} else {
    stop("Data set must be provided", call.=FALSE)
}


## Check whether will be using a database
if (myArgs$myDbNodePortFile != "" ) {

    ## Read the dbName
    userConfig <- c(userConfig, list(dbName = readLines(myArgs$myDbNameFile))    

    ## Define db node, port and name
    myDbNodePort <- str_split(readLines(myArgs$myDbNodePortFile), ":", simplify=TRUE)
    jdbcUrl <- paste0("jdbc:mysql://", myDbNodePort[1], ":", myDbNodePort[2], "/", myDbName)

    ## Retrieve user name and password for DB
    myUserCred <- readLines(myArgs$myUserCredFile)

    userConfig <- c(userConfig, list(dbNode = myDbNodePort[1],
                                     dbPort = myDbNodePort[2],
                                     dbUser = myUserCred[1],
                                     dbPass = myUserCred[2])
                           
    if (myArgs$myDbNodesFile != "") {
        
        ## Retireve back-end user name and password for DB
        myBEUserCred <- readLines(myArgs$myBECredFile)
        
        ## Add all nodes and backend credentials to the list
        userConfig <- c(userConfig, list(dbNodes = readLines(myArgs$myDbNodesFile),
                                         dbBEUser = myBEUserCred[1],
                                         dbBEPass = myBEUserCred[2])
    }
}

## Report the variables that are available to the user
print("User config values available in addition to jdbcUrl:")
print(names(userConfig))
