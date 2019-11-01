#!/usr/local/bin python

################################################################################
### Parsing the arguments passed to a spark script
##
##
##
##
##
## Simo Goshev
## Oct 30, 2019


## Arguments passed to the script:
## "$MY_SPARK_DATASET"
## "$MDB_MASTER_NODE"
## "$MDB_NODES"
## "$MDB_USER_CREDENTIALS"
## "$MDB_BACKEND_CREDENTIALS"
## "$MDB_DATABASE_NAME"

import sys
from collections import defaultdict

def extractInfo(myFile):
    ''' Read info from a file '''
    
    with open(myFile, 'r') as f:
        myInfo = f.readlines()
        myInfo = [item.replace('\n','') for item in myInfo]
    return(myInfo)


def parseArguments(myArgs):
    ''' Parsing the arguments passed to the script '''

    myInNames = ["myDataSetFile", "myDbNodePortFile", "myDbNodesFile",
                  "myUserCredFile", "myBECredFile", "myDbNameFile"]

    ## Store all file info into a dictionary
    fileDict = dict(zip(myInNames, myArgs[1::]))

    ## Retrieve info
    outDict = defaultdict(list)
    outDict['dataSet'] = fileDict['myDataSetFile']
    outDict['dbName'] = extractInfo(fileDict['myDbNameFile'])[0]
    outDict['dbNode'] = extractInfo(fileDict['myDbNodePortFile'])[0] \
      .split(':')[0]
    outDict['dbPort'] = extractInfo(fileDict['myDbNodePortFile'])[0] \
      .split(":")[1]
    outDict['dbUrl'] = ("jdbc:mysql://" + outDict['dbNode'] +
                        ":" + outDict['dbPort'] + "/" +
                        outDict['dbName'])
    outDict['dbUser'] = extractInfo(fileDict['myUserCredFile'])[0]
    outDict['dbPass'] = extractInfo(fileDict['myUserCredFile'])[1]

    outDict['dbNodes'] = extractInfo(fileDict['myDbNodesFile'])

    if len(outDict['dbNodes']) > 1:
        outDict['dbBENodes'] = outDict['dbNodes'][1::]
        outDict['dbBEUser'] = extractInfo(fileDict['myBECredFile'])[0]
        outDict['dbBEPass'] = extractInfo(fileDict['myBECredFile'])[1]
        
    return(outDict)

    
