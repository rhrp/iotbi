/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements mechanisms for data conversion
 *
 */
const debug = require('debug')('iotbi.webhdfsobjects');


exports.toLocatedBlocks = toLocatedBlocks;
exports.toFileStatus = toFileStatus;
exports.toFileStatuses = toFileStatuses;
exports.toError = toError

function toLocatedBlocks(pFileLength,pBlocksize)
{
  var lBlockId=1073742398;
  var lTotalBlocks=Math.floor(pFileLength/pBlocksize);
  var lLastBlockSize=pFileLength-(lTotalBlocks*pBlocksize);
  debug('toLocatedBlocks() FileSize:'+pFileLength+'   BlockSize:'+pBlocksize+'   TotalBlocks:'+lTotalBlocks+'    LastBlockSize:'+lLastBlockSize);

  var lLocatedBlocks={};
  lLocatedBlocks.fileLength=pFileLength;
  lLocatedBlocks.isLastBlockComplete=true;
  lLocatedBlocks.isUnderConstruction=false;

  lLocatedBlocks.lastLocatedBlock={};
  lLocatedBlocks.locatedBlocks=[];

  // Initial blocks
  var lStartOffset=0;
  for(var i=0;i<lTotalBlocks;i++)
  {
      lBlockId=lBlockId+1;
      lLocatedBlocks.locatedBlocks.push(toLocatedBlock(lBlockId,pBlocksize,lStartOffset));
      lStartOffset=lStartOffset+pBlocksize;
  }
  //Last block
  lBlockId=lBlockId+1;
  lLocatedBlocks.locatedBlocks.push(toLocatedBlock(lBlockId,lLastBlockSize,lStartOffset));

  lLocatedBlocks.lastLocatedBlock=toLocatedBlock(lBlockId,lLastBlockSize,lStartOffset);
  
  //debug(JSON.stringify(lLocatedBlocks,null,2));
  return {'LocatedBlocks':lLocatedBlocks};
}

function toFileStatus(pFilesize,pBlocksize,pAccTime,pModTime,pOwner,pGroup)
{
   //ex: 1320173277227
   var lCurrTime = new Date().getTime();
   var lAccTime=pAccTime!=undefined?pAccTime:lCurrTime;
   var lModTime=pModTime!=undefined?pModTime:lCurrTime;
//lAccTime=1320173277227;
//lModTime=1320173277227;
   var lType=pFilesize==0?'DIRECTORY':'FILE';
   var lFileStatus={
         "FileStatus":
          {
            "accessTime"      : lAccTime,
            "blockSize"       : pBlocksize,
            "group"           : pGroup,
            "length"          : pFilesize,             //in bytes, zero for directories
            "modificationTime": lModTime,
            "owner"           : pOwner,
            "pathSuffix"      : "",
            "permission"      : "555",
            "replication"     : 1,
            "type"            : lType
          }
        };
    //debug('lFileStatus=\n'+JSON.stringify(lFileStatus,null,2));
    return lFileStatus;
}
function toFileStatusArray(pFilesArray,pBlocksize)
{
   var lCurrTime = new Date().getTime();
   var lFileStatus={
          "FileStatus":[]
         };
   for(var i=0;i<pFilesArray.length;i++)
   {
      var lType=pFilesArray[i].fileSize==0?'DIRECTORY':'FILE';
      //debug(lType+' :: '+pFilesArray[i].fileType);
      var lFile={
              "accessTime"      : pFilesArray[i].atime,//ex: 1320171722771
              "blockSize"       : pBlocksize,
              "group"           : pFilesArray[i].group,
              "length"          : pFilesArray[i].fileSize,
              "modificationTime": pFilesArray[i].mtime,
              "owner"           : pFilesArray[i].owner,
              "pathSuffix"      : pFilesArray[i].fileName,
              "permission"      : "555",
              "replication"     : 1,
              "type"            : pFilesArray[i].fileType
             };
      lFileStatus.FileStatus.push(lFile);
   }
   return lFileStatus;
}

function toLocatedBlock(pBlockId,pNumBytes,pStartOffset)
{
   var lLocatedBlock={
         "block": {
           "blockId": pBlockId,
           "blockPoolId": "BP-1208840272-127.0.1.1-1705344023214",
           "generationStamp": 1574,
           "numBytes": pNumBytes
         },
         "blockToken": {
           "urlString": "AAAAAA"
         },
         "cachedLocations": [],
         "isCorrupt": false,
         "locations": [],
         "startOffset": pStartOffset,
         "storageTypes": ["DISK"]
       }
   lLocatedBlock.locations.push(toBlockLocation());
   return lLocatedBlock;
}
function toBlockLocation()
{
  return  {
           "adminState": "NORMAL",
           "blockPoolUsed": 67038371840,
           "cacheCapacity": 0,
           "cacheUsed": 0,
           "capacity": 85857402880,
           "dfsUsed": 67038371840,
           "hostName": "hadoop.iotbi.tech",
           "infoPort": 8181,
           "infoSecurePort": 0,
           "ipAddr": "193.136.59.27",
           "ipcPort": 9001,
           "lastBlockReportMonotonic": 1500297190,
           "lastBlockReportTime": 1718907674957,
           "lastUpdate": 1718917692521,
           "lastUpdateMonotonic": 1510314753,
           "name": "193.136.59.27:9002",
           "networkLocation": "/default-rack",
           "remaining": 18128465920,
           "storageID": "6fbd5ce4-ad1c-47bd-bef7-daa4db30d805",
           "xceiverCount": 0,
           "xferPort": 9002
         }
}

function toFileStatuses(pFilesArray,pBlocksize)
{
   var lFileStatuses={"FileStatuses":toFileStatusArray(pFilesArray,pBlocksize)};
   //debug(JSON.stringify(lFileStatuses,null,2));
   return lFileStatuses;

}

function toError(pCode,pMsg)
{
   if(typeof pMsg === 'string')
   {
      lMsg=pMsg;
   }
   else
   {
      debug(JSON.stringify(pMsg));
      lMsg='The pMsg parameter is invalid.  Uncapable of sending the messsage detail!'
   }
   if(pCode==401) return toError401(lMsg);
   else if(pCode==403) return toError403(lMsg);
   else if(pCode==404) return toError404(lMsg);
   else if(pCode==500) return toError500(lMsg);
   else {
     debug('The code error '+pCode+' is not valid');
     return toError500('Unknown error: '+pCode);
   }
}
function toError401(pMsg)
{
  return {
    "RemoteException":
      {
        "exception"    : "SecurityException",
        "javaClassName": "java.lang.SecurityException",
        "message"      : pMsg
      }
   };
}
function toError403(pMsg)
{
  return {
    "RemoteException":
      {
        "exception"    : "AccessControlException",
        "javaClassName": "org.apache.hadoop.security.AccessControlException",
        "message"      : pMsg
      }
   };
}

function toError404(pMsg)
{
  return {
    "RemoteException":
      {
        "exception"    : "FileNotFoundException",
        "javaClassName": "java.io.FileNotFoundException",
        "message"      : pMsg
      }
   };
}
function toError500(pMsg)
{
  return {
    "RemoteException":
      {
        "exception"    : "RumtimeException",
        "javaClassName": "java.lang.RuntimeException",
        "message"      : pMsg
      }
   };
}

