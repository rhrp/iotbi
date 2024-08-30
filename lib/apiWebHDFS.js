/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the minimal srvices of an Hadoop HDFS REST API.
 * 
 */
var url  = require('url');
var fs = require('fs');
var configsys = require('./configsys.js');
var apiCache = require('./cache.js');
var webhdfsobjs = require('./webhdfsv1objects.js');
var schema = require('./schema.js');
var sdm = require('./smartdatamodels.js');
var ngsiv2 = require('./ngsiv2.js');
var ngsildv1 = require('./ngsildv1.js');
var ngsildcontext = require('./ngsildcontext.js');
var formats = require('./formats.js');
var utils = require('./utils.js');
var output = require('./outputTable.js');
var debug = require('debug')('iotbi.api.webhdfs');

/**
 * Exposed services for webhdfs route
 */
exports.serviceDebug 	 	     = serviceWebhdfsDebug;
exports.serviceWebhdfsAccess         = serviceWebhdfsAccess;
exports.serviceFileStats 	     = serviceWebhdfsFileStats;
exports.servicePreEntity             = serviceWebhdfsPreEntity;
exports.serviceCommandRoot     	     = serviceWebhdfsCommandRoot;
exports.serviceCommandFiwareService  = serviceWebhdfsCommandFiwareService;
exports.serviceCommandEntity         = serviceWebhdfsCommandEntity;
exports.serviceCommandEntityJoins    = serviceWebhdfsCommandEntityJoins; 
exports.serviceCommandEntityJoinAttrib    = serviceWebhdfsCommandEntityJoinAttrib;
exports.serviceCommandCurrentDataset = serviceWebhdfsCommandCurrentDataset;
exports.serviceNotFound              = serviceWebhdfsNotFound

/**
 * HDFS params
 */
const PARAM_OP		= 'op';

/**
 * Implemented Hdfs operations
 */
const OP_OPEN			= 'OPEN';
const OP_GETFILESTATUS 		= 'GETFILESTATUS';
const OP_LISTSTATUS		= 'LISTSTATUS';
const OP_GET_BLOCK_LOCATIONS	= 'GET_BLOCK_LOCATIONS';

/**
 * Logical paths
 */
const PATH_ROOT			= "root";		// Root = > List all Brokers
const PATH_FSERVICE             = "fs";                 // Fiware Service => List all types in the broker
const PATH_FS_ENTITY            = "fe";                 // Entity as folder = >  its current data and joins folder
const PATH_FS_ENTITY_JOINS      = "joins";		// All Entity's attributes the relate with another Entity
const PATH_FS_ENTITY_J_ATTRIB   = "joinAttrib";         // All Entities's type that are refered by a Entity's fk
const PATH_CURRENT		= "current";		// Entity's data in their current state 

/**
 * Names for virtual files
 */
const FILE_CURRENT_PARQUET	= "current.parquet";
const FILE_CURRENT_JSON         = "current.json";
const FILE_CURRENT_CSV          = "current.csv";
const DIR_JOINS                 = "joins";

/**
 * Sizes of blocks and buffers
 */
const BLOCK_SIZE = 128*1024;
const BUFFER_SIZE = 64*1024;

/**
 * Hadoop user and group of generated virtual files
 */
const DFL_OWNER			= "iotbi";
const DFL_GROUP			= "iotbi";

/**
 * Registry of operations per logical path and operation type
 */
const OP_HANDLERS={}
OP_HANDLERS[PATH_ROOT]={};
OP_HANDLERS[PATH_ROOT][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_ROOT][OP_GETFILESTATUS]=doGetFileStatusRoot;
OP_HANDLERS[PATH_ROOT][OP_LISTSTATUS]=doListStatusRoot;
OP_HANDLERS[PATH_ROOT][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_FSERVICE]={};
OP_HANDLERS[PATH_FSERVICE][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_FSERVICE][OP_GETFILESTATUS]=doGetFileStatusFiwareService;
OP_HANDLERS[PATH_FSERVICE][OP_LISTSTATUS]=doListStatusFiwareService;
OP_HANDLERS[PATH_FSERVICE][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_FS_ENTITY]={};
OP_HANDLERS[PATH_FS_ENTITY][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_FS_ENTITY][OP_GETFILESTATUS]=doGetFileStatusEntity;
OP_HANDLERS[PATH_FS_ENTITY][OP_LISTSTATUS]=doListStatusEntity;
OP_HANDLERS[PATH_FS_ENTITY][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_FS_ENTITY_JOINS]={};
OP_HANDLERS[PATH_FS_ENTITY_JOINS][OP_OPEN]=doNothing;
OP_HANDLERS[PATH_FS_ENTITY_JOINS][OP_GETFILESTATUS]=doGetFileStatusEntity;
OP_HANDLERS[PATH_FS_ENTITY_JOINS][OP_LISTSTATUS]=doListStatusEntityJoins;
OP_HANDLERS[PATH_FS_ENTITY_JOINS][OP_GET_BLOCK_LOCATIONS]=doNothing;
OP_HANDLERS[PATH_FS_ENTITY_J_ATTRIB]={};
OP_HANDLERS[PATH_FS_ENTITY_J_ATTRIB][OP_OPEN]=doNothing;
OP_HANDLERS[PATH_FS_ENTITY_J_ATTRIB][OP_GETFILESTATUS]=doGetFileStatusEntity;
OP_HANDLERS[PATH_FS_ENTITY_J_ATTRIB][OP_LISTSTATUS]=doListStatusEntityJoinAttrib;
OP_HANDLERS[PATH_FS_ENTITY_J_ATTRIB][OP_GET_BLOCK_LOCATIONS]=doNothing;
OP_HANDLERS[PATH_CURRENT]={};
OP_HANDLERS[PATH_CURRENT][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_CURRENT][OP_GETFILESTATUS]=doGetFileStatusCurrent;
OP_HANDLERS[PATH_CURRENT][OP_LISTSTATUS]=doListStatusCurrent;
OP_HANDLERS[PATH_CURRENT][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;

/**
 * Utility method for getting the Entity Type
 */
function getParamEntityType(req)
{
   return req.params.entityType;
}
/**
 * Utility method for getting the fiware service alias that enables to find the tenant/fiware-service
 */
function getParamFiwareService(req)
{
   return req.params.fiwareService;
}
/**
 * Utility method for getting the home user
 * Note: This is diferent of user.name in the Query string 
 */
function getParamHadoopUser(req)
{
   return req.params.hadoopUser;
}
/**
 * Utility method for getting the user name
 */
function getQueryParamHadoopUser(req,pSetDfl)
{
   var lHadoopUser=req.query['user.name'];
   if(lHadoopUser==undefined)
   {
      debug('Default user access set to hadoop user');
      return 'hadoop';
   }
   else
   {
      return lHadoopUser;
   }
}
/**
 * Utility method for getting the HDFS Operation
 */
function getParamOperation(req)
{
   var lOperation=req.query[PARAM_OP];
   if(lOperation==undefined)
   {
      //Power BI !!!
      debug('Search: '+PARAM_OP+' in '+JSON.stringify(req.query)+'  Result:'+lOperation);
      for(var lParamName in req.query)
      {
          debug(lParamName.toLowerCase()+' :: '+PARAM_OP.toLowerCase());
          if(lParamName.toLowerCase()==PARAM_OP.toLowerCase())
          {
             lOperation=req.query[lParamName];
             debug('Case insensitive search: '+lParamName+' == '+lOperation);
             return lOperation;
          }
      }
   }
   return lOperation;
}
/**
 * Utility method for getting the attribute that works a FK
 */
function getParamAttribFKey(req)
{
   return req.params.attribFKey;
}
/**
 * Utility method for getting the Entity Type to be joined
 */
function getParamJoinEntityType(req)
{
   return req.params.joinEntityType;
}
/**
 * Utility method for getting the output format
 */
function getParamOutputFormat(req)
{
   return req.params.outputFormat;
}
/**
 * Utility method for getting the hdfs path
 */
function getPath(req)
{
   return req.path;
}
/**
 * Service for debugging purposes
 */
function serviceWebhdfsDebug(req,res,next)
{
   var lOperation=getParamOperation(req);
   debug('URL:'+req.url+'   UrlPath:'+req.path+'   Path: '+getPath(req)+'   '+PARAM_OP+'='+lOperation+' Hadoop User: '+getQueryParamHadoopUser(req));
   debug('Params: '+JSON.stringify(req.params));
   debug('Query: '+JSON.stringify(req.query));
   next();
   debug('Request Debug::Waiting for WebHDFS...');
}
/**
 * Service for access control
 */
function serviceWebhdfsAccess(req,res,next)
{
   var lHadoopUser=getQueryParamHadoopUser(req,true);
   var lFiwareService=getParamFiwareService(req);
   if(!configsys.allowHadoopToUser(lHadoopUser))
   {
      debug('Your account does not have permitions for using WebHDFS :: Hadoop User='+lHadoopUser);
      sendError(res,403,'Your account does not have permitions for using WebHDFS');
   }
   else if(lFiwareService!=undefined && !configsys.allowScopeToUser(lHadoopUser,lFiwareService))
   {
      // In the case of operations in scope of a Broker, the access must be checked
      debug('Your account does not have access to this fiware service, or the service is not valid :: Hadoop User='+lHadoopUser+' FiwareService='+lFiwareService);
      sendError(res,403,'Your account does not have access to this fiware service, or the service is not valid');
   }
   else
   {
      next();
      debug('Control Access::Waiting for WebHDFS...');
   }
}

/**
 * Get the stats of the data set OR directory
 * In the case of a data set, updates the cached file
 */
function serviceWebhdfsFileStats(req,res,next)
{
   debug('webhdfsFileStats::path='+getPath(req));
   if(getParamFiwareService(req)==undefined || getParamEntityType(req)==undefined || getParamOutputFormat(req)==undefined)
   {
     debug('webhdfsFileStats:: Invalid request');
     sendError(res,500,'Invalid request');
     return;
   }
   var lFiwareService=getParamFiwareService(req);
   if(!configsys.isConfigOk(lFiwareService))
   {
      sendError(res,404,'Invalid tenant');
      return; 
   }
   var lEntityType=getParamEntityType(req);
   var lJoinEntityType=getParamJoinEntityType(req);
   var lJoinAttrib=getParamAttribFKey(req);
   var lFormat=getParamOutputFormat(req);
   debug('Format: '+lFormat);
   if(!formats.isValid(lFormat))
   {
      sendError(res,404,'Invalid format');
      return; 
   }
   var lCacheKey=apiCache.genCurrentWebHDFSCacheKey(lFiwareService,lEntityType,lJoinAttrib,lJoinEntityType);
   debug('Current Cache Key: '+lCacheKey);
   var lFilename=apiCache.genCacheFile(lCacheKey,lFormat);
   var lFileStats=saveFileStats(res,lFilename);

   if(lFileStats==undefined || !apiCache.isStillValid(lFiwareService,lFileStats))
   {
        //The file does not exists in cache Or is too old.
        debug('The file does not exists in cache Or is too old.');
        loadSaveSendCurrent(res,lFiwareService,lEntityType,lJoinAttrib,lJoinEntityType,lFilename,lFormat,next);
   }
   else
   {
        // The cached file exists and is still valid
        debug('The cached file exists and is still valid');
        next();
   }
}

/**
 * Load, saves (in the cache) and sends it to the hdfs client, the current dataset
 */
function loadSaveSendCurrent(res,pFiwareService,pEntityType,pJoinAttrib,pJoinEntityType,pCacheFile,pFormat,next)
{
      if(configsys.isVersionLDV1(pFiwareService) && !configsys.isKnownNgsiLdType(pEntityType))
      {
          // In case of NGSI-LD, avoid contact the Broker to kown if the EntityTyp exists
          debug('The entity does not exists: '+pEntityType);
          sendError(res,404,'The entity does not exists: '+pEntityType);
          return;
      } 
      if(configsys.isVersionV2(pFiwareService))
      {
          loadSaveSendCurrentNgsiV2(res,pFiwareService,pEntityType,pCacheFile,pFormat,next);
      }
      else if(configsys.isVersionLDV1(pFiwareService))
      {
          loadSaveSendCurrentNgsiLD(res,pFiwareService,pEntityType,pJoinAttrib,pJoinEntityType,pCacheFile,pFormat,next);
      }
      else
      {
          sendError(res,500,'Unknown NGSI version');
      }
}
/**
 * Load, saves (in the cache) and sends it to the hdfs client, the current dataset in case of NGSI-V2
 */
function loadSaveSendCurrentNgsiV2(res,pFiwareService,pEntityType,pCacheFile,pFormat,next)
{
      var lFiwareServicePath=undefined;
      var lExtended=false;
      ngsiv2.listEntities(undefined,undefined,pFiwareService,lFiwareServicePath,lExtended,pEntityType)
          .then((tableAndSchema) => {
                              saveDatasetInCache(res,tableAndSchema,pFiwareService,pEntityType,pCacheFile,pFormat,next);
                           })
          .catch((err) => {
                              debug('Error loading current: '+JSON.stringify(err));
                              sendError(res,500,err.name);
                          })
}
/**
 * Load, saves (in the cache) and sends it to the hdfs client, the current dataset in case of NGSI-LD
 */
function loadSaveSendCurrentNgsiLD(res,pFiwareService,pEntityType,pJoinAttrib,pJoinEntityType,pCacheFile,pFormat,next)
{
      createPromiseCurrentNgsiLD(pFiwareService,pEntityType,pJoinAttrib,pJoinEntityType)
          .then((tableAndSchema) => {
                              saveDatasetInCache(res,tableAndSchema,pFiwareService,pEntityType,pCacheFile,pFormat,next);
                           })
          .catch((err) => {
                              debug('Error loading current: '+JSON.stringify(err));
                              sendError(res,500,err.name);
                          })
}
/**
 * Saves data in cache and sends it the Hdfs client
 */
function saveDatasetInCache(res,pTableAndSchema,pFiwareService,pEntityType,pCacheFile,pFormat,next)
{
      var lTableValues=pTableAndSchema[0];
      var lTableSchema=pTableAndSchema[1];
      if(lTableValues.length==0 && formats.isParquet(pFormat))
      {
           debug('Is not possible the generate a empty Parquet table');
           sendError(res,404,'Empty table');
      }
      else
      {
           output.createPromiseSaveInCache(pCacheFile,lTableValues,pEntityType,lTableSchema,pFormat)
                 .then(() => {
                                         var lFileStats=saveFileStats(res,pCacheFile);
                                         debug('Output File: '+pCacheFile+' format='+pFormat+'  stats:'+JSON.stringify(lFileStats));
                                         //Pass the control do serviceWebhdfsCommand
                                         next();
                                     })
                 .catch((err) => {
                                         debug('Error saving in format '+pFormat+': '+JSON.stringify(err));
                                         sendError(res,500,err.name);
                                     })
                 };
}
/**
 * Saves the file's stats in the response
 */
function saveFileStats(res,pFilename)
{
   try 
   {
     var lFileStats=fs.statSync(pFilename);
     res.locals.iotbi_filepath=pFilename;
     res.locals.iotbi_filesize=lFileStats.size;
     res.locals.iotbi_filestats=lFileStats;
     return lFileStats;
   }
   catch(err) 
   {
      return undefined;
   }
}
/**
 * Service for handling a Command over the Hadoop's root
 */
function serviceWebhdfsCommandRoot(req,res,next)
{
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_ROOT);
}
/**
 * Service for handling a Command over a Fiware service (i.e., a Broker)
 */
function serviceWebhdfsCommandFiwareService(req,res,next)
{
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_FSERVICE);
}
/**
 * Service for handling a Command over an Entity
 */
function serviceWebhdfsCommandEntity(req,res,next)
{
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_FS_ENTITY);
}
/**
 * Service for handling a Command over aall possible joins of an Entity
 */
function serviceWebhdfsCommandEntityJoins(req,res,next)
{
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_FS_ENTITY_JOINS);
}
/**
 * Service for handling a Command over a joined attribute of an Entity
 */
function serviceWebhdfsCommandEntityJoinAttrib(req,res,next)
{
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_FS_ENTITY_J_ATTRIB);
}
/**
 * Service for handling a Command over athe current dataset
 */
function serviceWebhdfsCommandCurrentDataset(req,res,next)
{
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_CURRENT);
}
/**
 * Service for handling a Command over a Logical Path (e.g., /stellio/AirQualityObserved)
 */
function serviceWebhdfsCommandByLogicalPath(req,res,next,pLogicalPath)
{
   debug('webhdfsCommand::path='+getPath(req)+'  Logical path='+pLogicalPath);
   try
   {
       var lOperation=getParamOperation(req);
       var lHandler=OP_HANDLERS[pLogicalPath][lOperation];
       if(lHandler!=undefined)
       {
         lHandler(req,res,next);
       }
       else
       {
         debug('Operation '+lOperation+' not implemented')
         sendError(res,500,'Operation not implemented');
       }
   }
   catch(ex)
   {
       debug(ex);
       var stack = ex.stack;
       debug( stack );
       sendError(res,500,'Error handling a command');
   }
}
/**
 * Service for handling a Not Found case
 */
function serviceWebhdfsNotFound(req,res,next)
{
    debug('Resource not found');
    sendError(res,404,'Resource not found');
}

/**
 * TODO: handle errors
 * Handles the request for Open over a current dataset
 */
function doOpen(req,res,next)
{
     var lFilesize=res.locals.iotbi_filesize;
     var lFilename=res.locals.iotbi_filepath;

     if(lFilename==undefined || lFilesize==undefined)
     {
         sendError(res,500,'Invalid resource');
     }
     else
     {
         var lOffset=req.query['offset'];
         var lLength=req.query['length'];
         var lBuffersize=req.query['buffersize'];

         var lOffset=isNaN(lOffset)?0:parseInt(lOffset);
         var lLength=isNaN(lLength)?undefined:parseInt(lLength);
         var lBuffersize=isNaN(lBuffersize)?undefined:parseInt(lBuffersize);

         fs.open(lFilename, 'r', function(err, fd) {
            fs.fstat(fd, function(err, stats) {
                var lSize=lLength==undefined?lFilesize-lOffset:lLength; // Size of data to read
                var lBuffer=Buffer.alloc(lSize);
                var lRead = fs.readSync(fd,lBuffer,0,lSize,lOffset);
                res.status(200).send(Buffer.from(lBuffer, 'binary',lSize));
                // Close the file descriptor 
                fs.close(fd, (err) => { 
                    if (err) 
                      debug('Failed to close file', err); 
                    else { 
                      debug("File Closed successfully"); 
                   } 
                }); 
            });
        });
     }
}
/**
 * Handles the request for GetFileStaus over the Hadoop's root
 */
function doGetFileStatusRoot(req,res,next)
{
     debug('doGetFileStatusRoot'); 
     res.status(200).json(webhdfsobjs.toFileStatus(0,BLOCK_SIZE,undefined,undefined,DFL_OWNER,DFL_GROUP));
}
/**
 * Handles the request for GetFileStaus over a Fiware service (i.e., a Broker)
 */
function doGetFileStatusFiwareService(req,res,next)
{
     var lFiwareService=getParamFiwareService(req);
     debug('doGetFileStatusFiwareService of Fiware Service: '+lFiwareService); 
     res.status(200).json(webhdfsobjs.toFileStatus(0,BLOCK_SIZE,undefined,undefined,DFL_OWNER,DFL_GROUP));
}
/**
 * Handles the request for GetFileStaus over an Entity
 */
function doGetFileStatusEntity(req,res,next)
{
     var lFiwareService=getParamFiwareService(req);
     var lEntityType=getParamEntityType(req);
     debug('doGetFileStatusEntity of '+lEntityType+' at the Fiware Service: '+lFiwareService); 
     res.status(200).json(webhdfsobjs.toFileStatus(0,BLOCK_SIZE,undefined,undefined,DFL_OWNER,DFL_GROUP));
}
/**
 * Handles the request for GetFileStaus over the current dataset
 */
function doGetFileStatusCurrent(req,res,next)
{
     var lFilesize=res.locals.iotbi_filesize;
     var lFilename=res.locals.iotbi_filepath;
     debug('doGetFileStatusCurrent: '+lFilename+' Size:'+lFilesize+'  fileStats='+JSON.stringify(res.locals.iotbi_filestats));
     var lAccTime=res.locals.iotbi_filestats.mtimeMs
     var lModTime=res.locals.iotbi_filestats.mtimeMs
     res.status(200).json(webhdfsobjs.toFileStatus(lFilesize,BLOCK_SIZE,lAccTime,lModTime,DFL_OWNER,DFL_GROUP));
}
/**
 * Utility method for generating a file
 */
function genFile(pFilename,pSize,pModTime,pAccTime)
{
     var lCurrTime=new Date().getTime();
     var lModTime=pModTime!=undefined?pModTime:lCurrTime;
     var lAccTime=pAccTime!=undefined?pAccTime:lCurrTime;
     return {'fileName':pFilename,'fileSize':pSize,'fileType':'FILE','owner':DFL_OWNER,'group':DFL_GROUP,'mtime':lModTime,'atime':lAccTime};
}
/**
 * Utility method for generating a directory
 */
function genDirectory(pFilename,pModTime,pAccTime)
{
     var lCurrTime=new Date().getTime();
     var lModTime=pModTime!=undefined?pModTime:lCurrTime;
     var lAccTime=pAccTime!=undefined?pAccTime:lCurrTime;
     return {'fileName':pFilename,'fileSize':0,'fileType':'DIRECTORY','owner':DFL_OWNER,'group':DFL_GROUP,'mtime':lModTime,'atime':lAccTime};
}
/**
 * Generates an array with all possible dataset of an Entity
 */
function genCurrentFiles()
{
     var lFiles=[];
     var lFile;
     lFile=genFile(FILE_CURRENT_PARQUET,1,undefined,undefined);
     lFiles.push(lFile);
     lFile=genFile(FILE_CURRENT_JSON,1,undefined,undefined);
     lFiles.push(lFile);
     lFile=genFile(FILE_CURRENT_CSV,1,undefined,undefined);
     lFiles.push(lFile);
     return lFiles;
}
/**
 * Handles the request for ListStatus over the Hadoop root
 */
function doListStatusRoot(req,res,next)
{
     debug('doListStatusRoot');
     var lHadoopUser=getQueryParamHadoopUser(req,true);
     var lDirs=[];
     var lAliasArray=configsys.getBrokersAliasList();
     for(var i in lAliasArray)
     {
         var lAcessGranted=configsys.allowScopeToUser(lHadoopUser,lAliasArray[i]);
         if(lAcessGranted)
         {
           var lDir=genDirectory(lAliasArray[i],undefined,undefined);
           lDirs.push(lDir);
         }
         else
         {
           debug('Fiware service: '+lAliasArray[i]+' Acess Granted: '+lAcessGranted);
         }
     }
     res.status(200).json(webhdfsobjs.toFileStatuses(lDirs,BLOCK_SIZE));
}
/**
 * Handles the request for ListStatus  over an entity
 */
function doListStatusEntity(req,res,next)
{
    var lFiwareService=getParamFiwareService(req);
    var lEntityType=getParamEntityType(req);
    debug('doListStatusEntity of '+lEntityType+' at the Fiware Service: '+lFiwareService); 
    var lTenant=configsys.getBrokerTenant(lFiwareService);
    if(lTenant==undefined)
    {
        sendError(res,404,'Invalid Tenant');
    }
    else if(configsys.isVersionLDV1(lFiwareService))
    {
       ngsildcontext.createPromisesloadNgsiContext(lFiwareService,[lEntityType])
       .then(() =>  ngsildv1.createPromiseGetType(lFiwareService,lEntityType)
                            .then((type) => {
                                               res.status(200).json(webhdfsobjs.toFileStatuses(toEntityTypeToFiles(lEntityType,true),BLOCK_SIZE));
                                            })
                            .catch((err) => {
                                              debug('Error: '+JSON.stringify(err));
                                              sendError(res,404,err.name=='NotFound'?'Entity type '+lEntityType+' is not valid':err.name)
                                            })
       ).catch((err) => sendError(res,500,err));
    }
    else if(configsys.isVersionV2(lFiwareService))
    {
       ngsiv2.createPromiseGetType(lFiwareService,lEntityType)
       .then((types) =>  {
                            res.status(200).json(webhdfsobjs.toFileStatuses(toEntityTypeToFiles(lEntityType,false),BLOCK_SIZE))
                         }) 
       .catch((err) =>   {
                            debug('Error: '+JSON.stringify(err));
                            sendError(res,404,err.name)
                         });
    }
    else
    {
       sendError(res,500,'Unsupported NGSI version');
    }
}
/**
 * Handles the request for ListStatus over the Entity's possible joins
 */
function doListStatusEntityJoins(req,res,next)
{
    var lFiwareService=getParamFiwareService(req);
    var lEntityType=getParamEntityType(req);
    debug('doListStatusEntityJoins of '+lEntityType+' at the Fiware Service: '+lFiwareService); 
    var lTenant=configsys.getBrokerTenant(lFiwareService);
    if(lTenant==undefined)
    {
        sendError(res,404,'Invalid Tenant');
    }
    else if(!configsys.isVersionLDV1(lFiwareService))
    {
       sendError(res,404,'Joins are not supported in your NGSI version');
    }
    else
    {
       var lFiles=[];
       var lSchema=sdm.getEntitySchema(lEntityType);
       for(var lField of lSchema.getRelationShipFields())
       {
           debug('File: '+lField);
           var lDir=genDirectory(lField,undefined,undefined);
           lFiles.push(lDir);
       }
       res.status(200).json(webhdfsobjs.toFileStatuses(lFiles,BLOCK_SIZE));
    }
}
/**
 * Handles the request for ListStatus over a joined attribute
 * Get all entity types related by an attribute
 */
function doListStatusEntityJoinAttrib(req,res,next)
{
    var lFiwareService=getParamFiwareService(req);
    var lEntityType=getParamEntityType(req);
    var lAttribKey=getParamAttribFKey(req);
    var lJoinEntityType=getParamJoinEntityType(req);

    debug('doListStatusEntityJoin of '+lEntityType+' using '+lAttribKey+' and type '+lJoinEntityType+' at the Fiware Service: '+lFiwareService);
    var lTenant=configsys.getBrokerTenant(lFiwareService);
    if(lTenant==undefined)
    {
        sendError(res,404,'Invalid Tenant');
    }
    else if(!configsys.isVersionLDV1(lFiwareService))
    {
       sendError(res,404,'Joins are not supported in your NGSI version');
    }
    else if(lAttribKey==undefined)
    {
       sendError(res,404,'Invalid FK');
    }
    else
    {
       var lFiles=[];
       var lSchema=sdm.getEntitySchema(lEntityType);
       if(!lSchema.getRelationShipFields().includes(lAttribKey))
       {
           sendError(res,404,'Invalid FK: '+lAttribKey+' is not a key in '+lEntityType);
       }
       else
       {
           ngsildv1.createPromiseQueryEntitiesAttributes(lFiwareService,lEntityType,lAttribKey)
                    .then((ents) => {
                                       if(lJoinEntityType==undefined)
                                       {
                                          for(var lEntType of ngsildv1.getFkEntitiesTypes(ents.results,lAttribKey))
                                          {
                                             var lDir=genDirectory(lEntType,undefined,undefined);
                                             lFiles.push(lDir);
                                          }
                                          res.status(200).json(webhdfsobjs.toFileStatuses(lFiles,BLOCK_SIZE));
                                       }
                                       else if(ngsildv1.getFkEntitiesTypes(ents.results,lAttribKey).includes(lJoinEntityType))
                                       {
                                           res.status(200).json(webhdfsobjs.toFileStatuses(genCurrentFiles(),BLOCK_SIZE));
                                       }
                                       else
                                       {
                                           sendError(res,404,'There are '+lJoinEntityType+' entities related with '+lEntityType);
                                       }  
                                    })
                    .catch((err) => {
                                       debug('Error getting attribute data: '+JSON.stringify(err,null,2));
                                       sendError(res,500,'Error getting attribute data.');
                                    });
       }
    }
}
/**
 * Handles the request for ListStatus over a Fiware service (i.e., a Broker)
 */
function doListStatusFiwareService(req,res,next)
{
     var lFiwareService=getParamFiwareService(req);
     debug('doListStatusFiwareService of Fiware Service: '+lFiwareService); 
     var lTenant=configsys.getBrokerTenant(lFiwareService);
     if(lTenant==undefined)
     {
         sendError(res,404,'Invalid tenant');
     }
     else if(configsys.isVersionLDV1(lFiwareService))
     {
        ngsildcontext.createPromisesloadNgsiContext(lFiwareService,configsys.getKnownEntityTypes())
        .then(() =>  ngsildv1.createPromiseListTypes(lFiwareService)
                             .then((types) => {
                                                res.status(200).json(webhdfsobjs.toFileStatuses(toFilesFromListTypeLDV1(types),BLOCK_SIZE));
                                              }) 
                             .catch((err) => {
                                                debug('Error geting types: '+JSON.stringify(err,null,2));
                                                sendError(res,err.code,'Error getting types. Broker response: '+err.name);
                                             })
        ).catch((err) => sendError(res,500,err));
     }
     else if(configsys.isVersionV2(lFiwareService))
     {
        ngsiv2.createPromiseListTypes(lFiwareService,undefined)
        .then((types) =>  res.status(200).json(webhdfsobjs.toFileStatuses(toFilesFromListTypeV2(types),BLOCK_SIZE))) 
        .catch((err) => sendError(res,500,err));
     }
     else
     {
        sendError(res,500,'Unsupported NGSI version');
     }
}
/**
 * Service invoked before any operation related with an Entity.
 * Pre loads its Schema
 */ 
function serviceWebhdfsPreEntity(req,res,next)
{
    var lFiwareService=getParamFiwareService(req);
    var lEntityType=getParamEntityType(req);
    var lTenant=configsys.getBrokerTenant(lFiwareService);
    debug('serviceWebhdfsPreEntity');
    if(lTenant==undefined)
    {
        sendError(res,404,'Invalid Tenant');
    }
    else if(lEntityType==undefined)
    {
        sendError(res,404,'Invalid Entity Type');
    }
    else if(!configsys.isVersionLDV1(lFiwareService))
    {
       sendError(res,404,'Schemas are not supported in your NGSI version');
    }
    else if(sdm.getEntitySchema(lEntityType)!=undefined)
    {
       debug('The schema of '+lEntityType+' is already loaded');
       next();
    }
    else
    {
       ngsildcontext.createPromisesloadNgsiContext(lFiwareService,configsys.getKnownEntityTypes())
           .then(() => {
             var lSchema=sdm.getEntitySchema(lEntityType);
             debug('Schema of '+lEntityType+' loaded:'+(lSchema!=undefined));
             next();
          })
          .catch((err) => {
             sendError(res,500,err)
          });
     }
}
/**
 * Handles the request for ListStatus of the current dataset file
 */
function doListStatusCurrent(req,res,next)
{
     var lPath=getPath(req);
     var lFilesize=res.locals.iotbi_filesize;
     var lFilename=res.locals.iotbi_filepath;
     debug('doListStatusCurrent: '+lFilename+' Size:'+lFilesize);
     var lFile=genFile("",lFilesize,undefined,undefined);
     res.status(200).json(webhdfsobjs.toFileStatuses([lFile],BLOCK_SIZE));
}
/**
 * Handles the request for Get_Block_Locations
 */
function doGet_Block_Locations(req,res,next)
{
     var lPath=getPath(req);
     var lFilesize=res.locals.iotbi_filesize;
     var lFilename=res.locals.iotbi_filepath;

     debug('Sending lLocatedBlocks '+lFilename+' Size:'+lFilesize);
     res.status(200).json(webhdfsobjs.toLocatedBlocks(res.locals.iotbi_filesize,BLOCK_SIZE));
}
/**
 * Dummy task when it is required to do nothing
 */
function doNothing(req,res,next)
{
    debug('Do nothing');
    sendError(res,500,'Not Implemented');
}
/**
 * Send a error
 */
function sendError(res,pCode,pMsg)
{
   debug('Send ['+pCode+']:'+pMsg);
   res.status(pCode).json(webhdfsobjs.toError(pCode,pMsg));
}
/**
 * Transforms a List of Entity Type (NGSI-V2) into a List of Files
 */
function toFilesFromListTypeV2(pListTypes)
{
    debug('List Types:\n'+JSON.stringify(pListTypes,null,2));
    var lFiles=[];
    for(var i in pListTypes.results)
    {
      var lFilename=pListTypes.results[i].type;
      var lDir=genDirectory(lFilename,undefined,undefined);
      lFiles.push(lDir);
    }
    return lFiles;
}
/**
 * Transforms a List of Entity Type (NGSI-LD) into a List of Files
 */
function toFilesFromListTypeLDV1(pListTypes)
{
    var lFiles=[];
    for(var i in pListTypes.results.typeList)
    {
      var lContextURL=pListTypes.results.typeList[i];
      var lEntityType=ngsildcontext.findEntityTypeByContext(lContextURL);
      if(lEntityType==undefined)
      {
         debug('Unknown type: '+lContextURL);
         lFilename=lContextURL.replaceAll(/[^a-zA-Z0-1]/g,'_');
      }
      else
      {
         lFilename=lEntityType;
      }
      var lDir=genDirectory(lFilename,undefined,undefined);
      lFiles.push(lDir);
    }
    return lFiles;
}
/**
 * Produces a list of files associated to an entity type
 */
function toEntityTypeToFiles(pEntityType,pSupportsLD)
{
    var lFiles=genCurrentFiles();
    var lSchema=sdm.getEntitySchema(pEntityType);
    if(pSupportsLD && lSchema.getRelationShipFields().length>0)
    { 
      // The entity contains relation ships, thus, the join option is presented
      var lDir=genDirectory(DIR_JOINS,undefined,undefined);
      lFiles.push(lDir);
    }
    return lFiles;
}
/**
 * Creates a Promise for loading the current dataset from a NGSI-LD broker
 */
function createPromiseCurrentNgsiLD(pFiwareService,pEntityType,pJoinAttrib,pJoinEntityType)
{
    debug('FiwareService:'+pFiwareService);
    var lEndpoint=configsys.getBrokerEndpoint(pFiwareService);
    debug('Endpoint: '+lEndpoint);
    var lTenant=configsys.getBrokerTenant(pFiwareService);
    var lEntityId=undefined;
    var lFiwareServicePath=undefined;
    var lExtended=false;
    var lQuery={};
    debug('NGSI-LD :: OrionLD/Scorpio/Stellio: Tenant: '+lTenant);
    // Get the Query
    var lObjQuery = utils.getObjectQuery(lQuery,pEntityType,lEntityId);
    debug('Query: '+JSON.stringify(lObjQuery));
    return ngsildv1.listEntities(lEndpoint,lObjQuery,pFiwareService,lFiwareServicePath,lExtended,pEntityType,pJoinEntityType,pJoinAttrib);
}
