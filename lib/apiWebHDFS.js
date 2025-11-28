/*!
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the minimal services of an Hadoop HDFS REST API.
 * TODO: implement a route for temporal data /:fiwareService/:entityType/daily/hour_HHMM:outputFormat
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
var ngsiCache= require('./ngsiCache.js');
var formats = require('./formats.js');
var utils = require('./utils.js');
var output = require('./outputTable.js');
var metadatasys = require('./metadatasys.js');
var debug = require('debug')('iotbi.api.webhdfs');

/**
 * Exposed services for webhdfs route
 */
exports.serviceDebug 	 	                 = serviceWebhdfsDebug;
exports.serviceWebhdfsAccess                     = serviceWebhdfsAccess;
exports.servicePreEntity                         = serviceWebhdfsPreEntity;
exports.servicePreJoin                           = serviceWebhdfsPreJoin;
exports.servicePreService                        = serviceWebhdfsPreService;
exports.serviceLoadAllMetadata                   = serviceWebhdfsLoadAllMetadata;
exports.serviceServiceFileStats                  = serviceWebhdfsServiceFileStats;
exports.serviceServiceEntitySchemaFileStats      = serviceWebhdfsServiceFileStats;
exports.serviceServiceEntityGeoMetadataFileStats = serviceWebhdfsServiceFileStats;
exports.serviceSystemEntitySchemaFileStats       = serviceWebhdfsSystemEntitySchemaFileStats;
exports.serviceSystemListOfEntitiesFileStats              = serviceWebhdfsListOfEntitiesFileStats;
exports.serviceSystemEntitiesRelationshipsFileStats       = serviceWebhdfsSystemEntitiesRelationshipsFileStats;
                                                            
exports.serviceCommandRoot     	     	    = serviceWebhdfsCommandRoot;
exports.serviceCommandDirectoryData         = serviceWebhdfsCommandDirectoryData;
exports.serviceCommandDirectorySystem       = serviceWebhdfsCommandDirectorySystem;
exports.serviceCommandFiwareService  	    = serviceWebhdfsCommandFiwareService;
exports.serviceCommandEntity         	    = serviceWebhdfsCommandEntity;
exports.serviceCommandEntityJoins    	    = serviceWebhdfsCommandEntityJoins; 
exports.serviceCommandEntityJoinAttrib      = serviceWebhdfsCommandEntityJoinAttrib;
exports.serviceCommandCurrentDataset 	    = serviceWebhdfsCommandCurrentDataset;
exports.serviceCommandEntitiesDataset       = serviceWebhdfsCommandEntitiesDataset;
exports.serviceCommandEntitySchema          = serviceWebhdfsCommandEntitySchema;
exports.serviceCommandEntityGeoMetadata     = serviceWebhdfsCommandEntityGeoMetadata;
exports.serviceCommandFolderSysMetadata     = serviceWebhdfsCommandFolderSystemMetadata;
exports.serviceCommandFolderSystemEntity    = serviceWebhdfsCommandFolderSystemEntity;
exports.serviceCommandEntitiesRelationships = serviceWebhdfsCommandEntitiesRelationships;
exports.serviceNotFound                     = serviceWebhdfsNotFound

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
const PATH_ROOT			= "root";		// Root = > System (metadata) plus Data
const PATH_DATA                 = "data";		// List all Brokers
const PATH_FSERVICE             = "fs";                 // Fiware Service => List all types in the broker
const PATH_FS_ENTITY            = "fe";                 // Entity as folder = >  its current data and joins folder
const PATH_FS_ENTITY_JOINS      = "joins";		// All Entity's attributes the relate with another Entity
const PATH_FS_ENTITY_J_ATTRIB   = "joinAttrib";         // All Entities's type that are refered by a Entity's fk
const PATH_CURRENT		= "current";		// Entity's data in their current state 
const PATH_SCHEMA               = "schema";             // Entity's schema data
const PATH_GEOMETADATA          = "geometadata";        // Entity's geo metadata
const PATH_ENTITIES_RELS        = "relations";          // Entities' relationships
const PATH_ENTITIES             = "entities";           // This file contains the list of available Entities
const PATH_SYSTEM               = "system";             // System Service
const PATH_SYSTEM_METADATA      = "metadata";		// System sub-path for metadata
const PATH_SM_ENTITY            = "sme";                // System sub-path for entity's metadata
/**
 * Names for virtual files
 */
const DIR_JOINS                 = "joins";
const FILE_FORMATS              = ['json','csv','parquet'];
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
OP_HANDLERS[PATH_DATA]={};
OP_HANDLERS[PATH_DATA][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_DATA][OP_GETFILESTATUS]=doGetFileStatusDirectoryData;
OP_HANDLERS[PATH_DATA][OP_LISTSTATUS]=doListStatusDirectoryData;
OP_HANDLERS[PATH_DATA][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_SYSTEM]={};
OP_HANDLERS[PATH_SYSTEM][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_SYSTEM][OP_GETFILESTATUS]=doGetFileStatusDirectorySystem;
OP_HANDLERS[PATH_SYSTEM][OP_LISTSTATUS]=doListStatusDirectorySystem;
OP_HANDLERS[PATH_SYSTEM][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_FSERVICE]={};
OP_HANDLERS[PATH_FSERVICE][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_FSERVICE][OP_GETFILESTATUS]=doGetFileStatusFiwareService;
OP_HANDLERS[PATH_FSERVICE][OP_LISTSTATUS]=doListStatusFiwareService;
OP_HANDLERS[PATH_FSERVICE][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_FS_ENTITY]={};
OP_HANDLERS[PATH_FS_ENTITY][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_FS_ENTITY][OP_GETFILESTATUS]=doGetFileStatusServiceEntity;
OP_HANDLERS[PATH_FS_ENTITY][OP_LISTSTATUS]=doListStatusServiceEntity;
OP_HANDLERS[PATH_FS_ENTITY][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_FS_ENTITY_JOINS]={};
OP_HANDLERS[PATH_FS_ENTITY_JOINS][OP_OPEN]=doNothing;
OP_HANDLERS[PATH_FS_ENTITY_JOINS][OP_GETFILESTATUS]=doGetFileStatusServiceEntity;
OP_HANDLERS[PATH_FS_ENTITY_JOINS][OP_LISTSTATUS]=doListStatusServiceEntityJoins;
OP_HANDLERS[PATH_FS_ENTITY_JOINS][OP_GET_BLOCK_LOCATIONS]=doNothing;
OP_HANDLERS[PATH_FS_ENTITY_J_ATTRIB]={};
OP_HANDLERS[PATH_FS_ENTITY_J_ATTRIB][OP_OPEN]=doNothing;
OP_HANDLERS[PATH_FS_ENTITY_J_ATTRIB][OP_GETFILESTATUS]=doGetFileStatusServiceEntity;
OP_HANDLERS[PATH_FS_ENTITY_J_ATTRIB][OP_LISTSTATUS]=doListStatusServiceEntityJoinAttrib;
OP_HANDLERS[PATH_FS_ENTITY_J_ATTRIB][OP_GET_BLOCK_LOCATIONS]=doNothing;
OP_HANDLERS[PATH_CURRENT]={};
OP_HANDLERS[PATH_CURRENT][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_CURRENT][OP_GETFILESTATUS]=doGetFileStatusCachedFile;
OP_HANDLERS[PATH_CURRENT][OP_LISTSTATUS]=doListStatusCachedFile;
OP_HANDLERS[PATH_CURRENT][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_SCHEMA]={};
OP_HANDLERS[PATH_SCHEMA][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_SCHEMA][OP_GETFILESTATUS]=doGetFileStatusCachedFile;
OP_HANDLERS[PATH_SCHEMA][OP_LISTSTATUS]=doListStatusCachedFile;
OP_HANDLERS[PATH_SCHEMA][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_GEOMETADATA]={};
OP_HANDLERS[PATH_GEOMETADATA][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_GEOMETADATA][OP_GETFILESTATUS]=doGetFileStatusCachedFile;
OP_HANDLERS[PATH_GEOMETADATA][OP_LISTSTATUS]=doListStatusCachedFile;
OP_HANDLERS[PATH_GEOMETADATA][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_ENTITIES]={};
OP_HANDLERS[PATH_ENTITIES][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_ENTITIES][OP_GETFILESTATUS]=doGetFileStatusCachedFile;
OP_HANDLERS[PATH_ENTITIES][OP_LISTSTATUS]=doListStatusCachedFile;
OP_HANDLERS[PATH_ENTITIES][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;
OP_HANDLERS[PATH_SYSTEM_METADATA]={};
OP_HANDLERS[PATH_SYSTEM_METADATA][OP_OPEN]=doNothing;
OP_HANDLERS[PATH_SYSTEM_METADATA][OP_GETFILESTATUS]=doGetFileStatusFolderMetadata;
OP_HANDLERS[PATH_SYSTEM_METADATA][OP_LISTSTATUS]=doListStatusFolderMetadata;
OP_HANDLERS[PATH_SYSTEM_METADATA][OP_GET_BLOCK_LOCATIONS]=doNothing;
OP_HANDLERS[PATH_SM_ENTITY]={};
OP_HANDLERS[PATH_SM_ENTITY][OP_OPEN]=doNothing;
OP_HANDLERS[PATH_SM_ENTITY][OP_GETFILESTATUS]=doGetFileStatusSystemEntity;
OP_HANDLERS[PATH_SM_ENTITY][OP_LISTSTATUS]=doListStatusSystemEntity;
OP_HANDLERS[PATH_SM_ENTITY][OP_GET_BLOCK_LOCATIONS]=doNothing;
OP_HANDLERS[PATH_ENTITIES_RELS]={};
OP_HANDLERS[PATH_ENTITIES_RELS][OP_OPEN]=doOpen;
OP_HANDLERS[PATH_ENTITIES_RELS][OP_GETFILESTATUS]=doGetFileStatusCachedFile;
OP_HANDLERS[PATH_ENTITIES_RELS][OP_LISTSTATUS]=doListStatusCachedFile;
OP_HANDLERS[PATH_ENTITIES_RELS][OP_GET_BLOCK_LOCATIONS]=doGet_Block_Locations;




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
 * Utility method for getting the system service alias that enables to find a sub-system such as system, accounting...
 */
function getParamSystemService(req)
{
   return req.params.systemService;
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
      //debug('Search: '+PARAM_OP+' in '+JSON.stringify(req.query)+'  Result:'+lOperation);
      for(var lParamName in req.query)
      {
          //debug(lParamName.toLowerCase()+' :: '+PARAM_OP.toLowerCase());
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
 *
 */
function getFilename(req)
{
   var lPath=getPath(req);
   var lParts=lPath.split('/');
   if(lParts.length==0)
   {
     return undefined;
   }
   var lFilename=lParts[lParts.length-1];
  // debug('Filename:'+lFilename);
   return lFilename;
}

/**
 * Service for debugging purposes
 */
function serviceWebhdfsDebug(req,res,next)
{
   var lOperation=getParamOperation(req);
   debug('Request Debug::URL:'+req.url+'   UrlPath:'+req.path+'   Path: '+getPath(req)+'   '+PARAM_OP+'='+lOperation+' Hadoop User: '+getQueryParamHadoopUser(req));
   debug('Request Debug::Params: '+JSON.stringify(req.params));
   debug('Request Debug::Query: '+JSON.stringify(req.query));
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
   var lSystemService=getParamSystemService(req);
   var lEntityType=getParamEntityType(req);

   debug('serviceWebhdfsAccess::lHadoopUser='+lHadoopUser+'   lFiwareService='+lFiwareService+'   lSystemService='+lSystemService+'   lEntityType='+lEntityType);
   if(lFiwareService!=undefined && lEntityType!=undefined && configsys.isHiddenEntity(lHadoopUser,lFiwareService,lEntityType))
   {
      debug('Your account does not see the Entity Type '+lEntityType+' :: Hadoop User='+lHadoopUser);
      sendError(res,404,'Entity not found');
   }
   else if(!configsys.allowHadoopToUser(lHadoopUser))
   {
      debug('Your account does not have permitions for using WebHDFS :: Hadoop User='+lHadoopUser);
      sendError(res,403,'Your account does not have permitions for using WebHDFS');
   }
   else if((lFiwareService!=undefined && !configsys.allowScopeToUser(lHadoopUser,lFiwareService))
           ||
           (lSystemService!=undefined && !configsys.allowScopeToUser(lHadoopUser,lSystemService))) 
   {
      // In the case of operations in scope of a Broker, the access must be checked
      debug('Your account does not have access to this fiware service ('+lFiwareService+'), or the service is not valid :: Hadoop User='+lHadoopUser+' FiwareService='+lFiwareService+' SystemService='+lSystemService);
      sendError(res,403,'Your account does not have access to this fiware service, or the service is not valid');
   }
   else
   {
      next();
      debug('Control Access::Waiting for WebHDFS...');
   }
}
/**
 * Service invoked before any operation related with a Fiware Service
 * Pre loads its Schema
 */ 
function serviceWebhdfsPreService(req,res,next)
{
   var lFiwareService=getParamFiwareService(req);
   debug('serviceWebhdfsPreService Service='+lFiwareService);
   ngsildcontext.createPromisesloadNgsiContext([lFiwareService],configsys.getKnownEntityTypes())
        .then(() => {
           debug('All schemas of service '+lFiwareService+' were loaded');
           next();
        })
        .catch((err) => {
           debug(err);
           sendError(res,500,err)
        });
}

function serviceWebhdfsLoadAllMetadata(req,res,next)
{
   debug('serviceWebhdfsLoadAllMetadata');
   ngsildcontext.createPromisesloadNgsiContext(configsys.getBrokersAliasList(),configsys.getKnownEntityTypes())
        .then(() => {
           debug('All schemas were loaded');
           next();
        })
        .catch((err) => {
           debug(err);
           sendError(res,500,err)
        });
}
/**
 * Get the stats of the data about entities
 */
function serviceWebhdfsListOfEntitiesFileStats(req,res,next)
{
   debug('serviceWebhdfsListOfEntitiesFileStats::path='+getPath(req));
   var lSystemService=getParamSystemService(req);
   var lFiwareService=undefined;
   if((lSystemService==undefined && lFiwareService==undefined )|| getParamOutputFormat(req)==undefined)
   {
     debug('serviceWebhdfsListOfEntitiesFileStats:: Invalid system Service ('+lSystemService+') and fiware Service ('+lFiwareService+') or format');
     sendError(res,500,'Invalid request');
     return;
   }
   debug('System Service: '+lSystemService);
   if(lFiwareService!=undefined && !configsys.isConfigOk(lFiwareService))
   {
      sendError(res,404,'Invalid fiware service');
      return; 
   }
   var lFormat=getParamOutputFormat(req);
   debug('Format: '+lFormat);
   var lCacheKey=apiCache.genCurrentWebHDFSCacheKey(lFiwareService,'ListOfEntities',undefined,undefined,"XXX_TODO");
   debug('ListOfEntities Cache Key: '+lCacheKey);
   var lFilename=apiCache.genCacheFile(lCacheKey,lFormat);
   debug('ListOfEntities Filename=',lFilename);
   var lFileStats=saveFileStats(res,lFilename);
   if(lFileStats==undefined || !apiCache.isStillValid(lSystemService,lFileStats))
   {
      debug('The ListOfEntities file does not exists in cache Or is too old.');
      loadSaveSendListOfEntities(res,lFiwareService,lFilename,lFormat,next);
   }
   else
   {
      debug('The cached ListOfEntities file exists and is still valid');
      next();
   }
}

function serviceWebhdfsSystemEntitySchemaFileStats(req,res,next)
{
      debug('serviceWebhdfsSystemFileStats::path='+getPath(req));
      if(getParamSystemService(req)==undefined || getParamEntityType(req)==undefined || getParamOutputFormat(req)==undefined)
      {
        debug('serviceWebhdfsSystemEntitySchemaFileStats:: Invalid request');
        sendError(res,500,'Invalid request');
        return;
      }
      var lSystemService=getParamSystemService(req);
      debug('System Service: '+lSystemService);
      var lEntityType=getParamEntityType(req);
      debug('Entity: '+lEntityType);
      var lFormat=getParamOutputFormat(req);
      debug('Format: '+lFormat);
      var lCacheKey=apiCache.genCurrentWebHDFSCacheKey(lSystemService,lEntityType,undefined,undefined,'XXXX_TODO2');
      debug('Schema Cache Key: '+lCacheKey);
      var lFilename=apiCache.genCacheFile(lCacheKey,lFormat);
      debug('Schema Filename=',lFilename);
      var lFileStats=saveFileStats(res,lFilename);
      if(lFileStats==undefined || !apiCache.isStillValid(lSystemService,lFileStats))
      {
         debug('The schema file does not exists in cache Or is too old.');
         loadSaveSendSchema(res,lSystemService,lEntityType,lFilename,lFormat,next);
      }
      else
      {
         debug('The cached Schema file exists and is still valid');
         next();
      }
}
function serviceWebhdfsSystemEntitiesRelationshipsFileStats(req,res,next)
{
      debug('serviceWebhdfsSystemEntitiesRelationshipsFileStats::path='+getPath(req));
      var lSystemService=undefined;
      var lFiwareService=undefined;
      var lSelectedService=undefined;
      var lServices=[];
      if(getParamSystemService(req)!=undefined && getParamOutputFormat(req)!=undefined)
      {
         //Use System Service
         lSystemService=getParamSystemService(req);
         debug('System Service: '+lSystemService);
         lSelectedService=lSystemService;
         var lHadoopUser=getQueryParamHadoopUser(req,true);
         var lAliasArray=configsys.getBrokersAliasList();
         for(var i in lAliasArray)
         {
             if(configsys.allowScopeToUser(lHadoopUser,lAliasArray[i]) && configsys.isConfigOk(lAliasArray[i]) && configsys.isVersionLDV1(lAliasArray[i]))
             {
                 lServices.push(lAliasArray[i]);
             }
         }
      }
      else if(getParamFiwareService(req)!=undefined && getParamOutputFormat(req)!=undefined)
      {
         //Use Fiware Service
         lFiwareService=getParamFiwareService(req);
         debug('Fiware Service: '+lFiwareService);
         lSelectedService=lFiwareService;
         lServices.push(lFiwareService);
      }
      else
      {
         debug('serviceWebhdfsSystemEntitiesRelationshipsFileStats:: Invalid request');
         sendError(res,500,'Invalid request');
         return;
      }
      var lEntityType=getParamEntityType(req);
      debug('Entity: '+lEntityType);
      var lFormat=getParamOutputFormat(req);
      debug('Format: '+lFormat);
      var lCacheKey=apiCache.genCurrentWebHDFSCacheKey(lSelectedService,lEntityType,undefined,undefined,'XXXX_TODO2');
      debug('Schema Cache Key: '+lCacheKey);
      var lFilename=apiCache.genCacheFile(lCacheKey,lFormat);
      debug('Schema Filename=',lFilename);
      var lFileStats=saveFileStats(res,lFilename);
      if(lFileStats==undefined || !apiCache.isStillValid(lSelectedService,lFileStats))
      {
         debug('The schema file does not exists in cache Or is too old.');
         loadSaveSendEntityRelationships(res,lServices,lEntityType,lFilename,lFormat,next)
      }
      else
      {
        debug('The cached Schema file exists and is still valid');
        next();
      }
}

/**
 * Get the stats of the dataset OR directory
 * In the case of a data set, updates the cached file
 */
function serviceWebhdfsServiceFileStats(req,res,next)
{
   debug('serviceWebhdfsServiceFileStats::path='+getPath(req));
   if(getParamFiwareService(req)==undefined || getParamEntityType(req)==undefined || getParamOutputFormat(req)==undefined)
   {
     debug('serviceWebhdfsServiceFileStats:: Invalid request');
     sendError(res,500,'Invalid request');
     return;
   }
   var lFiwareService=getParamFiwareService(req);
   debug('Fiware Service: '+lFiwareService);
   if(!configsys.isConfigOk(lFiwareService))
   {
      sendError(res,404,'Invalid tenant ['+lFiwareService+'] for loading data!');
      return; 
   }
   var lEntityType=getParamEntityType(req);
   debug('Entity: '+lEntityType);
   var lJoinEntityType=getParamJoinEntityType(req);
   var lJoinAttrib=getParamAttribFKey(req);
   var lFormat=getParamOutputFormat(req);
   debug('Format: '+lFormat);
   var lReqFilename=getFilename(req);
   debug('Requested Filename: '+lReqFilename);
   var lFiwareServicePath=undefined;

   if(!formats.isValid(lFormat))
   {
      sendError(res,404,'Invalid format');
      return; 
   }
   if(lReqFilename.startsWith(PATH_SCHEMA))
   {
      var lCacheKey=apiCache.genCurrentWebHDFSCacheKey(lFiwareService,lEntityType,lJoinAttrib,lJoinEntityType,PATH_SCHEMA);
      debug('Schema Cache Key: '+lCacheKey);
      var lFilename=apiCache.genCacheFile(lCacheKey,lFormat);
      debug('Schema Filename=',lFilename);
      var lFileStats=saveFileStats(res,lFilename);
      if(lFileStats==undefined || !apiCache.isStillValid(lFiwareService,lFileStats))
      {
         debug('The schema file does not exists in cache Or is too old.');
         loadSaveSendSchema(res,lFiwareService,lEntityType,lFilename,lFormat,next);
      }
      else
      {
        debug('The cached Schema file exists and is still valid');
        next();
      }
   }
   else if(lReqFilename.startsWith(PATH_GEOMETADATA))
   {
      var lCacheKey=apiCache.genCurrentWebHDFSCacheKey(lFiwareService,lEntityType,lJoinAttrib,lJoinEntityType,PATH_GEOMETADATA);
      debug('Schema Cache Key: '+lCacheKey);
      var lFilename=apiCache.genCacheFile(lCacheKey,lFormat);
      debug('Schema Filename=',lFilename);
      var lFileStats=saveFileStats(res,lFilename);
      if(lFileStats==undefined || !apiCache.isStillValid(lFiwareService,lFileStats))
      {
         debug('The GeoMetadata file does not exists in cache Or is too old.');
         loadSaveSendGeoMetadata(res,lFiwareService,lFiwareServicePath,lEntityType,lJoinAttrib,lJoinEntityType,lFilename,lFormat,next);
      }
      else
      {
        debug('The cached GeoMetadata file exists and is still valid');
        next();
      }
   }
   else if(lReqFilename.startsWith(PATH_CURRENT))
   {
     var lCacheKey=apiCache.genCurrentWebHDFSCacheKey(lFiwareService,lEntityType,lJoinAttrib,lJoinEntityType,PATH_CURRENT);
     debug('Current Cache Key: '+lCacheKey);
     var lFilename=apiCache.genCacheFile(lCacheKey,lFormat);
     var lFileStats=saveFileStats(res,lFilename);

     if(lFileStats==undefined || !apiCache.isStillValid(lFiwareService,lFileStats))
     {
        //The file does not exists in cache Or is too old.
        debug('The dataset file does not exists in cache Or is too old.');
        loadSaveSendCurrent(res,lFiwareService,lEntityType,lJoinAttrib,lJoinEntityType,lFilename,lFormat,next);
     }
     else
     {
        // The cached file exists and is still valid
        debug('The cached dataset file exists and is still valid');
        next();
     }
   }
   else
   {
      sendError(res,404,'Invalid filename');
      return; 
   }
}
function serviceWebhdfsPreEntity(req,res,next)
{
    var lFiwareService=getParamFiwareService(req);
    var lEntityType=getParamEntityType(req);
    var lTenant=configsys.getBrokerTenant(lFiwareService);
    var lMethod='serviceWebhdfsPreEntity';

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
       //sendError(res,404,'Schemas are not supported in your NGSI version');
       debug(lMethod+'::Schemas are not supported in your NGSI version');
       next();
    }
    else if(!configsys.isKnownNgsiLdType(lEntityType))
    {
          // In case of NGSI-LD, avoid contact the Broker to kown if the EntityType exists
          debug(lMethod+'::The entity does not exists: '+lEntityType);
          sendError(res,404,'The entity does not exists: '+lEntityType);
          return;
    }
    else
    {
          ngsildv1.createPromiseGetType(lFiwareService,lEntityType)
             .then((lEntityTypeObject) => {
                  // The entity exists in this Broker  
                  if(sdm.getEntitySchema(lEntityType)!=undefined)
                  {
                      debug(lMethod+'::The schema of '+lEntityType+' is already loaded');
                      next();
                  }
                  else
                  {
                      debug(lMethod+'::The schema of '+lEntityType+' will be loaded');
                      ngsildcontext.createPromisesloadNgsiContext([lFiwareService],[lEntityType])
                         .then(() => {
                              var lSchema=sdm.getEntitySchema(lEntityType);
                              debug(lMethod+'::Schema of '+lEntityType+' was sucessufuly loaded:'+(lSchema!=undefined));
                              next();
                          }).catch((err) => {
                              debug(lMethod+'::Error loadin schema of '+lEntityType+' in service '+lFiwareService+': '+err);
                              sendError(res,404,err)
                          });
                  }
             }).catch((err) => {
                  debug(lMethod+'::Error testing the type '+lEntityType+' in service '+lFiwareService+': '+err);
                  sendError(res,404,err)
             });
     }
}
/**
 * PreJoin tasks
 * Check if lEntityType and lJoinEntityType are related by lJoinAttrib
 * TODO: do cache of the data
 */
function serviceWebhdfsPreJoin(req,res,next)
{
    var lFiwareService=getParamFiwareService(req);
    var lEntityType=getParamEntityType(req);
    var lTenant=configsys.getBrokerTenant(lFiwareService);
    var lJoinEntityType=getParamJoinEntityType(req);
    var lJoinAttrib=getParamAttribFKey(req);

    debug('serviceWebhdfsPreJoin');
    //The entity was tested in serviceWebhdfsPreEntity
    var lSchema=sdm.getEntitySchema(lEntityType);
    if(lSchema==undefined)
    {
         sendError(res,500,'The entity '+lEntityType+' dont have a schema');
         return;
    }
    if(!lSchema.getRelationShipFields().includes(lJoinAttrib))
    {
         sendError(res,404,'The entity '+lEntityType+' dont have an attribute  '+lJoinAttrib);
         return;
    }
    //Read data to find which Entity Types are related by lJoinAttrib
    //TODO: multipage
    ngsildv1.createPromiseQueryEntitiesAttributes(lFiwareService,lEntityType,lJoinAttrib,0,100)
                    .then((ents) => {
                                       if(ngsildv1.getFkEntitiesTypes(ents.results,lJoinAttrib).includes(lJoinEntityType))
                                       {
                                           // OK: lEntityType and lJoinEntityType are related by lJoinAttrib
                                           next();
                                       }
                                       else
                                       {
                                           sendError(res,404,'Not found '+lJoinEntityType+' entities related with '+lEntityType+' using attribute '+lJoinAttrib);
                                       }
                    })
                    .catch((err) => {
                                       debug('Error getting attribute data: '+JSON.stringify(err,null,2));
                                       sendError(res,500,'Error getting attribute data.');
                                    });

}
function loadSaveSendListOfEntities(res,pFiwareService,pCacheFile,pFormat,next)
{
      var lSchemaAsTableAndSchema=metadatasys.toTableListOfEntities();
      saveDatasetInCache(res,lSchemaAsTableAndSchema,pFiwareService,undefined,pCacheFile,pFormat,next);
}
function loadSaveSendSchema(res,pFiwareService,pEntityType,pCacheFile,pFormat,next)
{
      debug('loadSaveSendSchema: '+pFiwareService+' EntityType:'+pEntityType);
      var lSchemaAsTableAndSchema=metadatasys.toTableEntitySchema(pEntityType);
      if(lSchemaAsTableAndSchema!=undefined && lSchemaAsTableAndSchema[0].length>0)
         saveDatasetInCache(res,lSchemaAsTableAndSchema,pFiwareService,pEntityType,pCacheFile,pFormat,next);
      else
         sendError(res,404,'There is no schema information about '+pEntityType);
}
/**
 *
 */
function loadSaveSendEntityRelationships(res,pService,pEntityType,pCacheFile,pFormat,next)
{
      debug('loadSaveSendEntityRelationships:: pService='+pService+'  pEntityType='+pEntityType);
      metadatasys.toTableEntityRelationships(pService,pEntityType!=undefined?[pEntityType]:null).then((lSchemaAsTableAndSchema) => {
                                                      console.log(JSON.stringify(lSchemaAsTableAndSchema,null,2))
                                                      if(lSchemaAsTableAndSchema!=undefined && lSchemaAsTableAndSchema[0].length>0)
                                                         saveDatasetInCache(res,lSchemaAsTableAndSchema,undefined,undefined,pCacheFile,pFormat,next);
                                                      else
                                                         sendError(res,404,'There is no relationships information about Entities');
                                              })
                                              .catch((error) => {
                                                      console.log(error)
                                                      sendError(res,500,'loadSaveSendEntityRelationships::Error: '+error);
                                              });
}

function loadSaveSendGeoMetadata(res,pFiwareService,pFiwareServicePath,pEntityType,pJoinAttrib,pJoinEntityType,pCacheFile,pFormat,next)
{
      debug('loadSaveSendGeoMetadata: '+pFiwareService+' EntityType:'+pEntityType);
      if(configsys.isVersionV2(pFiwareService))
      {
          loadSaveSendGeoMetadataNgsiV2(res,pFiwareService,pFiwareServicePath,pEntityType,pCacheFile,pFormat,next);
      }
      else if(configsys.isVersionLDV1(pFiwareService))
      {
          loadSaveSendGeoMetadataNgsiLD(res,pFiwareService,pFiwareServicePath,pEntityType,pJoinAttrib,pJoinEntityType,pCacheFile,pFormat,next);
      }
      else
      {
          sendError(res,500,'Unknown NGSI version');
      }
}

/**
 * Load, saves (in the cache) and sends it to the hdfs client, the current dataset
 */
function loadSaveSendCurrent(res,pFiwareService,pEntityType,pJoinAttrib,pJoinEntityType,pCacheFile,pFormat,next)
{
      if(configsys.isVersionLDV1(pFiwareService) && !configsys.isKnownNgsiLdType(pEntityType))
      {
          // In case of NGSI-LD, avoid contact the Broker to kown if the EntityType exists
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
                              debug('Error loading current NGSI-V2: '+JSON.stringify(err));
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
                              debug('Error loading current NGSI-LD: '+JSON.stringify(err));
                              sendError(res,500,err.name);
                          })
}
/**
 * Load, saves (in the cache) and sends it to the hdfs client, the GeoMetadata dataset in case of NGSI-V2
 */
function loadSaveSendGeoMetadataNgsiV2(res,pFiwareService,pFiwareServicePath,pEntityType,pCacheFile,pFormat,next)
{
      var lFiwareServicePath=undefined;
      var lExtended=false;
      debug('loadSaveSendGeoMetadataNgsiV2');
      ngsiv2.listGeoMetadataEntities(pFiwareService,pFiwareServicePath,pEntityType)
          .then((tableAndSchema) => {
                              //debug('GeoMetadataNgsiV2: \n'+JSON.stringify(tableAndSchema,null,2));
                              saveDatasetInCache(res,tableAndSchema,pFiwareService,pEntityType,pCacheFile,pFormat,next);
                           })
          .catch((err) => {
                              debug('Error loading GeoMetadataNgsiV2: '+JSON.stringify(err));
                              sendError(res,500,err.name);
                          })
}
/**
 * Load, saves (in the cache) and sends it to the hdfs client, the GeoMetadata dataset in case of NGSI-LD
 */
function loadSaveSendGeoMetadataNgsiLD(res,pFiwareService,pFiwareServicePath,pEntityType,pJoinAttrib,pJoinEntityType,pCacheFile,pFormat,next)
{
      ngsildv1.listGeoMetadataEntities(pFiwareService,pFiwareServicePath,pEntityType)
          .then((tableAndSchema) => {
                              //debug('GeoMetadataNgsiLD: \n'+JSON.stringify(tableAndSchema,null,2));
                              saveDatasetInCache(res,tableAndSchema,pFiwareService,pEntityType,pCacheFile,pFormat,next);
                           })
          .catch((err) => {
                              debug('Error loading current: '+JSON.stringify(err));
                              sendError(res,500,err.name);
                          })
}

/**
 * Saves data in cache and sends it the Hdfs client
 * pService can be System or Fiware Service
 */
function saveDatasetInCache(res,pTableAndSchema,pService,pEntityType,pCacheFile,pFormat,next)
{
      var lTableValues=pTableAndSchema[0];
      var lTableSchema=pTableAndSchema[1];
      if(lTableSchema==null)
      {
           debug('Table Schema is not availlable, thus we consider that the entity does not exists');
           sendError(res,404,'Entity nof found');
      }
      else if(lTableValues.length==0 && formats.isParquet(pFormat))
      {
           debug('Is not possible the generate a empty Parquet table');
           sendError(res,404,'Empty table');
      }
      else
      {
           debug('Start Save in Cache');
           output.createPromiseSaveInCache(pCacheFile,lTableValues,pEntityType,lTableSchema,pFormat)
                 .then(() => {
                                         var lFileStats=saveFileStats(res,pCacheFile);
                                         //debug('Output File: '+pCacheFile+' format='+pFormat+'  stats:'+JSON.stringify(lFileStats));
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
 * Service for handling a Not Found case
 */
function serviceWebhdfsNotFound(req,res,next)
{
    debug('serviceWebhdfsNotFound::path='+getPath(req));
    sendError(res,404,'Resource not found');
}
/**
 * Service for handling a Command over the Hadoop's root
 */
function serviceWebhdfsCommandRoot(req,res,next)
{
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_ROOT);
}
/**
 * Service for handling a Command over the /data
 */
function serviceWebhdfsCommandDirectoryData(req,res,next)
{
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_DATA);
}
/**
 * Service for handling a Command over a System service (i.e., /system)
 */
function serviceWebhdfsCommandDirectorySystem(req,res,next)
{
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_SYSTEM);
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
 * Service for handling a Command over the current dataset
 */
function serviceWebhdfsCommandCurrentDataset(req,res,next)
{
   debug('serviceWebhdfsCommandCurrentDataset');
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_CURRENT);
}
/**
 * Service for handling a Command over the enities dataset
 */
function serviceWebhdfsCommandEntitiesDataset(req,res,next)
{
   debug('serviceWebhdfsCommandEntitiesDataset')
   serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_ENTITIES);
}

/**
 * TODO: describe
 */
function serviceWebhdfsCommandEntitySchema(req,res,next)
{
    debug('serviceWebhdfsCommandEntitySchema');
    serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_SCHEMA);
}
function serviceWebhdfsCommandEntityGeoMetadata(req,res,next)
{
    debug('serviceWebhdfsCommandEntityGeoMetadata');
    serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_GEOMETADATA);
}
/**
 * Service for handling a Command to obtain entities' relationships
 */
function serviceWebhdfsCommandEntitiesRelationships(req,res,next)
{
    debug('serviceWebhdfsCommandEntitiesRelationships');
    serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_ENTITIES_RELS);
}
/**
 * Service for handling a Command over system metadata folder
 */
function serviceWebhdfsCommandFolderSystemMetadata(req,res,next)
{
    serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_SYSTEM_METADATA);
}
/**
 * Service for handling a Command over system metadata folder of an entity
 */
function serviceWebhdfsCommandFolderSystemEntity(req,res,next)
{
    serviceWebhdfsCommandByLogicalPath(req,res,next,PATH_SM_ENTITY);
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
         debug('Handler: '+lHandler.name);
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
 * Handles the request for GetFileStatus over the Hadoop's root
 */
function doGetFileStatusRoot(req,res,next)
{
     debug('doGetFileStatusRoot'); 
     res.status(200).json(webhdfsobjs.toFileStatus(0,BLOCK_SIZE,undefined,undefined,DFL_OWNER,DFL_GROUP));
}
/**
 * Handles the request for GetFileStatus over the Hadoop's /data
 */
function doGetFileStatusDirectoryData(req,res,next)
{
     debug('doGetFileStatusDirectoryData'); 
     res.status(200).json(webhdfsobjs.toFileStatus(0,BLOCK_SIZE,undefined,undefined,DFL_OWNER,DFL_GROUP));
}
/**
 * Handles the request for GetFileStatus over the Hadoop's /:systemService  (/system)
 */
function doGetFileStatusDirectorySystem(req,res,next)
{
     debug('doGetFileStatusDirectorySystem'); 
     res.status(200).json(webhdfsobjs.toFileStatus(0,BLOCK_SIZE,undefined,undefined,DFL_OWNER,DFL_GROUP));
}

/**
 * Handles the request for GetFileStatus over a Fiware service (i.e., a Broker)
 */
function doGetFileStatusFiwareService(req,res,next)
{
     var lFiwareService=getParamFiwareService(req);
     debug('doGetFileStatusFiwareService of Fiware Service: '+lFiwareService); 
     res.status(200).json(webhdfsobjs.toFileStatus(0,BLOCK_SIZE,undefined,undefined,DFL_OWNER,DFL_GROUP));
}
/**
 * Handles the request for GetFileStatus over an Entity in the context of a Broker (i.e., a Fiware Service)
 */
function doGetFileStatusServiceEntity(req,res,next)
{
     var lFiwareService=getParamFiwareService(req);
     var lEntityType=getParamEntityType(req);
     debug('doGetFileStatusServiceEntity of '+lEntityType+' at the Fiware Service: '+lFiwareService); 
     res.status(200).json(webhdfsobjs.toFileStatus(0,BLOCK_SIZE,undefined,undefined,DFL_OWNER,DFL_GROUP));
}
/**
 * Handles the request for GetFileStatus over an Entity in the context of the system's metadata 
 */
function doGetFileStatusSystemEntity(req,res,next)
{
     var lSystemService=getParamSystemService(req);
     var lEntityType=getParamEntityType(req);
     debug('doGetFileStatusSystemEntity of '+lEntityType+' at the System Service: '+lSystemService); 
     res.status(200).json(webhdfsobjs.toFileStatus(0,BLOCK_SIZE,undefined,undefined,DFL_OWNER,DFL_GROUP));
}
/**
 *
 */
function doGetFileStatusCachedFile(req,res,next)
{
     var lFilesize=res.locals.iotbi_filesize;
     var lFilename=res.locals.iotbi_filepath;
     //debug('doGetFileStatusCachedFile: '+lFilename+' Size:'+lFilesize+'  fileStats='+JSON.stringify(res.locals.iotbi_filestats));
     var lAccTime=res.locals.iotbi_filestats.mtimeMs
     var lModTime=res.locals.iotbi_filestats.mtimeMs
     res.status(200).json(webhdfsobjs.toFileStatus(lFilesize,BLOCK_SIZE,lAccTime,lModTime,DFL_OWNER,DFL_GROUP));
}
/**
 * Handles the request for GetFileStatus over the Schema of an entity
 */
function doGetFileStatusFolderMetadata(req,res,next)
{
     debug('doGetFileStatusFolderMetadata'); 
     res.status(200).json(webhdfsobjs.toFileStatus(0,BLOCK_SIZE,undefined,undefined,DFL_OWNER,DFL_GROUP));
}
/**
 *  Handles the request for ListStatus over system's metadata folder
 */
function doListStatusFolderMetadata(req,res,next)
{
     debug('doListStatusFolderMetadata');
     var lHadoopUser=getQueryParamHadoopUser(req,true);
     res.status(200).json(webhdfsobjs.toFileStatuses(toFilesSystemServiceMetadata(),BLOCK_SIZE));
}
/**
 * Handles the request for ListStatus over the Hadoop root
 */
function doListStatusRoot(req,res,next)
{
     debug('doListStatusRoot');
     var lHadoopUser=getQueryParamHadoopUser(req,true);
     var lDirs=[];
     // Present System Service as a directory
     var lDir=genDirectory(configsys.SERVICE_SYSTEM,undefined,undefined);
     lDirs.push(lDir);
     // Present the Data directory
     var lDir=genDirectory(PATH_DATA,undefined,undefined);
     lDirs.push(lDir);
     res.status(200).json(webhdfsobjs.toFileStatuses(lDirs,BLOCK_SIZE));
}
function doListStatusDirectoryData(req,res,next)
{
     debug('doListStatusDirectoryData');
     var lHadoopUser=getQueryParamHadoopUser(req,true);
     var lDirs=[];
     // Present FiwareServices as directories
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
function doListStatusDirectorySystem(req,res,next)
{
     debug('doListStatusDirectorySystem');
     var lHadoopUser=getQueryParamHadoopUser(req,true);
     var lDirs=[];
     // Present the metadata directory
     var lDir=genDirectory(PATH_SYSTEM_METADATA,undefined,undefined);
     lDirs.push(lDir);
     res.status(200).json(webhdfsobjs.toFileStatuses(lDirs,BLOCK_SIZE));
}

/**
 * Handles the request for ListStatus over an entity
 */
function doListStatusSystemEntity(req,res,next)
{
    var lSystemService=getParamSystemService(req);
    // System services
    var lEntityType=getParamEntityType(req);
    debug('doListStatusSystemEntity of '+lEntityType+' at the System Service: '+lSystemService);
    res.status(200).json(webhdfsobjs.toFileStatuses(genServiceEntityFiles(true,false,true,false,true),BLOCK_SIZE));
}
/**
 * Handles the request for ListStatus over an entity
 */
function doListStatusServiceEntity(req,res,next)
{
    var lFiwareService=getParamFiwareService(req);
    // Normal services
    var lEntityType=getParamEntityType(req);
    debug('doListStatusServiceEntity of '+lEntityType+' at the Fiware Service: '+lFiwareService); 
    var lTenant=configsys.getBrokerTenant(lFiwareService);
    if(lTenant==undefined)
    {
        sendError(res,404,'Invalid Tenant');
    }
    else if(configsys.isVersionLDV1(lFiwareService))
    {
        ngsildv1.createPromiseGetType(lFiwareService,lEntityType)
                            .then((type) => {
                                               res.status(200).json(webhdfsobjs.toFileStatuses(toEntityTypeToFiles(lEntityType,true,true,true),BLOCK_SIZE));
                                            })
                            .catch((err) => {
                                              debug('doListStatusServiceEntity::LDV1::Error: '+JSON.stringify(err));
                                              sendError(res,404,err.name=='NotFound'?'Entity type '+lEntityType+' is not valid':err.name)
                                            });
    }
    else if(configsys.isVersionV2(lFiwareService))
    {
       ngsiv2.createPromiseGetType(lFiwareService,lEntityType)
       .then((types) =>  {
                            res.status(200).json(webhdfsobjs.toFileStatuses(toEntityTypeToFiles(lEntityType,false,false,false),BLOCK_SIZE))
                         }) 
       .catch((err) =>   {
                            debug('doListStatusServiceEntity::V2::Error: '+JSON.stringify(err));
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
function doListStatusServiceEntityJoins(req,res,next)
{
    var lFiwareService=getParamFiwareService(req);
    var lEntityType=getParamEntityType(req);
    debug('doListStatusServiceEntityJoins of '+lEntityType+' at the Fiware Service: '+lFiwareService); 
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
function doListStatusServiceEntityJoinAttrib(req,res,next)
{
    var lFiwareService=getParamFiwareService(req);
    var lEntityType=getParamEntityType(req);
    var lAttribKey=getParamAttribFKey(req);
    var lJoinEntityType=getParamJoinEntityType(req);

    debug('doListStatusServiceEntityJoin of '+lEntityType+' using '+lAttribKey+' and type '+lJoinEntityType+' at the Fiware Service: '+lFiwareService);
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
           //TODO: multipage
           ngsildv1.createPromiseQueryEntitiesAttributes(lFiwareService,lEntityType,lAttribKey,0,100)
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
                                           res.status(200).json(webhdfsobjs.toFileStatuses(genServiceEntityFiles(true,true,false,false,false),BLOCK_SIZE));
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
     var lHadoopUser=getQueryParamHadoopUser(req,true);
     var lFiwareService=getParamFiwareService(req);
     debug('doListStatusFiwareService of Fiware Service: '+lFiwareService); 
     // Special services
     if(lFiwareService==configsys.SERVICE_SYSTEM)
     {
        res.status(200).json(webhdfsobjs.toFileStatuses(toFilesSystemService(),BLOCK_SIZE));
        return;
     }
     // Normal Services
     var lTenant=configsys.getBrokerTenant(lFiwareService);
     if(lTenant==undefined)
     {
         sendError(res,404,'Invalid tenant');
     }
     else if(configsys.isVersionLDV1(lFiwareService))
     {
           if(ngsiCache.getTypes(lFiwareService)!=undefined)
           {
              debug('The available types in tenant '+lTenant+', lFiwareService '+lFiwareService+' is cached!')
              var lItemCacheTypes=ngsiCache.getItemTypes(lFiwareService);
              res.status(200).json(webhdfsobjs.toFileStatuses(toFilesFromListTypeLDV1(lItemCacheTypes.types,lItemCacheTypes.time,lHadoopUser,lFiwareService),BLOCK_SIZE));
           }
           else
           {
               ngsildv1.createPromiseListTypes(lFiwareService)
                             .then((types) => {
                                                ngsiCache.saveTypes(lFiwareService,types);
                                                res.status(200).json(webhdfsobjs.toFileStatuses(toFilesFromListTypeLDV1(types,undefined,lHadoopUser,lFiwareService),BLOCK_SIZE));
                                              }) 
                             .catch((err) => {
                                                debug('Error geting types: '+JSON.stringify(err,null,2));
                                                sendError(res,err.code,'Error getting types. Broker response: '+err.name);
                                             });
           }
     }
     else if(configsys.isVersionV2(lFiwareService))
     {
           if(ngsiCache.getTypes(lFiwareService)!=undefined)
           {
              debug('The available types in tenant '+lTenant+', lFiwareService '+lFiwareService+' is cached!')
              var lItemCacheTypes=ngsiCache.getItemTypes(lFiwareService);
              res.status(200).json(webhdfsobjs.toFileStatuses(toFilesFromListTypeV2(lItemCacheTypes.types,lItemCacheTypes.time,lHadoopUser,lFiwareService),BLOCK_SIZE));
           }
           else
           {
              ngsiv2.createPromiseListTypes(lFiwareService,undefined)
                            .then((types) =>  {
                                                 ngsiCache.saveTypes(lFiwareService,types);
                                                 res.status(200).json(webhdfsobjs.toFileStatuses(toFilesFromListTypeV2(types,undefined,lHadoopUser,lFiwareService),BLOCK_SIZE)) ;
                                              })
                            .catch((err) =>   {
                                                 sendError(res,500,err);
                                              });
           }
     }
     else
     {
        sendError(res,500,'Unsupported NGSI version');
     }
}
/**
 * Handles the request for ListStatus of the current dataset file
 */
function doListStatusCachedFile(req,res,next)
{
     var lPath=getPath(req);
     var lFilesize=res.locals.iotbi_filesize;
     var lFilename=res.locals.iotbi_filepath;
     debug('doListStatusCachedFile: '+lFilename+' Size:'+lFilesize);
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
   var lCode=pCode;
   if(pCode==undefined)
   {
      debug('The error code is not defined. Sending error 500');
      lCode=500;
   }
   debug('Send ['+lCode+']:'+pMsg);
   res.status(lCode).json(webhdfsobjs.toError(lCode,pMsg));
}
/**
 * Transforms a List of Entity Type (NGSI-V2) into a List of Files
 */
function toFilesFromListTypeV2(pListTypes,pCacheTime,pHadoopUser,pFiwareService)
{
    //debug('List Types:\n'+JSON.stringify(pListTypes,null,2));
    var lFiles=[];
    for(var i in pListTypes.results)
    {
      var lEntityType=pListTypes.results[i].type;
      if(configsys.isHiddenEntity(pHadoopUser,pFiwareService,lEntityType))
      {
         debug('The Entity type '+lEntityType+' id hidden for user '+pHadoopUser+'  in service '+pFiwareService);
      }
      else
      {
         var lFilename=pListTypes.results[i].type;
         var lDir=genDirectory(lFilename,pCacheTime,pCacheTime);
         lFiles.push(lDir);
      }
    }
    return lFiles;
}
/**
 * Transforms a List of Entity Type (NGSI-LD) into a List of Files
 */
function toFilesFromListTypeLDV1(pListTypes,pCacheTime,pHadoopUser,pFiwareService)
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
      if(configsys.isHiddenEntity(pHadoopUser,pFiwareService,lEntityType))
      {
         debug('The Entity type '+lEntityType+' id hidden for user '+pHadoopUser+'  in service '+pFiwareService);
      }
      else
      {
         var lDir=genDirectory(lFilename,pCacheTime,pCacheTime);
         lFiles.push(lDir);
      }
    }
    return lFiles;
}
/**
 * List of files of path /system
 */
function toFilesSystemService()
{
    var lFiles=[];
    var lDir=genDirectory(PATH_SYSTEM_METADATA,undefined,undefined);
    lFiles.push(lDir);
    return lFiles;
}
/**
 * List of files of path /system/metadata
 */
function toFilesSystemServiceMetadata()
{
    var lFilesAndDirs=genMetadataEntitiesFiles();
    for(lEntityType in configsys.getNgsiLdEntityAllContexts())
    {
       lFilesAndDirs.push(genDirectory(lEntityType,undefined,undefined));
    }
    return lFilesAndDirs;
}

/**
 * Produces a list of files associated to an entity type
 */
function toEntityTypeToFiles(pEntityType,pSupportsLD,pAddSchemas,pAddRelations)
{
    var lFiles=genServiceEntityFiles(pSupportsLD,true,pAddSchemas,false,pAddRelations);
    if(pSupportsLD)
    {
      var lSchema=sdm.getEntitySchema(pEntityType);
      if(lSchema.getRelationShipFields().length>0)
      { 
         // The entity contains relation ships, thus, the join option is presented
         var lDir=genDirectory(DIR_JOINS,undefined,undefined);
         lFiles.push(lDir);
      }
    }
    return lFiles;
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
function genCurrentDatasetFiles()
{
   var lFiles=[];
   for (f of FILE_FORMATS)
   {
        var lFile=PATH_CURRENT+'.'+f;
        lFiles.push(genFile(lFile,1,undefined,undefined));
   }
   return lFiles;
}
/**
 * 
 */
function genSchemaFiles()
{
   debug('genSchemaFiles');
   var lFiles=[];
   for (f of FILE_FORMATS)
   {
        var lFile=PATH_SCHEMA+'.'+f;
        lFiles.push(genFile(lFile,1,undefined,undefined));
   }
   return lFiles;
}
function genGeoMetadataFiles()
{
   debug('genGeoMetadataFiles');
   var lFiles=[];
   for (f of FILE_FORMATS)
   {
        var lFile=PATH_GEOMETADATA+'.'+f;
        lFiles.push(genFile(lFile,1,undefined,undefined));
   }
   return lFiles;
}
function genEntitiesRelationshipsFiles()
{
   debug('genEntitiesRelationshipsFiles');
   var lFiles=[];
   for (f of FILE_FORMATS)
   {
        var lFile=PATH_ENTITIES_RELS+'.'+f;
        lFiles.push(genFile(lFile,1,undefined,undefined));
   }
   return lFiles;
}

/**
 * Generates an array with all possible dataset of an Entity
 */
function genServiceEntityFiles(pSupportsLD,pAddDatasets,pAddSchemas,pAddGeoMetadata,pAddEntitiesRelations)
{
     debug('genServiceEntityFiles');
     var lFiles=[];
     if(pAddDatasets)
     {
        lFiles=genCurrentDatasetFiles();
     }
     if(pSupportsLD && pAddSchemas)
     {
       debug('Adding Schema files')
       for(f of genSchemaFiles())
       {
          debug('Adding Schema file '+f.fileName)
          lFiles.push(f);
       }
     }
     if(pAddGeoMetadata)
     {
       debug('Adding GeoMetadata files')
       for(f of genGeoMetadataFiles())
       {
          debug('Adding Schema file '+f.fileName)
          lFiles.push(f);
       }
     }
    if(pAddEntitiesRelations)
     {
       debug('Adding EntitiesRelationships files')
       for(f of genEntitiesRelationshipsFiles())
       {
          debug('Adding EntitiesRelationship  file '+f.fileName)
          lFiles.push(f);
       }
     }

     return lFiles;
}
function genMetadataEntitiesFiles()
{
   debug('genMetadataEntitiesFiles');
   var lFiles=[];
   for (f of FILE_FORMATS)
   {
        var lFile=PATH_ENTITIES+'.'+f;
        lFiles.push(genFile(lFile,1,undefined,undefined));
        lFile=PATH_ENTITIES_RELS+'.'+f;
        lFiles.push(genFile(lFile,1,undefined,undefined));
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
    debug('NGSI-LD :: OrionLD/Scorpio/Stellio: Tenant: '+lTenant+'    Entity: '+pEntityType);
    // Get the Query
    var lObjQuery = utils.getObjectQuery(lQuery,pEntityType,lEntityId);
    debug('Query: '+JSON.stringify(lObjQuery));
    return ngsildv1.listEntities(lEndpoint,lObjQuery,pFiwareService,lFiwareServicePath,lExtended,pEntityType,pJoinEntityType,pJoinAttrib);
}
