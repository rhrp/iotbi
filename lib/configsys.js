/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the API configuration subsystem.
 * 
 */
var debug = require('debug')('iotbi.configsys');
var { createHash } = require('crypto');
var {config} = require('../config.js')

debug('System configured for '+Object.keys(config.broker.servers).length+' Tenants');

// Exported configs
exports.security=config.security;
exports.logger=config.logger;
exports.broker=config.broker;
exports.quantumleap=config.quantumleap;

// Constants
const VERSION_NGSI_V1 = 'v1';
const VERSION_NGSI_V2 = 'v2';
const VERSION_NGSI_LD_V1 = 'ldv1';

const BROKER_ORION	= 'Orion';
const BROKER_ORION_LD   = 'OrionLD';
const BROKER_SCORPIO    = 'Scorpio';
const BROKER_STELLIO    = 'Stellio';

const SERVICE_SYSTEM	= 'system'

const TA_LIMIT={};      //  API Limit
TA_LIMIT[BROKER_ORION.toLowerCase()]=1000;
TA_LIMIT[BROKER_ORION_LD.toLowerCase()]=1000;
TA_LIMIT[BROKER_SCORPIO.toLowerCase()]=1000;
TA_LIMIT[BROKER_STELLIO.toLowerCase()]=100;


exports.BROKER_ORION_LD=BROKER_ORION_LD;
exports.BROKER_SCORPIO=BROKER_SCORPIO;
exports.BROKER_STELLIO=BROKER_STELLIO;

exports.SERVICE_SYSTEM=SERVICE_SYSTEM;


exports.isBrokerOrion = function(pFiwareService) {
	return this.isBrokerServerServerOk(pFiwareService) && config.broker.servers[pFiwareService].broker.toLowerCase()===BROKER_ORION.toLowerCase();
}
exports.isBrokerOrionLD = function(pFiwareService) {
        return this.isBrokerServerServerOk(pFiwareService) && config.broker.servers[pFiwareService].broker.toLowerCase()===BROKER_ORION_LD.toLowerCase();
}
exports.isBrokerScorpio = function(pFiwareService) {
        return this.isBrokerServerServerOk(pFiwareService) && config.broker.servers[pFiwareService].broker.toLowerCase()===BROKER_SCORPIO.toLowerCase();
}
exports.isBrokerStellio = function(pFiwareService) {
        return this.isBrokerServerServerOk(pFiwareService) && config.broker.servers[pFiwareService].broker.toLowerCase()===BROKER_STELLIO.toLowerCase();
}
/**
 * Returns true if the broker(s) is(are) weel defined for a fiware-service
 */
exports.isConfigOk = function(pFiwareService) {
	return this.isBrokerServerOk(pFiwareService)
            && (this.isBrokerSuportsTemporalAPI(pFiwareService) || this.getQuantumLeapServerOk(pFiwareService));
}


/**
 * Return True if the broker suports the NGSI temporal API.
 * If not, we must use  QuantumLeap
 */
exports.isBrokerSuportsTemporalAPI = function(pFiwareService) {
        return this.isBrokerScorpio(pFiwareService) || this.isBrokerStellio(pFiwareService)
}

// Broker function
exports.getBrokerName = function(pFiwareService) {
        return  this.isBrokerServerOk(pFiwareService)?config.broker.servers[pFiwareService].broker:undefined;
}
exports.isBrokerServerOk = function(pFiwareService) {
        return  config.broker.servers[pFiwareService]!=null;
}
exports.isBrokerHttpS = function(pFiwareService) {
   return config.broker.servers[pFiwareService].https;
}
exports.getBrokerHost = function(pFiwareService) {
   return config.broker.servers[pFiwareService].host;
}
exports.getBrokerPort = function(pFiwareService) {
   return config.broker.servers[pFiwareService].port;
}
exports.getBrokerNGSIVersion = function(pFiwareService) {
   return config.broker.servers[pFiwareService].ngsi;
}
exports.getBrokerNgsiLdContext = function(pFiwareService) {
   return config.broker.servers[pFiwareService].ngsildContext;
}
exports.getBrokerJsonLdContext = function(pFiwareService) {
   return config.broker.servers[pFiwareService].ngsildContexts;
}
exports.getBrokerCommonsSchemas = function(pFiwareService) {
   return config.broker.servers[pFiwareService].schemaCommon;
}
exports.isBrokerServerServerOk = function(pFiwareService) {
   return  config.broker.servers[pFiwareService]!=null;
}
/**
 * Check is a entity exists
 */
exports.isExistsEntity = function(pEntityType) {
   return getEntityTypeObject(pEntityType)!=undefined;
}

exports.getNgsiLdEntityContext = function(pEntityType) {
   var lCfgObj=getEntityTypeObject(pEntityType);
   return lCfgObj!=undefined?lCfgObj.urlContext:undefined;
}
exports.getNgsiLdEntityAllContexts = function() {
   var lAllContexts={};
   for(var lEntityType in config.entityTypes)
   {
        var lCfgObj=getEntityTypeObject(lEntityType);
        lAllContexts[lEntityType]=lCfgObj.urlContext;
   }
   debug(JSON.stringify(lAllContexts));
   return lAllContexts;
}
exports.getNgsiLdEntitySchema = function(pEntityType) {
   var lCfgObj=getEntityTypeObject(pEntityType);
   return lCfgObj!=undefined?lCfgObj.urlSchema:undefined;
}
exports.getNgsiLdEntityAllSchemas = function() {
   var lAllSchemas={};
   for(var lEntityType in config.entityTypes)
   {
        var lCfgObj=getEntityTypeObject(lEntityType);
        lAllSchemas[lEntityType]=lCfgObj.urlSchema;
   }
   return lAllSchemas;
}

exports.getBrokerTenant = function(pFiwareService) {
   return config.broker.servers[pFiwareService]==undefined?undefined:config.broker.servers[pFiwareService].tenant;
}

exports.findEntityTypeByContext = function(pUrlcontext)
{
   for(var lEntityType in config.entityTypes)
   {
        var lCfgObj=getEntityTypeObject(lEntityType);
        if(lCfgObj.urlContext==pUrlcontext)
        {
          return lEntityType;
        }
   }
   return undefined;
} 

// Exported functions
exports.isOrionLD = function(pFiwareService) {
   return this.getBrokerNGSIVersion(pFiwareService)==VERSION_NGSI_LD_V1 && this.isBrokerOrion(pFiwareService);
}
exports.isOrionV1 = function(pFiwareService) {
   return this.getBrokerNGSIVersion(pFiwareService)==VERSION_NGSI_V1 && this.isBrokerOrion(pFiwareService);
}
exports.isOrionV2 = function(pFiwareService) {
   return this.getBrokerNGSIVersion(pFiwareService)==VERSION_NGSI_V2 && this.isBrokerOrion(pFiwareService);
}

exports.isVersionLDV1 = function(pFiwareService) {
   return this.getBrokerNGSIVersion(pFiwareService)==VERSION_NGSI_LD_V1;
}
exports.isVersionV1 = function(pFiwareService) {
   return this.getBrokerNGSIVersion(pFiwareService)==VERSION_NGSI_V1;
}
exports.isVersionV2 = function(pFiwareService) {
   return this.getBrokerNGSIVersion(pFiwareService)==VERSION_NGSI_V2;
}


/**
 * Returns an array with Broker's FiwareService
 */

exports.getFiwareServiceList = function()
{
   var lList=[];
   for(var lKey in config.broker.servers)
   {
      var lBroker=config.broker.servers[lKey];
      lList.push(lBroker.tenant);
   }
   return lList;
}
/**
 * Returns an array with Broker's alias
 */
exports.getBrokersAliasList = function()
{
   var lList=[];
   for(var lKey in config.broker.servers)
   {
      lList.push(lKey);
   }
   return lList;
}

//Deprecated
exports.getOrionEndpoint = getBrokerEndpoint;

exports.getBrokerEndpoint = getBrokerEndpoint;
function getBrokerEndpoint(pFiwareService) 
{
   var lUrl=this.getBrokerURL(pFiwareService);
   var lVersion=this.getBrokerNGSIVersion(pFiwareService);
   if(lVersion==VERSION_NGSI_V1 || lVersion==VERSION_NGSI_V2)
   {
       // Orion 
       return lUrl+'/'+lVersion;
   }
   else if(lVersion==VERSION_NGSI_LD_V1)
   {   // OrionLD
       return lUrl+'/ngsi-ld/v1';
   }
   else
   {
      debug('Invalid NGSI version '+lVersion);
      return null;
   }
}
exports.getBrokerURL = function(pFiwareService) {
   var lServerHttpSecured=this.isBrokerHttpS(pFiwareService);
   var lServer=this.getBrokerHost(pFiwareService);
   var lPort=this.getBrokerPort(pFiwareService);
   return (lServerHttpSecured?'https':'http')+'://'+lServer+':'+lPort;
}
//QuantumLeap
exports.getQuantumLeapServerOk = function(pFiwareService) {
   return config.quantumleap.servers[pFiwareService]!=null;
}
exports.getQuantumLeapHttpS = function(pFiwareService) {
   return config.quantumleap.servers[pFiwareService].https;
}
exports.getQuantumLeapHost = function(pFiwareService) {
   return config.quantumleap.servers[pFiwareService].host;
}
exports.getQuantumLeapPort = function(pFiwareService) {
   return config.quantumleap.servers[pFiwareService].port;
}

// System
exports.getAppName = function (pAppKey)
{
   var lApp=config.security.appKeys[pAppKey];
   return lApp==undefined?undefined:lApp.name;
}
/**
 * Broker's Page size
 * Max value for parameter limit
 */
exports.getBrokerPageSize = function(pFiwareService)
{
   var lBroker=config.broker.servers[pFiwareService].broker.toLowerCase();
   //debug('getBrokerPageSize('+lBroker+'): '+TA_LIMIT[lBroker]);
   return TA_LIMIT[lBroker];
}

/**
 * Get the status of cache subsystem
 */
exports.isCacheEnabled = function(pFiwareService)
{
   return config.cache.enabled;
}
/**
 * TTL of cache um ms
 */
exports.getCacheTTL = function(pFiwareService)
{
  if(config.cache.ttlms==undefined || config.cache.ttlms<=0 || config.cache.ttlms>3600000)
  {
      debug('Invalid config config.cache.ttlms='+config.cache.ttlms);
      return 20000;
  }
  else
  {
      return config.cache.ttlms;
  }
}
/**
 * Get the cache path
 */
exports.getCacheLocalPath = function()
{
  if(config.cache.localPath==undefined || !config.cache.localPath.startsWith('/') || config.cache.localPath.endsWith('/'))
  {
      debug('Invalid config config.cache.localPath="'+config.cache.localPath+'"');
      return '/tmp/iotbi';
  }
  else
  {
      return config.cache.localPath;
  }
}
/**
 * Get the config object of a entity type
 */
exports.getEntityTypeObject = getEntityTypeObject;
function getEntityTypeObject(pEntityType)
{
   return config.entityTypes[pEntityType];
}
/**
 * Get an Array with all known EntityType
 */
exports.getKnownEntityTypes = getKnownEntityTypes;
function getKnownEntityTypes()
{
    var lTypes=[];
    for(var lType in config.entityTypes)
    {
        lTypes.push(lType);
    }

    return lTypes;
}
/**
 * Returns true, if it is a registred type
 */
exports.isKnownNgsiLdType = function(pEntityType)
{
    return getKnownEntityTypes().includes(pEntityType);
}
/**
 * Check is a user have access to webhdfs API
 * In the case of wbhdfs access, the Hadoop username plays the role of the appKey
 */
exports.allowHadoopToUser = function (pHadoopUserName)
{
    if(pHadoopUserName==undefined)
    {
        debug('Undefined hadoop user');
        return false;
    }
    else if(config.security.appKeys[pHadoopUserName]==undefined)
    {
        debug('Invalid hadoop user: '+pHadoopUserName);
        return false;
    }
    else
    {
        return config.security.appKeys[pHadoopUserName].allowHadoop;
    }
}
/**
 * Check if the appKey have granted access to a pFiwareService
 */
exports.allowScopeToUser = function(pHadoopUserName,pFiwareService)
{
    if(pHadoopUserName==undefined)
    {
        debug('Undefined pHadoopUserName');
        return false;
    }
    else if(pFiwareService==undefined)
    {
        debug('Undefined pFiwareService');
        return false;
    }
    else if(config.security.appKeys[pHadoopUserName]==undefined)
    {
        debug('Invalid pHadoopUserName: '+pHadoopUserName);
        return false; 
    }
    else
    {
        return config.security.appKeys[pHadoopUserName].scopes.includes(pFiwareService);
    }
}

/**
 * Find the appKey searching by its sha256 hash
 */
exports.findAppKeyBySha256 = function (pAppKeySha256)
{
   for(lAppKey in config.security.appKeys)
   {
      lSha256=createHash('sha256').update(lAppKey).digest('hex');
      //debug(pAppKeySha256+'   '+lSha256+' ('+lAppKey+')');
      if(lSha256==pAppKeySha256)
      {
         return lAppKey;
      }
   } 
   return undefined;
}
/**
 * Return the appKey used in the MCP endpoint 
 */
exports.getMcpAppKey = function()
{
   return (process.env.MCP_APPKEY || 'noAppKey');
}

/**
 * Check is a user have access to REST API 
 */
exports.allowApiToKey = function(pAppKey)
{
    if(pAppKey==undefined)
    {
        debug('Undefined pAppKey');
        return false;
    }
    else if(config.security.appKeys[pAppKey]==undefined)
    {
        debug('Invalid pAppKey: '+pAppKey);
        return false; 
    }
    else
    {
        return config.security.appKeys[pAppKey].allowAPI;
    }
}
/**
 * Check if the appKey have granted access to a pFiwareService
 */
exports.allowScopeToKey = function(pAppKey,pFiwareService)
{
    if(pAppKey==undefined)
    {
        debug('Undefined pAppKey');
        return false;
    }
    else if(pFiwareService==undefined)
    {
        debug('Undefined pFiwareService');
        return false;
    }
    else if(config.security.appKeys[pAppKey]==undefined)
    {
        debug('Invalid pAppKey: '+pAppKey);
        return false; 
    }
    else
    {
        return config.security.appKeys[pAppKey].scopes.includes(pFiwareService);
    }
}
exports.getLimitDay = function(pAppKey)
{
    if(pAppKey==undefined)
    {
        debug('Undefined pAppKey');
        return false;
    }
    else if(config.security.appKeys[pAppKey]==undefined)
    {
        debug('Invalid pAppKey: '+pAppKey);
        return false; 
    }
    else
    {
        return config.security.appKeys[pAppKey].limitDay;
    }
}
exports.isHiddenEntity = function (pAppKey,pFiwareService,pEntityType)
{
    if(pAppKey==undefined)
    {
        debug('Undefined pAppKey');
        return true;
    }
    else if(config.security.appKeys[pAppKey]==undefined)
    {
        debug('Invalid pAppKey: '+pAppKey);
        return true; 
    }
    else if(config.security.appKeys[pAppKey].hiddenEntities==undefined)
    {
        debug('Invalid definitions hiddenEntities');
        return true; 
    }
    else if(config.security.appKeys[pAppKey].hiddenEntities[pFiwareService]==undefined)
    {
        // The list for this pFiwareService is empty
        debug('The list of hiddenEntities for '+pAppKey+' in '+pFiwareService+' is empty');
        return false;
    }
    else
    {
        // The pEntityType is in the list
        var lIsHidden=config.security.appKeys[pAppKey].hiddenEntities[pFiwareService].includes(pEntityType);
        debug('For user/key '+pAppKey+' in '+pFiwareService+' the entity type '+pEntityType+' is '+(lIsHidden?'hidden':'not hidden'));
        return lIsHidden;
    }
}

/**
 * This are hardcoded options
 */ 

// The geoquery is done after loading from broker. In my Stellio there is a problem with geo queries
const LOCAL_GEOQUERY=true;
//The seach based on text pattern is done after loading from broker.
const LOCAL_SEARCHTEXTPATTERN   = false;

exports.isLocalGeoQueryOn = function()
{
   return LOCAL_GEOQUERY;
}
exports.isLocalSearchTextPatternOn = function()
{
   return LOCAL_SEARCHTEXTPATTERN;
}
