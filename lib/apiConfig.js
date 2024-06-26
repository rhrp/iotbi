var debug = require('debug')('iotbi.config');
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

exports.BROKER_ORION_LD=BROKER_ORION_LD;
exports.BROKER_SCORPIO=BROKER_SCORPIO;
exports.BROKER_STELLIO=BROKER_STELLIO;



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
exports.isBrokerServerServerOk = function(pFiwareService) {
   return  config.broker.servers[pFiwareService]!=null;
}
exports.getNgsiLdEntityContext = function(pEntityType) {
   return config.contexts[pEntityType];
}
exports.getBrokerTenant = function(pFiwareService) {
   return config.broker.servers[pFiwareService].tenant;
}

// Exported functions
exports.isOrionLD = function(pFiwareService) {
   return this.getBrokerNGSIVersion(pFiwareService)==VERSION_NGSI_LD_V1;
}
exports.isOrionV1 = function(pFiwareService) {
   return this.getBrokerNGSIVersion(pFiwareService)==VERSION_NGSI_V1;
}
exports.isOrionV2 = function(pFiwareService) {
   return this.getBrokerNGSIVersion(pFiwareService)==VERSION_NGSI_V2;
}


exports.getOrionEndpoint = function(pFiwareService) {
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
