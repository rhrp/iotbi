
var config = {};
config.security={enabled:true,appKeys:{}}
config.security.appKeys['segredo']={name:'Public Key',limitDay:100,scopes:['owm_v1','test_ld']};
config.security.appKeys['32a75e5ee9290de53af0b0e55eceea8de88125020b0889e3fa600e956f7r654s']={name:'RHP Key',limitDay:1000,scopes:['owm_v1','test_ld']};

config.logger = {};
config.logger.access={file:'/tmp/access_pbim.log',format:'combined'}

config.orion = {};
config.orion.servers= {};
config.orion.servers.owm_v1={host:'127.0.0.1',port:'1026',https:false,broker:'Orion',ngsi:'v2'};
config.orion.servers.test_ld={host:'127.0.0.1',port:'2026',https:false,ngsi:'ldv1',broker:'OrionLD',ngsildContext:'http://context/ngsi-context.jsonld',jsonldContext:'http://context/json-context.jsonld'};
config.orion.servers['urn:ngsi-ld:test_ld']={host:'172.22.0.6',port:'9090',https:false,ngsi:'ldv1',broker:'Scorpio',ngsildContext:'http://context/ngsi-context.jsonld',jsonldContext:'http://context/json-context.jsonld'};
// 'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.3.jsonld'
// 'http://context/ngsi-context.jsonld'

config.quantumleap = {};
config.quantumleap.servers= {};
config.quantumleap.servers.owm_v1={host:'127.0.0.1',port:'8668',https:false};
config.quantumleap.servers.test_ld={host:'127.0.0.1',port:'8668',https:false};

// Exported configs
exports.security=config.security;
exports.logger=config.logger;
exports.orion=config.orion;
exports.quantumleap=config.quantumleap;


// Constants
const VERSION_NGSI_V1 = 'v1';
const VERSION_NGSI_V2 = 'v2';
const VERSION_NGSI_LD_V1 = 'ldv1';

const BROKER_ORION	= 'Orion';
const BROKER_ORION_LD   = 'OrionLD';
const BROKER_SCORPIO    = 'Scorpio';

exports.isBrokerOrion = function(pFiwareService) {
	return this.getOrionServerOk(pFiwareService) && config.orion.servers[pFiwareService].broker.toLowerCase()===BROKER_ORION.toLowerCase();
}
exports.isBrokerOrionLD = function(pFiwareService) {
        return this.getOrionServerOk(pFiwareService) && config.orion.servers[pFiwareService].broker.toLowerCase()===BROKER_ORION_LD.toLowerCase();
}
exports.isBrokerScorpio = function(pFiwareService) {
        return this.getOrionServerOk(pFiwareService) && config.orion.servers[pFiwareService].broker.toLowerCase()===BROKER_SCORPIO.toLowerCase();
}

/**
 * Returns true if the broker(s) is(are) weel defined for a fiware-service
 */
exports.isConfigOk = function(pFiwareService) {
	return this.isBrokerServerOk(pFiwareService)
            && (this.isBrokerSuportsTemporalAPI(pFiwareService) || getQuantumLeapServerOk(pFiwareService));
}


/**
 * Return True if the broker suports the NGSI temporal API.
 * If not, we must use  QuantumLeap
 */
exports.isBrokerSuportsTemporalAPI = function(pFiwareService) {
        return this.isBrokerScorpio(pFiwareService)
}

// Broker function
exports.isBrokerServerOk = function(pFiwareService) {
        return  config.orion.servers[pFiwareService]!=null;
}
exports.isBrokerHttpS = function(pFiwareService) {
   return config.orion.servers[pFiwareService].https;
}
exports.getBrokerHost = function(pFiwareService) {
   return config.orion.servers[pFiwareService].host;
}
exports.getBrokerPort = function(pFiwareService) {
   return config.orion.servers[pFiwareService].port;
}
exports.getBrokernNGSIVersion = function(pFiwareService) {
   return config.orion.servers[pFiwareService].ngsi;
}
exports.getBrokerNgsiLdContext = function(pFiwareService) {
   return config.orion.servers[pFiwareService].ngsildContext;
}
exports.getBrokerJsonLdContext = function(pFiwareService) {
   return config.orion.servers[pFiwareService].ngsildContexts;
}


// Exported functions
//Orion (DEPRECATED)
exports.getOrionServerOk = function(pFiwareService) {
	return  config.orion.servers[pFiwareService]!=null;
}
exports.getOrionHttpS = function(pFiwareService) {
   return config.orion.servers[pFiwareService].https;
}
exports.getOrionHost = function(pFiwareService) {
   return config.orion.servers[pFiwareService].host;
}
exports.getOrionPort = function(pFiwareService) {
   return config.orion.servers[pFiwareService].port;
}
exports.getOrionNGSIVersion = function(pFiwareService) {
   return config.orion.servers[pFiwareService].ngsi;
}
exports.getOrionNgsiLdContext = function(pFiwareService) {
   return config.orion.servers[pFiwareService].ngsildContext;
}
exports.getOrionJsonLdContext = function(pFiwareService) {
   return config.orion.servers[pFiwareService].ngsildContexts;
}
exports.isOrionLD = function(pFiwareService) {
   return this.getOrionNGSIVersion(pFiwareService)==VERSION_NGSI_LD_V1;
}
exports.isOrionV1 = function(pFiwareService) {
   return this.getOrionNGSIVersion(pFiwareService)==VERSION_NGSI_V1;
}
exports.isOrionV2 = function(pFiwareService) {
   return this.getOrionNGSIVersion(pFiwareService)==VERSION_NGSI_V2;
}


exports.getOrionEndpoint = function(pFiwareService) {
   var lUrl=this.getOrionURL(pFiwareService);
   var lVersion=this.getOrionNGSIVersion(pFiwareService);
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
      console.log('Invalide NGSI version '+lVersion);
      return null;
   }
}
exports.getOrionURL = function(pFiwareService) {
   var lServerHttpSecured=this.getOrionHttpS(pFiwareService);
   var lServer=this.getOrionHost(pFiwareService);
   var lPort=this.getOrionPort(pFiwareService);
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
