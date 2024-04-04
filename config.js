var config = {};
config.security={enabled:true,appKeys:{}}
config.security.appKeys['segredo']={name:'Public Key',limitDay:100,scopes:['owm_v1','test_ld']};
config.security.appKeys['32a75e5ee9290de53af0b0e55eceea8de88125020b0889e3fa600e956f7r654s']={name:'RHP Key',limitDay:1000,scopes:['owm_v1','cmm_pcp','test_ld']};
config.security.appKeys['37ceec9e755d3ad9f5d4fea8eb0cde8453af0b0e55eceea8de88125020b0884a']={name:'Pedro Pimenta',limitDay:1000,scopes:['owm_v1','cmm_pcp','test_ld']};
config.logger = {};
config.logger.access={file:'/tmp/access_iotbi.log',format:'combined'}

config.broker = {};
config.broker.servers= {};
config.broker.servers.owm_v1={
		  tenant:'owm_v1',
		  host:'orion.orion_net',port:'1026', https:false,
		  broker:'Orion',ngsi:'v2'
		};
config.broker.servers.orion_test_ld={
                  tenant:'test_ld',
		  host:'52.49.232.90', port:'2026', https:false,
		  ngsi:'ldv1', broker:'OrionLD',
		  ngsildContext:'http://context/ngsi-context.jsonld',
		  jsonldContext:'http://context/json-context.jsonld'
                };
config.broker.servers.scorpio_test_ld={
                  tenant:'urn:ngsi-ld:test_ld',
		  host:'172.19.0.6',port:'9090',https:false,
                  ngsi:'ldv1',broker:'Scorpio',
		  ngsildContext:'http://context/ngsi-context.jsonld',
		  jsonldContext:'http://context/json-context.jsonld'
                };
config.broker.servers.stellio_default={
                  tenant:'urn:ngsi-ld:tenant:default',
                  host:'stellio.stellio_net',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
		  ngsildContext:'http://context/ngsi-context.jsonld',
		  jsonldContext:'http://context/json-context.jsonld'
                };
config.broker.servers.stellio_test_ld={
                  tenant:'urn:ngsi-ld:tenant:test_ld',
                  host:'stellio.stellio_net',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld'
                };
config.broker.servers.stellio_owm_ld={
                  tenant:'urn:ngsi-ld:tenant:owm_ld',
                  host:'stellio.stellio_net',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld'
                };
config.broker.servers.cmm_pcp={
                  tenant:'urn:ngsi-ld:tenant:pcp',
                  host:'stellio.stellio_net',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld'
                };
config.broker.servers.portodigital={
                  tenant:'',
                  host:'broker.fiware.urbanplatform.portodigital.pt',port:'443', https:true,
                  broker:'Orion',ngsi:'v2'
                };

config.quantumleap = {};
config.quantumleap.servers= {};
config.quantumleap.servers.owm_v1={host:'quantumleap.orion_net',port:'8668',https:false};
config.quantumleap.servers.test_ld={host:'52.49.232.90',port:'8668',https:false};
config.quantumleap.servers.portodigital={}

config.contexts = {};
config.contexts.PointOfInterest='https://raw.githubusercontent.com/smart-data-models/dataModel.PointOfInterest/master/context.jsonld';
config.contexts.AirQualityObserved='https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/context.jsonld';

exports.config=config;
