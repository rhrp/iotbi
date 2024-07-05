var config = {};
config.security={enabled:true,appKeys:{}}
config.security.appKeys['segredo']={name:'Public Key',limitDay:100,scopes:['owm_v1','test_ld','stellio_owm_ld','system'],allowAPI:true,allowHadoop:false};
config.security.appKeys['hadoop']={name:'Hadoop user',limitDay:100,scopes:['owm_v1','test_ld','stellio_owm_ld','portodigital'],allowAPI:false,allowHadoop:true};
config.security.appKeys['rhp']={name:'Hadoop RHP user',limitDay:100,scopes:['owm_v1','test_ld','stellio_owm_ld','portodigital'],allowAPI:false,allowHadoop:true};
config.security.appKeys['32a75e5ee9290de53af0b0e55eceea8de88125020b0889e3fa600e956f7r654s']={name:'RHP Key',limitDay:1000,scopes:['owm_v1','cmm_pcp','test_ld'],allowAPI:true,allowHadoop:true};
config.cache={enabled:true,ttlms:60000,localPath:'/tmp/iotbi/cache'};
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
		  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json'],
                };
config.broker.servers.scorpio_test_ld={
                  tenant:'urn:ngsi-ld:test_ld',
		  host:'172.19.0.6',port:'9090',https:false,
                  ngsi:'ldv1',broker:'Scorpio',
		  ngsildContext:'http://context/ngsi-context.jsonld',
		  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json'],
                };
config.broker.servers.stellio_default={
                  tenant:'urn:ngsi-ld:tenant:default',
                  host:'stellio.stellio_net',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
		  ngsildContext:'http://context/ngsi-context.jsonld',
		  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json']
                };
config.broker.servers.stellio_test_ld={
                  tenant:'urn:ngsi-ld:tenant:test_ld',
                  host:'stellio.stellio_net',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json']
                };
config.broker.servers.stellio_owm_ld={
                  tenant:'urn:ngsi-ld:tenant:owm_ld',
                  host:'stellio.stellio_net',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json',
                                'https://smart-data-models.github.io/dataModel.Device/device-schema.json',
                                'https://smart-data-models.github.io/dataModel.Environment/Environment-schema.json',
                                'https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/Environment-schema.json']
                };
config.broker.servers.cmm_pcp={
                  tenant:'urn:ngsi-ld:tenant:pcp',
                  host:'stellio.stellio_net',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json']
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
config.contexts.Building='http://context/ngsi-context.jsonld';
config.contexts.TemperatureSensor='http://context/ngsi-context.jsonld';
config.contexts.FillingLevelSensor='http://context/ngsi-context.jsonld';

config.schemas = {};
//Bug typeOfLocation? https://smart-data-models.github.io/dataModel.Environment/AirQualityObserved/schema.json
config.schemas.AirQualityObserved=['http://context/smart-data-models/dataModel.Environment/AirQualityObserved/schema.json'];
config.schemas.PointOfInterest=['https://raw.githubusercontent.com/smart-data-models/dataModel.PointOfInterest/master/PointOfInterest/schema.json'];
config.schemas.Building=['https://smart-data-models.github.io/dataModel.Building/Building/schema.json'];
config.schemas.TemperatureSensor=['http://context/smart-data-models/rhp/TemperatureSensor.json',
                                  'https://smart-data-models.github.io/dataModel.Device/Device/schema.json'];
config.schemas.FillingLevelSensor=['http://context/smart-data-models/rhp/FillingLevelSensor.json',
                                  'https://smart-data-models.github.io/dataModel.Device/Device/schema.json'];
exports.config=config;
