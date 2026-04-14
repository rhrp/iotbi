var config = {};
/**********************************************************
 * Authentication and security
 **********************************************************/
config.security={enabled:true,appKeys:{}}
config.security.appKeys['segredo']={
                                    name:'Public Key',
                                    limitDay:1000,
                                    scopes:['owm_v1','orionld_test_1','stellio_owm_ld','stellio_test_1','scorpio_test_1','portodigital','madrid','porto','system'],
                                    allowAPI:true,
                                    allowHadoop:false,
                                    allowMcp:false,
                                    hiddenEntities:{}
                                   };
config.security.appKeys['estagio']={
                                    name:'Estagio Key',
                                    limitDay:100,
                                    scopes:['owm_v1','stellio_owm_ld','stellio_test_1','portodigital','madrid','porto','system'],
                                    allowAPI:true,
                                    allowHadoop:false,
                                    allowMcp:false,
                                    hiddenEntities:{}
                                   };
config.security.appKeys['benchmark']={
                                    name:'Benchmark user',
                                    limitDay:1000000,
                                    scopes:['stellio_test_1','scorpio_test_1','orionld_test_1'],
                                    _scopes:['scorpio_test_1'],
                                    allowAPI:true,
                                    allowHadoop:false,
                                    allowMcp:false,
                                    hiddenEntities:{}
                                   };
config.security.appKeys['hadoop']={
                                    name:'Hadoop user',
                                    limitDay:100,
                                    scopes:['owm_v1','stellio_owm_ld','stellio_test_1','portodigital','madrid','porto','system'],
                                    allowAPI:false,
                                    allowHadoop:true,
                                    allowMcp:false,
                                    hiddenEntities:{'portodigital':['Alert','Event']}
                                  };
config.security.appKeys['rhp']={
                                    name:'RHP user',
                                    limitDay:1000,
                                    scopes:['owm_v1','stellio_owm_ld','stellio_test_1','portodigital','madrid','porto','system'],
                                    allowAPI:true,
                                    allowHadoop:true,
                                    allowMcp:true,
                                    hiddenEntities:{'portodigital':['Alert']}
                               };
//60000 (60s)
/**********************************************************
 * Cache
 **********************************************************/
config.cache={enabled:true,ttlms:3600000,localPath:'/tmp/iotbi/cache'};

/**********************************************************
 * Logger
 **********************************************************/
config.logger = {};
config.logger.access={file:'/tmp/access_iotbi.log',format:'combined'}

/**********************************************************
 * Brokers
 **********************************************************/
config.broker = {};
config.broker.servers= {};
config.broker.servers.owm_v1={
		  tenant:'owm_v1',
		  host:'orion-broker',port:'1026', https:false,
		  broker:'Orion',ngsi:'v2'
		};
config.broker.servers.orionld_test_1={
                  tenant:'test_1',
                  host:'orionld-broker',port:'1026',https:false,
                  ngsi:'ldv1',broker:'OrionLD',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json'],
                };
config.broker.servers.scorpio_test_1={
                  tenant:'urn:ngsi-ld:test_1',
                  host:'scorpio-broker',port:'9090',https:false,
                  ngsi:'ldv1',broker:'Scorpio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json'],
                };
config.broker.servers.stellio_default={
                  tenant:'urn:ngsi-ld:tenant:default',
                  host:'stellio-api-gateway',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
		  ngsildContext:'http://context/ngsi-context.jsonld',
		  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json']
                };
config.broker.servers.stellio_test_1={
                  tenant:'urn:ngsi-ld:tenant:test_1',
                  host:'stellio-api-gateway',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json',
                                'https://smart-data-models.github.io/dataModel.Device/device-schema.json',
                                'https://smart-data-models.github.io/dataModel.Environment/Environment-schema.json',
                                'https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/Environment-schema.json']

                };
config.broker.servers.madrid={
                  tenant:'urn:ngsi-ld:tenant:cy_madrid',
                  host:'stellio-api-gateway',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json',
                                'https://smart-data-models.github.io/dataModel.Device/device-schema.json',
                                'https://smart-data-models.github.io/dataModel.Environment/Environment-schema.json',
                                'https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/Environment-schema.json']

                };
config.broker.servers.porto={
                  tenant:'urn:ngsi-ld:tenant:cy_porto',
                  host:'stellio-api-gateway',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json',
                                'https://smart-data-models.github.io/dataModel.Device/device-schema.json',
                                'https://smart-data-models.github.io/dataModel.Environment/Environment-schema.json',
                                'https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/Environment-schema.json']

                };

config.broker.servers.stellio_test_ld={
                  tenant:'urn:ngsi-ld:tenant:test_ld',
                  host:'stellio-api-gateway',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json']
                };

config.broker.servers.stellio_owm_ld={
                  tenant:'urn:ngsi-ld:tenant:owm_ld',
                  host:'stellio-api-gateway',port:'8080',https:false,
                  ngsi:'ldv1',broker:'Stellio',
                  ngsildContext:'http://context/ngsi-context.jsonld',
                  jsonldContext:'http://context/json-context.jsonld',
                  schemaCommon:['https://smart-data-models.github.io/data-models/common-schema.json',
                                'https://smart-data-models.github.io/dataModel.Device/device-schema.json',
                                'https://smart-data-models.github.io/dataModel.Environment/Environment-schema.json',
                                'https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/Environment-schema.json']
                };
config.broker.servers.portodigital={
                  tenant:'',
                  host:'broker.fiware.urbanplatform.portodigital.pt',port:'443', https:true,
                  broker:'Orion',ngsi:'v2'
                };

config.quantumleap = {};
config.quantumleap.servers= {};
config.quantumleap.servers.owm_v1={host:'orion-quantumleap',port:'8668',https:false};
config.quantumleap.servers.test_ld={host:'52.49.232.90',port:'8668',https:false};
config.quantumleap.servers.portodigital={};
config.quantumleap.servers.orionld_test_1={};
//http://context/smart-data-models/dataModel.PointOfInterest/PointOfInterest/context.jsonld
//https://raw.githubusercontent.com/smart-data-models/dataModel.PointOfInterest/master/context.jsonld

//http://context/smart-data-models/dataModel.Environment/context.jsonld
//https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/context.jsonld


//http://context/smart-data-models/dataModel.Building/context.jsonld
//https://raw.githubusercontent.com/smart-data-models/dataModel.Building/refs/heads/master/context.jsonld

/**********************************************************
 * Entity types
 **********************************************************/
config.entityTypes = {};
config.entityTypes.PointOfInterest={
			             description:'This entity contains a harmonised geographic description of a Point of Interest.',
                                     urlSchema:['https://smart-data-models.github.io/dataModel.PointOfInterest/PointOfInterest/schema.json'],
                                     urlContext:'http://context/smart-data-models/dataModel.PointOfInterest/PointOfInterest/context.jsonld'
			           }
config.entityTypes.AirQualityObserved={
                                         description:'An observation of air quality conditions at a certain place and time.',
                                         urlSchema:['http://context/smart-data-models/dataModel.Environment/AirQualityObserved/schema.json'],
                                         urlContext:'http://context/smart-data-models/dataModel.Environment/context.jsonld'
                                      }
config.entityTypes.Building={
                              description:'Information on a given Building',
                              urlSchema:['https://smart-data-models.github.io/dataModel.Building/Building/schema.json'],
                              __urlContext:'http://context/ngsi-context.jsonld',
                              urlContext:'http://context/smart-data-models/dataModel.Building/context.jsonld'
                            }
config.entityTypes.TemperatureSensor={
                                       description:'Sensor of temperature',
                                       urlSchema:['http://context/smart-data-models/rhp/TemperatureSensor.json','https://smart-data-models.github.io/dataModel.Device/Device/schema.json'],
                                       urlContext:'http://context/ngsi-context.jsonld'
                                     }
config.entityTypes.FillingLevelSensor={
                                         description:'Sensor of Filling Level',
                                         urlSchema:['http://context/smart-data-models/rhp/FillingLevelSensor.json','https://smart-data-models.github.io/dataModel.Device/Device/schema.json'],
                                         urlContext:'http://context/ngsi-context.jsonld'
                                      }
/**********************************************************
 * Strategy for obtain similarity
 *  'fastFuzzy' - Based on fast-fuzzy
 *  'huggingfaceSentence' - Based on Huggingface Transformer Sentence 
 *
 **********************************************************/
config.transformer={};
config.transformer.similariy={default:'huggingfaceSentence'};
//config.transformer.similariy={default:'fastFuzzy'};
/**********************************************************
 * Export config
 **********************************************************/
exports.config=config;
