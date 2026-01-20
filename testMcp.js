// Run:
// clear; DEBUG=iotbi.parallelize,iotbi.aaa,iotbi.broker MCP_APPKEY=rhp  nodejs testMcp.js

var configsys = require('./lib/configsys.js');
var debug = require('debug')('iotbi.aaa');
var ngsildcontext = require('./lib/ngsildcontext.js');


function getAllowedServices()
{
     var lUser=configsys.getMcpAppKey();
     var lAliasArray=configsys.getBrokersAliasList();
     var lServices=[];
     for(var i in lAliasArray)
     {
         if(configsys.allowScopeToUser(lUser,lAliasArray[i]) && configsys.isConfigOk(lAliasArray[i]) && configsys.isVersionLDV1(lAliasArray[i]))
         {
             lServices.push(lAliasArray[i]);
         }
     }
     debug('User: '+lUser+' Allowed Services: '+JSON.stringify(lServices));
     return lServices
}


function showData()
{
var BrokerParallelize=require('./lib/brokerParallelize.js');
var lEntity;
var lAttribs=undefined;
var lQuery=undefined;

//lEntity='AirQualityObserved';
//lAttribs='refPointOfInterest,temperature,streetAddress,windSpeed,relativeHumidity';
//lQuery='temperature>=13.65;temperature<=13.7;relativeHumidity>=88';
//lQuery='(temperature>=9.65;temperature<=9.66) | (relativeHumidity<=0.8)'
lQuery='dateCreated~="conocida por su ar"|description~="conocida por su ar"'

//lEntity='TemperatureSensor';
//lAttribs="mcc,serialNumber";

//lEntity='Building';

lEntity='PointOfInterest';

//lEntity='FillingLevelSensor';
//lAttribs='id,type,description';


var lBrokerParallel=new BrokerParallelize(getAllowedServices())
var lValidQuery=lBrokerParallel.setQuery(lQuery,lEntity);
lBrokerParallel.setTextPattern('conocida por su arqui');
//var lValidGeoQuery=lBrokerParallel.setGeoQuery("near;maxDistance==10","Point","[-8.624887029888889, 41.15198003988889]","location",lEntity);
//var lValidGeoQuery=lBrokerParallel.setGeoQuery("within","Polygon","[[[-8.7, 41.1], [-8.7, 41.3], [-8.5, 41.3], [-8.5, 41.1], [-8.7, 41.1]]]","location",lEntity);

if(lValidQuery!=undefined)
{
    debug(lValidQuery);
    return;
}
lBrokerParallel.getCurrentData(lEntity,lAttribs,undefined)
        .then((lTable) => {
           debug('were loaded');
           var lJson=JSON.stringify(lTable,null,2);
           console.log(lJson); 
           console.log('Rows: '+lTable.countRows());
        })
        .catch((err) => {
           debug(err);
        });
}


function showGeoMetadata()
{
var BrokerParallelize=require('./lib/brokerParallelize.js');
var lEntity;
var lAttribs;
lEntity='AirQualityObserved';
lAttribs='refPointOfInterest,temperature,streetAddress,windSpeed';

//lEntity='TemperatureSensor';
//lAttribs="mcc,serialNumber";

//lEntity='Building';

lEntity='PointOfInterest';
var lBrokerParallel=new BrokerParallelize(getAllowedServices())

lBrokerParallel.getGeoMetadata(lEntity,20)
        .then((lResult) => {
           debug('were loaded');
           var lJson=JSON.stringify(lResult,null,2);
           console.log(lJson); 
        })
        .catch((err) => {
           debug(err);
        });
}



ngsildcontext.createPromisesloadNgsiContext(configsys.getBrokersAliasList(),configsys.getKnownEntityTypes())
        .then(() => {
           debug('All schemas were loaded');


           //var graph = require('./lib/graphbuilder.js');
           //var lJson=JSON.stringify(graph.getGraph('Teste').toZ(),null,2);
           //console.log(lJson); 

          showData();

          //showGeoMetadata();

        })
        .catch((err) => {
           debug(err);
        });
