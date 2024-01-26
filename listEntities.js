/**
 * This app intends to test all means to connect to a NGSIV2-based broker  
 */
var request = require('request');
var ngsildv1 = require('./lib/ngsildv1.js');
var { exec } = require("child_process");

function createPromiseRequest()
{
   return new Promise(function(resolve, reject)
   {
      var lOptions={
        "url": "http://orion.orion_net:1026/v2/entities/?attrs=temperature",
        "headers": {
        "fiware-service":"owm_v1",
        "Accept": "application/json"
       } 
      };

      request(lOptions, function (error, response, pBody) {

        try
        {
                if(error!=null)
                {
                   console.error('error:', error);
                   return reject({'description':error});
                }
                else
                {
                   //console.log('statusCode:', response && response.statusCode);
                   if(response.statusCode<200 || response.statusCode>299)
                   {
                        //console.log('Body: '+pBody);
                        return reject({'step':'Loading Main Table','Code':response.statusCode});
                   }
                   else
                   {
                        //console.log('Body: '+pBody);
                        return resolve(JSON.parse(pBody));
                   }
                }
        }
        catch(ex)
        {
             console.log(ex);
             return reject(ex);
        }
     });
   });
}




  // Using request
  // Returns name and category
//  createPromiseRequest().then((result) => console.log('Using request: '+JSON.stringify(result,null,2)));

  //
  var ocb = require('ocb-sender')
  var ngsi = require('ngsi-parser');

  var lEndpoint='http://orion.orion_net:1026/v2';
  var lHeaders={"fiware-service":"owm_v1"};
  //var lObjQuery={"id":"AirQualityObserved:S0005","type":"multiSensor","attrs":"temperature,humidity,windSpeed","limit":1000};
  var lObjQuery={"id":"AirQualityObserved:S0005","type":"multiSensor","limit":1000};

  ocb.config(lEndpoint,lHeaders)
         .then((result) => console.log('Config: '+JSON.stringify(result)))
         .catch((err) => console.log('Config:'+err));

  var lOcbQuery = ngsi.createQuery(lObjQuery);
  //The attribs must be here  
  lOcbQuery=lOcbQuery+'&attrs=temperature,humidity';

  console.log('OCB Query: '+lOcbQuery);
  ocb.getWithQuery(lOcbQuery,lHeaders)
       .then((result) => console.log(JSON.stringify(result,null,2)))
       .catch((err) => console.log(err));


  
/*
  //Using NGSIJS Lib
  // Returns ngsi-ld:name and fiware:category
  ngsildv1.createPromiseQueryEntities('urn:ngsi-ld:test_ld',null,null,'urn:ngsi-ld:Building:barn002')
          .then((resultEntities) => console.log('Using Lib (module ngsildv1):'+JSON.stringify(resultEntities,null,2)));

*/

/*
  //Using NGSIJS Lib
  // Returns name and category
  var NGSI = require('ngsijs');
  var lConnection = new NGSI.Connection('http://172.19.0.6:9090');
  lConnection.ld.queryEntities({"tenant":'urn:ngsi-ld:test_ld',"@context":'http://context/ngsi-context.jsonld',"id":'urn:ngsi-ld:Building:barn002',"limit":1000,"offset": 0})
          .then((result) => console.log('Using Lib: '+JSON.stringify(result,null,2)));
*/


/*
  // Using Curl
  // Returns ngsi-ld:name and fiware:category
  var lCommand='curl -G -X GET "http://172.19.0.6:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Building:barn002" -H \'NGSILD-Tenant: urn:ngsi-ld:test_ld\' -H \'Accept: application/ld+json\' -H \'Link: <http://context/json-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"\'';

  exec(lCommand, (error, stdout, stderr) => {
     console.log('---------------------------------------');
     console.log(`stdout: ${stdout}`);
     console.log('---------------------------------------');

     if (error) {
        console.log(`error: ${error.message}`);
        console.log('---------------------------------------');
        return;
     }
     if (stderr) {
        console.log(`stderr: ${stderr}`);
        console.log('---------------------------------------');
        return;
     }
  });
*/

  /*
curl -G -X GET "http://172.19.0.6:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Building:barn002" \
-H 'NGSILD-Tenant: urn:ngsi-ld:test_ld' \
-H 'Accept: application/ld+json' \
-H 'Link: <http://context/json-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"' 
   */
