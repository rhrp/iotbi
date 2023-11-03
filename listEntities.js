/**
 * This app intends to show a possible bug in Scorpio, NGSIJS, nodejs...
 * It is a strange problem in which the atributes name and category are returned as 'name' | 'category' and, in some cases, 'ngsi-ld:name' | 'fiware:category'
 */
var request = require('request');
var ngsildv1 = require('./lib/ngsildv1.js');
var { exec } = require("child_process");

function createPromiseRequest()
{
   return new Promise(function(resolve, reject)
   {
      var lOptions={
        "url": "http://172.19.0.6:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Building:barn002",
        "headers": {
        "NGSILD-Tenant": "urn:ngsi-ld:test_ld",
        "Link": "<http://context/ngsi-context.jsonld>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\"",
        "Accept": "application/ld+json"
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
  createPromiseRequest().then((result) => console.log('Using request: '+JSON.stringify(result,null,2)));
  
  //Using NGSIJS Lib
  // Returns ngsi-ld:name and fiware:category
  ngsildv1.createPromiseQueryEntities('urn:ngsi-ld:test_ld',null,null,'urn:ngsi-ld:Building:barn002')
          .then((resultEntities) => console.log('Using Lib (module ngsildv1):'+JSON.stringify(resultEntities,null,2)));


  //Using NGSIJS Lib
  // Returns name and category
  var NGSI = require('ngsijs');
  var lConnection = new NGSI.Connection('http://172.19.0.6:9090');
  lConnection.ld.queryEntities({"tenant":'urn:ngsi-ld:test_ld',"@context":'http://context/ngsi-context.jsonld',"id":'urn:ngsi-ld:Building:barn002',"limit":1000,"offset": 0})
          .then((result) => console.log('Using Lib: '+JSON.stringify(result,null,2)));




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


  /*
curl -G -X GET "http://172.19.0.6:9090/ngsi-ld/v1/entities/urn:ngsi-ld:Building:barn002" \
-H 'NGSILD-Tenant: urn:ngsi-ld:test_ld' \
-H 'Accept: application/ld+json' \
-H 'Link: <http://context/json-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"' 
   */
