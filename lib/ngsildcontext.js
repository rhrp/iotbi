var apiConfig = require('./apiConfig.js');
var request = require('request');


exports.createPromisesloadNgsiContext = createPromisesloadNgsiContext;


async function createPromisesloadNgsiContext(pFiwareService)
{
   var lNgsiContext={};
   var lPromises=[];
   var lUrls=getNgsiLdContexts(pFiwareService);
   for(var lUrl of lUrls)
   {
      lPromises.push(createPromiseLoadNGSIContextObject(lUrl));
   }
   return Promise.all(lPromises).then(function(values) {
        //console.log('Promise.all:'+values.length);
        for(var i=0;i<values.length;i++)
        {
            var lUrl=lUrls[i];
            var lContext=values[i];
            lNgsiContext[lUrl]=lContext;
            //console.log(lUrl+'  ===  '+JSON.stringify(lContext));
            //console.log(lUrl+'  ===  '+lContext);
            console.log('Context: '+lUrl);
        }
        return lNgsiContext;
    }).catch(function(err) { 
          //console.log(err);
          //The error is handled in the upper Promise
          throw err;
    });
}


function createPromiseLoadNGSIContextObject(pUrl)
{
   //console.log('URL Context='+pUrl);
   return new Promise(function(resolve, reject)
   {
     var lOptions = {
       url: pUrl,
       headers: {}
     };
     lOptions.headers['Accept']='application/ld+json';

     request(lOptions, function (error, response, pBody) {
        try
        {
                if(error!=null)
                {
                   console.error('error:', error);
                   return reject({'step':'Loading Context','description':error});
                }
                else
                {
                   if(response.statusCode<200 || response.statusCode>299)
                   {
                        console.log('statusCode:', response && response.statusCode);
                        //console.log('Body: '+pBody);
                        return reject({'step':'Loading Context','HTTP Code':response.statusCode});
                   }
                   else
                   {
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


/**
 * Get the @context from Config
 * This module is prepared for multiple @context.
 * Thus, the config is converted to an array
 */
function getNgsiLdContexts(pFiwareService)
{
   var lNgsiContext=apiConfig.getBrokerNgsiLdContext(pFiwareService);
   var lOut=[lNgsiContext,'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld'];
   console.log("NGSI Contexts:"+lOut);
   return lOut;
}
