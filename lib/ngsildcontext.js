var apiConfig = require('./apiConfig.js');
var request = require('request');
var debug = require('debug')('iotbi.ngsildcontexts');

exports.createPromisesloadNgsiContext = createPromisesloadNgsiContext;
exports.cachedContexts =cachedContexts;

var gNgsiContext={};
var gNgsiCacheCount={};
var gNgsiCacheTime={};
var gNgsiCacheLast={};

function cachedContexts()
{
  var lOut=[];
  for (const [lKey,lContextData] of Object.entries(gNgsiContext)) 
  {
    lOut.push({
		'context':lKey,
		'firstAcess':gNgsiCacheTime[lKey],
		'lastAccess':gNgsiCacheLast[lKey],
                'count':gNgsiCacheCount[lKey]
		});
  }
  return lOut;
}


/**
 *
 */
function needLoading(pUrl)
{
  if(pUrl==undefined)
  {
     return false;
  }
  else
  {
     var lNow=new Date();
     var lCount=gNgsiCacheCount[pUrl];
     lCount=gNgsiCacheCount[pUrl]=lCount==undefined?1:lCount+1
     gNgsiCacheLast[pUrl]=lNow;
     if(gNgsiContext[pUrl]==undefined)
     {  
        gNgsiCacheTime[pUrl]=lNow;        
        debug('Context '+pUrl+' needs to be loaded');
        return true;
     }
     else
     {
        debug('Context '+pUrl+' already in cache');
       return false;
     }
  }
}




/**
 * Creates a set of Promises for loading Contexts
 * TODO: The first invocation fails, the next dont.  
 *       If you stop invoking the service, the problem repeats ate the first invocation
 */
async function createPromisesloadNgsiContext(pFiwareService)
{
   var lNgsiContext={};
   var lPromises=[];
   var lUrls=getNgsiLdContexts(pFiwareService);
   for(var lUrl of lUrls)
   {
      if(needLoading(lUrl))
      {
        debug('Create promise of loading Context: '+lUrl);
        lPromises.push(createPromiseLoadNGSIContextObject(lUrl));
      }
      else
      {
        debug('Context in cache '+lUrl);
        lNgsiContext[lUrl]=gNgsiContext[lUrl];
      }
   }
   return Promise.all(lPromises).then(function(values) {
        //console.log('Promise.all:'+values.length);
        for(var i=0;i<values.length;i++)
        {
            var lUrl=lUrls[i];
            var lContext=values[i];
            gNgsiContext[lUrl]=lContext;
            lNgsiContext[lUrl]=lContext;
            //console.log(lUrl+'  ===  '+JSON.stringify(lContext));
            //console.log(lUrl+'  ===  '+lContext);
            debug('Context: '+lUrl);
        }
        return lNgsiContext;
    }).catch(function(err) { 
          debug('Error while creating the Promises for loading contexts: '+lUrls);
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
                        debug('statusCode:', response && response.statusCode);
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
               debug(ex);
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
   debug("NGSI Contexts:"+lOut);
   return lOut;
}
