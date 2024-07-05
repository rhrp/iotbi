/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the context and schemas loading management subsystem.
 * It will be renamed as it is very tight to context management.
 */
var configsys = require('./configsys.js');
var request = require('request');
var schema = require('./schema.js');
var sdm = require('./smartdatamodels.js');
var debug = require('debug')('iotbi.ngsildcontexts');


/**
 * TODO: separate the Loading process from NGSI-LD Contexts and Models management
 */
exports.createPromisesloadNgsiContext = createPromisesloadNgsiContext;
exports.cachedContexts =cachedContexts;
exports.getTypeOf = getTypeOf;
exports.findEntityTypeByContext = findEntityTypeByContext;
exports.getNgsiLdEntityContext = getNgsiLdEntityContext;
exports.getNgsiLdPropertyContext = getNgsiLdPropertyContext;

//Collection of @context by its URL
var gNgsiContext={};
//Colection of known namespaces refered in @contexts
var gNameSpaces=undefined;
//For cache management
var gNgsiCacheCount={};
var gNgsiCacheTime={};
var gNgsiCacheLast={};

/**
 * Returns the list of cached contexts
 */
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
 * Creates a set of Promises for loading Contexts and Schemas
 * @param pUrlContext,pUrlSchema - If defined, loads to cache the context for that pEntityTypes
 */
async function createPromisesloadNgsiContext(pFiwareService,pEntityTypes)
{
//TODO: caso nao haja definicao para uma da Entities, o carregamento baralha-se 

   var lPromises=[];
   var lUrls=[];
   var lUrlsSchemas={};

   // Context defined for this Service
   var lUrlsNgsiLd=getNgsiLdContexts(pFiwareService);
   for(var lUrl of lUrlsNgsiLd)
   {
      if(needLoading(lUrl))
      {
        debug('Create promise of loading Context: '+lUrl);
        lPromises.push(createPromiseLoadNGSIContextObject(lUrl));
        lUrls.push(lUrl);
      }
   }
   // Common Schemas for this Service
   var lUrlsCommonSchemas=configsys.getBrokerCommonsSchemas(pFiwareService); 
   for(var lUrl of lUrlsCommonSchemas)
   {
      if(sdm.needLoadingCommonSchema(lUrl))
      {
         debug('Create promise of loading Common Schema: '+lUrl);
         lPromises.push(createPromiseLoadNGSIContextObject(lUrl));
         lUrls.push(lUrl);
      }
   }
   // For this entity
   debug('Entity Type: '+JSON.stringify(pEntityTypes));
   if(pEntityTypes!=undefined)
   {
      for(var lEntityType of pEntityTypes)
      { 
        debug('Start loading Context and Schema for '+lEntityType);
        var lCtxUrl=configsys.getNgsiLdEntityContext(lEntityType);
        if(lCtxUrl!=undefined)
        {
           lUrlsNgsiLd.push(lCtxUrl);
           if(needLoading(lCtxUrl))
           {
             if(lUrls.includes(lCtxUrl))
             {
                debug('Requires '+lEntityType+'\'s Context:'+lCtxUrl+', but it is already marked to be loaded'); 
             }
             else
             {
                debug('Requires '+lEntityType+'\'s Context:'+lCtxUrl);
                lPromises.push(createPromiseLoadNGSIContextObject(lCtxUrl));
                lUrls.push(lCtxUrl);
             }
           }
        }
        if(sdm.needLoadingSchema(lEntityType))
        {
           var lSchemaUrls=configsys.getNgsiLdEntitySchema(lEntityType);
           if(lSchemaUrls!=undefined)
           {
              lUrlsSchemas[lEntityType]=[];
              for(var lSchemaUrl of lSchemaUrls)
              {
                 //debug('Requires '+lEntityType+'\'s Schema:'+lSchemaUrl);
                 lUrlsSchemas[lEntityType].push(lSchemaUrl);
                 lPromises.push(createPromiseLoadNGSIContextObject(lSchemaUrl));
                 lUrls.push(lSchemaUrl);
              }
           }
           else
           {
             debug('The '+lEntityType+'\'s Schema is missing in the config file');
           }
        }
        else
        {
           debug('Schema of '+lEntityType+' is cached');  
        }
      }
   }
   //debug('lUrls='+JSON.stringify(lUrls));
   //debug('lUrlsNgsiLd='+JSON.stringify(lUrlsNgsiLd));
   //debug('lUrlsSchemas='+JSON.stringify(lUrlsSchemas)); 
   //debug('lUrlsCommonSchemas='+JSON.stringify(lUrlsCommonSchemas));

   return Promise.all(lPromises).then(function(values) {
        //console.log('Promise.all:'+values.length);
        for(var i=0;i<values.length;i++)
        {
            var lUrl=values[i].url;
            if(lUrlsNgsiLd.includes(lUrl))
            {
              var lContext=values[i].value;
              gNgsiContext[lUrl]=lContext;
              debug('Load Context: '+lUrl);
              //debug('Context:\n'+JSON.stringify(lContext,null,2));
            }
            else if(lUrlsCommonSchemas.includes(lUrl))
            {
              var lSchema=values[i].value;
              sdm.addCommonSchema(lUrl,lSchema);
              //debug('Load Common Schema of '+pFiwareService+': '+lUrl);
            }
            else if(pEntityTypes!=undefined)
            {
              for(var lEntityType of pEntityTypes)
              {
                 //debug(lEntityType+'\'s Schemas:'+JSON.stringify(lUrlsSchemas[lEntityType]));
                 if(lUrlsSchemas[lEntityType]!=undefined && lUrlsSchemas[lEntityType].includes(lUrl))
                 {
                      var lSchema=values[i].value;
                      sdm.addSchema(lEntityType,lUrl,lSchema);
                      //debug('Loaded Schema of '+lEntityType+': '+lUrl);
                      //debug('Schema:\n'+JSON.stringify(lSchema,null,2));
                 }
              }  
            }
            else
            {
              debug('Unexpected case!');
            }
        }
        debug('Schemas:\n'+JSON.stringify(sdm.loadedSchemas(),null,2));
        gNameSpaces=mapNameSpaces();
        debug('Namespaces:\n'+JSON.stringify(gNameSpaces,null,2));
        return [gNgsiContext,sdm];
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
                        var lRet={};
                        lRet['url']=pUrl;
                        lRet['value']=JSON.parse(pBody)
                        return resolve(lRet);
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

//deprecated
function getTypeOf(pEntityType,pPropName)
{
   return sdm.getTypeOf(pEntityType,pPropName);
}

/**
 * Get the @context from Config
 * This module is prepared for multiple @context.
 * Thus, the config is converted to an array
 */
function getNgsiLdContexts(pFiwareService)
{
   var lNgsiContext=configsys.getBrokerNgsiLdContext(pFiwareService);
   var lOut=[lNgsiContext,'https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.7.jsonld'];
   debug("NGSI Contexts:"+lOut);
   return lOut;
}
/**
 * Returns the Context object of an entity type
 */
function getNgsiLdEntityContext(pEntityType)
{
   var lUrlContext=configsys.getNgsiLdEntityContext(pEntityType);
   if(lUrlContext==undefined)
   { 
     return undefined; 
   }
   else
   {
     return gNgsiContext[lUrlContext];
   }
}
/**
 * Get the @context defintion of a Entity's porperty
 */
function getNgsiLdPropertyContext(pEntityType,pProperty)
{
   var lCtx=getNgsiLdEntityContext(pEntityType);
   if(lCtx==undefined)
      return undefined;
   lCtx=lCtx['@context'];
   if(lCtx==undefined)
      return undefined;
   return lCtx[pProperty];
}

function mapNameSpaces()
{
   var lNameSpaces={};
   // First step: find the existing NameSpaces
   //debug(JSON.stringify(gNgsiContext,null,2));
   for(var lKey in gNgsiContext)
   {
       var lContext=gNgsiContext[lKey];
       if(lContext['@context']!=undefined)
       {
           for(var lType in lContext['@context'])
           {
              var lUrl=lContext['@context'][lType];
              var lIsString=typeof lUrl === 'string';
              if(lIsString && !lUrl.startsWith('http://') && !lUrl.startsWith('https://'))
              {
                    //NameSpace
                    var lUrlParts=lUrl.split(':');
                    if(lUrlParts.length==2)
                    {
                        //One namespace found. Next, we will find its URL
                        lNameSpaces[lUrlParts[0]]='Unknown URL';
                    }
              }
           }
       }
   }
   // Second Step: Find the url of the Namespace
   for(var lKey in gNgsiContext)
   {
       var lContext=gNgsiContext[lKey];
       if(lContext['@context']!=undefined)
       {
           for(var lType in lContext['@context'])
           {
              if(lType!=undefined && lNameSpaces[lType]!=undefined)
              {
                var lUrl=lContext['@context'][lType];
                var lIsString=typeof lUrl === 'string';
                //TODO: it is required?     && lUrl.endsWith('#')
                if(lIsString && (lUrl.startsWith('http://') || lUrl.startsWith('https://')))
                {
                    //NameSpace
                    lNameSpaces[lType]=lUrl;
                }
              }
           }
       }
   }
   return lNameSpaces;
}

/**
 * Find the Entity Type that used such context URL
 */
function findEntityTypeByContext(pUrlContext)
{
   //debug(JSON.stringify(gNgsiContext,null,2));
   for(var lKey in gNgsiContext)
   {
       var lContext=gNgsiContext[lKey];
       if(lContext['@context']!=undefined)
       {
           for(var lType in lContext['@context'])
           {
              var lUrl=lContext['@context'][lType];
              var lIsString=typeof lUrl === 'string';
              if(lIsString)
              {
                 var lUrlToCompare=lUrl;
                 if(!lUrl.startsWith('http://') && !lUrl.startsWith('https://'))
                 {
                    //The Type is defined using a namespace
                    var lNamespace=lUrl.split(':')[0];
                    var lUrlNameSpace=gNameSpaces[lNamespace];
                    if(lUrlNameSpace!=undefined)
                    {
                        lUrlToCompare=lUrlNameSpace+lType;
                    }
                 }
                 var lCompare=lUrlToCompare==pUrlContext;
                 if(lCompare)
                 {
                    return lType;
                 }
              }
           }
       }
   }
   return undefined;
}
