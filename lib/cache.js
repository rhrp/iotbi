/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the cache subsystem.
 */
var fs=require('fs');
const url  = require('url');
var crypto = require('crypto');
var configsys = require('./configsys.js');
var utils = require('./utils.js');
var formats = require('./formats.js');
var debug = require('debug')('iotbi.cache');

exports.initCache = function()
{
    var lCachePath=configsys.getCacheLocalPath();
    if (!fs.existsSync(lCachePath))
    {
        fs.mkdirSync(lCachePath);
        debug('Cache directory created: '+lCachePath);
    }
    else
    {
        debug('Cache directory exists: '+lCachePath);
    }
}
exports.sendCachedFile = sendCachedFile;
function sendCachedFile(res,pAttachName,pFilename,pType)
{
   debug('Sending '+pAttachName+' of type ' +pType+' in file:'+pFilename);
   res.attachment(pAttachName).type(pType).sendFile(pFilename);
}
exports.genCacheFile = genCacheFile;
function genCacheFile(pCacheKey,pType)
{
   var lLocalPath=configsys.getCacheLocalPath();
   return lLocalPath+'/'+pCacheKey+'.'+pType;
}
/**
 * Generates the key (plain) of a Request in the Cache 
 */
exports.genRequestCacheKeyPlainText = function (req)
{
    var lCachePlainKey=JSON.stringify(req.headers)+req.url;
    debug('Cache Key Plain: '+lCachePlainKey);
    return lCachePlainKey
}
/**
 * Generates the key (plain) of a Current webhdfs Request in the Cache
 */
exports.genCurrentWebHDFSCacheKey = function(pFiwareService,pEntityType,pJoinAttrib,pJoinEntityType)
{
    var lCachePlainKey=pFiwareService+'_'+pEntityType+(pJoinAttrib!=undefined?'_'+pJoinAttrib:'')+(pJoinEntityType!=undefined?'_'+pJoinEntityType:'');
    debug('Cache Key Plain: '+lCachePlainKey);
    return genHash(lCachePlainKey);
}
/**
 * Generates the key of a Request in the Cache 
 */
exports.genCacheKey = function (reqOrPlainString)
{
   var lCachePlainKey;
   if(typeof reqOrPlainString === 'string')
   {
      lCachePlainKey=reqOrString;
   }
   else
   {
      lCachePlainKey=this.genRequestCacheKeyPlainText(reqOrPlainString);
   }
   return genHash(lCachePlainKey);
}
function genHash(pPlainText)
{
   return crypto.createHash('sha256').update(pPlainText).digest('hex');
}
/**
 * Check the cached file validity
 */
exports.isStillValid = isStillValid
function isStillValid(pFiwareService,pFileStats)
{
    var lMtimeMs=pFileStats.mtimeMs;
    var lNow=new Date().getTime();
    var lDelta=lNow-lMtimeMs;
    var lCacheTTL=configsys.getCacheTTL(pFiwareService);
    debug('Output is cached!  Delta time: '+lNow+' - '+lMtimeMs+' = '+lDelta+' cacheTTL='+lCacheTTL);
    return lDelta<lCacheTTL;
}

/**
 * Handles the cache when calling the API
 * Used in the API route 
 */
exports.doCacheAPI = function  (req,res,next)
{

   var lFiwareService = utils.getParamFiwareService(req);
   var lFormat= utils.getParamFormat(req);
   if(formats.PARQUET != lFormat)
   {
     debug('Cache is not used in case of '+lFormat+' format');
   }
   else if(!configsys.isCacheEnabled(lFiwareService))
   {
     debug('Cache is disabled for '+lFiwareService+' tenant')
   }
   else
   {
     var lUrlParts = url.parse(req.url, true);
     var lQuery = lUrlParts.query;

     debug('URL: '+req.url);
     debug('Query: '+JSON.stringify(lQuery));
     debug('Headers: '+JSON.stringify(req.headers));

     var lCachePlainKey=JSON.stringify(req.headers)+req.url;
     res.locals.iotbi_cache_key=crypto.createHash('sha256').update(lCachePlainKey).digest('hex');
     debug('Cache Key: '+res.locals.iotbi_cache_key);

     var lFile=genCacheFile(res.locals.iotbi_cache_key,formats.PARQUET);
     if (fs.existsSync(lFile))
     {
         var stats = fs.statSync(lFile);
         if(isStillValid(lFiwareService,stats))
         {
            debug('Send cached file');
            sendCachedFile(res,'cached.parquet',lFile,formats.PARQUET);
            debug('Cache management::Waiting for cache...');
            return;
         }
         else
         {
            debug('Cache is outdated :-(');
         }
     }
     else
     {
         debug('Output is not cached!'); 
     }
   }
   next();
   debug('Cache management::next...');
}

