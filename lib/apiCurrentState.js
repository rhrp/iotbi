/**
 * API CurrentState based on:
 * NGSI library for JavaScrip - https://ngsi-js-library.readthedocs.io/en/latest/ocb/
 * Ficodes NGSI Lib (suports NGSI V1,V2 and LD) - https://github.com/ficodes/ngsijs
 *
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 */

var url  = require('url');
var apiConfig = require('./apiConfig.js');
var utils = require('./utils.js')
var turf = require('turf');
var ocb = require('ocb-sender')
var ngsi = require('ngsi-parser');
var debug = require('debug')('iotbi.api.service.currentstate');

exports.service = function(req,res,next)
{
   //Path params
   var lEntityType=req.params.entityType;
   var lEntityId=(req.params.entityId==undefined?null:req.params.entityId);
   var lFiwareService=req.params.fiwareService;
   //Query params
   var lUrlParts = url.parse(req.url, true);
   var lQuery = lUrlParts.query;
   var lFiwareServicePath=lQuery.fiwareServicePath;
   var lExtended=(lQuery.extended!=undefined);
   var lJoinTable=(lQuery.join==undefined?null:lQuery.join);  // NGSI-LD
   // Format
   var lFormat= utils.getFormat(req); 

   //Check the Fiware Service
   if(!apiConfig.isConfigOk(lFiwareService))
   {
        debug('Invalid Service '+lFiwareService);
        res.status(404).json({'description':'Invalid Service '+lFiwareService});
        return;
   }

   var lEndpoint=apiConfig.getOrionEndpoint(lFiwareService);
   debug('Endpoint: '+lEndpoint);

   if(apiConfig.isOrionLD(lFiwareService))
   {
      debug('NGSI-LD :: OrionLD/Scorpio/Stellio');
      // Get the Query
      var lObjQuery = utils.getObjectQuery(lQuery,lEntityType,lEntityId);
      debug('Query: '+JSON.stringify(lObjQuery));
      var ngsild = require('./ngsildv1.js');
      ngsild.listEntities(lEndpoint,lObjQuery,lFiwareService,lFiwareServicePath,lExtended,lEntityType,lJoinTable)
      .then((table) => utils.sendTable(res,table,lFormat))
      .catch((err) => utils.sendError(res,500,err))
   }
   else
   {
      debug('Orion V2');
      var ngsiv2 = require('./ngsiv2.js');
      var lHeaders=ngsiv2.getInvokeFiwareHeaders(lFiwareService,lFiwareServicePath);
      // Config  the framework
      ocb.config(lEndpoint,lHeaders)
         .then((result) => {
                   debug('OCB Config: '+JSON.stringify(result));
                   // Get the Query
                   var lObjQuery = utils.getObjectQuery(lQuery,lEntityType,lEntityId,1000);
                   debug('Headers: '+JSON.stringify(lHeaders));
                   debug('Query: '+JSON.stringify(lObjQuery));

                   // Adapts the query to the Lib
                   var lOcbQueryExt='';
                   // For this lib
                   if(lObjQuery['attrs']!=undefined)
                   {
                     //The lib puts this parameter into the 'q='
                     lOcbQueryExt='&attrs='+lObjQuery.attrs;
                     delete lObjQuery.attrs;
                   }
                   // Invoke
                   var lOcbQuery = ngsi.createQuery(lObjQuery);
                   lOcbQuery=lOcbQuery+lOcbQueryExt;
                   debug('OcbQuery: '+JSON.stringify(lOcbQuery));
                   ocb.getWithQuery(lOcbQuery,lHeaders)
                   .then((result) => utils.sendTable(res,ngsiv2.entitiesToTable(result.body,lExtended),lFormat))
                   .catch((err) => {debug(err); utils.sendError(res,500,err,'OCB getWithQuery')})
         })
         .catch((err) => {
                   debug('OCB Config: '+err);
                   utils.sendError(res,500,err,'OCB Config')
         });
   }
}
