#!/usr/bin/env node

var url  = require('url');
var apiConfig = require('./apiConfig.js');
var utils = require('./utils.js')
var turf = require('turf');
var ocb = require('ocb-sender')
var ngsi = require('ngsi-parser');

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
        console.log('Invalid Service '+lFiwareService);
        res.status(404).json({'description':'Invalid Service '+lFiwareService});
        return;
   }

   var lEndpoint=apiConfig.getOrionEndpoint(lFiwareService);
   console.log('Endpoint: '+lEndpoint);

   if(apiConfig.isOrionLD(lFiwareService))
   {
      console.log('NGSI-LD :: OrionLD/Scorpio/Stellio');
      // Get the Query
      var lObjQuery = utils.getObjectQuery(lQuery,lEntityType,lEntityId);
      console.log('Query: '+JSON.stringify(lObjQuery));
      var ngsild = require('./ngsildv1.js');
      ngsild.listEntities(lEndpoint,lObjQuery,lFiwareService,lFiwareServicePath,lExtended,lJoinTable)
      .then((table) => utils.sendTable(res,table,lFormat))
      .catch((err) => utils.sendError(res,500,err))
   }
   else
   {
      console.log('Orion V2');
      
      var lHeaders=utils.getInvokeFiwareHeaders(lFiwareService,false,lFiwareServicePath);
      var ngsiv2 = require('./ngsiv2.js');
      // Config  the framework
      ocb.config(lEndpoint,lHeaders)
         .then((result) => console.log('Config: '+JSON.stringify(result)))
         .catch((err) => console.log('Config:'+err));

      // Get the Query
      var lObjQuery = utils.getObjectQuery(lQuery,lEntityType,lEntityId,1000);
      console.log('Query: '+JSON.stringify(lObjQuery));

     // Invoke
     var lOcbQuery = ngsi.createQuery(lObjQuery);
     ocb.getWithQuery(lOcbQuery,lHeaders)
       .then((result) => utils.sendTable(res,ngsiv2.entitiesToTable(result.body,lExtended),lFormat))
      .catch((err) => utils.sendError(res,500,err))
   }
}
