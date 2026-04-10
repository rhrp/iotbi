/**
 * API CurrentState based on:
 *
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */

const url  = require('url');
const configsys = require('./configsys.js');
const utils = require('./utils.js');
const BrokerParallelize=require('./brokerParallelize.js');
const Broker=require('./broker.js');
const debug = require('debug')('iotbi.api.service.currentstate');

exports.service = function(req,res,next)
{
   var lFiwareServices=utils.getRequestAllowedFiwareServices(res)
   //Path params
   var lEntityType=req.params.entityType;
   var lEntityId=(req.params.entityId==undefined?null:req.params.entityId);
   //Query params
   var lUrlParts = url.parse(req.url, true);
   var lQuery = lUrlParts.query;
   var lFiwareServicePath=lQuery.fiwareServicePath;
   var lExtended=(lQuery.extended!=undefined);
   var lJoinTable=(lQuery.join==undefined?null:lQuery.join);  // NGSI-LD
   // Format
   var lFormat= utils.getParamFormat(req); 

   //Check the Fiware Service
   for(let lFiwareService of lFiwareServices)
   {
     if(!configsys.isConfigOk(lFiwareService))
     {
        debug('Invalid Service '+lFiwareService);
        utils.saveOutputError(res,404,'Invalid Service '+lFiwareService);
        next();
        return;
     }
   }

   //var lEndpoint=configsys.getOrionEndpoint(lFiwareService);
   //debug('Endpoint: '+lEndpoint);

   let lBrokerParallel=new BrokerParallelize(lFiwareServices);
   lBrokerParallel.setRequestId(req.iotbi_reqId);
   if(lQuery.coordinates!=undefined)
   {
     let lGeorel;
     let lGeometry;
     if(lQuery.georel=='near' || lQuery.minDistance!=undefined || lQuery.maxDistance!=undefined)
     {
        lGeorel='near;'+(lQuery.minDistance!=undefined?'minDistance=='+lQuery.minDistance:'')+(lQuery.maxDistance!=undefined?'maxDistance=='+lQuery.maxDistance:'');
        lGeometry=Broker.GEOMETRY_POINT;
     }
     else
     {
        lGeorel=lQuery.georel;
        lGeometry=lQuery.geometry;
     }
     let lGeoQueryValidation=lBrokerParallel.setGeoQuery(lGeorel,lGeometry,lQuery.coordinates,'location',lEntityType);
     if(lGeoQueryValidation!=undefined)
     {
         utils.saveOutputError(res,400,lGeoQueryValidation);
         next();
         return;
     }
   }

   //let lValidQuery=lBrokerParallel.setQuery(lQuery,lEntityType);
      
   lBrokerParallel.getCurrentData(lEntityType,undefined,lEntityId,lExtended,lJoinTable,undefined)
        .then((lSortedResult) => {
           let lTable=lSortedResult.table;
           utils.saveOutputTable(res,lTable,lFormat,lEntityType);
           next();
        })
        .catch((err) => {
           debug('getCurrentData::Error: '+JSON.stringify(err));
           utils.saveOutputError(res,500,err);
           next();
        });
}
