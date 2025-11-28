/*!
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements cache services for NGSI-based brokers.
 */
var debug = require('debug')('iotbi.ngsiCache');

exports.saveTypes=saveTypes;
exports.getTypes=getTypes;
exports.getItemTypes=getItemTypes;
gTypes={}

/**
 * Saves in the cache pFiwareService's List of Types
 */

function saveTypes(pFiwareService,pTypes)
{
   debug('Save Types of '+pFiwareService)
   var lNow=new Date().getTime();
   gTypes[pFiwareService]={'time':lNow,'types':pTypes};
}

/**
 * Get the pFiwareService's List of Types
 */
function getTypes(pFiwareService)
{
   var lItem=getItemTypes(pFiwareService);
   var lTypes=lItem!=undefined?lItem.types:undefined;
   //debug(lTypes);
   return lTypes;   
}
/**
 * Get the pFiwareService's List of Types as an Cache Object {time,types}
 */
function getItemTypes(pFiwareService)
{
   var lItem=gTypes[pFiwareService];
   if (lItem==undefined)
   {
     debug('Cache miss for Types of '+pFiwareService);
     return undefined;
   }
   debug('Cache hit for Types of '+pFiwareService+' saved at '+lItem.time); 
   //debug(JSON.stringify(lItem.types,null,2));
   return lItem;
}
