/*!
 * Copyright(c) 2023-2023 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module provides general utilities
 */

var url  = require('url');
var debug = require('debug')('iotbi.utils');

// Utils

exports.getRequestAllowedFiwareServices = function(res)
{
   return res.locals.iotbi_AllowedServices;
}
exports.setRequestAllowedFiwareServices = function(res,pAllowedServices)
{
   res.locals.iotbi_AllowedServices=pAllowedServices;
}
exports.getRequestConcurrency = function(res)
{
   return res.locals.iotbi_concurrency;
}
exports.setRequestConcurrency = function(res,pConcurrency)
{
   res.locals.iotbi_concurrency=pConcurrency;
}


exports.saveOutputTable = function(res,pTable,pFormat,pEntityType)
{
    res.locals.outputTable=pTable;
    res.locals.outputFormat=pFormat;
    res.locals.entityType=pEntityType;
    debug('Save output: '+pTable.countRows()+' rows ');
}
exports.saveOutputError = function(res,pCode,pMessage,pEntityType)
{
    res.locals.errorCode=pCode;
    res.locals.errorMessage=pMessage;
    res.locals.entityType=pEntityType;
    debug('Save Error: '+pCode+' Entity: '+pEntityType,'   msg: '+JSON.stringify(pMessage));
}
exports.getOutputTable = function(res)
{
    return res.locals.outputTable;
}
exports.getRequestId = function(req)
{
    return req.iotbi_reqId;
}
exports.getParamFormat = function(req) {
   var lUrlParts = url.parse(req.url, true);
   var lQuery = lUrlParts.query;
   var lFormatParam=lQuery.format;

   if(lFormatParam!=null)
   {
	// Overcomes Format header
	debug('Format param='+lFormatParam);
	return lFormatParam;
   }
   //Header params
   var lFormatHeader=req.headers['format'];
   debug('Format header='+lFormatHeader);
   return lFormatHeader;
}
exports.getParamFiwareService = function(req)
{
   //TODO: this also may came as an header!
   return req.params.fiwareService;
}
exports.getObjectQuery = function(pQuery,pEntityType,pEntityId,pLimitDefault,pIsNgsiLd)
{
   var lIdPattern=pQuery.idPattern;
   var lAttrs=pQuery.attrs;
   var lGeorel=pQuery.georel;
   var lCoords=pQuery.coords;
   var lMinDistance=pQuery.minDistance;
   var lMaxDistance=pQuery.maxDistance;
   var lLimit=pQuery.limit;
   var lNgsiQ=pQuery.q;

   // Get the Query
   var lObjQuery = {};
   if(pEntityId!=null)
   {
     lObjQuery.id=pEntityId;
   } 
   if(pEntityType!=null)
   {
     lObjQuery.type=pEntityType;
   } 
   if(lAttrs!=null)
   {
     debug('Selected Attributes:'+lAttrs);
     lObjQuery.attrs=lAttrs;
   }
   if(lNgsiQ!=null)
   {
     debug('NGSI Query:'+lNgsiQ);
     lObjQuery.q=lNgsiQ;
   }
   if(lIdPattern!=null)
   {
     lObjQuery.idPattern=lIdPattern;
   }
   if(lLimit!=null)
   {
     lObjQuery.limit=lLimit;
   }
   else if(pLimitDefault!=null)
   {
     lObjQuery.limit=pLimitDefault;
   }
   else
   {
     debug('No limit defined. The limit is defined by the broker');
   }
   this.addGeoLocation(null,lObjQuery,lCoords,lMinDistance,lMaxDistance,lGeorel,pIsNgsiLd); 
   return lObjQuery;
}
/**
 * 
 */
exports.addGeoLocation = function(pUrl,pObjQuery,pCoords,pMinDistance,pMaxDistance,pGeorel,pIsNgsiLd) {
   var lUrl=pUrl!=null?pUrl:'';
   var lOperMaxDistance=pIsNgsiLd?'==':':';
   var lParamCoordinates=pIsNgsiLd?'coordinates':'coords';
   if(pCoords!=null && (pMaxDistance!=null || pMinDistance!=null))
   {
        //Point
        var lCoords=pCoords.replace(/[ ]/g,'');	// See FIWARE-NGSI v2 Specification
        if(pIsNgsiLd && !lCoords.startsWith('[') )
        {
           lCoords='['+lCoords+']';
        }
        debug('coords: <'+pCoords+'> ->  <'+lCoords+'>');
        if(!pIsNgsiLd)
        {
           let lTmp=JSON.parse(lCoords);
           lCoords=lTmp[1]+','+lTmp[0];
        }
        //georel=near
	var lParam=(pGeorel==null?'near':pGeorel)
                  +(pMinDistance!=null?';minDistance'+lOperMaxDistance+pMinDistance:'')
                  +(pMaxDistance!=null?';maxDistance'+lOperMaxDistance+pMaxDistance:'');
	addParam(pObjQuery,'georel',lParam);
	lUrl=lUrl+'&georel='+lParam;

	//geometry and coords
        if(pIsNgsiLd)
        {
   	   addParam(pObjQuery,'geometry','Point');
        }
        else
        {
           addParam(pObjQuery,'geometry','point');
        }
	addParam(pObjQuery,lParamCoordinates,lCoords);
        addParam(pObjQuery,'geoproperty','location');
        lUrl=lUrl+'&geometry=point&'+lParamCoordinates+'='+lCoords+'&geoproperty=location';
        debug('Point: Coords='+lCoords+'   min distance: '+pMinDistance+'   max distance: '+pMaxDistance);
   }
   else if(pCoords!=null && pGeorel!=null)
   {
        //Polygon
	var lCoords=pCoords.replace(/[ ]/g,'');  // See FIWARE-NGSI v2 Specification
//TODO: this should return data
// http://52.49.232.90:5000/api/v1/ql/owm_v1/multiSensor/AirQualityObserved:S0005?appKey=segredo&georel=coveredBy&geometry=polygon&coords=41.2589,%20-8.5806;41.2405,-8.5872;41.2523,-8.6587;41.2589,-8.5806

	//georel
        lUrl=lUrl+'&georel='+pGeorel;
	addParam(pObjQuery,'georel',pGeorel);
        //geometry and coords
        addParam(pObjQuery,'geometry','polygon');
        addParam(pObjQuery,'coords',lCoords);
        lUrl=lUrl+'&geometry=polygon&coords='+lCoords;
        debug('Polygon: '+lCoords);
   }
   else
   {
        debug('No geoquery');
   }
   return lUrl;
}
function addParam(pObjQuery,pParamName,pParamValue)
{
	if(pObjQuery!=null)
	{
		pObjQuery[pParamName]=pParamValue;
	}
}

exports.isObject = function(value) {  
  return Object.prototype.toString.call(value) === '[object Object]'
}



exports.describeTable = function(pTable)
{
     //console.table(pTable);
     if(pTable.length>0)
     {
         var lFirstRow=pTable[0];
         return 'Rows: '+pTable.length+'  Cols:'+Object.keys(lFirstRow).length;
     }
     else
     {
         return 'Rows: 0 Cols:0';
     }
}
/**
 * Count the number of rows  of the table
 */
exports.countRows = function(pTable)
{
     if(pTable==undefined || pTable[0]==undefined)
     {
        return undefined;
     }
     return pTable[0].length;
}
exports.toArray = function(pEntity1,pEntity2)
{
   var lEntityTypes=[];
   if(pEntity1!=undefined)
   {
      lEntityTypes.push(pEntity1);
   }
   if(pEntity2!=undefined)
   {
     lEntityTypes.push(pEntity2);
   }
   return lEntityTypes;
}
