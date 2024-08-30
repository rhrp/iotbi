/*!
 * Copyright(c) 2023-2023 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module provides general utilities
 */

var url  = require('url');
var debug = require('debug')('iotbi.utils');

// Utils
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
exports.getObjectQuery = function(pQuery,pEntityType,pEntityId,pLimitDefault)
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
     debug('No limit defined. The limit is defined by Orion');
   }
   this.addGeoLocation(null,lObjQuery,lCoords,lMinDistance,lMaxDistance,lGeorel); 
   return lObjQuery;
}
/**
 * 
 */
exports.addGeoLocation = function(pUrl,pObjQuery,pCoords,pMinDistance,pMaxDistance,pGeorel) {
   var lUrl=pUrl!=null?pUrl:'';
   if(pCoords!=null && (pMaxDistance!=null || pMinDistance!=null))
   {
        var lCoords=pCoords.replace(/[ ]/g,'');	// See FIWARE-NGSI v2 Specification
        //georel=near
	var lParam=(pGeorel==null?'near':pGeorel)
                  +(pMinDistance!=null?';minDistance:'+pMinDistance:'')
                  +(pMaxDistance!=null?';maxDistance:'+pMaxDistance:'');
	addParam(pObjQuery,'georel',lParam);
	lUrl=lUrl+'&georel='+lParam;

	//geometry and coords
	addParam(pObjQuery,'geometry','point');
	addParam(pObjQuery,'coords',lCoords);
        lUrl=lUrl+'&geometry=point&coords='+lCoords;
   }
   else if(pCoords!=null && pGeorel!=null)
   {
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
