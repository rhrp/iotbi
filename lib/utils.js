var url  = require('url');
var apiConfig = require('./apiConfig.js');
var  { Parser, StreamParser } = require('@json2csv/plainjs');

// Utils

/**
 * Response Table
 */
exports.sendTable = function(res,pTable,pFormat)
{
   lAsCsv=pFormat!=null && pFormat.toLowerCase()=='csv';
   console.log('Format='+pFormat+' asCsv='+lAsCsv);
   try
   {
        if(lAsCsv)
        {
           var lOut=sendAsCSV(pTable);
           //res.status(200).attachment('orion.csv').send(lOut);
           res.status(200).type('text/csv').send(lOut);
        }
        else
        {
           var lOut=sendAsJSON(pTable);
           res.status(200).json(lOut);
        }
   }
   catch(ex)
   {
      console.log(ex);
      res.status(500).json({'description':ex});
   }
}
/**
 * Response Error
 */
exports.sendError = function(res,code,err)
{
    console.log('sendError(code='+code+'): '+err);
    res.status(code).json({'description':err});
}

function sendAsJSON(pTableValues)
{
    return pTableValues;
}
function sendAsCSV(lTableValues) 
{
   const opts = {};
   const parser = new Parser(opts);
   var lCsv = parser.parse(lTableValues);
   //console.log('----------------------');
   //console.log(lCsv);
   //console.log('----------------------');
   return lCsv;
}



exports.getFormat = function(req) {
   var lUrlParts = url.parse(req.url, true);
   var lQuery = lUrlParts.query;
   var lFormatParam=lQuery.format;

   if(lFormatParam!=null)
   {
	// Overcomes Format header
	console.log('Format param='+lFormatParam);
	return lFormatParam;
   }
   //Header params
   var lFormatHeader=req.headers['format'];
   console.log('Format header='+lFormatHeader);
   return lFormatHeader;
}

/**
 * Init the Headers for pFiwareService,pFiwareServicePath
 */
exports.getInvokeFiwareHeaders = function(pFiwareService,pNGSILDTenant,pFiwareServicePath) {
   console.log(pFiwareService+'/'+pNGSILDTenant+'/'+pFiwareServicePath);
   const headers= {};
   if(pNGSILDTenant==true)
   {
      headers['NGSILD-Tenant']=pFiwareService;
      // According to following doc, the @context may be defined using the NGSILD Core and additional files
      // HEADER 'Link' = '<http://context/ngsi-context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
      // https://github.com/FIWARE/context.Orion-LD/blob/develop/doc/manuals-ld/the-context.md
      // 
      // The OrionLD only process the last ocurrence of the Link header   :-(
      headers['Link']='<'+apiConfig.getBrokerNgsiLdContext(pFiwareService)+'>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"';
      headers['Accept']='application/ld+json'; 
   }
   else
   {
      headers['fiware-service']=pFiwareService;
   }
   if(pFiwareServicePath!=null)
   {
       headers['fiware-servicepath']=pFiwareServicePath;
       console.log('Set header fiware-servicepath='+pFiwareServicePath);
   }
   return headers;
}

exports.getInvokeFiwareOptions = function(pUrl,pFiwareService,pNGSILDTenant,pFiwareServicePath) {
   const options = {
       url: pUrl,
       headers: this.getInvokeFiwareHeaders(pFiwareService,pNGSILDTenant,pFiwareServicePath)
    }
    return options;
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
     lObjQuery.attrs=lAttrs;
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
     console.log('No limit defined. The limit is defined by Orion');
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

