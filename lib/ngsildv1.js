#!/usr/bin/env node

var request = require('request');
var url  = require('url');
var apiConfig = require('./apiConfig.js');
var utils = require('./utils.js');
var utilsTemporal = require('./utilsTemporal.js');
var ngsildcontext = require('./ngsildcontext.js');
var turf = require('turf');
var NGSI = require('ngsijs');
var debug = require('debug')('iotbi.ngsildv1');

// Added prefix. Only for debug purposes ;-)
const PREFIX_EXT_O = '';

// Used URL @context
var gNgsiContext;
var gNgsiTypeHandle={};
//These handlers  may not be necessary
//gNgsiTypeHandle['fiware:verified']=__handleFiwareXXXToCell;
//gNgsiTypeHandle['fiware:controlledProperty']=__handleFiwareXXXToCell;
//gNgsiTypeHandle['fiware:category']=__handleFiwareXXXToCell;
//gNgsiTypeHandle['ngsi-ld:name']=__handleNgsiXXXToCell;

/**
 * Exports
 */
exports.listEntities = listEntities;
exports.entitiesToTable = entitiesToTable;
exports.entityToRow = entityToRow;
exports.joinTable = joinTable;
exports.temporalDataToTable = temporalDataToTable;
exports.setNgsiContextData = setNgsiContextData;
exports.createPromiseQueryEntities = createPromiseQueryEntities;
exports.getNgsiLdEntityContext = getNgsiLdEntityContext;

const ENTITY_LIMIT={};      // API' Limit of entities
ENTITY_LIMIT[apiConfig.BROKER_ORIONLD]=1000;
ENTITY_LIMIT[apiConfig.BROKER_SCORPIO]=1000;
ENTITY_LIMIT[apiConfig.BROKER_STELIO]=100;

/**
 * Set the default context
 */
function setNgsiContextData(pNgsiContext)
{
    gNgsiContext=pNgsiContext;
    //console.log(JSON.stringify(gNgsiContext,null,2));
}

/**
 * List the Entities by invoking Orion LD
 */
function listEntities(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pEntityType,pTableNameJoin)
{
   // Start the loading by the context data and then the data
   return  loadNGSIContextObject(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pEntityType,pTableNameJoin);
}

/**
 * Find the context url for an Entity
 */
function getNgsiLdEntityContext(pFiwareService,pEntityType)
{
   var lContext=apiConfig.getNgsiLdEntityContext(pEntityType);
   if(lContext==undefined) 
   {   
       lContext=apiConfig.getBrokerNgsiLdContext(pFiwareService);
       debug('NGSI-LD Context NOT defined for \"'+pEntityType+'\". Using Service Default: <'+lContext+'>');
   }
   else
   {
      debug('NGSI-LD Context defined for '+pEntityType+' is <'+lContext+'>');
   }
   return lContext;
}
/**
 * Create the options to invoke a request to the Broker
 */
function getInvokeFiwareOptions(pUrl,pFiwareService,pFiwareServicePath,pEntityType) {
   const options = {
       url: pUrl,
       headers: getInvokeFiwareHeaders(pFiwareService,pFiwareServicePath,pEntityType)
    }
    return options;
}

/**
 * Create the headers to invoke a request to the Broker
 */
function getInvokeFiwareHeaders(pFiwareService,pFiwareServicePath,pEntityType) 
{
   debug('NGSI-LD-V1 Headers for: '+pFiwareService+'/'+pFiwareServicePath+'/'+pEntityType);
   const headers= {};
   headers['NGSILD-Tenant']=pFiwareService;
   headers['Link']='<'+getNgsiLdEntityContext(pFiwareService,pEntityType)+'>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"';
   headers['Accept']='application/ld+json'; 
   if(pFiwareServicePath!=null)
   {
       headers['fiware-servicepath']=pFiwareServicePath;
       debug('Set header fiware-servicepath='+pFiwareServicePath);
   }
   return headers;
}


/**
 * Load the @context data structures to the global gNgsiContext
 */
function loadNGSIContextObject(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pEntityType,pTableNameJoin)
{
   gNgsiContext={};
   var lPromises=[];
   var lUrls=[]
  
   // Default Fiware Service context
   var lUrlDefaultContext=apiConfig.getBrokerNgsiLdContext(pFiwareService)
   lUrls.push(lUrlDefaultContext);
   // Entity Context
   var lUrlEntityContext=apiConfig.getNgsiLdEntityContext(pEntityType);
   if(lUrlEntityContext!=undefined && lUrlEntityContext!=lUrlDefaultContext)
   {
     lUrls.push(lUrlEntityContext);
   }
   for(var lUrl of lUrls)
   {
      lPromises.push(createPromiseLoadNGSIContextObject(lUrl));
   }
   return Promise.all(lPromises).then(function(values) {
        //console.log('Promise.all:'+values.length);
        for(var i=0;i<values.length;i++)
        {
            var lUrl=lUrls[i];
            var lContext=values[i];
            gNgsiContext[lUrl]=lContext;
            //console.log(lUrl+'  ===  '+JSON.stringify(lContext));
            //console.log(lUrl+'  ===  '+lContext);
            debug('Context: '+lUrl);
        }
        return loadMainTable(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pTableNameJoin);
    }).catch(function(err) { 
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
                   debug('error:', error);
                   return reject({'step':'Loading Context','description':error});
                }
                else
                {
                   debug('statusCode:', response && response.statusCode);
                   if(response.statusCode<200 || response.statusCode>299)
                   {
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
 * Find the ctx type
 */
function findContextType(pType)
{
    var lDebugOn=pType=='fiware:controlledProperty';
    //console.log('Init search for type '+pType+' on '+JSON.stringify(gNgsiContext,null,2));
    //console.log('Init search for type '+pType+' on '+(gNgsiContext!=undefined?Object.keys(gNgsiContext).length:undefined)+'  ctxs');
    for (var lCtx in gNgsiContext)
    {
        var lNgsiContextItem=gNgsiContext[lCtx]['@context'];
        //console.log('Find:<'+pType+'> in <'+lCtx+'>')
        for(var lCtxProp in lNgsiContextItem)
        {
              var lNgsiType=lNgsiContextItem[lCtxProp];
              if(lNgsiType==pType)
              {
                 //console.log('Found: <'+pType+'> in <'+lCtxProp+'> = '+lNgsiType);
                 return lNgsiType;
              }
        }
    }
    return undefined;
}
function isContextTypeFiware(pAttrName)
{
    var lType=findContextType(pAttrName);
    var lResult=lType!=undefined && lType.startsWith('fiware:');
    //console.log('pAttrName= <'+pAttrName+'> of type <'+lType+'> is Fiware context type:'+lResult);
    return lResult;
}
function isContextTypeJsonLdKeyword(pAttrName)
{
    var lType=findContextType(pAttrName);
    return lType!=undefined && ('@'+pAttrName)==lType;
}
function isTypeGeoProperty(pType)
{
    return pType!=undefined && 'GeoProperty'==pType;
}
function isTypeProperty(pType)
{
    return pType!=undefined && 'Property'==pType;
}

function calcCentroid(pField)
{
    var lPoint=null;
    if(pField.value.type=='Point__force_turf_to_work')
    {
         // Case of a Point
         lPoint=pField.value;
         //console.log('Point='+JSON.stringify(lPoint));
    }
    else
    {
         // Case of a Polygon - Use Turf for calculate the Centroid
         //ToDo: test with real data
         var lPolygon=pField.value;
//var maia = require('../../webserver/wwwroot/data/maia.json');
//lPolygon=maia.geojsons.municipio;
         lPoint=turf.centroid(lPolygon,null).geometry;
         //console.log('Turf Centroid='+JSON.stringify(lPoint));
    }
    return lPoint;
}
/**
 * Get the entities as a table.  Main table because may required a joined table
 */
function loadMainTable(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pTableNameJoin)
{
   var lTableName=pObjQuery.type;

   var lUrl=pEndpoint+'/entities?type='+pObjQuery.type
           +(pObjQuery.id!=undefined?'&id='+pObjQuery.id:'')
	   +(pObjQuery.attrs!=null?'&attrs='+pObjQuery.attrs:'')
	   +(pObjQuery.idPattern!=null?'&idPattern='+pObjQuery.idPattern:'')
	   //+(lLimit!=null?'&limit='+lLimit:'');
   //TODO lUrl=utils.addGeoLocation(lUrl,null,lCoords,lMinDistance,lMaxDistance,lGeorel); 
   return new Promise(function(resolve, reject)
   {
     var lOptions=getInvokeFiwareOptions(lUrl,pFiwareService,pFiwareServicePath,pObjQuery.type);
     //Init output table
     request(lOptions, function (error, response, pBody) {
        try
        {
                //console.log('@context: '+JSON.stringify(gNgsiContext));
                debug('['+pFiwareService+']['+pFiwareServicePath+'] Invoke:'+lUrl);
                //console.log('Options: '+JSON.stringify(lOptions,null,2));
                //console.log('Headers: '+JSON.stringify(lOptions.headers,null,2));
                if(error!=null)
                {
                   debug('error:', error);
                   return reject({'description':error});
                }
                else
                {
                   debug('statusCode:', response && response.statusCode);
                   if(response.statusCode<200 || response.statusCode>299)
                   {
                        //console.log('Body: '+pBody);
                        return reject({'step':'Loading Main Table','Code':response.statusCode});
                   }
                   else
                   {
			var lTable=parseToTable(pBody,pExtended);
                        //console.log('Body: '+pBody);
                        //console.log('Main Table '+lTableName+' ::  size:'+lTable.length);
                        if(pTableNameJoin==undefined)
                        {
                            return resolve(lTable);
                        }
                        debug('JOIN: '+pTableNameJoin);
                        return resolve(loadJoinTable(pEndpoint,pTableNameJoin,pFiwareService,pFiwareServicePath,lTable,pExtended));
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
 * Parse the body of the response to a Table
 */
function parseToTable(pBody,pExtended)
{
  //console.log('parseToTable :: body:', pBody); 
  return entitiesToTable(JSON.parse(pBody),pExtended)
}
/**
 * Convert an array of Entities to a Table
 */
function entitiesToTable(pEntities,pExtended)
{
  var lTable=[];
  for (var lEntity of pEntities)
  {
       var lReg=entityToRow(lEntity,pExtended);
       lTable.push(lReg);
  }
  return lTable;
}

/**
 * Convert an entity to a Table Row
 *
 */
function entityToRow(pEntity,pExtended)
{
    //console.log('NGSI Object:'+JSON.stringify(lEntity));
    lReg={};
    lReg._entityId=pEntity.id;
    for(var lDataField in pEntity)
    {
        var lEntityAttrib=pEntity[lDataField];
        //console.log('Object Id '+pEntity.id+'  :: Prop='+lDataField);
        if(lDataField=='id')
        {
        }
        else if(lDataField=='type')
        {
        }
        else if(lDataField=='@context')
        {
           if(pExtended)
           {
		var lContextField=pEntity[lDataField];
                if(Array.isArray(lContextField)) {
                  for(var i=0;i<lContextField.length;i++) {
                     lReg[PREFIX_EXT_O+'_context_'+i]=lContextField[i];
                  }
                } else {
                  lReg[PREFIX_EXT_O+'_context']=lContextField;
                }
           }
        }
        else if(isTypeGeoProperty(pEntity[lDataField].type))   // in NGSI-V" is 'geo:json'
        {
           geoPropertyToCell(lReg,lDataField,lEntityAttrib,pExtended);
        }
        else if(pEntity[lDataField].type=='Relationship')
        {
             //console.log('Relationship: ['+lDataField+'] -> '+pEntity[lDataField].object);
             lReg[lDataField]=pEntity[lDataField].object
        }
        else if(lEntityAttrib.type=='Property')
        {
             propertyToCell(lReg,lDataField,lEntityAttrib,pExtended);
        }
        else
        {
             debug('Ignored Type:'+lEntityAttrib.type+'  Attrib: ['+lDataField+'] -> '+JSON.stringify(lEntityAttrib));
        }
    }
    return lReg;
}


/**
 * Generic handler for all "fiware:"
 */
function __handleFiwareXXXToCell(pReg,pPropName,lPropertyField,pProperty,pExtendedMode)
{
   var lPropName=pPropName.replace('fiware:','');// Remove the namespace
   lPropName=pExtendedMode?lPropName+'_'+lPropertyField:lPropName;
   pReg[PREFIX_EXT_O+lPropName]=pProperty[lPropertyField];
}
/**
 * Generic handler for all "ngsi-ld:"
 */
function __handleNgsiXXXToCell(pReg,pPropName,lPropertyField,pProperty,pExtendedMode)
{
   var lPropName=pPropName.replace('ngsi-ld:','');// Remove the namespace
   lPropName=pExtendedMode?lPropName+'_'+lPropertyField:lPropName;
   pReg[PREFIX_EXT_O+lPropName]=pProperty[lPropertyField];
}


function propertyToCell(pReg,pPropName,pProperty,pExtended)
{
      var lCtxType=findContextType(pPropName);
      var lIsContextTypeFiware=isContextTypeFiware(pPropName);
      var lIsTypeJsonLdKeyword=isContextTypeJsonLdKeyword(pPropName);
      var lHandler=lCtxType==undefined?undefined:gNgsiTypeHandle[lCtxType];

      //console.log('Property: ['+pPropName+'] -> '+JSON.stringify(pProperty));
      if(lHandler==undefined)
      {
         addValue(1,pReg,pPropName,pProperty.value)
      }
      else
      {
         // The type is kown, thus, a special handler will care the data
         lHandler(pReg,pPropName,'value',pProperty,false);
      }
      if(pExtended)
      {
            //console.log('Extend Property '+pPropName);
            for(var lPropertyField in pProperty)
            {
               var lTmp='    Attrib:'+lPropertyField+'   Type: <'+lCtxType+'> TypeFiware='+lIsContextTypeFiware+' TypeJsonLdKeyword='+lIsTypeJsonLdKeyword+' ::  ';
               if(lPropertyField=='type' || lPropertyField == 'value')
               { 
                   //console.log(lTmp+'Extend Ignore '+pPropName+'.'+lPropertyField+' because is already included');
               }
               else if(lHandler!=undefined)
               {
                   //console.log(lTmp+'Extend '+pPropName+'.'+lPropertyField+'  with default Handler');
                   lHandler(pReg,pPropName,lPropertyField,pProperty,true);
               }
               else
               {
                   //console.log(lTmp+'Extend '+pPropName+'.'+lPropertyField);
                   lReg[PREFIX_EXT_O+pPropName+'_'+lPropertyField]=pProperty[lPropertyField];
               }
            }
      }
}
function geoPropertyToCell(pReg,pPropName,pProperty,pExtended)
{
      var lPoint=calcCentroid(pProperty);
      addValue(1,pReg,pPropName,pProperty.value)
      pReg[pPropName+'_centroid']=lPoint.coordinates[1]+', '+lPoint.coordinates[0];
      if(pExtended)
      {
          pReg[pPropName+'_centroid_lat']=(lPoint!=null?lPoint.coordinates[1]:'');
          pReg[pPropName+'_centroid_lon']=(lPoint!=null?lPoint.coordinates[0]:'');
      }
}

/**
* Add value to table
*/
function addValue(pLevel,pTableReg,pEntityAttribName,pEntityAttribValue)
{
        if(utils.isObject(pEntityAttribValue))
        {
           // console.log('['+pLevel+']['+pEntityAttribName+'] Expand value '+JSON.stringify(pEntityAttribValue));
           for(var lValueFieldName in pEntityAttribValue)
           {
                addValue(pLevel+1,pTableReg,pEntityAttribName+'_'+lValueFieldName,pEntityAttribValue[lValueFieldName]) 
           }
        }
        else
        {
           //console.log('['+pLevel+']['+pEntityAttribName+'] Value='+pEntityAttribValue);
           pTableReg[pEntityAttribName]=pEntityAttribValue;
        }
}

/**
 * Find the references to the entity
 * ToDo:  Implement a metadata structure that allows to know which are de FK in the table
 *        For now, we search any reference :-(
 */
function getFkReferences(pTable,pEntityName)
{
  var lFKs=[];
  for(var lRowKey in pTable)
  {
       var lRow=pTable[lRowKey];
       for(var lCellKey in lRow)
       {
            var lCell=new String(lRow[lCellKey]);
            if(lCell.startsWith('urn:ngsi-ld:'+pEntityName))
            {
                //console.log('Found FK: '+lCell+'  on colunm '+lCellKey+'    of type '+(typeof lCell));
                lFKs.push(lRow[lCellKey]);
            }
       }
  }
  return lFKs;
}

/**
 * TODO: Use NGSIJS lib
 */
function loadJoinTable(pEndpoint,pEntityName,pFiwareService,pFiwareServicePath,pMainTable,pExtended)
{
  var lIds=''; 
  for(var lEntityReference of getFkReferences(pMainTable,pEntityName))
  {
     //console.log('Filter FK: '+lEntityReference);
     lIds=lIds==''?'&id='+lEntityReference:lIds+','+lEntityReference;
  }
  debug('Filter ids='+lIds);    

  var lUrl=pEndpoint+'/entities?type='+pEntityName+lIds;

  return new Promise(function(resolve, reject)
  {
     var lOptions=getInvokeFiwareOptions(lUrl,pFiwareService,pFiwareServicePath,pEntityName);
     //Init output table
     var lTable=[];
     request(lOptions, function (error, response, pBody) {
        try
        {
                debug('['+pFiwareService+']['+pFiwareServicePath+'] Invoke:'+lUrl+'   Header: '+JSON.stringify(lOptions.headers));
                if(error!=null)
                {
                   console.error('error:', error);
                   return reject({'step':'Loading Join Table '+pEntityName,'description':error});
                }
                else
                {
                   debug('statusCode:', response && response.statusCode);
                   if(response.statusCode<200 || response.statusCode>299)
                   {
                        //console.log('Body: '+pBody);
                        return reject({'step':'Loading Join Table '+pEntityName,'HTTP Code':response.statusCode});
                   }
                   else
                   {
                        var lJoinTable=parseToTable(pBody,pExtended);
                        //console.log('Body: '+pBody);
                        //console.log('Join Table '+pEntityName+' ::  size:'+lJoinTable.length);
                        return resolve(joinTable(pMainTable,lJoinTable));
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
 * Join two tables where pMainTable has a FK to the PK (_entity_id) in pJoinTable
 *   [pMainTable] (1) --- (M) [pJoinTable]
 */
function joinTable(pMainTable,pJoinTable)
{
   debug('Start the join of Main table with '+utils.describeTable(pMainTable)+' rows and a join table containing '+utils.describeTable(pJoinTable)+' rows ...');
   //console.log(pMainTable);
   //console.log(pJoinTable);
   //Organize foreign Entities as a dicionary by id
   var lJoinEntitiesDic={};
   for(var lEntityKey in pJoinTable)
   {
       var lJoinEntity=pJoinTable[lEntityKey];
       lJoinEntitiesDic[lJoinEntity._entityId]=lJoinEntity;
       //console.log('Entity['+lJoinEntity._entityId+']='+JSON.stringify(lJoinEntity));
   }
   for(var lEntityKey in pMainTable)
   {
        var lEntity=pMainTable[lEntityKey];
        for (var lAttribName in lEntity)
        {
            var lAttribValue=lEntity[lAttribName];
            var lJoinRecord=lJoinEntitiesDic[lAttribValue];
            if(lJoinRecord!=undefined)
            {
               //console.log(lAttribName+' =  '+lAttribValue);
               for(var lJoinField in lJoinRecord)
               {
                  if(lJoinField!='_entityId')
                  {
                    //console.log('   '+lJoinField);
                    lEntity[lAttribName+'_'+lJoinField]=lJoinRecord[lJoinField];
                  }
               }
            }
        }
   }
   debug('Finish the join getting a table containing '+utils.describeTable(pMainTable)+' rows ...');
   return pMainTable;
}


//TODO: uniformizar com format QL
function temporalDataToTable(pJson,pExtended)
{  
    //console.log('Temporal data for convertiing to a Table');
    //console.log(JSON.stringify(pJson,null,2));
    var lTable=[];
    var i=0;
    for (var lKeyEntity in pJson.results) 
    {
       var lTmp={};// Dict  attr+time/value
       i=i+1;
       var lEntity=pJson.results[lKeyEntity];
       //console.log('Entity '+lEntity.id);
       for(var lKeyAttr in lEntity)
       {
	 //console.log('Row #'+i+'  '+lKeyAttr);
         var lAttrib=lEntity[lKeyAttr];
         if(lKeyAttr=='id' || lKeyAttr=='type' || lKeyAttr=='@context')  {
	    //console.log('System attrs '+lKeyAttr);
         } else if (Array.isArray(lAttrib)){
	    for(var idx in lAttrib)
            {
                var lValueAtTime=lAttrib[idx];
                //console.log(lValueAtTime);
                var lTimeIndex=utilsTemporal.formatDateVal(lValueAtTime.observedAt);
                var lEntityIndex=lKeyAttr;
                //console.log(lTimeIndex);
                if(lTmp[lTimeIndex]==undefined)
                {
                  lTmp[lTimeIndex]={};
                }
                lTmp[lTimeIndex][lEntityIndex]=lValueAtTime;
            }
         }
       }
       for(var lTimeKey in lTmp)
       {
          var lRow={};
          var lTimeIndex=parseInt(lTimeKey);
          lRow['_entityId']=lEntity.id;
          lRow['_timeIndex']=lTimeIndex;
          lRow['_dateTime']=utilsTemporal.timestampToDate(lTimeIndex);
          for(var lPropKey in lTmp[lTimeKey])
          {
             var lValueTime=lTmp[lTimeKey][lPropKey];
             if(isTypeGeoProperty(lValueTime.type))
             {
                //console.log(lEntity.id+'      '+lTimeKey+'      '+lPropKey+'    '+JSON.stringify(lValueTime));
                geoPropertyToCell(lRow,lPropKey,lValueTime,pExtended);
             }
             else if(isTypeProperty(lValueTime.type))
             { 
                //console.log(lEntity.id+'   '+lTimeKey+'  '+lPropKey+' '+JSON.stringify(lValueTime));
                for(var lValueKey in lValueTime)
                {
                  lRow[lPropKey+'_'+lValueKey]=lValueTime[lValueKey];
                }
             }
             else
             {
                 debug('Warning:   Ignored '+lEntity.id+'   '+lTimeKey+'  '+lPropKey+' '+JSON.stringify(lValueTime));
             }
          }
          lTable.push(lRow);
       }
    }
    return lTable;
}

/**
 * This function create a Promise to query the Broker 
 */
function createPromiseQueryEntities(pFiwareService,pFiwareServicePath,pEntityType,pEntityId)
{
   var lBrokerURL=apiConfig.getBrokerURL(pFiwareService);
   var lConnection = new NGSI.Connection(lBrokerURL);
   var lContext=getNgsiLdEntityContext(pFiwareService,pEntityType)
   var lOptions = {"tenant":pFiwareService,
                   "@context":lContext,
                   "limit":ENTITY_LIMIT[apiConfig.getBrokerName(pFiwareService)]
                  };
   if(pEntityType!=undefined)
   {
      lOptions['type']=pEntityType;
   }
   if(pEntityId!=undefined)
   {
      lOptions['id']=pEntityId;
   }
   debug('Broker URL: '+lBrokerURL);
   //console.log('Options: '+JSON.stringify(lOptions,null,2));
   //Load the entitiy's data
   return lConnection.ld.queryEntities(lOptions);
}
