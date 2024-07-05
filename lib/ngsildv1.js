/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the NGSI-LD-V1 subsystem.
 *
 */

var request = require('request');
var url  = require('url');
var configsys = require('./configsys.js');
var utils = require('./utils.js');
var utilsTemporal = require('./utilsTemporal.js');
var ngsildcontext = require('./ngsildcontext.js');
var schema = require('./schema.js');
var turf = require('turf');
var NGSI = require('ngsijs');
var debug = require('debug')('iotbi.ngsildv1');

// Added prefix. Only for debug purposes ;-)
const PREFIX_EXT_O = '';

// Used URL @context
var gNgsiContext={};
var gSDM=null;

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
exports.createPromiseListTypes = createPromiseListTypes;
exports.createPromiseGetType = createPromiseGetType;
exports.createPromiseQueryEntitiesAttributes = createPromiseQueryEntitiesAttributes;
exports.getNgsiLdEntityContext = getNgsiLdEntityContext;
exports.getFkEntitiesTypes = getFkEntitiesTypes;

const ENTITY_LIMIT={};      // API' Limit of entities
ENTITY_LIMIT[configsys.BROKER_ORIONLD]=1000;
ENTITY_LIMIT[configsys.BROKER_SCORPIO]=1000;
ENTITY_LIMIT[configsys.BROKER_STELIO]=100;

/**
 * Set the default context
 */
function setNgsiContextData(pNgsiContext)
{
    gNgsiContext=pNgsiContext[0];
    gSDM=pNgsiContext[1];
    //debug('setContexts:\n'+JSON.stringify(gNgsiContext,null,2));
}

/**
 * List the Entities by invoking Orion LD
 */
function listEntities(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pEntityType,pTableNameJoin,pJoinAttribute)
{
   // Start the loading by the context data and then the data
   return ngsildcontext.createPromisesloadNgsiContext(pFiwareService,utils.toArray(pEntityType,pTableNameJoin)).then(function (lNgsiContextData) {
                 setNgsiContextData(lNgsiContextData);
                 return loadMainTable(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pTableNameJoin,pJoinAttribute);
   }).catch(function(err) {
                 debug('Context loading :: Failed making request with error: ', err);
                 throw err;
   });
}

/**
 * Find the context url for an Entity
 */
function getNgsiLdEntityContext(pFiwareService,pEntityType)
{
   var lContext=configsys.getNgsiLdEntityContext(pEntityType);
   if(lContext==undefined) 
   {   
       lContext=configsys.getBrokerNgsiLdContext(pFiwareService);
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
   headers['NGSILD-Tenant']=configsys.getBrokerTenant(pFiwareService);
   headers['Link']='<'+getNgsiLdEntityContext(pFiwareService,pEntityType)+'>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"';
   headers['Accept']='application/ld+json'; 
   if(pFiwareServicePath!=null)
   {
       headers['fiware-servicepath']=pFiwareServicePath;
       debug('Set header fiware-servicepath='+pFiwareServicePath);
   }
   //debug('Header:\n'+JSON.stringify(headers,null,2));
   return headers;
}

/**
 * Find the ctx type RHP
 */
function findContextType(pType)
{
    //var lDebugOn=pType=='windSpeed';
    //console.log('Init search for type '+pType+' on '+JSON.stringify(gNgsiContext,null,2));
    //console.log('Init search for type '+pType+' on '+(gNgsiContext!=undefined?Object.keys(gNgsiContext).length:undefined)+'  ctxs');
    for (var lCtx in gNgsiContext)
    {
        //debug('Find:<'+pType+'> in <'+lCtx+'>')
        var lNgsiContextItem=gNgsiContext[lCtx]['@context'];
        //if(lDebugOn)
        //{
          //debug('Find:<'+pType+'> in <'+lCtx+'>: \n'+JSON.stringify(lNgsiContextItem,null,2));
        //}
        for(var lCtxProp in lNgsiContextItem)
        {
              var lNgsiType=lNgsiContextItem[lCtxProp];
              //if(lDebugOn)
              //{
              //    debug('Check: <'+lCtxProp+'>::<'+JSON.stringify(lNgsiType)+'>');
              //}
              if(lCtxProp==pType)
              {
                 console.log('Found: <'+pType+'> in <'+lCtxProp+'> = '+lNgsiType);
                 return lNgsiType;
              }
        }
    }
    //if(lDebugOn)
    //{
    //  debug('Not found:<'+pType+'>');
    //}
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
function loadMainTable(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pTableNameJoin,pJoinAttribute)
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
                debug('['+pFiwareService+']['+pFiwareServicePath+'] Invoke:'+lUrl);
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
                        return reject({'step':'Loading Main Table','Code':response.statusCode});
                   }
                   else
                   {
			var lTable=parseToTable(pBody,pExtended);
                        if(pTableNameJoin==undefined)
                        {
                            return resolve(lTable);
                        }
                        debug('JOIN: '+pTableNameJoin+' using attribute '+pJoinAttribute);
                        return resolve(loadJoinTable(pEndpoint,pObjQuery.type,pTableNameJoin,pJoinAttribute,pFiwareService,pFiwareServicePath,lTable,pExtended));
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
 * Parse the body of the response to a [Table,Schema]
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
  var lSchema={};
  for (var lEntity of pEntities)
  {
       var lRegPlusSchema=entityToRow(lEntity,pExtended);
       lTable.push(lRegPlusSchema[0]);
       lSchema=lRegPlusSchema[1];
  }
  //debug('Schema:\n '+JSON.stringify(lSchema,null,2));
  return [lTable,lSchema];
}

/**
 * Convert an entity to a Table Row
 *
 */
function entityToRow(pEntity,pExtended)
{
    //debug('NGSI-LD Object:'+JSON.stringify(pEntity,null,2));
    var lSchema={};
    var lReg={};
    lReg._entityId=pEntity.id;
    lSchema._entityId=schema.STRING;

    for(var lDataField in pEntity)
    {
        var lEntityAttrib=pEntity[lDataField];
        //debug('Object Id '+pEntity.id+'  :: Prop='+lDataField);
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
                     var lFlatName=PREFIX_EXT_O+'_context_'+i;
                     lReg[lFlatName]=lContextField[i];
                     lSchema[lFlatName]=schema.STRING;
                  }
                } else {
                  var lFlatName=PREFIX_EXT_O+'_context';
                  lReg[lFlatName]=lContextField;
                  lSchema[lFlatName]=schema.STRING;
                }
           }
        }
        else if(isTypeGeoProperty(pEntity[lDataField].type))   // in NGSI-V" is 'geo:json'
        {
             geoPropertyToCell(lReg,lSchema,lDataField,lEntityAttrib,pExtended);
        }
        else if(pEntity[lDataField].type=='Relationship')
        {
             //console.log('Relationship: ['+lDataField+'] -> '+pEntity[lDataField].object);
             lReg[lDataField]=pEntity[lDataField].object
             lSchema[lDataField]=schema.STRING;
        }
        else if(lEntityAttrib.type=='Property')
        {
             propertyToCell(lReg,lSchema,pEntity.type,lDataField,lEntityAttrib,pExtended);
             //debug('Out property Schema: ['+lDataField+'] '+JSON.stringify(lSchema,null,2));
        }
        else
        {
             debug('Ignored Type:'+lEntityAttrib.type+'  Attrib: ['+lDataField+'] -> '+JSON.stringify(lEntityAttrib));
        }
    }
    //debug('Out Row:\n '+JSON.stringify(lReg,null,2));
    //debug('Out Schema:\n '+JSON.stringify(lSchema,null,2));
    return [lReg,lSchema];
}


function propertyToCell(pReg,pSchema,pEntityType,pPropName,pProperty,pExtended)
{
      addValue(1,pReg,pSchema,pPropName,pProperty.value,pEntityType,{},pPropName);
      if(pExtended)
      {
            // Handle all fields according to NGSI-LD CIM
            for(var lPropertyField in pProperty)
            {
               var lFlatName=PREFIX_EXT_O+pPropName+'_'+lPropertyField;
               var lTmp='+Attrib:'+lPropertyField+'  ::  ';
               if(lPropertyField=='type' || lPropertyField == 'value')
               { 
                   //Already included
               }
               else if(lPropertyField.match(/^(observedAt|createdAt|modifiedAt|deletedAt)$/))
               {
                   pReg[lFlatName]=pProperty[lPropertyField];
                   pSchema[lFlatName]=schema.DATETIME;
               }
               else if(lPropertyField.match(/^(unitCode|datasetId|instanceId)$/))
               {
                   pReg[lFlatName]=pProperty[lPropertyField];
                   pSchema[lFlatName]=schema.STRING;
               }
               else
               {
                   var lPropCtx=ngsildcontext.getNgsiLdPropertyContext(pEntityType,lPropertyField);
                   if(lPropCtx=='fiware:verified')
                   {
                        //debug(pEntityType+' :: '+lPropertyField+' :: '+lPropCtx);
                        // TODO: this is a correct approach?
                        // In case of unitCode and observedAt, for instance, these are defined in CIM.  
                        pReg[lFlatName]=pProperty[lPropertyField];
                        pSchema[lFlatName]=schema.BOOLEAN;
                   }
                   else
                   {
                        debug('Unknown property '+lPropertyField+' of property '+pPropName+' in the Schema of '+pEntityType+'  and @context '+lPropCtx+': \n'+JSON.stringify(pProperty,null,2));
                        lTmpType=schema.OBJECT;
                   }
               }
            }
      }
}
function geoPropertyToCell(pReg,pSchema,pPropName,pProperty,pExtended)
{
      var lPoint=calcCentroid(pProperty);
      pReg[pPropName+'_coordinates_lat']=(lPoint!=null?lPoint.coordinates[1]:'');
      pReg[pPropName+'_coordinates_lon']=(lPoint!=null?lPoint.coordinates[0]:'');
      pSchema[pPropName+'_coordinates_lat']=schema.DOUBLE;
      pSchema[pPropName+'_coordinates_lon']=schema.DOUBLE;
      pReg[pPropName+'_centroid']=lPoint.coordinates[1]+', '+lPoint.coordinates[0];
      pSchema[pPropName+'_centroid']=schema.STRING;
      if(pExtended)
      {
          pReg[pPropName+'_centroid_lat']=(lPoint!=null?lPoint.coordinates[1]:'');
          pReg[pPropName+'_centroid_lon']=(lPoint!=null?lPoint.coordinates[0]:'');
          pSchema[pPropName+'_centroid_lat']=schema.DOUBLE;
          pSchema[pPropName+'_centroid_lon']=schema.DOUBLE;
      }
}

/**
* Add value to table
*/
function addValue(pLevel,pTableReg,pSchema,pEntityAttribName,pEntityAttribValue,pEntityType,pAttribPath,pLevelAttrib)
{
        var lTmp=pAttribPath[pLevel]==undefined?[]:pAttribPath[pLevel];
        lTmp.push(pLevelAttrib);
        pAttribPath[pLevel]=lTmp;

        if(utils.isObject(pEntityAttribValue))
        {
           // console.log('['+pLevel+']['+pEntityAttribName+'] Expand value '+JSON.stringify(pEntityAttribValue));
           for(var lValueFieldName in pEntityAttribValue)
           {
                addValue(pLevel+1,pTableReg,pSchema,pEntityAttribName+'_'+lValueFieldName,pEntityAttribValue[lValueFieldName],pEntityType,pAttribPath,lValueFieldName); 
           }
        }
        else
        {
           //TODO: May occur an ambiguity if there is another Type with name pLevelAttrib in another hierarchy 
           pTableReg[pEntityAttribName]=pEntityAttribValue;
           var lType=gSDM.getTypeOf(pEntityType,pLevelAttrib);
           if(lType==undefined)
           {
              debug('['+pLevel+'] Unknown property '+pEntityAttribName+' in the Schema of '+pEntityType);
              debug('Path: '+JSON.stringify(pAttribPath));
              pSchema[pEntityAttribName]=schema.OBJECT;
           }
           else
           {
                //debug('Known property '+lPropKey+' in the Schema of '+lEntityType+' schema type: '+lType);
              pSchema[pEntityAttribName]=lType;
           }
           //debug('['+pLevel+'] addValue: '+pEntityAttribName+' = '+pEntityAttribValue);
        }
}

/**
 * Find the references to the entity
 * ToDo:  Implement a metadata structure that allows to Known which are de FK in the table
 *        For now, we search any reference :-(
 *
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
*/
/**
 * Search in the table references to the pEntityName using the attribute pJoinAttribute, or the possible ones
 */
function getFkReferences(pTable,pSchema,pEntityName,pJoinPossibleAttributes)
{
  var lFKs={};
  lFKs.cols=[];
  lFKs.entityRefs=[];
  for(var lRowKey in pTable)
  {
       var lRow=pTable[lRowKey];
       for(var lCellKey of pJoinPossibleAttributes)
       {
           var lCell=new String(lRow[lCellKey]);
           if(lCell.startsWith('urn:ngsi-ld:'+pEntityName))
           {
                  if(!lFKs.cols.includes(lCellKey))
                  {
                     lFKs.cols.push(lCellKey); 
                  }
                  if(!lFKs.entityRefs.includes(lCell))
                  {
                     lFKs.entityRefs.push(lCell);
                  }
           }
       }
  }
  return lFKs;
}

/**
 * pEntities - Entities as NGSIJS lib results
 */
function getFkEntitiesTypes(pEntities,pAttribKey)
{
     var lUniques=[];
     for(var lEnt of pEntities)
     {
          var lAtt=lEnt[pAttribKey];
          if(lAtt!=undefined && lAtt.type=='Relationship')
          {
                var lAttVal=lAtt.object;
                if(lAttVal!=undefined)
                {
                       for(var lEntType of configsys.getKnownEntityTypes())
                       {
                            if(lAttVal.startsWith('urn:ngsi-ld:'+lEntType))
                            {
                                 if(!lUniques.includes(lEntType))
                                 {
                                     lUniques.push(lEntType);
                                 }
                            }
                       }
                 }
           }
    }
    return lUniques;
}
/**
 * TODO: Use NGSIJS lib
 */
function loadJoinTable(pEndpoint,pMainEntityName,pEntityName,pJoinAttribute,pFiwareService,pFiwareServicePath,pMainTablePlusSchema,pExtended)
{
  var lSearchRefs;
  if(pJoinAttribute==undefined)
  {
     //Use all possible ones :-(
     var lSchema=gSDM.getEntitySchema(pMainEntityName);
     debug('Possible FKs: '+JSON.stringify(lSchema.getRelationShipFields()));
     lSearchRefs=lSchema.getRelationShipFields();
  }
  else
  {
     lSearchRefs=[pJoinAttribute];
  }
  var lFks=getFkReferences(pMainTablePlusSchema[0],pMainTablePlusSchema[1],pEntityName,lSearchRefs);
  var lIds='';
  if(lFks.cols.length!=1)
  {
     debug('Warning: Was expected one attribute. Founded refrences to theses atributes: '+JSON.stringify(lFks.cols));
  }
  for(var lEntityReference of lFks.entityRefs)
  {
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
                        var lJoinTablePlusSchema=parseToTable(pBody,pExtended);
                        //console.log('Body: '+pBody);
                        //console.log('Join Table '+pEntityName+' ::  size:'+lJoinTablePlusSchema.length);
                        return resolve(joinTable(pMainTablePlusSchema,lJoinTablePlusSchema));
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
 * TODO: O schema da Main table não vem completo!!!  Seguramente, o problema deve-se ao TODO sobre o Schema
 * Join two tables where pMainTable has a FK to the PK (_entity_id) in pJoinTable
 *   [pMainTable] (1) --- (M) [pJoinTable]
 */
function joinTable(pMainTablePlusSchema,pJoinTablePlusSchema)
{
   var lMainTable=pMainTablePlusSchema[0];
   var lJoinTable=pJoinTablePlusSchema[0];
   var lMainSchema=pMainTablePlusSchema[1];
   var lJoinSchema=pJoinTablePlusSchema[1];
   debug('Start the join of Main table with '+utils.describeTable(lMainTable)+' rows and a join table containing '+utils.describeTable(lJoinTable)+' rows ...');
   //console.table(lMainTable);
   //console.table(lJoinTable);
   //debug('Main table Schema:\n'+JSON.stringify(lMainSchema,null,2));
   //debug('Join table Schema:\n'+JSON.stringify(lJoinSchema,null,2));
   //Organize foreign Entities as a dicionary by id
   var lJoinEntitiesDic={};
   for(var lEntityKey in lJoinTable)
   {
       var lJoinEntity=lJoinTable[lEntityKey];
       lJoinEntitiesDic[lJoinEntity._entityId]=lJoinEntity;
       //console.log('Entity['+lJoinEntity._entityId+']='+JSON.stringify(lJoinEntity));
   }
   for(var lEntityKey in lMainTable)
   {
        var lEntity=lMainTable[lEntityKey];
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
                    var lFlatName=lAttribName+'_'+lJoinField;
                    var lTypeJoinField=lJoinSchema[lJoinField];
                    lEntity[lFlatName]=lJoinRecord[lJoinField];
                    lMainSchema[lFlatName]=lTypeJoinField; 
                    //debug('Join: '+lAttribName+' + '+lJoinField+': '+lTypeJoinField);
                  }
               }
            }
        }
   }
   debug('Finish the join getting a table containing '+utils.describeTable(lMainTable)+' rows ...');
   //console.table(lMainTable);
   //debug('Joined Schema:\n'+JSON.stringify(lMainSchema,null,2));
   return [lMainTable,lMainSchema];
}


//TODO: uniformizar com format QL
function temporalDataToTable(pJson,pExtended)
{  
    //console.log('Temporal data for converting to a Table');
    //console.log(JSON.stringify(pJson,null,2));
    var lTable=[];
    var lSchema={};
    //Only these fields are defined. The ones related with the entity are defined while merging with it
    lSchema._timeIndex=schema.TIMESTAMP;
    lSchema._dateTime=schema.DATETIME;
    lSchema._entityId=schema.STRING;

    var i=0;
    for (var lKeyEntity in pJson.results) 
    {
       var lTmp={};// Dict  attr+time/value
       i=i+1;
       var lEntity=pJson.results[lKeyEntity];
       //debug(JSON.stringify(lEntity,null,2));
       var lEntityType=lEntity.type;
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
                geoPropertyToCell(lRow,lSchema,lPropKey,lValueTime,pExtended);
             }
             else if(isTypeProperty(lValueTime.type))
             { 
                //If extended, all field's property are prefixed by its name. Else, only the value is sent.
                //debug(lEntity.id+'   '+lTimeKey+'  '+lPropKey+': '+JSON.stringify(lValueTime,null,2));
                for(var lValueKey in lValueTime)
                {
                  var lFlatName;
                  if(pExtended)
                  {
                     lFlatName=lPropKey+'_'+lValueKey;
                  }
                  else
                  {
                     lFlatName=lPropKey;
                  }
                  var lValue=lValueTime[lValueKey];
                  if(lValueKey=='value')
                  {
                     lRow[lFlatName]=lValue;
                     var lType=gSDM.getTypeOf(lEntityType,lPropKey);
                     if(lType==undefined)
                     {
                        debug('Unknown property '+lPropKey+' in the Schema of '+lEntityType);
                     }
                     else
                     {
                        //debug('Known property '+lPropKey+' in the Schema of '+lEntityType+' schema type: '+lType);
                        lSchema[lFlatName]=lType;
                     }
                  }
                  else if(!pExtended)
                  {
                      //debug('Extended is Off :: Ignore '+lValueKey);
                  }
                  else if(lValueKey.match(/^(instanceId|type)$/))
                  {
                      //debug('known field of type String: '+lValueKey);
                      lRow[lFlatName]=lValue;
                      lSchema[lFlatName]=schema.STRING;
                  }
                  else if(lValueKey.match(/^(observedAt|xxxx)$/))
                  {
                      //debug('kown field of type DateTime: '+lValueKey+'   value='+lValue+'   flatname='+lFlatName);
                      lRow[lFlatName]=lValue;
                      lSchema[lFlatName]=schema.DATETIME;
                  }
                  else
                  {
                      lRow[lFlatName]=lValue;
                      debug('Unknown field: '+lValueKey+' value:'+lValue);
                  }
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
    return [lTable,lSchema];
}

/**
 * This function create a Promise to query the Broker 
 */
function createPromiseQueryEntities(pFiwareService,pFiwareServicePath,pEntityType,pEntityId)
{
   var lBrokerURL=configsys.getBrokerURL(pFiwareService);
   var lConnection = new NGSI.Connection(lBrokerURL);
   var lContext=getNgsiLdEntityContext(pFiwareService,pEntityType);
   var lOptions = {"tenant":configsys.getBrokerTenant(pFiwareService),
                   "@context":lContext,
                   "limit":ENTITY_LIMIT[configsys.getBrokerName(pFiwareService)]
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
/**
 *  Note: The entities are sent with the context prefix
 */
function createPromiseListTypes(pFiwareService)
{
   var lBrokerURL=configsys.getBrokerURL(pFiwareService);
   var lTenant=configsys.getBrokerTenant(pFiwareService);
   var lConnection = new NGSI.Connection(lBrokerURL);
   var lOptions = {
                   "tenant":lTenant,
                   "limit":100
                  };
   debug('Broker URL: '+lBrokerURL);
   debug('Options: '+JSON.stringify(lOptions));
   //Load the type's data
   return lConnection.ld.listTypes(lOptions);
}
function createPromiseGetType(pFiwareService,pEntityType)
{
   var lBrokerURL=configsys.getBrokerURL(pFiwareService);
   var lTenant=configsys.getBrokerTenant(pFiwareService);
   var lContext=getNgsiLdEntityContext(pFiwareService,pEntityType);
   var lConnection = new NGSI.Connection(lBrokerURL);
   var lOptions = {
                   "tenant":lTenant,
                   "@context":lContext,
                   "type":pEntityType,
                   "limit":100
                  };
   debug('Type: '+pEntityType);
   debug('Broker URL: '+lBrokerURL);
   debug('Options: '+JSON.stringify(lOptions));
   //Load the type's data
   return lConnection.ld.getType(lOptions);
}
function createPromiseQueryEntitiesAttributes(pFiwareService,pEntityType,pAttribName)
{
   var lBrokerURL=configsys.getBrokerURL(pFiwareService);
   var lTenant=configsys.getBrokerTenant(pFiwareService);
   var lContext=getNgsiLdEntityContext(pFiwareService,pEntityType);
   var lConnection = new NGSI.Connection(lBrokerURL);
   var lOptions = {
                   "tenant":lTenant,
                   "@context":lContext,
                   "type":pEntityType,
                   "attrs":pAttribName,
                   "limit":100
                  };
   debug('Type: '+pEntityType);
   debug('Broker URL: '+lBrokerURL);
   debug('Options: '+JSON.stringify(lOptions));

   return lConnection.ld.queryEntities(lOptions);
}
