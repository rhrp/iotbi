#!/usr/bin/env node

var request = require('request');
var url  = require('url');
var apiConfig = require('./apiConfig.js');
var utils = require('./utils.js')
var turf = require('turf');

// Added prefix. Only for debug purposes ;-)
const PREFIX_EXT_O = '';

// Used URL @context
var gNgsiContext;
var gNgsiTypeHandle={};
gNgsiTypeHandle['fiware:verified']=__typeFiwareVerifiedToCell;

/**
 * Exports
 */
exports.listEntities = listEntities;
exports.entitiesToTable = entitiesToTable;
exports.entityToRow = entityToRow;
exports.joinTable = joinTable;
/**
 * List the Entities by invoking Orion LD
 */
function listEntities(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pTableNameJoin)
{
   //return loadMainTable(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pTableNameJoin);
   return  loadNGSIContextObject(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pTableNameJoin);
}

/**
 * Get the @context from Config
 * This module is prepaared for multiple @context.
 * Thus, the config is converted to an array
 */
function getNgsiLdContexts(pFiwareService)
{
   var lNgsiContext=apiConfig.getOrionNgsiLdContext(pFiwareService);
   console.log("NGSI Context:"+lNgsiContext);
   return [lNgsiContext];
}
/**
 * Load the @context data structures to the global gNgsiContext
 */
function loadNGSIContextObject(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pTableNameJoin)
{
   gNgsiContext={};
   var lPromises=[];
   var lUrls=getNgsiLdContexts(pFiwareService);
   for(var lUrl of lUrls)
   {
      lPromises.push(createPromiseLoadNGSIContextObject(lUrl));
   }
   return Promise.all(lPromises).then(function(values) {
        console.log('Promise.all:'+values.length);
        for(var i=0;i<values.length;i++)
        {
            var lUrl=lUrls[i];
            var lContext=values[i];
            gNgsiContext[lUrl]=lContext;
            //console.log(lUrl+'  ===  '+JSON.stringify(lContext));
            console.log(lUrl+'  ===  '+lContext);
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
   console.log('URL Context='+pUrl);
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
                   console.error('error:', error);
                   return reject({'step':'Loading Context','description':error});
                }
                else
                {
                   console.log('statusCode:', response && response.statusCode);
                   if(response.statusCode<200 || response.statusCode>299)
                   {
                        //console.log('Body: '+pBody);
                        return reject({'step':'Loading Context','HTTP Code':response.statusCode});
                   }
                   else
                   {
                        //gNgsiContext[lUrl]=JSON.parse(pBody);
                        //console.log('gNgsiContext: '+JSON.stringify(gNgsiContext));
                        //return resolve(loadMainTable(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pTableNameJoin));
                        return resolve(JSON.parse(pBody));
                   }
                }
        }
        catch(ex)
        {
               console.log(ex);
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
    for (var lCtx in gNgsiContext)
    {
        var lNgsiContextItem=gNgsiContext[lCtx]['@context'];
        //console.log('Find:<'+pType+'> in <'+lCtx+'>')
        //for(var lCtxProp in lNgsiContextItem)
        //{
        //      var lNgsiType=lNgsiContextItem[lCtxProp];
        //    console.log('    Find:<'+pType+'> in <'+lCtxProp+'> = '+lNgsiType);
        //}
        var lCtxType=lNgsiContextItem[pType];
        if(lCtxType!=undefined)
        {
           //console.log('Type <'+pType+'> known in ontology as <'+lCtxType+'> found in <'+lCtx+'>');
           return lCtxType;
        }
    }
    return undefined;
}
function isTypeFiware(pType)
{
    var lType=findContextType(pType);
   return lType!=undefined && lType.startsWith('fiware:');
}
function isTypeJsonLdKeyword(pType)
{
    var lType=findContextType(pType);
   return lType!=undefined && ('@'+pType)==lType;
}

/**
 * Get the entities as a table.  Main table because may required a joined table
 */
function loadMainTable(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pTableNameJoin)
{
   var lTableName=pObjQuery.type;

   var lUrl=pEndpoint+'/entities?type='+pObjQuery.type
           +(pObjQuery.id!=undefined?'&id='+pObjQuery.id:'')
	   //+(lAttrs!=null?'&attrs='+lAttrs:'')
	   //+(lIdPattern!=null?'&idPattern='+lIdPattern:'')
	   //+(lLimit!=null?'&limit='+lLimit:'');
   //TODO lUrl=utils.addGeoLocation(lUrl,null,lCoords,lMinDistance,lMaxDistance,lGeorel); 
   return new Promise(function(resolve, reject)
   {
     var lOptions=utils.getInvokeFiwareOptions(lUrl,pFiwareService,true,pFiwareServicePath);
     //Init output table
     request(lOptions, function (error, response, pBody) {
        try
        {
                //console.log('@context: '+JSON.stringify(gNgsiContext));
                console.log('['+pFiwareService+']['+pFiwareServicePath+'] Invoke:'+lUrl+'   Header: '+JSON.stringify(lOptions.headers));
                if(error!=null)
                {
                   console.error('error:', error);
                   return reject({'description':error});
                }
                else
                {
                   console.log('statusCode:', response && response.statusCode);
                   if(response.statusCode<200 || response.statusCode>299)
                   {
                        console.log('Body: '+pBody);
                        return reject({'step':'Loading Main Table','Code':response.statusCode});
                   }
                   else
                   {
			var lTable=parseToTable(pBody,pExtended);
                        console.log('Main Table '+lTableName+' ::  size:'+lTable.length);
                        if(pTableNameJoin==undefined)
                        {
                            return resolve(lTable);
                        }
                        console.log('JOIN: '+pTableNameJoin);
                        return resolve(loadJoinTable(pEndpoint,pTableNameJoin,pFiwareService,pFiwareServicePath,lTable,pExtended));
                   }
                }
         }
         catch(ex)
         {
                console.log(ex);
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
  console.log('body:', pBody); 
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
        else if(pEntity[lDataField].type=='GeoProperty')   // in NGSI-V" is 'geo:json'
        {
	   //In order to behave similar to QuantumLeap add this centroid
           var lPoint=null;
	   if(pEntity[lDataField].value.type=='Point__force_turf_to_work')
           {
		// Case of a Point
		lPoint=pEntity[lDataField].value;
		//console.log('Point='+JSON.stringify(lPoint));
	   }
	   else
           {
		// Case of a Polygon - Use Turf for calculate the Centroid
		//ToDo: test with real data
		var lPolygon=pEntity[lDataField].value;
//var maia = require('../../webserver/wwwroot/data/maia.json');
//lPolygon=maia.geojsons.municipio;
		lPoint=turf.centroid(lPolygon,null).geometry;
		//console.log('Turf Centroid='+JSON.stringify(lPoint));
           }
	   lReg[lDataField+'_centroid']=lPoint.coordinates[1]+', '+lPoint.coordinates[0];
           if(pExtended)
           {
             lReg[lDataField+'_centroid_lat']=(lPoint!=null?lPoint.coordinates[1]:'');
             lReg[lDataField+'_centroid_lon']=(lPoint!=null?lPoint.coordinates[0]:'');
           }
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
             console.log('Ignored Type:'+lEntityAttrib.type+'  Attrib: ['+lDataField+'] -> '+JSON.stringify(lEntityAttrib));
        }
    }
    return lReg;
}


function __typeFiwareVerifiedToCell(pReg,pPropName,lPropertyField,pProperty)
{
   pReg[PREFIX_EXT_O+pPropName+'_'+lPropertyField]=pProperty[lPropertyField].value;
}
function propertyToCell(pReg,pPropName,pProperty,pExtended)
{
      //console.log('Property: ['+pPropName+'] -> '+JSON.stringify(pProperty));
      addValue(1,pReg,pPropName,pProperty.value)
      if(pExtended)
      {
            //console.log('Extend Property '+pPropName);
            for(var lPropertyField in pProperty)
            {
               var lCtxType=findContextType(lPropertyField);
               var lIsTypeFiware=isTypeFiware(lPropertyField);
               var lIsTypeJsonLdKeyword=isTypeJsonLdKeyword(lPropertyField);
               var lTmp='    Attrib:'+lPropertyField+'   Type: <'+lCtxType+'> TypeFiware='+lIsTypeFiware+' TypeJsonLdKeyword='+lIsTypeJsonLdKeyword+' ::  ';

               if(lPropertyField=='type' || lPropertyField == 'value')
               { 
                   //console.log(lTmp+'Extend Ignore '+pPropName+'.'+lPropertyField);
               }
               else if(lIsTypeFiware)
               {
                   var lHandler=gNgsiTypeHandle[lCtxType];
                   if(lHandler==undefined)
                   {
                       //console.log(lTmp+'Extend '+pPropName+'.'+lPropertyField+'  with default Handler');
                       lReg[PREFIX_EXT_O+pPropName+'_'+lPropertyField]=pProperty[lPropertyField];
                   }
                   else
                   {
                       //console.log(lTmp+'Extend '+pPropName+'.'+lPropertyField+'  with kown Handler');
                       lHandler(pReg,pPropName,lPropertyField,pProperty);
                   }
               }
               else
               {
                   //console.log(lTmp+'Extend '+pPropName+'.'+lPropertyField);
                   lReg[PREFIX_EXT_O+pPropName+'_'+lPropertyField]=pProperty[lPropertyField];
               }
            }
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

function loadJoinTable(pEndpoint,pEntityName,pFiwareService,pFiwareServicePath,pMainTable,pExtended)
{
  var lIds=''; 
  for(var lEntityReference of getFkReferences(pMainTable,pEntityName))
  {
     //console.log('Filter FK: '+lEntityReference);
     lIds=lIds==''?'&id='+lEntityReference:lIds+','+lEntityReference;
  }
  console.log('Filter ids='+lIds);    

  var lUrl=pEndpoint+'/entities?type='+pEntityName+lIds;

  return new Promise(function(resolve, reject)
  {
     var lOptions=utils.getInvokeFiwareOptions(lUrl,pFiwareService,true,pFiwareServicePath);
     //Init output table
     var lTable=[];
     request(lOptions, function (error, response, pBody) {
        try
        {
                console.log('['+pFiwareService+']['+pFiwareServicePath+'] Invoke:'+lUrl+'   Header: '+JSON.stringify(lOptions.headers));
                if(error!=null)
                {
                   console.error('error:', error);
                   return reject({'step':'Loading Join Table '+pEntityName,'description':error});
                }
                else
                {
                   console.log('statusCode:', response && response.statusCode);
                   if(response.statusCode<200 || response.statusCode>299)
                   {
                        console.log('Body: '+pBody);
                        return reject({'step':'Loading Join Table '+pEntityName,'HTTP Code':response.statusCode});
                   }
                   else
                   {
                        var lJoinTable=parseToTable(pBody,pExtended);
                        console.log('Join Table '+pEntityName+' ::  size:'+lJoinTable.length);
                        return resolve(joinTable(pMainTable,lJoinTable));
                   }
                }
         }
         catch(ex)
         {
                console.log(ex);
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
   return pMainTable;
}
