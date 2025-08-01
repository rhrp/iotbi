/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the NGSI-V2 subsystem.
 * 
 */
var configsys = require('./configsys.js');
var utils = require('./utils.js')
var turf = require('turf');
var schema=require('./schema.js');
var NGSI = require('ngsijs');
var debug = require('debug')('iotbi.ngsiv2');

/**
 * Exports
 */
exports.listEntities = listEntities;
exports.createPromiseListEntities = createPromiseListEntities;
exports.createPromiseListTypes = createPromiseListTypes;
exports.createPromiseGetType = createPromiseGetType;
exports.entitiesToTable = entitiesToTable;
exports.entityToRow = entityToRow;
exports.getInvokeFiwareHeaders = getInvokeFiwareHeaders;
exports.getInvokeFiwareOptions =  getInvokeFiwareOptions;
/**
 * Init the Headers for pFiwareService,pFiwareServicePath
 */
function getInvokeFiwareHeaders(pFiwareService,pFiwareServicePath) {
   debug(pFiwareService+'/'+pFiwareServicePath);
   const headers= {};
   var lTenant=configsys.getBrokerTenant(pFiwareService);
   debug('Tenant: '+lTenant)
   headers['fiware-service']=lTenant;
   if(pFiwareServicePath!=null)
   {
       headers['fiware-servicepath']=pFiwareServicePath;
       debug('Set header fiware-servicepath='+pFiwareServicePath);
   }
   return headers;
}

/**
 * Used only for NGSI V2
 */
function getInvokeFiwareOptions(pUrl,pFiwareService,pFiwareServicePath) {
   const options = {
       url: pUrl,
       headers: this.getInvokeFiwareHeaders(pFiwareService,pFiwareServicePath)
    }
    return options;
}


function mergeSchemaRow(pSchema,pSchemaNew)
{
    var lSchema=pSchema==undefined?{}:pSchema;
    for(var lDataField in pSchemaNew)
    {
        if(lSchema[lDataField]==undefined)
        {
            lSchema[lDataField]=pSchemaNew[lDataField];
            //debug('New field: '+lDataField);
        }
        else if(lSchema[lDataField]==pSchemaNew[lDataField])
        {
            //Ok
        }
        else
        {
            //debug('Incompatible field: '+lDataField+'    '+lSchema[lDataField]+' <>  '+pSchemaNew[lDataField]);
        }
    }
    return lSchema;
}
/**
 * Convert an array of Entities to a Table+Schema
 */
function entitiesToTable(pEntities,pExtended)
{
   //debug(JSON.stringify(pEntities,null,2));
   //Init output table
   var lSchema=undefined;
   debug('Extended='+pExtended);
   var lTable=[];
   for (var lEntity of pEntities) 
   {
        try
        {
          var lRegPlusSchema=entityToRow(lEntity,pExtended);
          lSchema=mergeSchemaRow(lSchema,lRegPlusSchema[1]);
          lTable.push(lRegPlusSchema[0]);
        }
        catch(err)
        {
          debug(err.stack || err)
          debug('Error '+err+' while processing:\n'+JSON.stringify(lEntity,null,2) );
          throw err;
        }
   }
   return [lTable,lSchema];
}
/**
 * Convert an entity to a Table Row
 *
 */
function entityToRow(pEntity,pExtended)
{
    var lDoDebug=false;
    //debug('NGSI Object:'+JSON.stringify(pEntity,null,2));
    lSchema={};
    lReg={};
    var lUniqueCols={};
    lSchema._entityId=schema.STRING;
    lReg._entityId=pEntity.id;
    for(var lDataField in pEntity)
    {
        var lColName;
	//debug('Object Id '+pEntity.id+'  :: Prop='+lDataField);
        //Check duplicated cols
        if(lUniqueCols[lDataField.toLowerCase()]!=undefined)
           lColName=lDataField+'_duplicated';
        else
           lColName=lDataField;
        lUniqueCols[lDataField.toLowerCase()]=lDataField;
        // 
	if(lDataField=='id')
	{
            //debug('Entity Id ');
        }
        else if(lDataField=='type')
        {
           //debug('Entity Id ');
        }
        else
        {
             var lEntityAttrib=pEntity[lDataField];
             //debug('Object Id '+pEntity.id+'  :: Prop='+lDataField+' ::  '+JSON.stringify(lEntityAttrib));
             if(('\"'+lEntityAttrib.value+'\"')==JSON.stringify(lEntityAttrib.value)
             || (''+lEntityAttrib.value)==JSON.stringify(lEntityAttrib.value))
	     {
                  var lType=schema.parseType(lEntityAttrib.type);
                  if(lType==undefined || lType==schema.OBJECT)
                  {
                     lType=schema.inferType(lEntityAttrib.value);
                  }
                  lSchema[lColName]=lType;
                  var lValue=schema.parseValue(lEntityAttrib.value,lType);
 		  lReg[lColName]=lValue;
                  //debug('Col: '+lDataField+'   Type='+lType+'  Value='+lValue+'  Original value='+lEntityAttrib.value);
             }
             else if(lEntityAttrib.value['type']=='Point')
             {
                  //debug('Object Id '+pEntity.id+'  :: Prop='+lDataField+' :: Type=Point   Expand value <'+JSON.stringify(lEntityAttrib.value)+'>');
                  var lValue=lEntityAttrib.value[lValueField];
                  var lType=lEntityAttrib.value['type'];
                  lReg[lColName+'_value']=lValue;
                  lSchema[lColName+'_value']=schema.POINT;
                  lReg[lColName+'_type']=lType;
                  lSchema[lColName+'_type']=schema.STRING;
                  for(i in lEntityAttrib.value.coordinates)
                  {
                    var c;
                    if(i==1)
                    {
                       c='lat';
                    }
                    else if(i==0)
                    {
                       c='lon';
                    }
                    else
                    {
                       c=str(i);
                    }
                    lReg[lColName+'_coordinates_'+c]=lEntityAttrib.value.coordinates[i];
                    lSchema[lColName+'_coordinates_'+c]=schema.DOUBLE;
                  }
             }
             else if(lEntityAttrib.value['type']=='MultiLineString')
             {
                  //debug('Object Id '+pEntity.id+'  :: Prop='+lDataField+' :: Type=MultiLineString   Expand value <'+JSON.stringify(lEntityAttrib.value)+'>');
                  var lValue=lEntityAttrib.value[lValueField];
                  var lType=lEntityAttrib.value['type'];
                  lReg[lColName+'_value']=lValue;
                  lSchema[lColName+'_value']=schema.STRING;
                  lReg[lColName+'_type']=lType;
                  lSchema[lColName+'_type']=schema.STRING;
             }
             else if(Array.isArray(lEntityAttrib.value))
             {
                  //var lTmp='Object Id '+pEntity.id+'  :: Prop='+lDataField+' ::  Expand Array <'+JSON.stringify(lEntityAttrib.value)+'>';
                  for(var lValueFieldName in lEntityAttrib.value)
                  {
                        var lValue=lEntityAttrib.value[lValueFieldName];
                        var lType=schema.inferType(lValue);
                        lReg[lColName+'_value_'+lValueFieldName]=lValue;
                        lSchema[lColName+'_value_'+lValueFieldName]=lType;
                        //debug(lTmp+'  Type='+lType+'   Value='+lValue);
                  }
                  lReg[lColName+'_type']='Array['+lType+']';
                  lSchema[lColName+'_type']=schema.STRING;
             }
             else
             {
                  //var lTmp='Object Id '+pEntity.id+'  :: Prop='+lDataField+' ::  Expand value <'+JSON.stringify(lEntityAttrib.value)+'>';
                  for(var lValueField in lEntityAttrib.value)
                  {
                        var lValue=lEntityAttrib.value[lValueField];
                        if(lValue!=undefined)
                        {
                           var lTypeString=lValue['type'];
                           if(lTypeString!=undefined)
                           {
                              var lType=schema.parseType(lTypeString);
                              lReg[lColName+'_value_'+lValueField]=lValue;
                              lSchema[lColName+'_value_'+lValueField]=lType;
                           }
                           else
                           {
                              lReg[lColName+'_value_'+lValueField]=lValue;
                              lSchema[lColName+'_value_'+lValueField]=schema.OBJECT;
                           }
                        }
                        else
                        {
                           lReg[lColName+'_value_'+lValueField]=undefined;
                           lSchema[lColName+'_value_'+lValueField]=schema.OBJECT;
                        }
                        //debug(lTmp+' TypeString='+lTypeString+'  Type='+lType+'   Value='+lValue);
                  }
             }
             if(pEntity[lDataField].type=='geo:json')
             {
                 addCentroid(lReg,lSchema,pEntity,lDataField,pExtended);
                 //debug('Object Id '+pEntity.id+'  :: Prop='+lDataField+' :: addCentroid');
             }
             if(pExtended)
             {
                  var lFieldType=lEntityAttrib['type'];
                  //debug('Extend: '+pEntity.id+' Field: '+lDataField+'  of type :'+lFieldType);
                  for(var lEntityAttribField in lEntityAttrib)
		  {
                        //lReg[lDataField+'_type']=lEntity[lDataField].type;
                        if(lEntityAttribField == 'metadata')
                        {
                             var lMetadata=lEntityAttrib[lEntityAttribField];
                             if(utils.isObject(lMetadata))
                             {
                                for(var lMetadataAttrib in lMetadata)
                                {
                                    var lMetadataTypeString=lMetadata[lMetadataAttrib].type;
                                    var lMetadataType=schema.parseType(lMetadataTypeString);
                                    //debug('Type of metadata '+lMetadataAttrib+' ::  ' +lMetadataTypeString+'='+lMetadataType);                                    
                                    lReg[lColName+'_'+lMetadataAttrib+'_value']=lMetadata[lMetadataAttrib].value;
                                    lReg[lColName+'_'+lMetadataAttrib+'_type']=lMetadata[lMetadataAttrib].type;

                                    lSchema[lColName+'_'+lMetadataAttrib+'_value']=lMetadataType;
                                    lSchema[lColName+'_'+lMetadataAttrib+'_type']=schema.STRING;
                                }
                             }
                             else
                             {
                                  //console.log('Extend ignore '+lEntityAttribField+' expected an object!');
                             }
                        }
                        else if(lEntityAttribField == 'type')
                        { 
                              lReg[lColName+'_'+lEntityAttribField]=lEntityAttrib[lEntityAttribField];
                              lSchema[lColName+'_'+lEntityAttribField]=schema.STRING;
                        }
		        else if(lEntityAttribField != 'value')
		        { 
                              lReg[lColName+'_'+lEntityAttribField]=lEntityAttrib[lEntityAttribField];
                              lSchema[lColName+'_'+lEntityAttribField]=schema.OBJECT;
		        }
                        else
                        {
                              // The value is already added
                              //console.log('Extend ignore '+lEntityAttribField);
                        }
                  }
             }
	}
    }
    if(lDoDebug)
    {
    debug('Table Reg: '+JSON.stringify(lReg,null,2))
    debug('Schema:'+JSON.stringify(lSchema,null,2));
    }
    return [lReg,lSchema];
}
function addCentroid(pTableRow,pTableSchema,pEntity,pDataField,pExtended)
{
   // In order to behave similar to QuantumLeap add this centroid
   var lPoint;
   if(pEntity[pDataField].value==null)
   {
        debug('In Entity '+JSON.stringify(pEntity)+' the value of '+pDataField+' is null');
        lPoint=null;
   }
   else if(pEntity[pDataField].value.type=='Point__force_turf_to_work')
   {
        // Case of a Point
        lPoint=pEntity[pDataField].value;
        //console.log('Point='+JSON.stringify(lPoint));
   }
   else
   {
        // Case of a Polygon
        //ToDo: test with real data
        var lPolygon=pEntity[pDataField].value;
//var maia = require('../../webserver/wwwroot/data/maia.json');
//lPolygon=maia.geojsons.municipio;
        lPoint=turf.centroid(lPolygon,null).geometry;
        //console.log('Turf Centroid='+JSON.stringify(lPoint));
    }
    pTableRow[pDataField+'_centroid']=(lPoint!=null?lPoint.coordinates[1]+', '+lPoint.coordinates[0]:'');
    pTableSchema[pDataField+'_centroid']=schema.STRING;
    if(pExtended)
    {
      pTableRow[pDataField+'_centroid_lat']=(lPoint!=null?lPoint.coordinates[1]:'');
      pTableRow[pDataField+'_centroid_lon']=(lPoint!=null?lPoint.coordinates[0]:'');
      pTableSchema[pDataField+'_centroid_lat']=schema.DOUBLE;
      pTableSchema[pDataField+'_centroid_lon']=schema.DOUBLE;
    }
}


function createPromiseListEntities(pFiwareService,pFiwareServicePath,pExtended,pEntityType,pObjQuery,pOffset,pLimit)
{
   var lBrokerURL=configsys.getBrokerURL(pFiwareService);
   var lTenant=configsys.getBrokerTenant(pFiwareService);
   var lConnection = new NGSI.Connection(lBrokerURL);
   var lOptions = {
                 'service':lTenant,
                 'type':pEntityType,
                 'limit':pLimit==undefined?1000:pLimit,
                 'count':true
                };
   if(pOffset!=undefined)
   {
      lOptions['offset']=pOffset;
   }
   if(pFiwareServicePath!=undefined)
   {
      lOptions['servicepath']=pFiwareServicePath;
   }
   if(pObjQuery!=undefined)
   {
     var lList=['q','attrs'];
     for(var a in pObjQuery)
     {
       if(lList.includes(a) && pObjQuery[a]!=undefined)
       {
         lOptions[a]=pObjQuery[a];
       }
     }
   }
   debug('Broker URL: '+lBrokerURL);
   debug('Options: '+JSON.stringify(lOptions));
   //Load entities
   return lConnection.v2.listEntities(lOptions);
}

function createPromiseListTypes(pFiwareService,pFiwareServicePath)
{
   var lBrokerURL=configsys.getBrokerURL(pFiwareService);
   var lTenant=configsys.getBrokerTenant(pFiwareService);
   var lConnection = new NGSI.Connection(lBrokerURL);
   var lOptions = {
                   "service":lTenant,
                   "limit":100
                  };
   if(pFiwareServicePath!=undefined)
   {
      lOptions['servicepath']=pFiwareServicePath;
   }
   debug('Broker URL: '+lBrokerURL);
   debug('Options: '+JSON.stringify(lOptions));
   //Load the type's data
   return lConnection.v2.listTypes(lOptions);
}
/**
 * Note: the NGSIJs documentation is incomplete. The type parameter must be 'id'
 */
function createPromiseGetType(pFiwareService,pEntityType)
{
   var lBrokerURL=configsys.getBrokerURL(pFiwareService);
   var lTenant=configsys.getBrokerTenant(pFiwareService);
   var lConnection = new NGSI.Connection(lBrokerURL);
   var lOptions = {
                   "service":lTenant,
                   'servicepath':undefined,
                   "id":pEntityType
                  };
   debug('Type: '+pEntityType);
   debug('Broker URL: '+lBrokerURL);
   debug('Options: '+JSON.stringify(lOptions));
   //Load the type's data
   return lConnection.v2.getType(lOptions);
}
/**
 * Return a the table and schema
 */
function listEntities(pEndpoint,pObjQuery,pFiwareService,pFiwareServicePath,pExtended,pEntityType)
{
   return listEntitiesPagined([],pFiwareService,pFiwareServicePath,pExtended,pEntityType,pObjQuery,0,500)
}
/**
 * The listEntities to be recursive called 
 */
function listEntitiesPagined(pEntities,pFiwareService,pFiwareServicePath,pExtended,pEntityType,pObjQuery,pOffset,pLimit)
{
   return createPromiseListEntities(pFiwareService,pFiwareServicePath,pExtended,pEntityType,pObjQuery,pOffset,pLimit)
          .then((result) => {
                               var lTotal=result.count;
                               var lCorrelator=result.correlator;
                               var lEntities=result.results;
                               var lAllEntities=pEntities.concat(lEntities);
                               debug('Correlator:'+lCorrelator);
                               if(lTotal<=pLimit)
                               {
                                   debug('All data in one page.  Total='+lTotal);
                                   return new Promise(function(resolve, reject)
                                          {
                                              var lTable=entitiesToTable(lEntities,pExtended)
                                              resolve(lTable);
                                          });
                               }
                               else if(lEntities.length<pLimit)
                               {
                                   debug('Last page. Records: '+lEntities.length+' of '+lTotal+' Previous loaded: '+lAllEntities.length);
                                   return new Promise(function(resolve, reject)
                                          {
                                              var lTable=entitiesToTable(lAllEntities,pExtended);
                                              resolve(lTable);
                                          });
                               }
                               else
                               {
                                   var lOffset=pOffset+pLimit;
                                   debug('Next page  Total='+lTotal+'  Limit='+pLimit+'   Offset='+lOffset+'   Loaded='+lEntities.length+' Previous loaded: '+lAllEntities.length); 
                                   return listEntitiesPagined(lAllEntities,pFiwareService,pFiwareServicePath,pExtended,pEntityType,pObjQuery,lOffset,pLimit)
                               }
                            })
          .catch((err) => {
                              debug('Error loading current: '+JSON.stringify(err));
                              sendError(res,500,err.name);
                          })
};
