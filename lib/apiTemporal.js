/**
 * API Temporal based on Ficodes NGSI Lib (suports NGSI V1,V2 and LD) - https://github.com/ficodes/ngsijs
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var request = require('request');
var url  = require('url');
var configsys = require('./configsys.js');
var utils = require('./utils.js');
var output = require('./outputTable.js')
var utilsTemporal = require('./utilsTemporal.js');
var ngsildcontext = require('./ngsildcontext.js');
var ngsiv2 = require('./ngsiv2.js');
var ngsildv1 = require('./ngsildv1.js');
var ngsitsdb = require('./ngsitsdb.js');
var schema = require('./schema.js'); 
var debug = require('debug')('iotbi.api.service.temporal');
const HARD_LIMIT = 500000;
const QL_LIMIT=5000;	// QuantumLeap Limit
const TA_LIMIT={};	// Temporal API Limit
TA_LIMIT[configsys.BROKER_ORIONLD]=1000;
TA_LIMIT[configsys.BROKER_SCORPIO]=1000;
TA_LIMIT[configsys.BROKER_STELIO]=100;
const PREFIX_EXT_QL = '';

/**
 * Merge data from Temporal Table (QL or NGSI-LD-Temporal) and Entities Objects (NGSI V2/LD) 
 * The pFuncMergeRow and pFuncEntityToRow functions are specific for the type of NGSI
 * @param pExtended - If true, all properties's metadata are added as: xxx_value, xxx_type, xxx_observedAt, etc
 * @param pAddEntityProps - If true, all entity's properties are added only as xxx_value
 */
function merge(pTableTemporal,pEntities,pFuncMergeRow,pFuncEntityToRow,pExtended,pAddEntityProps)
{
     var lTableTemporal=pTableTemporal[0];
     var lSchemaTemporal=pTableTemporal[1];
     //debug('Start merge '+pEntities.length+' entities with a table with '+utils.describeTable(lTableTemporal)+' extended='+pExtended+' pAddEntityProps='+pAddEntityProps+' ...');
     //debug('Initial Temporal Table: ');
     //console.table(lTableTemporal);
     //debug('Initial Temporal Schema: '+JSON.stringify(lSchemaTemporal,null,2));
     //debug('Entities to merge:');
     //debug(JSON.stringify(pEntities,null,2));

     var lEntity;
     //Organize Entities in a dicionary by id
     //TODO: convert attrs to lower case the method getAttrIgnoreCase()
     var lDictEntities={};
     var lDictTableSchema={};
     for(var lEntityKey in pEntities)
     {
            lEntity=pEntities[lEntityKey];
            lDictEntities[lEntity.id]=lEntity;
            lDictTableSchema[lEntity.id]=pFuncEntityToRow(lEntity,pExtended);
            //debug('add to dict Entity['+lEntity.id+']='+JSON.stringify(lEntity));
     }
     if(pAddEntityProps)
     {
         debug('The attributes of Main entity will be added to temporal data');
     }
     else
     {
         debug('Skip merging temporal data with its Entities')
         //debug('Final temporal table:');
         //console.table(lTableTemporal);
         return findSchema([lTableTemporal,lSchemaTemporal],lDictTableSchema);
     }
     //Continue and adds the cols of the associated Entity 
     var lTotal=0;
     var lTotalNotMatch={};
     var lTotalMatch={};
     //console.log(pEntities);
     for(var lRowKey in lTableTemporal)
     {
             var lRow=lTableTemporal[lRowKey];
             //console.log(lRow._entityId);
             lEntity=lDictEntities[lRow._entityId];
             if(lEntity!=undefined)
             {
		  //debug('Merge :: Row '+lRow._entityId+'    '+JSON.stringify(lEntity));
                  pFuncMergeRow(lRow,lSchemaTemporal,lEntity);
    		  if(pExtended)
                  {
                     var lEntityAsRowPlusSchema=pFuncEntityToRow(lEntity,pExtended);
                     var lEntityAsRow=lEntityAsRowPlusSchema[0];
                     var lEntityAsSchema=lEntityAsRowPlusSchema[1];
                     for(var lEntityAsRowKey in lEntityAsRow)
                     {
                           if(lRow[lEntityAsRowKey]==undefined)
                           {
//RHP
                             var lFlatName=PREFIX_EXT_QL+lEntityAsRowKey;
                             var lTmpType=lEntityAsSchema[lEntityAsRowKey];
                             if(lTmpType==undefined)
                             {
                                  lTmpType=ngsildcontext.getTypeOf(lEntity.type,lEntityAsRowKey);
                                  //debug('Entity: '+lEntity.type+' Attribute: '+lEntityAsRowKey+' SchemaType: '+lTmpType);
                             }
                             else
                             {
                                  //debug('Entity: '+lEntity.type+' Attribute: '+lEntityAsRowKey+' Known Type: '+lTmpType);
                             } 
                             lRow[lFlatName]=lEntityAsRow[lEntityAsRowKey];
                             lSchemaTemporal[lFlatName]=lTmpType;
                           }
                           else
                           {
                              //console.log('Ignore Extend: '+lEntityAsRowKey);
                           }
                     }
                  }
                  var lTmp=lTotalMatch[lRow._entityId];
                  lTotalMatch[lRow._entityId]=(lTmp==undefined?1:lTmp+1);
             }
             else
             {
                  debug('Not Match lRow._entityId=['+lRow._entityId+']');
                  var lTmp=lTotalNotMatch[lRow._entityId];
                  lTotalNotMatch[lRow._entityId]=(lTmp==undefined?1:lTmp+1);
             }
             lTotal=lTotal+1;
     }
     for(var lEntityKey in lTotalMatch)
     {
          debug('Merge :: '+lEntityKey+'   Match='+lTotalMatch[lEntityKey]);
     }
     for(var lEntityKey in lTotalNotMatch)
     {
          debug('Merge :: '+lEntityKey+'   Not Match='+lTotalNotMatch[lEntityKey]);
     }
     //debug('Merged temporal table and its entities:');
     //console.table(lTableTemporal);
     debug('Finishing the merge getting a new table containing '+utils.describeTable(lTableTemporal)+' ...');

     return findSchema([lTableTemporal,lSchemaTemporal],lDictTableSchema);
}
function findSchema(pTablePlusSchema,pDictTableSchema)
{
    debug('Starting find Schema')
    var lTable=pTablePlusSchema[0];
    var lSchema=pTablePlusSchema[1];
    //debug('Table: '+JSON.stringify(lTable,null,2));
    //debug('Schema: '+JSON.stringify(lSchema,null,2));
    if(lTable.length==0)
    {
       debug('The table is empty. Cannot enrich the schema!')
       return pTablePlusSchema;
    }
    var lFirstRow=lTable[0];
    var lEntityId=lFirstRow['_entityId'];
    var lEntitySchema=pDictTableSchema[lEntityId][1]
    debug('Rows: '+lTable.length+'  Cols:'+Object.keys(lFirstRow).length+'   Analyse first Row of entity ID '+lEntityId);
    for(var lColKey in lFirstRow)
    {
        var lSchemaCol=lSchema[lColKey];
        if(lSchemaCol==undefined)
        {
           var lColType=undefined;
           var lCel=lFirstRow[lColKey];
           lColType=lEntitySchema[lColKey];
           if(lColType==undefined || lColType==schema.OBJECT)
           {
              var lTmp=lColType
              lColType=schema.inferType(lFirstRow[lColKey]);
              debug('Table col:'+lColKey+'  Cel: '+JSON.stringify(lCel)+' of type: '+lTmp+' => Infered Schema: '+lColType);
           }
           else
           {
              //debug('Table col:'+lColKey+'  Cel: '+JSON.stringify(lCel)+'   Kown Schema: '+lColType);
           }
           lSchema[lColKey]=lColType;
        }
        else
        {
          //debug('Table col:'+lColKey+'  Schema Ok: '+lSchemaCol);
        }
    }
    //debug('Schema Final: '+JSON.stringify(pTablePlusSchema[1],null,2));
    return pTablePlusSchema;
}

function mergeEntityV1(pTableRow,pEntity)
{
     debug('mergeEntityV1 not implemented');
}

function mergeEntityV2(pTableRow,pEntity)
{
    for(var lRowAttrKey in pTableRow)
    {
         var lRowAttr=pTableRow[lRowAttrKey];
         //console.log('Merge :: Attr  '+pTableRow._entityId+'['+lRowAttrKey+']');
         var lMatchEntityAttr=utilsTemporal.getAttrIgnoreCase(pEntity,lRowAttrKey);
         if(lMatchEntityAttr != undefined)
         {
              //console.log('        Match ::   '+JSON.stringify(lMatchEntityAttr));
              var lEntityAttrType=lMatchEntityAttr.type;
              if(lEntityAttrType!=undefined)
              {
                   pTableRow[lRowAttrKey+'_type']=lEntityAttrType;
              }
         }
         else
         {
              //console.log('        Not Match :: '+lRowAttrKey);
         }
         //console.log(lRow._entityId+'  Row['+lRowAttrKey+']='+JSON.stringify(lRowAttr));
    }
}

/**
 * Merge NGSI-LD temporal table with Entity's props values
 *
 */
function mergeEntityLD(pTemporalTableRow,pTemporalTableSchema,pEntity)
{
    //debug('mergeEntityLD::TemporalTableRow:\n'+JSON.stringify(pTemporalTableRow,null,2));
    //debug('mergeEntityLD::TemporalTableSchema:\n'+JSON.stringify(pTemporalTableSchema,null,2));
    //debug('mergeEntityLD::pEntity:\n'+JSON.stringify(pEntity,null,2));
    for(var lEntityAttribKey in pEntity)
    {
         var lEntityAttrib=pEntity[lEntityAttribKey];
         //debug('mergeEntityLD::Add: '+lEntityAttribKey+' :: '+JSON.stringify(lEntityAttrib));
         var lEntityAttribType=lEntityAttrib['type'];
         var lFlatName=lEntityAttribKey;
         if(lEntityAttribType=='Property')
         {
            if(pTemporalTableRow[lFlatName]!=undefined)
            {
               // The attribute already exists in the Table?!
               lFlatName=lEntityAttribKey+'_value';
            }
            pTemporalTableRow[lFlatName]=lEntityAttrib['value'];
            var lType=ngsildcontext.getTypeOf(pEntity.type,lEntityAttribKey);
            //debug('mergeEntityLD::Attribute: '+lEntityAttribKey+' Type: '+lType);
            pTemporalTableSchema[lFlatName]=lType;
         }
         else if(lEntityAttribType=='Relationship')
         {
            if(pTemporalTableRow[lFlatName]!=undefined)
            {
               // The attribute already exists in the Table?!
               lFlatName=lEntityAttribKey+'_object';
            }
            pTemporalTableRow[lFlatName]=lEntityAttrib['object'];
            //debug('mergeEntityLD::Attribute: '+lEntityAttribKey+' Type: '+schema.STRING);
            pTemporalTableSchema[lFlatName]=schema.STRING;
         }
         else if(lEntityAttribKey.match(/^(id|@context|type)$/))
         {
            //debug('mergeEntityLD::Attribute: '+lEntityAttribKey+' already handled');
         }
         else
         {
            debug('mergeEntityLD::Attribute: '+lEntityAttribKey+'  Ignored');
         }
    }
}

/**
 * Merge QuantumLeap temporal table with Entity's data obtained from Orion/OrionLD
 * ToDo: analyse not match case due to fiwareService-path
 */
function mergeEntitiesToTemporalQL(res,pTemporalTable,pEntityType,pFiwareService,pFiwareServicePath,pFormat,pExtended,pAddEntityProps)
{
        const USE_NEW_LIB=true;

    	// Get data about Entities in Broker
	var lHeaders=ngsiv2.getInvokeFiwareHeaders(pFiwareService,pFiwareServicePath);
        var lEndpoint=configsys.getOrionEndpoint(pFiwareService);
	var lOrionURL=configsys.getBrokerURL(pFiwareService);
        var lOrionNgsiVersion=configsys.getBrokerNGSIVersion(pFiwareService);
	debug('Broker: '+configsys.getBrokerName(pFiwareService)+' URL: '+lOrionURL+' NGSI: '+lOrionNgsiVersion);
	debug('Header:'+JSON.stringify(lHeaders));
	debug('Type:'+pEntityType);

        if(configsys.isOrionLD(pFiwareService))
        {
          debug("Using ngsijs for connecting Orion LD");
          ngsildv1.createPromiseQueryEntities(pFiwareService,pFiwareServicePath,pEntityType)
          .then((result) => output.sendTable(res,merge(pTemporalTable,result.results,mergeEntityLD,ngsildv1.entityToRow,pExtended,pAddEntityProps),pFormat,pEntityType))
         .catch((err) => output.sendError(res,500,err));
        }
        else if(configsys.isOrionV2(pFiwareService) && USE_NEW_LIB)
        {
          debug("Using ngsijs for connecting Orion V2");
          var NGSI = require('ngsijs');
          var lConnection = new NGSI.Connection(lOrionURL);
          lConnection.v2.listEntities({"service":pFiwareService,"type":pEntityType,"limit":1000})
          .then((result) => output.sendTable(res,merge(pTemporalTable,result.results,mergeEntityV2,ngsiv2.entityToRow,pExtended,pAddEntityProps),pFormat,pEntityType))
          .catch((err) => output.sendError(res,500,err));
        }
        else if(configsys.isOrionV2(pFiwareService) && !USE_NEW_LIB)
        {
          debug("Using ocb-sender/ngsi-parser for connectiing Orion V2");
   	  var ocb = require('ocb-sender')
          var ngsi = require('ngsi-parser');

	  ocb.config(lEndpoint,lHeaders)
      	  .then((result) => debug('Config: '+JSON.stringify(result)))
      	  .catch((err) => debug('Config:'+err));

          var lOcbQuery = ngsi.createQuery({
             "type":pEntityType,
            "limit":100});
          ocb.getWithQuery(lOcbQuery,lHeaders)
	  .then((result) => output.sendTable(res,merge(pTemporalTable,result.body,mergeEntityV2,ngsiv2.entityToRow,pExtended,pAddEntityProps),pFormat,pEntityType))
	  .catch((err) => output.sendError(res,500,err));
          /* Limited to 20
           ocb.getEntityListType(pEntityType,lHeaders)
           .then((entities) => output.sendTable(res,merge(pTable,entities),pFormat))
           .catch((err) => output.sendError(res,500,err));
          */
        }
        else
        {
            output.sendError(res,500,'Invalid Orion version config')
        }

        debug('Wait for Orion CB response...');
}

function invokePartQuantumLeap(req,res,pUrl,lEntityType,pFiwareService,pFiwareServicePath,lFormat,lExtended,pOffset,pPageSize)
{
   debug('Invoke:'+pUrl);
   var lOptions=ngsiv2.getInvokeFiwareOptions(pUrl,pFiwareService,pFiwareServicePath);
   //console.log('Options: '+JSON.stringify(lOptions,null,2));
   return new Promise(function(resolve, reject)
   {
     request(lOptions, function (error, response, pBody) {
	try
	{
                debug('Invoking finished');
		if(error!=null)
		{ 
                   return reject(error);
		}
		else
		{
                    debug('statusCode:', response && response.statusCode);
		    if(response.statusCode<200 || response.statusCode>299)
		    {
                        //ToDo: parse body in order to improve the output
                        return reject('Code: '+response.statusCode);
		    }
		    else
		    {
                	//console.log('body:', pBody);
                        var lJson=JSON.parse(pBody);
                        var lTable=ngsitsdb.temporalDataToTable(lJson,lExtended); 
                       return resolve(lTable);
                    }
		}
        }
        catch(ex)
        {
                return reject(ex);
        }
    });
  });
}

/**
 * Recursive invocation
 */
function invokeRequestQuantumLeap(req,res,pUrl,lEntityType,lFiwareService,lFiwareServicePath,lFormat,pExtended,pOffset,pPageSize,pLimit,pTablePlusSchema)
{
   var lUrl=pUrl+"&limit="+pPageSize+"&offset="+pOffset;
   invokePartQuantumLeap(req,res,lUrl,lEntityType,lFiwareService,lFiwareServicePath,lFormat,pExtended,pOffset,pPageSize).then(function(result) {
        //console.log('Successfully made request with result: ', result);
        var lTablePart=result[0];
        var lSchemaPart=result[1];
        debug('Schema: '+JSON.stringify(lSchemaPart));
        debug('Table Part: '+utils.describeTable(lTablePart));
        var lRowIndex=0;
        for(let lRow of lTablePart)
        {
           lRowIndex=lRowIndex+1;
           pTablePlusSchema[0].push(lRow);
           //console.log('  Row '+lRowIndex);
        }
        debug('QuantumLeap: PageSize='+pPageSize+' Offset='+pOffset+'  Rows '+lRowIndex+'  total='+pTablePlusSchema[0].length+'   Limit='+pLimit);
        if(pTablePlusSchema[0].length>pLimit)
        {
           debug('Limit exceded: '+pLimit);
           output.sendError(res,500,'Limit exceded: '+pLimit);
        }
        else if(lRowIndex<pPageSize)
        {
           //Last Page
           //if(pExtended)
           //{
             mergeEntitiesToTemporalQL(res,pTablePlusSchema,lEntityType,lFiwareService,lFiwareServicePath,lFormat,pExtended,pExtended);
           //}
           //else
           //{
           // output.sendTable(res,pTable,lFormat);
           //}
        }
        else
        {
           //Load next page
           invokeRequestQuantumLeap(req,res,pUrl,lEntityType,lFiwareService,lFiwareServicePath,lFormat,pExtended,pOffset+pPageSize,pPageSize,pLimit,pTablePlusSchema);
        }
     })
    .catch(function(err) {
       debug('Failed making request with error: ', err);
       output.sendError(res,500,err)
    });
}

function toArrayOfEntities(pEntity1,pEntity2)
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
/**
 * pTable - is reserved for future recursive implementation for pagination
 */ 
function invokeRequestTemporalAPI(req,res,pConnection,pOptions,pEntityType,pFiwareService,pFiwareServicePath,pFormat,pExtended,pOffset,pPageSize,pLimit,pTable,pJoinTableName)
{
   var pEntityTypes=[];
   pEntityTypes.push(pEntityType);
   if(pJoinTableName!=undefined)
   {
     pEntityTypes.push(pJoinTableName);
   }
   debug('Options:'+JSON.stringify(pOptions,null,2));
   var lEntitiesToLoad=toArrayOfEntities(pEntityType,pJoinTableName)
   debug('lEntitiesToLoad='+JSON.stringify(lEntitiesToLoad));
   ngsildcontext.createPromisesloadNgsiContext(pFiwareService,lEntitiesToLoad)
   .then(function (lNgsiContextData) {
      debug('Loaded Context Data');
      ngsildv1.setNgsiContextData(lNgsiContextData);
      pConnection.ld.queryTemporalEntities(pOptions)
            .then(function (resultTemporal) {
		 //debug('TemporalResult:\n'+JSON.stringify(resultTemporal,null,2));
                 var lTable=ngsildv1.temporalDataToTable(resultTemporal,pExtended);
                 mergeEntitiesToTemporalNGSI(req,res,lTable,pFiwareService,pFiwareServicePath,pEntityType,pJoinTableName,pExtended,pFormat);
            }).catch(function(err) {
                 debug('Temporal Query :: Failed making request with error: ', err);
                 output.sendError(res,500,err)
            });
    }).catch(function(err) {
                 debug('Context loading :: Failed making request with error: ', err);
                 output.sendError(res,500,err)
    });
}

/**
 * Merge NGSI-LD temporal table with Entity's data obtained from OrionLD/Scorpio/Stellio
 * Merge Temporal data and Entities' data and then, if asked, join a table
 * This is important in order to get the relationship field that will enable the join operation
 */
function mergeEntitiesToTemporalNGSI(req,res,pTemporalTable,pFiwareService,pFiwareServicePath,pEntityType,pJoinTableName,pExtended,pFormat)
{
        ngsildv1.createPromiseQueryEntities(pFiwareService,pFiwareServicePath,pEntityType)
        .then((resultEntities) => {
                           var lAddEntityProps=pJoinTableName!=undefined;
                           //console.log('Temporal Table');
                           //console.table(pTemporalTable);
                           var lMergedTable=merge(pTemporalTable,resultEntities.results,mergeEntityLD,ngsildv1.entityToRow,pExtended,lAddEntityProps);
                           //console.log('Merged Table');
                           //console.table(lMergedTable);
                           //console.log('Loaded Entities from the Broker');
                           //console.log(JSON.stringify(resultEntities.results,null,2));
                           if(pJoinTableName==undefined)
                           {
                              output.sendTable(res,lMergedTable,pFormat,pEntityType);
                           }
                           else
                           {
                              loadAndJoinTable(req,res,lMergedTable,pFiwareService,pFiwareServicePath,pEntityType,pJoinTableName,pExtended,pFormat);
                           }
                         })
        .catch((err) => output.sendError(res,500,err));
}
/**
 * Join a table
 */
function loadAndJoinTable(req,res,pMasterTable,pFiwareService,pFiwareServicePath,pMainEntityType,pJoinTableName,pExtended,pFormat)
{
   //console.log('Master table');
   //console.table(pMasterTable);
   ngsildv1.createPromiseQueryEntities(pFiwareService,pFiwareServicePath,pJoinTableName)
          .then((resultEntities) => {
                             var lJoinTable=ngsildv1.entitiesToTable(resultEntities.results,pExtended);
                             //console.log('Loaded Entities from the Broker to join');
                             //console.log(JSON.stringify(resultEntities.results,null,2));
                             //console.log('Join table '+pJoinTableName);
                             //console.table(lJoinTable);
                             var lJoinedTable=ngsildv1.joinTable(pMasterTable,lJoinTable)
                            output.sendTable(res,lJoinedTable,pFormat,pMainEntityType)})
          .catch((err) => output.sendError(res,500,err));
}
exports.service = function(req,res,next)
{
   //Path params
   var lEntityType=req.params.entityType;
   var lEntityId=(req.params.entityId==undefined?null:req.params.entityId);
   var lFiwareService=req.params.fiwareService;
   //Query params
   var lUrlParts = url.parse(req.url, true);
   var lQuery = lUrlParts.query;
   var lAttrs=lQuery.attrs;
   var lIdPattern=lQuery.idPattern;
   var lCoords=lQuery.coords;
   var lGeorel=lQuery.georel;
   var lMinDistance=lQuery.minDistance;
   var lMaxDistance=lQuery.maxDistance;
   var lLimit=lQuery.limit;
   var lFromDate=utilsTemporal.parseFromDate(lQuery.fromDate);
   var lToDate=utilsTemporal.parseToDate(lQuery.toDate);
   var lFiwareServicePath=(lQuery.fiwareServicePath==undefined?'/*':lQuery.fiwareServicePath);
   var lExtended=(lQuery.extended!=undefined);
   var lJoinTable=(lQuery.join==undefined?null:lQuery.join);  // NGSI-LD

   // Format
   var lFormat= utils.getParamFormat(req); 

   if(lLimit==undefined)
   {
        lLimit=HARD_LIMIT;
   }
   if(lLimit>HARD_LIMIT)
   {
        var lError='The limit is limited to '+HARD_LIMIT;
        res.status(413).json({'description':lError});
        debug(lError);
        return;
   }
   if(configsys.isBrokerSuportsTemporalAPI(lFiwareService))
   {
        debug('Using NGSI-LD Temporal API');
        serviceNGSITemporalAPI(req,res,lFiwareService,lFiwareServicePath,lEntityType,lEntityId,lAttrs,lIdPattern,lFromDate,lToDate,lCoords,lMinDistance,lMaxDistance,lGeorel,lFormat,lExtended,lLimit,lJoinTable);
   }
   else if(configsys.getQuantumLeapServerOk(lFiwareService))
   {
        debug('Using NGSI-TSDB API');
        serviceQuantumLeap(req,res,lFiwareService,lFiwareServicePath,lEntityType,lEntityId,lAttrs,lIdPattern,lFromDate,lToDate,lCoords,lMinDistance,lMaxDistance,lGeorel,lFormat,lExtended,lLimit);
   }
   else
   {
        var lError='Invalid Service '+lFiwareService;
        res.status(404).json({'description':lError});
        return;
   }
}

function serviceQuantumLeap(req,res,pFiwareService,pFiwareServicePath,pEntityType,pEntityId,pAttrs,pIdPattern,pFromDate,pToDate,pCoords,pMinDistance,pMaxDistance,pGeorel,pFormat,pExtended,pLimit)
{
   var lServer=configsys.getQuantumLeapHost(pFiwareService);
   var lPort=configsys.getQuantumLeapPort(pFiwareService);
   var lUrl='http://'+lServer+':'+lPort+'/v2/attrs'
           +'?type='+pEntityType
           +(pEntityId!=null?'&id='+pEntityId:'')
           +(pAttrs!=null?'&attrs='+pAttrs:'')
           +(pIdPattern!=null?'&idPattern='+pIdPattern:'')
           +(pFromDate!=null?'&fromDate='+pFromDate:'')
           +(pToDate!=null?'&toDate='+pToDate:'');
   lUrl=utils.addGeoLocation(lUrl,null,pCoords,pMinDistance,pMaxDistance,pGeorel);

   invokeRequestQuantumLeap(req,res,lUrl,pEntityType,pFiwareService,pFiwareServicePath,pFormat,pExtended,0,QL_LIMIT,pLimit,[[],{}])
   debug('QuantumLeap Service Ok!');
}
function serviceNGSITemporalAPI(req,res,pFiwareService,pFiwareServicePath,pEntityType,pEntityId,pAttrs,pIdPattern,pFromDate,pToDate,pCoords,pMinDistance,pMaxDistance,pGeorel,pFormat,pExtended,pLimit,pJoinTable)
{
   debug('Start serving serviceNGSITemporalAPI...');
   var lServer=configsys.getBrokerHost(pFiwareService);
   var lPort=configsys.getBrokerPort(pFiwareService);
   var lTenant=configsys.getBrokerTenant(pFiwareService);
   var lContext=ngsildv1.getNgsiLdEntityContext(pFiwareService,pEntityType)
   var NGSI = require('ngsijs');
   var lConnection = new NGSI.Connection('http://'+lServer+':'+lPort);
   var lOptions = {"tenant":lTenant,
                   "@context":lContext,
		   "limit":TA_LIMIT[configsys.getBrokerName(pFiwareService)],
                   "type":pEntityType, 
                   "timeproperty":"observedAt",  // This is the default value. Thus, just to remenber!
		    //"temporalValues":true,     // Aparentemente, é ignorado no Scorpio, mas no Stellio não!   Se true, a informação vem simplificada sem instanceId
                    "temporalValues":false,
		    "sysAttrs":true
                  };
   // Add Temporal parameters to options
   utilsTemporal.addTemporalParams(lOptions,pFromDate,pToDate)

   if(pAttrs!=null) {
	lOptions.attrs=pAttrs;
   }
   var lTable=[];
   invokeRequestTemporalAPI(req,res,lConnection,lOptions,pEntityType,pFiwareService,pFiwareServicePath,pFormat,pExtended,0,TA_LIMIT,pLimit,lTable,pJoinTable)
   debug('NGSI Temporal Service Ok!');
}
