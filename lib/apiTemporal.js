/**
 * API Temporal based on Ficodes NGSI Lib (suports NGSI V1,V2 and LD) - https://github.com/ficodes/ngsijs
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var request = require('request');
var url  = require('url');
var apiConfig = require('./apiConfig.js');
var utils = require('./utils.js');
var utilsTemporal = require('./utilsTemporal.js');
var ngsildcontext = require('./ngsildcontext.js');
var ngsiv2 = require('./ngsiv2.js');
var ngsildv1 = require('./ngsildv1.js');
var ngsitsdb = require('./ngsitsdb.js');
var debug = require('debug')('iotbi.api.service.temporal');
const HARD_LIMIT = 500000;
const QL_LIMIT=5000;	// QuantumLeap Limit
const TA_LIMIT={};	// Temporal API Limit
TA_LIMIT[apiConfig.BROKER_ORIONLD]=1000;
TA_LIMIT[apiConfig.BROKER_SCORPIO]=1000;
TA_LIMIT[apiConfig.BROKER_STELIO]=100;
const PREFIX_EXT_QL = '';

/**
 * Merge data from Temporal Table (QL or NGSI-LD-Temporal) and Entities Objects (NGSI V2/LD) 
 * The pFuncMergeRow  and pFuncEntityToRow functions are specific for the type of NGSI
 */
function merge(pTable,pEntities,pFuncMergeRow,pFuncEntityToRow,pExtended)
{
     debug('Start merge '+pEntities.length+' entities with a table with '+utils.describeTable(pTable)+' ...');
     //console.log('Initial table');
     //console.table(pTable);
     //console.log('Entities to merge');
     //console.log(JSON.stringify(pEntities,null,2));

     var lEntity;
     //Organize Entities in a dicionary by id
     //TODO: convert attrs to lower case the method getAttrIgnoreCase()
     var lDictEntities={};
     for(var lEntityKey in pEntities)
     {
            lEntity=pEntities[lEntityKey];
            lDictEntities[lEntity.id]=lEntity;
            //console.log('add to dict Entity['+lEntity.id+']='+JSON.stringify(lEntity));
     }
     var lTotal=0;
     var lTotalNotMatch={};
     var lTotalMatch={};
     //console.log(pEntities);
     for(var lRowKey in pTable)
     {
             var lRow=pTable[lRowKey];
             //console.log(lRow._entityId);
             lEntity=lDictEntities[lRow._entityId];
             if(lEntity!=undefined)
             {
		  //console.log('Merge :: Row '+lRow._entityId+'    '+JSON.stringify(lEntity));
                  pFuncMergeRow(lRow,lEntity);
    		  if(pExtended)
                  {
                     var lEntityAsRow=pFuncEntityToRow(lEntity,pExtended)
                     for(var lEntityAsRowKey in lEntityAsRow)
                     {
                           if(lRow[lEntityAsRowKey]==undefined)
                           {
                             //console.log('Extend: '+lEntityAsRowKey);
                             lRow[PREFIX_EXT_QL+lEntityAsRowKey]=lEntityAsRow[lEntityAsRowKey];
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
     //console.log('Merged table');
     //console.table(pTable);
     debug('Finishing the merge getting a new table containing '+utils.describeTable(pTable)+' ...');
     return pTable;
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
function mergeEntityLD(pTableRow,pEntity)
{
    for(var lRowAttrKey in pTableRow)
    {
         var lRowAttr=pTableRow[lRowAttrKey];
         //console.log('Merge :: Attr  '+pTableRow._entityId+'['+lRowAttrKey+']');
         var lMatchEntityAttr=utilsTemporal.getAttrIgnoreCase(pEntity,lRowAttrKey);
         if(lMatchEntityAttr != undefined)
         {
              //console.log('   Match ::   '+lRowAttrKey+' == '+JSON.stringify(lMatchEntityAttr));
              for(var lMatchEntityAttrKey in lMatchEntityAttr)
              {
                 //console.log('     Add: '+lMatchEntityAttrKey);
                 pTableRow[lRowAttrKey+'_'+lMatchEntityAttrKey]=lMatchEntityAttr[lMatchEntityAttrKey];
              }
         }
         else
         {
              //console.log('   Not Match :: '+lRowAttrKey);
         }
    }
}

/**
 * Merge QuantumLeap temporal table with Entity's data obtained from Orion/OrionLD
 * ToDo: analyse not match case due to fiwareService-path
 */
function mergeEntitiesToTemporalQL(res,pTemporalTable,pEntityType,pFiwareService,pFiwareServicePath,pFormat,pExtended)
{
        const USE_NEW_LIB=true;

    	// Get data about Entities in Broker
	var lHeaders=ngsiv2.getInvokeFiwareHeaders(pFiwareService,pFiwareServicePath);
        var lEndpoint=apiConfig.getOrionEndpoint(pFiwareService);
	var lOrionURL=apiConfig.getBrokerURL(pFiwareService);
        var lOrionNgsiVersion=apiConfig.getBrokerNGSIVersion(pFiwareService);
	debug('Broker: '+apiConfig.getBrokerName(pFiwareService)+' URL: '+lOrionURL+' NGSI: '+lOrionNgsiVersion);
	debug('Header:'+JSON.stringify(lHeaders));
	debug('Type:'+pEntityType);
 

        if(apiConfig.isOrionLD(pFiwareService))
        {
          debug("Using ngsijs for connecting Orion LD");
          ngsildv1.createPromiseQueryEntities(pFiwareService,pFiwareServicePath,pEntityType)
          .then((result) => utils.sendTable(res,merge(pTemporalTable,result.results,mergeEntityLD,ngsildv1.entityToRow,pExtended),pFormat))
         .catch((err) => utils.sendError(res,500,err));
        }
        else if(apiConfig.isOrionV2(pFiwareService) && USE_NEW_LIB)
        {
          debug("Using ngsijs for connecting Orion V2");
          var NGSI = require('ngsijs');
          var lConnection = new NGSI.Connection(lOrionURL);
          lConnection.v2.listEntities({"service":pFiwareService,"type":pEntityType,"limit":1000})
          .then((result) => utils.sendTable(res,merge(pTemporalTable,result.results,mergeEntityV2,ngsiv2.entityToRow,pExtended),pFormat))
          .catch((err) => utils.sendError(res,500,err));
        }
        else if(apiConfig.isOrionV2(pFiwareService) && !USE_NEW_LIB)
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
	  .then((result) => utils.sendTable(res,merge(pTemporalTable,result.body,mergeEntityV2,ngsiv2.entityToRow,pExtended),pFormat))
	  .catch((err) => utils.sendError(res,500,err));
          /* Limited to 20
           ocb.getEntityListType(pEntityType,lHeaders)
           .then((entities) => utils.sendTable(res,merge(pTable,entities),pFormat))
           .catch((err) => utils.sendError(res,500,err));
          */
        }
        else
        {
            utils.sendError(res,500,'Invalid Orion version config')
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
function invokeRequestQuantumLeap(req,res,pUrl,lEntityType,lFiwareService,lFiwareServicePath,lFormat,pExtended,pOffset,pPageSize,pLimit,pTable)
{
   var lUrl=pUrl+"&limit="+pPageSize+"&offset="+pOffset;
   invokePartQuantumLeap(req,res,lUrl,lEntityType,lFiwareService,lFiwareServicePath,lFormat,pExtended,pOffset,pPageSize).then(function(result) {
        //console.log('Successfully made request with result: ', result);
        lTablePart=result;
        var lRowIndex=0;
        for(let lRow of lTablePart)
        {
           lRowIndex=lRowIndex+1;
           pTable.push(lRow);
           //console.log('  Row '+lRowIndex);
        }
        debug('QuantumLeap: PageSize='+pPageSize+' Offset='+pOffset+'  Rows '+lRowIndex+'  total='+pTable.length+'   Limit='+pLimit);
        if(pTable.length>pLimit)
        {
           debug('Limit exceded: '+pLimit);
           utils.sendError(res,500,'Limit exceded: '+pLimit);
        }
        else if(lRowIndex<pPageSize)
        {
           //Last Page
           if(pExtended)
           {
             mergeEntitiesToTemporalQL(res,pTable,lEntityType,lFiwareService,lFiwareServicePath,lFormat,pExtended);
           }
           else
           {
             utils.sendTable(res,pTable,lFormat);
           }
        }
        else
        {
           //Load next page
           invokeRequestQuantumLeap(req,res,pUrl,lEntityType,lFiwareService,lFiwareServicePath,lFormat,pExtended,pOffset+pPageSize,pPageSize,pLimit,pTable);
        }
     })
    .catch(function(err) {
       debug('Failed making request with error: ', err);
       utils.sendError(res,500,err)
    });
}

/**
 * pTable - is reserved for future recursive implementation for pagination
 */ 
function invokeRequestTemporalAPI(req,res,pConnection,pOptions,pEntityType,pFiwareService,pFiwareServicePath,pFormat,pExtended,pOffset,pPageSize,pLimit,pTable,pJoinTableName)
{
   //console.log('Options:'+JSON.stringify(pOptions,null,2));
   ngsildcontext.createPromisesloadNgsiContext(pFiwareService)
   .then(function (lNgsiContextData) {
      ngsildv1.setNgsiContextData(lNgsiContextData);
      pConnection.ld.queryTemporalEntities(pOptions)
            .then(function (resultTemporal) {
		 //console.log('resultTemporal');
		 //console.log('TemporalResult:'+JSON.stringify(resultTemporal,null,2));
                 var lTable=ngsildv1.temporalDataToTable(resultTemporal,pExtended);
                 mergeEntitiesToTemporalNGSI(req,res,lTable,pFiwareService,pFiwareServicePath,pEntityType,pJoinTableName,pExtended,pFormat);
            }).catch(function(err) {
                 debug('Temporal Query :: Failed making request with error: ', err);
                 utils.sendError(res,500,err)
            });
    }).catch(function(err) {
                 debug('Context loading :: Failed making request with error: ', err);
                 utils.sendError(res,500,err)
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
                           var lMergedTable=merge(pTemporalTable,resultEntities.results,mergeEntityLD,ngsildv1.entityToRow,pExtended);
                           //console.log('Temporal Table');
                           //console.table(pTemporalTable);
                           //console.log('Merged Table');
                           //console.table(lMergedTable);
                           //console.log('Loaded Entities from the Broker');
                           //console.log(JSON.stringify(resultEntities.results,null,2));
                           if(pJoinTableName==undefined)
                           {
                              utils.sendTable(res,lMergedTable,pFormat);
                           }
                           else
                           {
                              loadAndJoinTable(req,res,lMergedTable,pFiwareService,pFiwareServicePath,pJoinTableName,pExtended,pFormat);
                           }
                         })
        .catch((err) => utils.sendError(res,500,err));
}
/**
 * Join a table
 */
function loadAndJoinTable(req,res,pMasterTable,pFiwareService,pFiwareServicePath,pJoinTableName,pExtended,pFormat)
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
                             utils.sendTable(res,lJoinedTable,pFormat)})
          .catch((err) => utils.sendError(res,500,err));
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
   var lFormat= utils.getFormat(req); 

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
   if(apiConfig.isBrokerSuportsTemporalAPI(lFiwareService))
   {
        debug('Using NGSI-LD Temporal API');
        serviceNGSITemporalAPI(req,res,lFiwareService,lFiwareServicePath,lEntityType,lEntityId,lAttrs,lIdPattern,lFromDate,lToDate,lCoords,lMinDistance,lMaxDistance,lGeorel,lFormat,lExtended,lLimit,lJoinTable);
   }
   else if(apiConfig.getQuantumLeapServerOk(lFiwareService))
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
   var lServer=apiConfig.getQuantumLeapHost(pFiwareService);
   var lPort=apiConfig.getQuantumLeapPort(pFiwareService);
   var lUrl='http://'+lServer+':'+lPort+'/v2/attrs'
           +'?type='+pEntityType
           +(pEntityId!=null?'&id='+pEntityId:'')
           +(pAttrs!=null?'&attrs='+pAttrs:'')
           +(pIdPattern!=null?'&idPattern='+pIdPattern:'')
           +(pFromDate!=null?'&fromDate='+pFromDate:'')
           +(pToDate!=null?'&toDate='+pToDate:'');
   lUrl=utils.addGeoLocation(lUrl,null,pCoords,pMinDistance,pMaxDistance,pGeorel);

   invokeRequestQuantumLeap(req,res,lUrl,pEntityType,pFiwareService,pFiwareServicePath,pFormat,pExtended,0,QL_LIMIT,pLimit,[])
   debug('QuantumLeap Service Ok!');
}
function serviceNGSITemporalAPI(req,res,pFiwareService,pFiwareServicePath,pEntityType,pEntityId,pAttrs,pIdPattern,pFromDate,pToDate,pCoords,pMinDistance,pMaxDistance,pGeorel,pFormat,pExtended,pLimit,pJoinTable)
{
   var lServer=apiConfig.getBrokerHost(pFiwareService);
   var lPort=apiConfig.getBrokerPort(pFiwareService);
   var lTenant=apiConfig.getBrokerTenant(pFiwareService);
   var lContext=ngsildv1.getNgsiLdEntityContext(pFiwareService,pEntityType)
   var NGSI = require('ngsijs');
   var lConnection = new NGSI.Connection('http://'+lServer+':'+lPort);
   var lOptions = {"tenant":lTenant,
                   "@context":lContext,
		   "limit":TA_LIMIT[apiConfig.getBrokerName(pFiwareService)],
                   "type":pEntityType, 
		    //"temporalValues":true,  // Aparentemente, é ignorado no Scorpio, mas no Stellio não!   Se true, a informação vem simplificada sem instanceId
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
