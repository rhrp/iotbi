/*!
 * API QuantumLeap based on Ficodes NGSI Lib (suports NGSI V1,V2 and LD) - https://github.com/ficodes/ngsijs
 * Copyright(c) 2023-2023 Rui Humberto Pereira
 * MIT Licensed
 *
 * ToDo: ld.queryEntities() is limited to 1000
 */
var request = require('request');
var url  = require('url');
var apiConfig = require('./apiConfig.js');
var utils = require('./utils.js');
var utilsTemporal = require('./utilsTemporal.js');
var ngsiv2 = require('./ngsiv2.js');
var ngsildv1 = require('./ngsildv1.js');
var ngsildv1temporal = require('./ngsildv1temporal.js');
var ngsitsdb = require('./ngsitsdb.js');
const HARD_LIMIT = 500000;
const QL_LIMIT=5000;
const TA_LIMIT=1000;	// Temporal API Limit
const PREFIX_EXT_QL = '';

/**
 * Returns a Table containing data
 */
function __off_procAttrsTSDB(pBody)
{
    var lListAttrs='';
    var lTmp={};
    var lData=JSON.parse(pBody);

    //console.log(pBody);
    //Organize data in two dimwnsions:  entityId x timeIndex
    for (var i=0;i<lData.attrs.length;i++) 
    {
         var lAttrib=lData.attrs[i];
         //console.log(lAttrib.attrName);
	 lListAttrs=lListAttrs+(lListAttrs==''?'':', ')+lAttrib.attrName;
         for(var j=0;j<lAttrib.types.length;j++)           
         {
             var lType=lAttrib.types[j];
             //console.log('   type:'+lType.entityType);
             for(var k=0;k<lType.entities.length;k++)           
             {
                 var lEntity=lType.entities[k];
                 //console.log('      Entity:'+lEntity.entityId+'   sizeIndex:'+lEntity.index.length+'   sizeValues:'+lEntity.values.length);
		 var lTimeIndex=utilsTemporal.formatDate(lEntity.index);
		 for(var l=0;l<lTimeIndex.length;l++)
	         {
			var lTime=lTimeIndex[l];
			var lDate=lEntity.index[l];
			var lRow;
			var lKey=''+lTime+'_'+lEntity.entityId;
			if (lTmp[lKey]===undefined)
			{
			   lRow={};
			   lRow._timeIndex=lTime;
			   lRow._dateTime=lDate;
			   lRow._entityId=lEntity.entityId;
			   lTmp[lKey]=lRow;
                        }
			else
			{
			   lRow=lTmp[lKey];
			}
			lRow[lAttrib.attrName]=lEntity.values[l];
                 }          
             }
         }
     }
     //Organize data as a table
     var lTable=[];
     var lCols=null;
     for(var lKey in lTmp)
     {
	var lColsNew=Object.keys(lTmp[lKey]).length;
	if(lCols!=null && lCols!=lColsNew)
	{
	   console.log('Warning: This row has '+lColsNew+' cols, while others '+lCols);
	}
	lCols=lColsNew;
	lTable.push(lTmp[lKey])
     }
     console.log('Table: rows='+lTable.length+'   cols='+lCols+' :: '+lListAttrs);
     return lTable;
}
//TODO: while merging, the atribute type of the string property is handled as an object
/**
 * Merge data from Temporal Table (QL) and Entities Objects (Orion V2/LD) 
 */
function merge(pTable,pEntities,pFuncMergeRow,pFuncEntityToRow,pExtended)
{
     console.log('Start merge '+pEntities.length+' entities to table with '+pTable.length+' rows ...');
     console.table(pTable);
     console.log(pEntities);

     var lEntity;
     //Organize Entities in a dicionary by id
     //TODO: convert attrs to lower case th method getAttrIgnoreCase()
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
                  console.log('Not Match lRow._entityId=['+lRow._entityId+']');
                  var lTmp=lTotalNotMatch[lRow._entityId];
                  lTotalNotMatch[lRow._entityId]=(lTmp==undefined?1:lTmp+1);
             }
             lTotal=lTotal+1;
     }
     for(var lEntityKey in lTotalMatch)
     {
          console.log('Merge :: '+lEntityKey+'   Match='+lTotalMatch[lEntityKey]);
     }
     for(var lEntityKey in lTotalNotMatch)
     {
          console.log('Merge :: '+lEntityKey+'   Not Match='+lTotalNotMatch[lEntityKey]);
     }
     return pTable;
}
function mergeEntityV1(pTableRow,pEntity)
{
     console.log('mergeEntityV1 not implemented');
}

function mergeEntityV2(pTableRow,pEntity)
{
    for(var lRowAttrKey in pTableRow)
    {
         var lRowAttr=pTableRow[lRowAttrKey];
         console.log('Merge :: Attr  '+pTableRow._entityId+'['+lRowAttrKey+']');
         var lMatchEntityAttr=utilsTemporal.getAttrIgnoreCase(pEntity,lRowAttrKey);
         if(lMatchEntityAttr != undefined)
         {
              console.log('        Match ::   '+JSON.stringify(lMatchEntityAttr));
              var lEntityAttrType=lMatchEntityAttr.type;
              if(lEntityAttrType!=undefined)
              {
                   pTableRow[lRowAttrKey+'_type']=lEntityAttrType;
              }
         }
         else
         {
              console.log('        Not Match :: '+lRowAttrKey);
         }
         //console.log(lRow._entityId+'  Row['+lRowAttrKey+']='+JSON.stringify(lRowAttr));
    }
}
function mergeEntityLD(pTableRow,pEntity)
{
    for(var lRowAttrKey in pTableRow)
    {
         var lRowAttr=pTableRow[lRowAttrKey];
         console.log('Merge :: Attr  '+pTableRow._entityId+'['+lRowAttrKey+']');
         var lMatchEntityAttr=utilsTemporal.getAttrIgnoreCase(pEntity,lRowAttrKey);
         if(lMatchEntityAttr != undefined)
         {
              console.log('   Match ::   '+lRowAttrKey+' == '+JSON.stringify(lMatchEntityAttr));
              for(var lMatchEntityAttrKey in lMatchEntityAttr)
              {
                 console.log('     Add: '+lMatchEntityAttrKey);
                 pTableRow[lRowAttrKey+'_'+lMatchEntityAttrKey]=lMatchEntityAttr[lMatchEntityAttrKey];
              }
         }
         else
         {
              console.log('   Not Match :: '+lRowAttrKey);
         }
    }
}

/**
 * Send the QuantumLeap table extended with Entity's data obtained from Orion/OrionLD
 * ToDo: analyse not match case due to fiwareService-path
 */
function sendTableMerge(res,pTable,pEntityType,pFiwareService,pFiwareServicePath,pFormat,pExtended)
{
        const USE_NEW_LIB=true;

    	// Get data about Entities in Orion
	var lHeaders=utils.getInvokeFiwareHeaders(pFiwareService,false,pFiwareServicePath);
        var lEndpoint=apiConfig.getOrionEndpoint(pFiwareService);
	var lOrionURL=apiConfig.getOrionURL(pFiwareService);
        var lOrionNgsiVersion=apiConfig.getOrionNGSIVersion(pFiwareService);
	console.log('Orion URL='+lOrionURL);
        console.log('Orion NGSI='+lOrionNgsiVersion);
	console.log('Header:'+JSON.stringify(lHeaders));
	console.log('Type:'+pEntityType);
 

        if(apiConfig.isOrionLD(pFiwareService))
        {
          console.log("Using ngsijs for connecting Orion LD");
          var NGSI = require('ngsijs');

          var lOrionContext=apiConfig.getOrionNgsiLdContext(pFiwareService);
          var lConnection = new NGSI.Connection(lOrionURL);
          lConnection.ld.queryEntities({"tenant":pFiwareService,"@context":lOrionContext,"type":pEntityType,"limit":1000})
          .then((result) => utils.sendTable(res,merge(pTable,result.results,mergeEntityLD,ngsildv1.entityToRow,pExtended),pFormat))
         .catch((err) => utils.sendError(res,500,err));
        }
        else if(apiConfig.isOrionV2(pFiwareService) && USE_NEW_LIB)
        {
          console.log("Using ngsijs for connecting Orion V2");
          var NGSI = require('ngsijs');
          var lConnection = new NGSI.Connection(lOrionURL);
          lConnection.v2.listEntities({"service":pFiwareService,"type":pEntityType,"limit":1000})
          .then((result) => utils.sendTable(res,merge(pTable,result.results,mergeEntityV2,ngsiv2.entityToRow,pExtended),pFormat))
          .catch((err) => utils.sendError(res,500,err));
        }
        else if(apiConfig.isOrionV2(pFiwareService) && !USE_NEW_LIB)
        {
          console.log("Using ocb-sender/ngsi-parser for connectiing Orion V2");
   	  var ocb = require('ocb-sender')
          var ngsi = require('ngsi-parser');

	  ocb.config(lEndpoint,lHeaders)
      	  .then((result) => console.log('Config: '+JSON.stringify(result)))
      	  .catch((err) => console.log('Config:'+err));

          var lOcbQuery = ngsi.createQuery({
             "type":pEntityType,
            "limit":100});
          ocb.getWithQuery(lOcbQuery,lHeaders)
	  .then((result) => utils.sendTable(res,merge(pTable,result.body,mergeEntityV2,ngsiv2.entityToRow,pExtended),pFormat))
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

        console.log('Wait for Orion CB response...');
}

function invokePartQuantumLeap(req,res,pUrl,lEntityType,pFiwareService,pFiwareServicePath,lFormat,lExtended,pOffset,pPageSize)
{
   console.log('Invoke:'+pUrl);
   var lOptions=utils.getInvokeFiwareOptions(pUrl,pFiwareService,false,pFiwareServicePath);
   return new Promise(function(resolve, reject)
   {
     request(lOptions, function (error, response, pBody) {
	try
	{
                console.log('Invoking finished');
		if(error!=null)
		{ 
                   return reject(error);
		}
		else
		{
                    console.log('statusCode:', response && response.statusCode);
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
        console.log('PageSize='+pPageSize+' Offset='+pOffset+'  Rows '+lRowIndex+'  total='+pTable.length+'   Limit='+pLimit);
        if(pTable.length>pLimit)
        {
           console.log('Limit exceded: '+pLimit);
           utils.sendError(res,500,'Limit exceded: '+pLimit);
        }
        else if(lRowIndex<pPageSize)
        {
           //Last Page
           if(pExtended)
           {
             sendTableMerge(res,pTable,lEntityType,lFiwareService,lFiwareServicePath,lFormat,pExtended);
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
       console.log('Failed making request with error: ', err);
       utils.sendError(res,500,err)
    });
}

/**
 * pTable - is reserved for future recursive implementation for pagination
 */ 
function invokeRequestTemporalAPI(req,res,pConnection,pOptions,pEntityType,pFiwareService,pFiwareServicePath,pFormat,pExtended,pOffset,pPageSize,pLimit,pTable,pJoinTableName)
{
   pConnection.ld.queryTemporalEntities(pOptions)
    .then(function (result) {
           var lTable=ngsildv1temporal.temporalDataToTable(result);
           if(pJoinTableName==undefined)
           {
              //console.table(lTable);
              if(pExtended)
              {
                console.log('Warning: The extended option is not aplicable when using temporal API since all data is loaded from the Broker');
                // In the case of QuantumLeap, additional data could be loaded grom Orion/OrionLD
              }
              utils.sendTable(res,lTable,pFormat);
           } else {
             //loadJoinTable(req,res,pConnection,lTable,pFiwareService,pFiwareServicePath,pJoinTableName,pExtended,pFormat);
              loadEntities(req,res,pConnection,lTable,pFiwareService,pFiwareServicePath,pEntityType,pJoinTableName,pExtended,pFormat);
          }
    }).catch(function(err) {
       console.log('Failed making request with error: ', err);
       utils.sendError(res,500,err)
    });
}

/**
 * Merge Temporal data and Entities' data
 * This is important in order to get the relationship field that will enable the join operation
 */
function loadEntities(req,res,pConnection,pTemporalTable,pFiwareService,pFiwareServicePath,pEntityType,pJoinTableName,pExtended,pFormat)
{
   //console.log('Temporal Table');
   //console.table(pTemporalTable);

   var NGSI = require('ngsijs');
   var lOptions = {"tenant":pFiwareService,
                   "@context":"http://context/json-context.jsonld",
                   "limit":1000,
                   "type":pEntityType 
                  };

   pConnection.ld.queryEntities(lOptions)
        .then((result) => {
                           var lMergedTable=merge(pTemporalTable,result.results,mergeEntityLD,ngsildv1.entityToRow,pExtended);
                           loadJoinTable(req,res,pConnection,lMergedTable,pFiwareService,pFiwareServicePath,pJoinTableName,pExtended,pFormat);
                         })
        .catch((err) => utils.sendError(res,500,err));
}

/**
 * Join a table
 */
function loadJoinTable(req,res,pConnection,pMasterTable,pFiwareService,pFiwareServicePath,pJoinTableName,pExtended,pFormat)
{
   console.log('Master table');
   console.table(pMasterTable);
         //ngsildv1.joinTable(lTable);
         //var lJoinTable=xxx

   var NGSI = require('ngsijs');
   var lOptions = {"tenant":pFiwareService,
                   "@context":"http://context/json-context.jsonld",
                   "limit":1000,
                   "type":pJoinTableName 
                  };

   pConnection.ld.queryEntities(lOptions)
          .then((result) => {
                             var lJoinTable=ngsildv1.entitiesToTable(result.results,pExtended);
                             console.log('Join table '+pJoinTableName);
                             console.table(lJoinTable);
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
   var lFiwareServicePath=lQuery.fiwareServicePath;
   var lExtended=(lQuery.extended!=undefined);

   //????
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
        console.log(lError);
        return;
   }
   if(apiConfig.isBrokerSuportsTemporalAPI(lFiwareService))
   {
        console.log('Using NGSI-LD Temporal API');
        serviceNGSITemporalAPI(req,res,lFiwareService,lFiwareServicePath,lEntityType,lEntityId,lAttrs,lIdPattern,lFromDate,lToDate,lCoords,lMinDistance,lMaxDistance,lGeorel,lFormat,lExtended,lLimit,lJoinTable);
   }
   else if(apiConfig.getQuantumLeapServerOk(lFiwareService))
   {
        console.log('Using NGSI-TSDB API');
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
   console.log('Ok!');
}
function serviceNGSITemporalAPI(req,res,pFiwareService,pFiwareServicePath,pEntityType,pEntityId,pAttrs,pIdPattern,pFromDate,pToDate,pCoords,pMinDistance,pMaxDistance,pGeorel,pFormat,pExtended,pLimit,pJoinTable)
{
   var NGSI = require('ngsijs');

   var lServer=apiConfig.getBrokerHost(pFiwareService);
   var lPort=apiConfig.getBrokerPort(pFiwareService);

   var lConnection = new NGSI.Connection('http://'+lServer+':'+lPort);
   var lOptions = {"tenant":pFiwareService,
	   	   "@context":"http://context/json-context.jsonld",
		   "limit":TA_LIMIT,
                   "type":pEntityType, 
		    "temporalValues":true,
		    "sysAttrs":true
                  };
   if(pAttrs!=null) {
	lOptions.attrs=pAttrs;
   }
   var lTable=[];
   invokeRequestTemporalAPI(req,res,lConnection,lOptions,pEntityType,pFiwareService,pFiwareServicePath,pFormat,pExtended,0,TA_LIMIT,pLimit,lTable,pJoinTable)
   console.log('Ok!');

}
