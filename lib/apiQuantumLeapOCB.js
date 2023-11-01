/*!
 * API QuantumLeap based on Conwetlab OCB/Parser  NGSI Lib (suports NGSI v1 and v2) - https://github.com/conwetlab/ngsijs - Deprecated
 * Copyright(c) 2023-2023 Rui Humberto Pereira
 * MIT Licensed
 */
#!/usr/bin/env node

var request = require('request');
var url  = require('url');
var apiConfig = require('./apiConfig.js');
var utils = require('./utils.js');

const HARD_LIMIT = 300000;
/**
 * Formata o array de datas em time ms
 */
function formatDate(p)
{
   var out=[];
   for(var i=0;i<p.length;i++)
   {
         d=new Date(p[i]);
         //console.log(item_i,d.getTime());
         out.push(d.getTime());
   };        
   return out;
}

/**
 * Calcula a data do dia anterior
 */
function getPreviousDay(date = new Date()) {
  const previous = new Date(date.getTime());
  previous.setDate(date.getDate() - 1);

  return previous;
}

/**
 *
 */
function parseFromDate(pParamFromDate)
{
   var lDate;
   if(pParamFromDate!=null && pParamFromDate=='today')
   {
	var lNow=new Date();
	// Forget the hour/min
	lDate=new Date(lNow.getFullYear(),lNow.getMonth(),lNow.getDate());
        //console.log('From Date Today='+lDate+'    now '+lNow);
   }
   else if(pParamFromDate!=null && pParamFromDate=='ever')
   {
	lDate=new Date(1970,1,1); 
   }
   else if(pParamFromDate!=null)
   {
	return pParamFromDate;
   }	
   else
   {
	//Default
	//Last 24h
	lDate=getPreviousDay();
   }
   return lDate.toISOString();
}
/**
 *
 */
function parseToDate(pParamToDate)
{
   var lDate;
   if(pParamToDate==null || pParamToDate=='today' || pParamToDate=='ever')
   {
        lDate=new Date();
   }
   else if(pParamToDate=='yesterday')
   {
        // up to 24 hours before
        lDate=getPreviousDay();
   }
   else
   {
	return pParamToDate;
   }
   return lDate.toISOString();
}


/**
 * Returns a Table containing data
 */
function procAttrs(pBody)
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
		 var lTimeIndex=formatDate(lEntity.index);
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
/**
 * Get the attribute of an entity ignoring case
 */ 
function getAttrIgnoreCase(pEntity,pAttrKey)
{
   for(var lEntityAttrKey in pEntity)
   {
	if(lEntityAttrKey.toLowerCase()==pAttrKey.toLowerCase())
	{
	    return pEntity[lEntityAttrKey];
	}
   }
   return undefined;
}
/**
 * Merge data from QL and Orion 
 */
function merge(pTable,pEntities)
{
     console.log('Start merge...');
     //console.log(pEntities);
     var lEntity;
     //Organize Entities in a dicionary by id
     //TODO: convert attrs to lower case th method getAttrIgnoreCase()
     var lEntities={};
     for(var lEntityKey in pEntities)
     {
            lEntity=pEntities[lEntityKey];
            lEntities[lEntity.id]=lEntity;
            console.log('Entity['+lEntity.id+']');
     }
     var lTotal=0;
     var lTotalNotMatch={};
     var lTotalMatch={};
     //console.log(pEntities);
     for(var lRowKey in pTable)
     {
             var lRow=pTable[lRowKey];
             //console.log(lRow._entityId);
             lEntity=lEntities[lRow._entityId];
             if(lEntity!=undefined)
             {
		  //console.log('Merge :: Row '+lRow._entityId+'    '+JSON.stringify(lEntity));
	          for(var lRowAttrKey in lRow)
		  {
			var lRowAttr=lRow[lRowAttrKey];
			//var lMatchEntityAttr=lEntity[lRowAttrKey];
                        var lMatchEntityAttr=getAttrIgnoreCase(lEntity,lRowAttrKey);
                        if(lMatchEntityAttr != undefined)
                        {
				//console.log('Merge :: Attr  '+lRow._entityId+'    '+JSON.stringify(lMatchEntityAttr));
				var lEntityAttrType=lMatchEntityAttr.type;
				if(lEntityAttrType!=undefined)
				{
				   lRow[lRowAttrKey+'_type']=lEntityAttrType;
				}
                        }
                        else
                        {
                               //console.log('Merge :: Attr  '+lRow._entityId+'  ::  '+lRowAttrKey+' != '+JSON.stringify(lEntity));
                        }
			//console.log(lRow._entityId+'  Row['+lRowAttrKey+']='+JSON.stringify(lRowAttr));
		  }
                  var lTmp=lTotalMatch[lRow._entityId];
                  lTotalMatch[lRow._entityId]=(lTmp==undefined?1:lTmp+1);
             }
             else
             {
                  //console.log('Not Match lRow._entityId=['+lRow._entityId+']');
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
/**
 * Send the table extended with Orion data
 * ToDo: analyse not match case due to fiwareService-path
 */
function sendTableMerge(res,pTable,pEntityType,pFiwareService,pFiwareServicePath,pFormat)
{
    	// Get data about Entities in Orion
	var lHeaders=utils.getInvokeFiwareHeaders(pFiwareService,false,pFiwareServicePath);
	var lEndpoint=apiConfig.getOrionEndpoint(pFiwareService);

	console.log('Endpoint='+lEndpoint);
	console.log('Header:'+JSON.stringify(lHeaders));
	console.log('Type:'+pEntityType);

	var ocb = require('ocb-sender')
        var ngsi = require('ngsi-parser');

	ocb.config(lEndpoint,lHeaders)
      	.then((result) => console.log('Config: '+JSON.stringify(result)))
      	.catch((err) => console.log('Config:'+err));

        var lOcbQuery = ngsi.createQuery({
            "type":pEntityType,
            "limit":100});
        ocb.getWithQuery(lOcbQuery,lHeaders)
	.then((result) => utils.sendTable(res,merge(pTable,result.body),pFormat))
	.catch((err) => utils.sendError(res,500,err));
       
/* Limited to 20
        ocb.getEntityListType(pEntityType,lHeaders)
        .then((entities) => utils.sendTable(res,merge(pTable,entities),pFormat))
        .catch((err) => utils.sendError(res,500,err));
*/
        console.log('Wait for OCB response...');
}

function invokePart(req,res,pUrl,lEntityType,pFiwareService,pFiwareServicePath,lFormat,lExtended,pOffset,pPageSize)
{
   var lOptions=utils.getInvokeFiwareOptions(pUrl,pFiwareService,false,pFiwareServicePath);
   return new Promise(function(resolve, reject)
   {
     request(lOptions, function (error, response, pBody) {
	try
	{
                console.log('Invoke:'+pUrl);
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
                        var lTable=procAttrs(pBody,lExtended); 
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
function invokeRequest(req,res,pUrl,lEntityType,lFiwareService,lFiwareServicePath,lFormat,lExtended,pOffset,pPageSize,pLimit,pTable)
{
   var lUrl=pUrl+"&limit="+pPageSize+"&offset="+pOffset;
   invokePart(req,res,lUrl,lEntityType,lFiwareService,lFiwareServicePath,lFormat,lExtended,pOffset,pPageSize).then(function(result) {
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
           if(lExtended)
           {
             sendTableMerge(res,pTable,lEntityType,lFiwareService,lFiwareServicePath,lFormat);
           }
           else
           {
             utils.sendTable(res,pTable,lFormat);
           }
        }
        else
        {
           invokeRequest(req,res,pUrl,lEntityType,lFiwareService,lFiwareServicePath,lFormat,lExtended,pOffset+pPageSize,pPageSize,pLimit,pTable);
        }
     })
    .catch(function(err) {
       console.log('Failed making request with error: ', err);
       utils.sendError(res,500,err)
    });
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
   var lFromDate=parseFromDate(lQuery.fromDate);
   var lToDate=parseToDate(lQuery.toDate);
   var lFiwareServicePath=lQuery.fiwareServicePath;
   var lExtended=(lQuery.extended!=undefined);

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
   if(!apiConfig.getQuantumLeapServerOk(lFiwareService))
   {
        var lError='Invalid Service '+lFiwareService;
        res.status(404).json({'description':lError});
        return;
   }
   var lServer=apiConfig.getQuantumLeapHost(lFiwareService);
   var lPort=apiConfig.getQuantumLeapPort(lFiwareService);
   var lUrl='http://'+lServer+':'+lPort+'/v2/attrs'
           +'?type='+lEntityType
           +(lEntityId!=null?'&id='+lEntityId:'')
           +(lAttrs!=null?'&attrs='+lAttrs:'')
           +(lIdPattern!=null?'&idPattern='+lIdPattern:'')
           +(lFromDate!=null?'&fromDate='+lFromDate:'')
           +(lToDate!=null?'&toDate='+lToDate:'');
   lUrl=utils.addGeoLocation(lUrl,null,lCoords,lMinDistance,lMaxDistance,lGeorel);


   var lTable=[];
   invokeRequest(req,res,lUrl,lEntityType,lFiwareService,lFiwareServicePath,lFormat,lExtended,0,5000,lLimit,lTable)
   console.log('Ok!');
}
