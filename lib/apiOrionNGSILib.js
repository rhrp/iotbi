#!/usr/bin/env node

var url  = require('url');
var apiConfig = require('./apiConfig.js');
var utils = require('./utils.js')
var turf = require('turf');
var ocb = require('ocb-sender')
var ngsi = require('ngsi-parser');

exports.service = function(req,res,next)
{
   //Path params
   var lEntityType=req.params.entityType;
   var lEntityId=(req.params.entityId==undefined?null:req.params.entityId);
   var lFiwareService=req.params.fiwareService;
   //Query params
   var lUrlParts = url.parse(req.url, true);
   var lQuery = lUrlParts.query;
   var lFiwareServicePath=lQuery.fiwareServicePath;
   var lExtended=(lQuery.extended!=undefined);
   var lJoinTable=(lQuery.join==undefined?null:lQuery.join);  // NGSI-LD
   // Format
   var lFormat= utils.getFormat(req); 

   //Check the Fiware Service
   if(!apiConfig.isConfigOk(lFiwareService))
   {
        console.log('Invalid Service '+lFiwareService);
        res.status(404).json({'description':'Invalid Service '+lFiwareService});
        return;
   }


   var lEndpoint=apiConfig.getOrionEndpoint(lFiwareService);
   console.log('Endpoint: '+lEndpoint);

   if(apiConfig.isOrionLD(lFiwareService))
   {
      console.log('OrionLD');
      //var lHeaders=utils.getInvokeFiwareHeaders(lFiwareService,true,lFiwareServicePath);
    
      // Get the Query
      var lObjQuery = utils.getObjectQuery(lQuery,lEntityType,lEntityId);
      console.log('Query: '+JSON.stringify(lObjQuery));
      var ngsild = require('./ngsildv1.js');
      ngsild.listEntities(lEndpoint,lObjQuery,lFiwareService,lFiwareServicePath,lExtended,lJoinTable)
      .then((table) => utils.sendTable(res,table,lFormat))
      .catch((err) => utils.sendError(res,500,err))
   }
   else
   {
      console.log('Orion V2');
      
      var lHeaders=utils.getInvokeFiwareHeaders(lFiwareService,false,lFiwareServicePath);
      var ngsiv2 = require('./ngsiv2.js');
      // Config  the framework
      ocb.config(lEndpoint,lHeaders)
         .then((result) => console.log('Config: '+JSON.stringify(result)))
         .catch((err) => console.log('Config:'+err));

      // Get the Query
      var lObjQuery = utils.getObjectQuery(lQuery,lEntityType,lEntityId,1000);
      console.log('Query: '+JSON.stringify(lObjQuery));

     // Invoke
     var lOcbQuery = ngsi.createQuery(lObjQuery);
     ocb.getWithQuery(lOcbQuery,lHeaders)
       .then((result) => utils.sendTable(res,ngsiv2.entitiesToTable(result.body,lExtended),lFormat))
      .catch((err) => utils.sendError(res,500,err))
   }
}
/**
 * Convert NGSI to a json-based table
 *
function __toTable(pNGSIEntities,pExtended)
{
   //console.log(pNGSIEntities);
   //Init output table
   console.log('Extended='+pExtended);
   var lTable=[];
   for (var lEntity of pNGSIEntities) 
   {
        //console.log('NGSI Object:'+JSON.stringify(lEntity));
	lReg={};
	lReg._entityId=lEntity.id;
	for(var lDataField in lEntity)
	{
		//console.log('Object Id '+lEntity.id+'  :: Prop='+lDataField);
		if(lDataField!='id' && lDataField!='type')
		{
                        var lEntityAttrib=lEntity[lDataField];
                        //console.log('Object Id '+lEntity.id+'  :: Prop='+lDataField+' ::  '+JSON.stringify(lEntityAttrib));
                        if(('\"'+lEntityAttrib.value+'\"')==JSON.stringify(lEntityAttrib.value)
                        || (''+lEntityAttrib.value)==JSON.stringify(lEntityAttrib.value))
			{
 			  lReg[lDataField]=lEntityAttrib.value;
                        }
                        else
                        {
                          console.log('Expand value ['+JSON.stringify(lEntityAttrib.value)+']');
                          for(var lValueField in lEntityAttrib.value)
                          {
                               lReg[lDataField+'_value_'+lValueField]=lEntityAttrib.value[lValueField];
                          }
                        }
                        if(pExtended)
                        {
                           for(var lEntityAttribField in lEntityAttrib)
			   {
                             //lReg[lDataField+'_type']=lEntity[lDataField].type;
			     if(lEntityAttribField != 'value')
			     { 
                                lReg[lDataField+'_'+lEntityAttribField]=lEntityAttrib[lEntityAttribField];
			     }
                           }
                        }
		}
                if(lEntity[lDataField].type=='geo:json')
                {
			addCentroid(lReg,lEntity,lDataField)
		}
	}
	lTable.push(lReg);
   }
   return lTable;
}
function addCentroid(pTableRow,pEntity,pDataField)
{
   // In order to behave similar to QuantumLeap add this centroid
   var lPoint=null;
   if(pEntity[pDataField].value.type=='Point__force_turf_to_work')
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
    pTableRow[pDataField+'_centroid']=lPoint.coordinates[1]+', '+lPoint.coordinates[0];
}
*/
