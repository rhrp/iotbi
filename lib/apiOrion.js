#!/usr/bin/env node

var request = require('request');
var url  = require('url');
var apiConfig = require('./apiConfig.js');
var utils = require('./utils.js')
var turf = require('turf');

exports.service = function(req,res,next)
{
   //Path params
   var lEntityType=req.params.entityType;
   var lEntityId=(req.params.entityId==undefined?null:req.params.entityId);
   var lFiwareService=req.params.fiwareService;
   //Query params
   var lUrlParts = url.parse(req.url, true);
   var lQuery = lUrlParts.query;
   var lIdPattern=lQuery.idPattern;
   var lAttrs=lQuery.attrs;
   var lGeorel=lQuery.georel;
   var lCoords=lQuery.coords;
   var lMinDistance=lQuery.minDistance;
   var lMaxDistance=lQuery.maxDistance;
   var lLimit=lQuery.limit;
   var lFiwareServicePath=lQuery.fiwareServicePath;

   var lServer=apiConfig.getOrionHost(lFiwareService);
   var lPort=apiConfig.getOrionPort(lFiwareService);
   var lUrl='http://'+lServer+':'+lPort+'/v2/entities?type='+lEntityType
           +(lEntityId!=null?'&id='+lEntityId:'')
	   +(lAttrs!=null?'&attrs='+lAttrs:'')
	   +(lIdPattern!=null?'&idPattern='+lIdPattern:'')
	   +(lLimit!=null?'&limit='+lLimit:'');
   lUrl=utils.addGeoLocation(lUrl,null,lCoords,lMinDistance,lMaxDistance,lGeorel); 
   var options=utils.getInvokeFiwareOptions(lUrl,lFiwareService,lFiwareServicePath);
   //Init output table
   var lTable=[];
   request(options, function (error, response, pBody) {
        try
        {
                console.log('['+lFiwareService+']['+lFiwareServicePath+'] Invoke:'+lUrl);
                if(error!=null)
                {
                   console.error('error:', error);
                   res.status(500).json({'description':error});
                }
                else
                {
                   console.log('statusCode:', response && response.statusCode);
                   if(response.statusCode<200 || response.statusCode>299)
                   {
                        var lContentType=response.headers['content-type'];
                        res.setHeader('content-type',lContentType);
                        res.status(response.statusCode).send(pBody);
                   }
                   else
                   {
                	//console.log('body:', pBody); 
			var lDataArray=JSON.parse(pBody);
			for (var lDataReg of lDataArray) 
			{
                                console.log('NGSI Object:'+JSON.stringify(lDataReg));
		    		lReg={};
		    		lReg._entityId=lDataReg.id;
                                lReg._dateObserved=lDataReg.dateObserved.value;
		    		for(var lDataField in lDataReg)
		    		{
					//console.log('Object Id '+lDataReg.id+'  :: Prop='+lDataField);
					if(lDataField!='id' && lDataField!='type' && lDataField!='dateObserved')
					{
			   			lReg[lDataField]=lDataReg[lDataField].value;
					}
                                        if(lDataReg[lDataField].type=='geo:json')
                                        {
						// In order to behave similar to QuantumLeap add this centroid
						var lPoint=null;
						if(lDataReg[lDataField].value.type=='Point__force_turf_to_work')
                                                {
							// Case of a Point
							lPoint=lDataReg[lDataField].value;
							console.log('Point='+JSON.stringify(lPoint));
						}
						else
                                                {
							// Case of a Polygon - Use Turf for calculate the Centroid
							//ToDo: test with real data
							var lPolygon=lDataReg[lDataField].value;
//var maia = require('../../webserver/wwwroot/data/maia.json');
//lPolygon=maia.geojsons.municipio;
							lPoint=turf.centroid(lPolygon,null).geometry;
							//console.log('Turf Centroid='+JSON.stringify(lPoint));
                                                }
						lReg[lDataField+'_centroid']=lPoint.coordinates[1]+', '+lPoint.coordinates[0];
                                        }
                   		}
		    		lTable.push(lReg);
			}
			res.status(200).json(lTable);
		   }
		}
         }
         catch(ex)
	 {
                console.log(ex);
		res.status(500).send(ex);
         }
     });
}
