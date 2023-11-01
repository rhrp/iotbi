#!/usr/bin/env node

var apiConfig = require('./apiConfig.js');
var utils = require('./utils.js')
var turf = require('turf');

/**
 * Exports
 */
exports.entitiesToTable = entitiesToTable;
exports.entityToRow = entityToRow;





/**
 * Convert an array of Entities to a Table
 */
function entitiesToTable(pEntities,pExtended)
{
   //console.log(pEntities);
   //Init output table
   console.log('Extended='+pExtended);
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
    //console.log('NGSI Object:'+JSON.stringify(pEntity));
    lReg={};
    lReg._entityId=pEntity.id;
    for(var lDataField in pEntity)
    {
	//console.log('Object Id '+lEntity.id+'  :: Prop='+lDataField);
	if(lDataField=='id')
	{
            //console.log('Entity Id ');
        }
        else if(lDataField=='type')
        {
           //console.log('Entity Id ');
        }
        else
        {
             var lEntityAttrib=pEntity[lDataField];
             //console.log('Object Id '+lEntity.id+'  :: Prop='+lDataField+' ::  '+JSON.stringify(lEntityAttrib));
             if(('\"'+lEntityAttrib.value+'\"')==JSON.stringify(lEntityAttrib.value)
             || (''+lEntityAttrib.value)==JSON.stringify(lEntityAttrib.value))
	     {
 		  lReg[lDataField]=lEntityAttrib.value;
             }
             else
             {
                  //console.log('Expand value ['+JSON.stringify(lEntityAttrib.value)+']');
                  for(var lValueField in lEntityAttrib.value)
                  {
                        lReg[lDataField+'_value_'+lValueField]=lEntityAttrib.value[lValueField];
                  }
             }
             if(pEntity[lDataField].type=='geo:json')
             {
                 addCentroid(lReg,pEntity,lDataField,pExtended);
             }
             if(pExtended)
             {
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
                                    
                                    lReg[lDataField+'_'+lMetadataAttrib+'_value']=lMetadata[lMetadataAttrib].value;
                                    lReg[lDataField+'_'+lMetadataAttrib+'_type']=lMetadata[lMetadataAttrib].type;
                                }
                             }
                             else
                             {
                                  //console.log('Extend ignore '+lEntityAttribField+' expected an object!');
                             }
                        }
		        else if(lEntityAttribField != 'value')
		        { 
                              lReg[lDataField+'_'+lEntityAttribField]=lEntityAttrib[lEntityAttribField];
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
    return lReg;
}
function addCentroid(pTableRow,pEntity,pDataField,pExtended)
{
   // In order to behave similar to QuantumLeap add this centroid
   var lPoint;
   if(pEntity[pDataField].value==null)
   {
        console.log('In Entity '+JSON.stringify(pEntity)+' the value of '+pDataField+' is null');
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
    if(pExtended)
    {
      pTableRow[pDataField+'_centroid_lat']=(lPoint!=null?lPoint.coordinates[1]:'');
      pTableRow[pDataField+'_centroid_lon']=(lPoint!=null?lPoint.coordinates[0]:'');
    }
}
