/*!
 * API QuantumLeap 
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var utilsTemporal = require('./utilsTemporal.js');
var schema = require('./schema.js');
var debug = require('debug')('iotbi.ngsitsdb');

/**
 * Returns a Table containing data
 */
exports.temporalDataToTable = temporalDataToTable;
function temporalDataToTable(pJson,pExtended)
{
    var lListAttrs='';
    var lTmp={};
    var lData=pJson;

    //debug(JSON.stringify(lData,null,2));
    //Organize data in two dimensions using the key:  entityId x timeIndex
    for (var i=0;i<lData.attrs.length;i++) 
    {
         var lAttrib=lData.attrs[i];
         //debug('Proc Entity: '+lAttrib.attrName);
	 lListAttrs=lListAttrs+(lListAttrs==''?'':', ')+lAttrib.attrName;
         for(var j=0;j<lAttrib.types.length;j++)           
         {
             var lType=lAttrib.types[j];
             //debug('Proc : '+lAttrib.attrName+'   EntityType:'+lType.entityType);
             for(var k=0;k<lType.entities.length;k++)           
             {
                 var lEntity=lType.entities[k];
                 //debug('Proc : '+lAttrib.attrName+'   Entity:'+lEntity.entityId+'   sizeIndex:'+lEntity.index.length+'   sizeValues:'+lEntity.values.length);
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
                        var lIndex=lEntity.index[l];
                        var lValue=lEntity.values[l];
                        //debug('Proc : '+lAttrib.attrName+'   Entity:'+lEntity.entityId+'  l='+l+' index='+lIndex+'  value='+lValue);
			lRow[lAttrib.attrName]=lValue;
                 }          
             }
         }
     }
     //Organize data as a table
     var lTable=[];
     //Only these fields are defined. The ones related with the entity are defined while merging with it
     var lSchema={};
     lSchema._timeIndex=schema.TIMESTAMP;
     lSchema._dateTime=schema.DATETIME;
     lSchema._entityId=schema.STRING;
     var lCols=null;
     for(var lKey in lTmp)
     {
        //debug('Table proc row:'+lKey); 
	var lColsNew=Object.keys(lTmp[lKey]).length;
	if(lCols!=null && lCols!=lColsNew)
	{
	   debug('Warning: This row has '+lColsNew+' cols, while others '+lCols);
	}
	lCols=lColsNew;
        var lRow=lTmp[lKey];
        var lNewRow={};
        //debug('Add Row: '+JSON.stringify(lRow));
        for(c in lRow)
        {
            var lCel=lRow[c];
            var lType=typeof lCel;
            if(lCel == undefined)
            {
                 debug('Ignored Null in Row/Cell: '+lKey+' / '+c);
            }
            else if(lType === 'object')
            {
               if(lCel['type'] === 'Point')
               {
                  var lCoords=lCel['coordinates'];
                  lNewRow[c+'_coordinates_lat']=lCoords[1];
                  lNewRow[c+'_coordinates_lon']=lCoords[0];
               }
               else
               {
                  debug('Ignored Object in a Cell: '+c+' ('+lType+') = '+JSON.stringify(lRow[c]));
               }
            }
            else
            {
               lNewRow[c]=lRow[c];
            }
        }
	lTable.push(lNewRow)
     }
     //debug('Table: '+JSON.stringify(lTable,null,2))
     debug('Table: rows='+lTable.length+'   cols='+lCols+' :: '+lListAttrs);
     return [lTable,lSchema];
}
