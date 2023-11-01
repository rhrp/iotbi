/*!
 * API QuantumLeap 
 * Copyright(c) 2023-2023 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var utilsTemporal = require('./utilsTemporal.js');

/**
 * Returns a Table containing data
 */
exports.temporalDataToTable = temporalDataToTable;
function temporalDataToTable(pJson)
{
    var lListAttrs='';
    var lTmp={};
    var lData=pJson;

    //console.log(pBody);
    //Organize data in two dimensions:  entityId x timeIndex
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
