/*!
 * API QuantumLeap 
 * Copyright(c) 2023-2023 Rui Humberto Pereira
 * MIT Licensed
 *
 */

var utils = require('./utils.js')
var utilsTemporal = require('./utilsTemporal.js')
exports.temporalDataToTable = temporalDataToTable;
function temporalDataToTable(pJson)
{

    //console.log(pJson);
    var lTable=[];
    var i=0;
    for (var lKeyEntity in pJson.results) 
    {
       var lTmp={};// Dict  attr+time/value
       i=i+1;
       var lEntity=pJson.results[lKeyEntity];
       //console.log('Entity '+lEntity.id);
       for(var lKeyAttr in lEntity)
       {
	 //console.log('Row #'+i+'  '+lKeyAttr);
         var lAttrib=lEntity[lKeyAttr];
         if(lKeyAttr=='id' || lKeyAttr=='type' || lKeyAttr=='@context')  {
	    //console.log('System attrs '+lKeyAttr);
         } else if (Array.isArray(lAttrib)){
	    for(var idx in lAttrib)
            {
                var lValueAtTime=lAttrib[idx];
                //console.log(lValueAtTime);
                var lTimeIndex=utilsTemporal.formatDateVal(lValueAtTime.observedAt);
                var lEntityIndex=lKeyAttr;
                //console.log(lTimeIndex);
                if(lTmp[lTimeIndex]==undefined)
                {
                  lTmp[lTimeIndex]={};
                }
                lTmp[lTimeIndex][lEntityIndex]=lValueAtTime;
            }
         }
       }
       for(var lTimeKey in lTmp)
       {
          var lRow={};
          lRow['_entityId']=lEntity.id;
          lRow['type']=lEntity.type;
          lRow['_timeIndex']=lTimeKey;
          for(var lPropKey in lTmp[lTimeKey])
          {
             var lValueTime=lTmp[lTimeKey][lPropKey];
             //console.log(lEntity.id+'   '+lTimeKey+'  '+lPropKey+' '+JSON.stringify(lValueTime));
             for(var lValueKey in lValueTime)
             {
                lRow[lPropKey+'_'+lValueKey]=lValueTime[lValueKey];
             }
            
          }
          lTable.push(lRow);
       }
    }
    return lTable;
}
