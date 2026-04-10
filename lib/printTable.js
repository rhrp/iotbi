/**
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.pt');
var Table = require('./model/tablemodel.js');

const COLORS = {
        red: '\x1b[31m',
        green: '\x1b[32m',
        yellow: '\x1b[33m',
        blue: '\x1b[34m',
        reset: '\x1b[0m'
       };


module.exports = class PrintTable 
{
   constructor() 
   {
   }
   static #formatCol(pValue,pWith)
   {
       let lValue=pValue==undefined?'':pValue.toString();
       let lLen=PrintTable.#length(lValue);
       return lValue+(" ".repeat(pWith-lLen));
   }
   static #withCols(table)
   {
      let lColsLen={};
      let lPadding=2;
      //Header
      for(let lColName in table.getSchema())
      {
          lColsLen[lColName]=lColName.length+lPadding;
      }
      //Body
      for(let lRow of table.rows)
      {
         for(let lColName in table.getSchema())
         {
             let lCell=lRow[lColName]!=undefined?lRow[lColName]:'';
             let lLen=PrintTable.#length(lCell);
             if(lColsLen[lColName]<lLen)
             {
               lColsLen[lColName]=lLen;
             }
         }
      }
      return lColsLen;
   }
  /**
   *
   */
   static printTable(table,printConsole=undefined) 
   {
       let lColsWith=PrintTable.#withCols(table);
       let lLinha=undefined;
       // Top border
       for(let lColName in table.getSchema())
       {
           if(lLinha==undefined)
           {
                lLinha='\u250C'+'\u2500'.repeat(lColsWith[lColName]);
           }
           else
           {
                lLinha=lLinha+'\u252C'+'\u2500'.repeat(lColsWith[lColName]);
           }
       } 
       lLinha=lLinha+'\u2510';
       PrintTable.#print(lLinha,printConsole);

       //Headers
       lLinha=undefined;
       for(let lColName in table.getSchema())
       {
           if(lLinha==undefined)
           {
             lLinha='\u2502'+PrintTable.#formatCol(lColName,lColsWith[lColName]);
           }
           else
           {
              lLinha=lLinha+'\u2502'+PrintTable.#formatCol(lColName,lColsWith[lColName]);
           }
       } 
       lLinha=lLinha+'\u2502';
       PrintTable.#print(lLinha,printConsole);

       // 
       lLinha=undefined;
       for(let lColName in table.getSchema())
       {
           if(lLinha==undefined)
           {
                lLinha='\u251C'+'\u2500'.repeat(lColsWith[lColName]);
           }
           else
           {
                lLinha=lLinha+'\u253C'+'\u2500'.repeat(lColsWith[lColName]);
           }
       }
       lLinha=lLinha+'\u2524';
       PrintTable.#print(lLinha,printConsole);

       for(let lRow of table.rows)
       {
         lLinha=undefined;
         for(let lColName in table.getSchema())
         {
             if(lLinha==undefined)
             {
               lLinha='\u2502'+PrintTable.#formatCol(lRow[lColName],lColsWith[lColName]);
             }
             else
             {
                lLinha=lLinha+'\u2502'+PrintTable.#formatCol(lRow[lColName],lColsWith[lColName]);
             }
         }
         lLinha=lLinha+'\u2502';
         PrintTable.#print(lLinha,printConsole);
       }

       // Bottom border
       lLinha=undefined;
       for(let lColName in table.getSchema())
       {
           if(lLinha==undefined)
           {
                lLinha='\u2514'+'\u2500'.repeat(lColsWith[lColName]);
           }
           else
           {
                lLinha=lLinha+'\u2534'+'\u2500'.repeat(lColsWith[lColName]);
           }
       } 
       lLinha=lLinha+'\u2518';
       PrintTable.#print(lLinha,printConsole);
    }
   /**
    *
    */
    static paintRow(pTable,pColName,pRowId,pTextSegment,pColor='red')
    {
       if(pRowId<0 || pRowId>=pTable.countRows())
       {
          debug('The row id '+pRowId+' is not valid');
          return false;
       }
       let lOldText=pTable.rows[pRowId][pColName];
       if(lOldText==undefined)
       {
          return false;
       }
       let lNewSegment=PrintTable.#paintText(pTextSegment,pColor);
       pTable.rows[pRowId][pColName]=lOldText.replace(pTextSegment,lNewSegment);
       return true;
    }
    static #paintText(text, color)
    {
       if(text==undefined)
       {
          return undefined;
       }
       return (COLORS[color]==undefined?'':COLORS[color]) + text + COLORS.reset;
    }
    static #length(pText)
    {
       if(pText==undefined)
       {
          return 'undefined'.length;
       }
//       let lText=pText.replace(/[^\x00-\x7F]/g,'X');
       let lText=pText.toString();
       for(let c in COLORS)
       {
           lText=lText.replace(COLORS[c],'');
       }
       return lText.length;
    }
    static #print(pText,debugConsole)
    {
        if(debugConsole==undefined)
          console.log(pText);
        else
          debugConsole(pText);
    }
}
