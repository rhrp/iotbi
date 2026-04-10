/**
 * API for providing Matadata
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.model.tablemodel');
var z = require('zod/v4');
var utilsGeo = require('../utilsGeo.js');
var schema=require('../schema.js');
var {Searcher} = require("fast-fuzzy");


module.exports = class Table {
  /**
   *
   */
   constructor(name,rows,schema) 
   {
      this.name = name;
      this.schema=(schema==undefined?{}:schema);
      this.rows=(rows==undefined?[]:rows);
      debug('Table '+this.name+'  Cols:'+this.countCols()+'  Rows:'+this.countRows());
      //debug(JSON.stringify(this,null,2));
   }
   filterGeorelNear(pointLat,pointLon,pMinDistance,pMaxDistance,pGeoproperty)
   {
       var lFiltredTable=new Table('Filtred by Georel minDistance='+pMinDistance+' maxDistance='+pMaxDistance,[],this.getSchema());
       if(pGeoproperty==undefined)
       {
            debug('Invalid geoproperty!');
            return lFiltredTable;
       } 
       for(var lRow of this.getRows())
       {
           var lCellLat=lRow[pGeoproperty+'_coordinates_lat'];
           var lCellLon=lRow[pGeoproperty+'_coordinates_lon'];
           if(lCellLat!=undefined && lCellLon!=undefined)
           {
              var lDistance=utilsGeo.calcDistanceMeters([lCellLat,lCellLon],[pointLat,pointLon]);
              if(lDistance<=pMaxDistance)
              {
                  //debug('Add row :: Distance '+lDistance+' <= '+pMaxDistance);
                  lFiltredTable.rows.push(lRow);
              }
              else
              {
                  //debug('Distance (['+lCellLat+',',lCellLon+'] ['+pointLat+','+pointLon+']):'+lDistance+' > '+pMaxDistance);
              }
           }
       }
       debug('Initial table '+this.countRows()+' rows and filtred table '+lFiltredTable.countRows()+' rows');
       return lFiltredTable;
   }
   select(attribs)
   {
       let lSchema={};
       for(let lCol in this.getSchema())
       {
          if(attribs==undefined || attribs.includes(lCol))
          {
             lSchema[lCol]=this.getSchema()[lCol];
          }
       }
       let lRows=[];
       for(let lRow of this.rows)
       {
         let lRowSelect={};
         for(let lCol in lSchema)
         {
             lRowSelect[lCol]=lRow[lCol];
         }
         lRows.push(lRowSelect);
       }
       return new Table('Selected attributes',lRows,lSchema);
   }
   addColumn(pColName,pColType,pRows=undefined)
   {
       if(pColName==undefined || this.getTypeOf(pColName)!=undefined)
       {
          return 'The column name "'+pColName+'" is not valid or already exists';
       }
       if(!schema.isValidType(pColType))
       {
          return 'The type '+pColType+' is not valid';
       }
       // Add Rows
       if(pRows==undefined)
       {
         for(let i=0;i<this.countRows();i++)
         {
           this.rows[i][pColName]=undefined;
         }
       }
       else if(pRows.length!=this.rows.length)
       {
          return 'The new column with shape 1x'+pRows.length+' does not have the expected one 1 x '+this.rows.length;
       }
       else
       {
          for(let i=0;i<this.countRows();i++)
          {
           this.rows[i][pColName]=pRows[i];
          }
       }
       // Update schema
       this.getSchema()[pColName]=pColType;
       //Ok
       return undefined;
   }
   /**
    * TODO: validate Schema
    */
   merge(table)
   {
      if(table==undefined)
      {
          debug('Invalid table');
          return false;
      }
      if(!(table instanceof Table))
      {
          debug('The table is not a valid class of Table');
          return false;
      }
      if(table.countRows()==0)
      {
          debug('The table is empty');
          return true;
      }
      //Merge
      for(var row of table.rows)
      {
         this.rows.push(row)
      }
      return true;
   }
  /**
   *
   */
   clone()
   {
      let lSchema={};
      for(let lColName in this.getSchema())
      {
         lSchema[lColName]=this.getSchema()[lColName]
      } 
      let lTable=new Table('Clone of '+this.name,[],lSchema);
      for(let lRow of this.rows)
      {
         let lRowClone={};
         for(let lColName in lRow)
         {
             lRowClone[lColName]=lRow[lColName];
         }
         lTable.rows.push(lRowClone);
      }
      return lTable;
   }
   countRows()
   {
      return this.rows==undefined?undefined:this.rows.length;
   }
   countCols()
   {
      return this.schema==undefined?undefined:Object.keys(this.schema).length;
   }
   getSchema()
   {
      return this.schema;
   }
   getRows()
   {
      return this.rows;
   }
   getRowByPos(pPos)
   {
      return this.rows[pPos];
   }
   getTypeOf(pColName)
   {
      return this.schema[pColName];
   }
   isTypeString(pColName)
   {
      return pColName!=undefined && schema.STRING==this.getTypeOf(pColName);
   }
}
