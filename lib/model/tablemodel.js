/**
 * API for providing Matadata
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.model.tablemodel');
var z = require('zod/v4');
var utilsGeo = require('../utilsGeo.js');


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
   /**
    *
    */
    filterTextPattern(textPattern)
    {
       var lFiltredTable=new Table('Filtred by '+textPattern,[],this.getSchema());
       if(textPattern==undefined || textPattern.length==0)
       {
            debug('Invalid textPattern!');
            return lFiltredTable;
       } 
       var lTextPattern=textPattern.toLowerCase();
       for(var lRow of this.getRows())
       {
           for(var lCellKey in lRow)
           {
              var lCell=lRow[lCellKey];
              if(lCell!=undefined && lCell.length>0)
              {
                   var lPosition = lCell.toLowerCase().search(lTextPattern);
                   if(lPosition>=0)
                   {
                      debug('filterTextPattern('+textPattern+') match Cell value: '+lCell)
                      lFiltredTable.rows.push(lRow);
                      break;
                   }
              }
           }
       }
       debug('Intial table '+this.countRows()+' rows and filtred table '+lFiltredTable.countRows()+' rows');
       return lFiltredTable;
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
       debug('Intial table '+this.countRows()+' rows and filtred table '+lFiltredTable.countRows()+' rows');
       return lFiltredTable;
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
}
