/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the schema management subsystem.
 */
var debug = require('debug')('iotbi.schema');

// These are the uniformized datatypes
exports.STRING    = 'string';
exports.DOUBLE    = 'double';
exports.INTEGER   = 'integer'
exports.DATE      = 'date';
exports.TIMESTAMP = 'timestamp';
exports.DATETIME  = 'date-time';
exports.ARRAY     = 'array';
exports.POINT     = 'point';
exports.OBJECT    = 'object';
exports.BOOLEAN   = 'boolean';

/**
 * Infers the datatype of an object 
 */
exports.inferType = function(pObject)
{
   if(pObject==undefined)
   {
     debug('Value undefined!');
     return this.STRING;
   }
   else if(typeof pObject === 'string')
   {
     return this.STRING;
   }
   else if(typeof pObject === 'number')
   {
     return this.DOUBLE;
   }
   else
   {
     debug('Unknown type '+(typeof pObject));
     return this.OBJECT;
   }
}
/**
 * Parses a NGSI datatype
 */
exports.parseType = function(pStringType)
{
   if(pStringType==undefined)
   {
      debug('parseType: Col undefined!');
      return this.STRING;
   }
   else if(pStringType=='Text')
   {
      return this.STRING;
   }
   else if(pStringType=='Number')
   {
      return this.DOUBLE;
   }
   else if(pStringType=='DateTime')
   {
      return this.DATETIME;
   }
   else if(pStringType=='Point')
   {
      return this.POINT;
   }
   else
   {
      //debug('Col '+pStringType+' is unknown!');
      return this.OBJECT;
   }
}
/**
 * Parses a value as a defined data type
 */
exports.parseValue = function(pStringValue,pType)
{
   if(pType==this.DOUBLE)
   {
      var lValue=parseFloat(pStringValue);
      //debug('Parse as '+pType+':   original='+pStringValue+'   Value='+lValue);
      return lValue;
   }
   else if(pType==this.TIMESTAMP)
   {
      var lValue=parseLong(pStringValue);
      //debug('Parse as '+pType+':   original='+pStringValue+'   Value='+lValue);
      return lValue;
   }
   else
   {
      return pStringValue;
   }
}

