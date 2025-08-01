/**
 * API for providing Matadata
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 */

//TODO:  falta o precarregamento de todas as entidades e contextos

var configsys = require('./configsys.js');
var sdm = require('./smartdatamodels.js');
var ngsildcontext = require('./ngsildcontext.js');
var schema = require('./schema.js');
var debug = require('debug')('iotbi.metadata');

exports.toTableEntityMetadata = toTableEntityMetadata; 
exports.toTableListOfEntities = toTableListOfEntities;

function toTableEntityMetadata(pNgsiEntityType)
{
   var lTable=[];
   var lSchema={};
   debug('toTableEntityMetadata('+pNgsiEntityType+')');
   var lEntitySchema=sdm.getEntitySchema(pNgsiEntityType);
   if(lEntitySchema==undefined)
   {
      debug('Schema not found for entity: '+pNgsiEntityType);
      return [lTable,lSchema];
   }
   //debug(pNgsiEntityType+' Schema='+JSON.stringify(lEntitySchema.getSchema(),null,2));
   //var lUrlContext=configsys.getNgsiLdEntityContext(pNgsiEntityType);
   debug(pNgsiEntityType+'  URL Context:'+lUrlContext);

   for(lPropName of lEntitySchema.getAllNameFields())
   {
      var lProp=lEntitySchema.getFieldByName(lPropName);
      var lType;
      var lIsArray;
      var lOneOf;
      var lAnyOf;
      var lDescription;
      var lUrlContext;

      if(lEntitySchema.isArrayByName(lPropName))
      {
         lIsArray=true;
         lType=lProp.itemType;
         lOneOf='';
         lAnyOf='';
      }
      else if(lEntitySchema.isOneOfByName(lPropName))
      {
         lIsArray=false;
         lType=lProp.type;
         lOneOf='';
         for(a of lProp.oneOf)
         {
           lOneOf+=lOneOf+JSON.stringify(a);
         }
         lAnyOf='';
      }
      else if(lEntitySchema.isAnyOfByName(lPropName))
      {
         lIsArray=false;
         lType=lProp.type;
         lOneOf='';
         lAnyOf='';
         for(a of lProp.anyOf)
         {
           lAnyOf+=lAnyOf+JSON.stringify(a);
         }
      }
      else
      {
         lIsArray=false;
         lType=lProp.type;
         lOneOf='';
      }
      var lUrlCtx=ngsildcontext.getNgsiLdPropertyContext(pNgsiEntityType,lPropName);
      lTable.push({'property':lPropName,'type':lType,'array':lIsArray,'oneOf':lOneOf,'anyOf':lAnyOf,'urlContext':lUrlCtx,'description':lProp.description});
   }
   lSchema['property']=schema.STRING;
   lSchema['type']=schema.STRING;
   lSchema['array']=schema.BOOLEAN;
   lSchema['oneOf']=schema.STRING;
   lSchema['anyOf']=schema.STRING;
   lSchema['urlContext']=schema.STRING;
   lSchema['description']=schema.STRING;

   return [lTable,lSchema];
}

function toTableListOfEntities(pFiwareService)
{
   var lTable=[];
   var lSchema={};
   debug('toTableListOfEntities('+pFiwareService+')');

   for(lEntityType in configsys.getNgsiLdEntityAllContexts())
   {
       var lEntitySchema=sdm.getEntitySchema(lEntityType);
       var lUrl=configsys.getNgsiLdEntityContext(lEntityType);
       var lDescription;
       var lTitle;
       if(lEntitySchema!=undefined)
       {
         //debug(lEntityType+' Schema='+JSON.stringify(lEntitySchema.getSchema(),null,2));
         lDescription=lEntitySchema.getDescription();
         lTitle=lEntitySchema.getTitle();
       }
       else
       {
         lDescription=undefined;
         lTitle=undefined;
         debug(lEntityType+' Schema=undefined')
       }

       lTable.push({'entityType':lEntityType,'urlSchema':lUrl,'title':lTitle,'description':lDescription});
   }
   lSchema['entityType']=schema.STRING;
   lSchema['urlSchema']=schema.STRING;
   lSchema['title']=schema.STRING;
   lSchema['description']=schema.STRING;
   
   return [lTable,lSchema];
}
