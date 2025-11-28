/**
 * API for providing Matadata
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 */

//TODO:  falta o precarregamento de todas as entidades e contextos

var configsys = require('./configsys.js');
var sdm = require('./smartdatamodels.js');
var ngsildv1 = require('./ngsildv1.js');
var ngsildcontext = require('./ngsildcontext.js');
var schema = require('./schema.js');
var debug = require('debug')('iotbi.metadata');

exports.toTableEntitySchema = toTableEntitySchema; 
exports.toTableListOfEntities = toTableListOfEntities;
exports.toTableEntityRelationships = toTableEntityRelationships;

/**
 * Generates a table containing the EntityType's Schema
 */
function toTableEntitySchema(pNgsiEntityType)
{
   var lTable=[];
   var lSchema={};
   debug('toTableEntitySchema('+pNgsiEntityType+')');
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

/**
 * Generates a table containing the list of all known EntityTypes in a fiwareService, with the folowing cols:
 *    entityType  schema.STRING;
 *    urlSchema   schema.STRING;
 *    title       schema.STRING;
 *    description schema.STRING;
 */
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

/**
 *  pService => All fiwareServices
 *
 */
function toTableEntityRelationships(pServices,pListEntityTypes)
{
   return new Promise(function(resolve, reject) {
      var lPromises=[];
      var lPromisesEntity=[];
      var lPromisesJoinAttrib=[];
      var lPromisesService=[];

      for(lService of pServices)
      {
        for(lEntityType in configsys.getNgsiLdEntityAllContexts())
        {
          if(pListEntityTypes==undefined || pListEntityTypes.includes(lEntityType))
          {
              var lSchema=sdm.getEntitySchema(lEntityType);
              if(lSchema==undefined)
              {
                  reject('Schema not found for '+lEntityType);
              }
              var lJoinAttribs=lSchema.getRelationShipFields();
              for(lJoinAttribIdx in lJoinAttribs)
              {
                  var lJoinAttrib=lJoinAttribs[lJoinAttribIdx];
                  debug('Entity '+lEntityType+'  -- ['+lJoinAttrib+']--> ???'); 
                  //TODO: multipage
                  lPromises.push(ngsildv1.createPromiseQueryEntitiesAttributes(lService,lEntityType,lJoinAttrib,0,100));
                  lPromisesJoinAttrib.push(lJoinAttrib);
                  lPromisesEntity.push(lEntityType);
                  lPromisesService.push(lService);
              }
           }
        }
      }
      Promise.all(lPromises)
            .then(results => {
               var lTable=[];
               var lSchema={};
               for(var lResultIdx in results)
               {
                  var lResult=results[lResultIdx];
                  var lResultJoinAttrib=lPromisesJoinAttrib[lResultIdx];
                  var lResultEntity=lPromisesEntity[lResultIdx];
                  var lResultService=lPromisesService[lResultIdx];
                  for(var lEntType of ngsildv1.getFkEntitiesTypes(lResult.results,lResultJoinAttrib))
                  {
                     debug('Entity '+lResultEntity+'  -- ['+lResultJoinAttrib+']--> '+lEntType);
                     lTable.push({'fiwareService':lResultService,'entityType':lResultEntity,'attribute':lResultJoinAttrib,'relEntityType':lEntType});
                  }
               }
               lSchema['fiwareService']=schema.STRING;
               lSchema['entityType']=schema.STRING;
               lSchema['attribute']=schema.STRING;
               lSchema['relEntityType']=schema.STRING;

               resolve([lTable,lSchema]);

       })
      .catch(error => {
          debug(error);
          reject(error);
      });
  });
}
