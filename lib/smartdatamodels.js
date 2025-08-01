/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements routines for managing schemas according the Smart Data Models
 */
var schema = require('./schema.js');
var EntitySchema = require('./EntitySchema.js');
var debug = require('debug')('iotbi.sdm');

var gSchemas={};
var gSchemasCommons={};

const TYPE_ENTITY_IDENTIFIER = "EntityIdentifierType";

// Methods for loading SDM Schemas
exports.addSchema = addSchema;
exports.addCommonSchema = addCommonSchema;
exports.needLoadingSchema = needLoadingSchema;
exports.needLoadingCommonSchema = needLoadingCommonSchema;
exports.loadedSchemas = loadedSchemas;
// Quering types 
exports.getTypeOf = getTypeOf;
// Get the EntitySchema object of a Entity
exports.getEntitySchema = getEntitySchema;

exports.TYPE_ENTITY_IDENTIFIER = TYPE_ENTITY_IDENTIFIER;

function addSchema(pEntityType,pUrl,pSchema)
{
    debug('Adding Schema for '+pEntityType+' :: '+pUrl);
    if(gSchemas[pEntityType]==undefined)
    {
        gSchemas[pEntityType]={};
    }
    gSchemas[pEntityType][pUrl]=pSchema;
}
function getSchema(pEntityType,pUrl)
{
    if(gSchemas[pEntityType]!=undefined)
       return gSchemas[pEntityType][pUrl];
    else
       return undefined;
}
function addCommonSchema(pUrl,pSchema)
{
    debug('Adding Common Schema: '+pUrl);
    gSchemasCommons[pUrl]=pSchema;
}
function getCommonSchema(pUrl)
{
    return gSchemasCommons[pUrl];
}
function needLoadingSchema(pEntityType)
{
   if(pEntityType==undefined)
   {
     return true;
   }
   return gSchemas[pEntityType]==undefined;
}
function needLoadingSchemaUrl(pEntityType,pUrl)
{
   if(needLoadingSchema(pEntityType))
   {
       return true
   }
   return gSchemas[pEntityType][pUrl]==undefined;
}
function needLoadingCommonSchema(pUrl)
{
   if(pUrl==undefined)
   {
     return undefined;  
   }
   return gSchemasCommons[pUrl]==undefined;
}

/**
 * Finds the type of a property by its name
 * TODO: May occur an ambiguity if there is another Type with name pLevelAttrib in another hierarchy 
 */
function getTypeOf(pEntityType,pPropName)
{
   var lTypeObj=findEntityTypeObjectOf(pEntityType,pPropName);
   if(lTypeObj==undefined)
   {
      debug('Entity: '+pEntityType+' Prop:'+pPropName+' not definition!');
      return undefined;
   }
   else if(lTypeObj['$ref']!=undefined)
   {
      var lRef=lTypeObj['$ref'];
      var lUrlRef=lRef.split('#')[0];
      var lDefRef=lRef.split('#')[1];
      var lSchemas=gSchemas[pEntityType];
      if(needLoadingSchemaUrl(pEntityType,lUrlRef) && needLoadingCommonSchema(lUrlRef))
      {
           debug('Add <'+lUrlRef+'>#<'+lDefRef+'> to '+pEntityType+'\'s config schema!');
      }
      else if(gSchemasCommons[lUrlRef]!=undefined)
      {
           // Reference to a Common Schema
           var lTypeObj=findCommonOnUrlTypeObjectOf(lUrlRef,pPropName);
           if(lTypeObj!=undefined)
           {
                 debug('Found <'+pEntityType+'> / <'+pPropName+'> in '+lUrlRef+'     '+JSON.stringify(lTypeObj));
                 return lTypeObj.type;
           }
           debug('Will be handled by another Schema: <'+pEntityType+'> / <'+pPropName+'> in '+lUrlRef+' :: '+lDefRef);
      }
      else if(lSchemas[lUrlRef]!=undefined)
      {
           // Reference to a Entity's Schema
           debug('Handled by another Schema <'+lUrlRef+'>#<'+lDefRef+'> of this Entity <'+pEntityType+'>')
           var lTypeObj=findOnUrlEntityTypeObjectOf(pEntityType,lUrlRef,pPropName);
           if(lTypeObj!=undefined)
           {
                 //debug('Found <'+pEntityType+'> / <'+pPropName+'> in '+lUrlRef);
                 return lTypeObj.type;
           }
      }
      else
      {
           debug('TODO search this Entity: '+pEntityType+' and Prop:'+pPropName+' in <'+lUrlRef+'>#<'+lDefRef+'>');
      }
      return undefined;
   }
   else if(lTypeObj['format']!=undefined && lTypeObj['format']==schema.DATETIME)
   {
      return schema.DATETIME;
   }
   else if(lTypeObj['format']=='object')
   {
      debug('TODO Entity: '+pEntityType+'  Prop:'+pPropName+' is an Object. How to handle it?');
      return schema.OBJECT;
   }
   else if(lTypeObj['type']=='array')
   {
      return lTypeObj.items.type;
   }
   else if(lTypeObj['anyOf']!=undefined)
   {
      //Any type is possible!
      debug('Found '+pEntityType+' /  '+pPropName+' :: AnyOf :: '+JSON.stringify(lTypeObj));
      return undefined;
   }
   else if(lTypeObj['type']==undefined)
   {
      debug('Found '+pEntityType+' /  '+pPropName+' :: Type='+JSON.stringify(lTypeObj));
      return undefined;
   }
   else
   {
      //debug('Found '+pEntityType+' /  '+pPropName+' :: Type='+lTypeObj.type);
      return lTypeObj.type;
   }
}
/**
 * Find a type in all Entity's URLs
 */
function findEntityTypeObjectOf(pEntityType,pPropName)
{
    var lSchemas=gSchemas[pEntityType];
    if(lSchemas!=undefined)
    {
       for(var lUrlSchema in lSchemas)
       {
          var lTypeObj=findOnUrlEntityTypeObjectOf(pEntityType,lUrlSchema,pPropName);
          if(lTypeObj!=undefined)
          {
            return lTypeObj;
          }
       }
    }
    return undefined;
}
/**
 * Find a type in a Entity's URL
 */
function findOnUrlEntityTypeObjectOf(pEntityType,lUrlSchema,pPropName)
{
    var lSchemasEntity=gSchemas[pEntityType];
    if(lSchemasEntity==undefined)
    {
       debug('Entity: '+pEntityType+' no schemas');
       return undefined;
    }
    var lSchema=lSchemasEntity[lUrlSchema];
    if(lSchema==undefined)
    {
      debug('Entity: '+pEntityType+' no schema for '+lUrlSchema);
      return undefined;
    }
    if(lSchema['allOf']!=undefined)
    {
            //debug('Search '+pPropName+' in '+pEntityType+' :: '+lUrlSchema);
            for(var lGroupType of lSchema.allOf)
            {
               var lRef=lGroupType['$ref'];
               if(lRef!=undefined)
               {
                   var lUrlRef=lRef.split('#')[0];
                   var lDefRef=lRef.split('#')[1];
                   if(lSchemasEntity[lUrlRef]==undefined && needLoadingCommonSchema(lUrlRef))
                   {
                       //console.log('<'+lUrlRef+'> :: '+JSON.stringify(lSchemasEntity,null,2));
                       debug('Add <'+lUrlRef+'> to '+pEntityType+'\'s config schema!');
                   }
                   else if(gSchemasCommons[lUrlRef]!=undefined)
                   {
                       // Reference to a Common Schema
                       var lTypeObj=findCommonOnUrlTypeObjectOf(lUrlRef,pPropName);
                       if(lTypeObj!=undefined)
                       {
                          //debug('Found <'+pEntityType+'> / <'+pPropName+'> in '+lUrlRef);
                          return lTypeObj;
                       }
                       //debug('Will be handled by another Schema: <'+pEntityType+'> / <'+pPropName+'> in '+lUrlRef+' :: '+lDefRef);
                   }
                   else if(lSchemasEntity[lUrlRef]!=undefined)
                   {
                       // Reference to a Entity's Schema
                       //debug('Will be handled by another Schema of this Entity')
                   }
                   else
                   {
                       debug('Unexpected case!');
                   }
               }
               else
               {
                   //debug(pPropName+'=='+JSON.stringify(lGroupType.properties,null,2));
                   //debug(pPropName+'=='+JSON.stringify(lGroupType.properties[pPropName],null,2));
                   var lType=lGroupType.properties[pPropName];
                   if(lType!=undefined)
                   {
                      return lType;
                   }
                   else
                   {
                      //debug('Entity: '+pEntityType+'  Prop:'+pPropName+' :: no type definition in this Group Type. We will check on common schemas!');
                   }
               }
            }
    }
    else
    {
            if(lSchema['definitions']==undefined)
            {
                debug('Definitions allOf not found in the Schema of '+pEntityType);
            }
            else
            {
                debug('Definitions allOf not found in the Schema of '+pEntityType+', but definitions exists! May be this is an Common schema');
            }
    }
    return undefined;
}

/**
 * Find the type of a property in Commons Schemas
 */
function findCommonTypeObjectOf(pPropName)
{
    //debug('Common Schemas:\n'+JSON.stringify(gSchemasCommons,null,2));
    for(var lCommonSchemaUrl in gSchemasCommons)
    {
        var lTypeObj=findCommonOnUrlTypeObjectOf(lCommonSchemaUrl,pPropName);
        if(lTypeObj!=undefined)
        {
            return lTypeObj;
        }
    }
    return undefined;
}
/**
 * Try to find a prop in a Common Schema
 */
function findCommonOnUrlTypeObjectOf(pCommonSchemaUrl,pPropName)
{
    //debug('Search <'+pPropName+'> in common Schema: '+pCommonSchemaUrl);
    var lCommonSchema=gSchemasCommons[pCommonSchemaUrl];
    if(lCommonSchema['definitions']==undefined)
    {
          if(lCommonSchema['allOf']==undefined)
          {
             debug('Definitions not found in common Schema.');
          }
          else
          {
             debug('Definitions not found in common Schema, but allOf exists. May be this is an entity schema'); 
          } 
    }
    else
    {
          //debug('lCommonSchema:\n'+JSON.stringify(lCommonSchema,null,2))
          for(var lDefName in lCommonSchema.definitions)
          {
             //debug('Search <'+pPropName+'> in Common Schema: '+pCommonSchemaUrl+'  /  '+lDefName);
             var lDefObj=lCommonSchema.definitions[lDefName];
             //debug('lDefObj:\n'+JSON.stringify(lDefObj,null,2));
             if(lDefName==pPropName)
             {
                return lCommonSchema.definitions[lDefName];
             }
             else if(lDefObj.type=='object')
             {
                //debug('Search <'+pPropName+'> in Common Schema: '+pCommonSchemaUrl+'  /  '+lDefName+' recursively in Object');
                var lTypeObject=recursiveSearchTypeObj(pCommonSchemaUrl,lCommonSchema.definitions,lDefObj,pPropName);
                if(lTypeObject!=undefined)
                {
                   //debug('Found in: '+pCommonSchemaUrl+'  /  '+lDefName+' :: Prop: '+pPropName+' Type='+JSON.stringify(lTypeObject));
                   return lTypeObject;
                }
            }
         }
   }
   return undefined;  
}
function recursiveSearchTypeObj(pUrlSchema,pDefSchema,pDefObj,pPropName)
{
     for(var lPropName in pDefObj.properties)
     {
        //debug('['+pPropName+'] -- '+lPropName);
        var lProp=pDefObj.properties[lPropName];
        if(lProp['type']!=undefined)
        {
             if(lProp.type=='object')
             {
                 //debug('Found an Object: '+JSON.stringify(lProp));
                 return recursiveSearchTypeObj(pUrlSchema,pDefSchema,lProp,pPropName);
             }
             else if(lPropName==pPropName)
             {
                //debug('Found '+pPropName+': '+JSON.stringify(lProp));
                return lProp;
             }
        }
        else if(lPropName==pPropName && lProp['$ref']!=undefined )
        {
             var lRefTypeStr=lProp['$ref'];
             var lRefType=lRefTypeStr.replaceAll('#/definitions/','');
             //debug('Reference to: '+lRefType+' in '+JSON.stringify(pDefSchema,null,2));
             var lDefObj=pDefSchema[lRefType];
             //debug('Search <'+pPropName+'> is mapped as <'+lRefType+'><'+lRefTypeStr+'> in object: '+JSON.stringify(lDefObj,null,2));
             //debug('Search <'+pPropName+'> is mapped as <'+lRefType+'><'+lRefTypeStr+'>');
             //Search for lRefType
             return findCommonTypeObjectOf(lRefType);
        }
     }
     return undefined;
}

/**
 * Produces the JSON to be presented when the user asks about the loaded schemas 
 */
function loadedSchemas()
{
  var lOut={};

  lOut['Schemas']={};
  for(var lEntityName in gSchemas)
  {
      lOut.Schemas[lEntityName]=[];
      for(var lSchemaUrl in gSchemas[lEntityName])
      {
         lOut.Schemas[lEntityName].push(lSchemaUrl);
      }
  }
  lOut['CommonSchemas']=[];
  for(var lSchemaUrl in gSchemasCommons)
  {
      lOut.CommonSchemas.push(lSchemaUrl);
  }
  return lOut;
}


/**
 * Get the Schema of a EntityType. Includes Properties and RelationShips
 * Returns a Class EntitySchema
 */
function getEntitySchema(pEntityType)
{
   debug('getEntitySchema('+pEntityType+')');
   var lSchemas=gSchemas[pEntityType];
   if(lSchemas!=undefined)
   {
       var lEntitySchema=new EntitySchema(pEntityType);
       for(var lUrlSchema in lSchemas)
       {
           var lSchema=lSchemas[lUrlSchema];
           if(lSchema['required']!=undefined)
           {
             lEntitySchema.addRequired(lSchema['required']);
           }
try
{
           debug('Parse: '+lUrlSchema);
           parseSchema(pEntityType,lSchema,lEntitySchema);
}
catch(eee)
{
   debug('xxxxxxx '+eee.stack);
}
       }
       return lEntitySchema;
    }
    return undefined;
}

/**
 * Parse the SDM schema and store it as an EntitySchema object
 */
function parseSchema(pEntityType,pSchema,pEntitySchema)
{
   pEntitySchema.setTitle(pSchema['title']);
   pEntitySchema.setDescription(pSchema['description']);
   for(var lAttrib of pSchema.allOf)
   {
       if(lAttrib['properties']!=undefined)
       {
          parseProperties(pEntityType,lAttrib.properties,pEntitySchema,'http://todo');
       }
       else if(lAttrib['$ref']!=undefined)
       {
          var lRef=lAttrib['$ref'];
          var lUrlRef=lRef.split('#')[0];
          var lDefRef=lRef.split('#')[1];

          var lEntitySchema=getSchema(pEntityType,lUrlRef);
          if(lEntitySchema==null)
          {
             // All Entity's Schemas are parsed in getEntitySchema(), so this may be a common Schema.
             var lCommonSchema=getCommonSchema(lUrlRef);
             if(lCommonSchema==undefined)
             {
                debug('Add this Schema to your Common schemas config: '+lUrlRef);
             }
             else if(lDefRef.startsWith('/definitions'))
             {
                var lDefRefGroup=lDefRef.replace('/definitions/','');
                parseRefDefinitionsGroup(pEntityType,lCommonSchema,lDefRefGroup,pEntitySchema,lUrlRef);
             }
             else
             {
                debug('TODO: $ref='+lUrlRef+'  DefRef='+lDefRef);
             }
          }
          else
          {
             debug('TODO: $ref='+lUrlRef+'  DefRef='+lDefRef+'  search in EntitySchema?');
          }
       }
       else
       {
          debug('Error: \n'+JSON.stringify(lAttrib,null,2))
       }
   }
   return pEntitySchema;
}
/**
 * Parse the properties section of the Schema
 */
function parseProperties(pEntityType,pSchemaProperties,pEntitySchema,pSchemaCtxUrl)
{
     var lParcialSchema=toEntityProperties(pEntityType,pSchemaProperties,pSchemaCtxUrl);
     for(var lKey in lParcialSchema)
     {
          var lAttr=lParcialSchema[lKey];
          pEntitySchema.addAttribute(lKey,lAttr.type,lAttr.itemsType,lAttr.oneOf,lAttr.anyOf,lAttr.object,lAttr.description,lAttr.ref,lAttr.refType);
     }
}
/**
 * Parses the Definition Group of a $ref url
 */
function parseRefDefinitionsGroup(pEntityType,pSchema,pDefRefGroup,pEntitySchema,pSchemaCtxUrl)
{
   lDefinitionsGroup=pSchema.definitions[pDefRefGroup];
   //debug('parseRefDefinitionsGroup('+pEntityType+','+pSchema+','+pDefRefGroup+','+pEntitySchema+'): '+pDefRefGroup+':\n'+JSON.stringify(lDefinitionsGroup,null,2));
   if(lDefinitionsGroup['type']=='object' && lDefinitionsGroup['properties']!=undefined)
   {
       parseProperties(pEntityType,lDefinitionsGroup.properties,pEntitySchema,pSchemaCtxUrl);
   }
   else
   {
       debug('TODO: parseRefDefinitionsGroup :: type='+lDefinitionsGroup['type']);
   }
}
/**
 * Converts the SDM model's properties substructure to EntitySchema's structure
 * pSchemaCtxUrl - Schema in the current Context 
 */
function toEntityProperties(pEntityType,pProperties,pSchemaCtxUrl)
{
   var lPropsList={};
   for(var lPropName in pProperties)
   {
         var lPropObj=pProperties[lPropName];
         var lPropType=toEntityProperty(pEntityType,lPropName,lPropObj,pSchemaCtxUrl);
         if(lPropType!=undefined)
         {
            lPropsList[lPropName]=lPropType;
         }
   }
   return lPropsList;
}
/**
 * Converts the SDM structure to EntitySchema's structure
 */
function toEntityProperty(pEntityType,pPropName,pProperty,pSchemaCtxUrl)
{
    var lProp={};
    if(pProperty['description']!=undefined)
    {
       lProp['description']=pProperty['description'];
    }
    if(pProperty['type']=='array')
    {
         var lItem=pProperty['items'];
         if(lItem['type']!=undefined)
         {
            lProp['type']='array';
            lProp['itemsType']=lItem['type'];
            return lProp;
         }
         else if(lItem['$ref']!=undefined)
         {
            var lTypeObj=parseDefinitionOf(pEntityType,lItem['$ref']);
            lProp['ref']=pProperty['$ref'];
            if(lTypeObj!=null)
            {
               //TODO: a recursive approach may be needed
               lProp['type']='array';
               lProp['itemsType']=lTypeObj.type;
               lProp['refType']=lTypeObj['itemsType'];
               return  lProp;
            }
         }
         else if(lItem['anyOf']!=undefined)
         {
            lProp['type']='anyOf';
            lProp['anyOf']=lItem['anyOf'];
            return lProp;
         }
         else
         {
            debug('TODO: '+pPropName+' = array of '+JSON.stringify(lItem));
         }
    }
    else if(pProperty['type']=='object')
    {
         lProp['type']=pProperty['type'];
         lProp['object']=pProperty['object'];
         return  lProp;
    }
    else if(pProperty['type']!=undefined)
    {
         lProp['type']=pProperty['type'];
         return  lProp;
    }
    else if(pProperty['$ref']!=undefined)
    {
        var lUrlRef=pProperty['$ref'];
        if(lUrlRef.startsWith('#'))
        {
           //Adapt to context
           debug('Define URL Ctx Schema : '+pSchemaCtxUrl+' + '+lUrlRef);
           lUrlRef=pSchemaCtxUrl+lUrlRef
           //debug('toEntityProperty('+pEntityType+','+pPropName+')    $ref='+lUrlRef);
        }
        var lTypeObj=parseDefinitionOf(pEntityType,lUrlRef);
        if(lTypeObj!=undefined)
        {
           lProp=toEntityProperty(pEntityType,pPropName,lTypeObj,pSchemaCtxUrl);  // Recursive parsing
           lProp['refType']=lTypeObj['itemsType'];
        }
        else
        {
           debug('TODO: toEntityProperty('+pEntityType+','+pPropName+')  $ref='+lUrlRef+' :: '+JSON.stringify(lProp));
        }
        lProp['ref']=pProperty['$ref'];
        return  lProp;
    }
    else if(pProperty['oneOf']!=undefined)
    {
         lProp['type']='oneOf';
         lProp['itemsType']=pPropName; // The property name is the name of the type
         lProp['oneOf']=pProperty['oneOf'];
         return  lProp;
    }
    else if(pProperty['anyOf']!=undefined)
    {
         lProp['type']='anyOf';
         lProp['anyOf']=pProperty['anyOf'];
         return  lProp;
    }
    else
    {
       debug('TODO:  '+pPropName+' = '+JSON.stringify(pProperty));
    }
}
/**
 * Get the Object Type from its $ref URL
 */
function parseDefinitionOf(pEntityType,pRef)
{
   var lUrlParts=pRef.split("#");
   if(lUrlParts.length!=2)
   {
      return undefined;
   }
   var lUrlSchema=lUrlParts[0];
   var lDefType=lUrlParts[1];
   //Search in definitions section
   if(lDefType.startsWith('/definitions/'))
   {
       var lDefPath=lDefType.replaceAll('/definitions/','');
       var lObj;
       //TODO: check if lUrlSchema is a common schema or entity's schema
       //if(lUrlSchema!=undefined && lUrlSchema.length>0)
       //{
          //debug('parseDefinitionOf('+pEntityType+','+pRef+') => Will search in commons schemas');
          lObj=findOnCommonSchemaUrlTypeObjectByPath(lUrlSchema,lDefPath);
          //debug(lDefPath+' :: ItemsType='+lObj['itemsType']);
       //}
       //else
       //{
       //   debug('parseDefinitionOf('+pEntityType+','+pRef+') => Will search in Entity '+pEntityType+' schemas');
       //   lObj=findOnEntitySchemaUrlTypeObjectByPath(pEntityType,lDefPath);
       //}
       return lObj;
   }
   return undefined;
}

/**
 * Find an object by its path in definitons section
 */
function findOnCommonSchemaUrlTypeObjectByPath(pCommonSchemaUrl,pDefPath)
{
    //debug('Search <'+pDefPath+'> in common Schema: '+pCommonSchemaUrl);
    var lCommonSchema=gSchemasCommons[pCommonSchemaUrl];
    if(lCommonSchema==undefined)
    {
       debug('Common Schema not found:'+pCommonSchemaUrl);
       return undefined;
    }
    if(lCommonSchema['definitions']==undefined)
    {
       return undefined;
    }
    return findOnUrlSchemaTypeObjectByPath(lCommonSchema,pDefPath)
}
function findOnEntitySchemaUrlTypeObjectByPath(pEntityType,pDefPath)
{
    //debug('Search <'+pDefPath+'> in '+pEntityType+' Schemas');
    var lEntitySchema=gSchemas[pEntityType];
    if(lEntitySchema==undefined)
    {
       debug('Entity Schema not found for: '+pEntityType);
       return undefined;
    }
    if(lEntitySchema['definitions']==undefined)
    {
       debug('Entity Schema of '+pEntityType+'  dont have definitions');
       return undefined;
    }
    return findOnUrlSchemaTypeObjectByPath(lEntitySchema,pDefPath)
}

function findOnUrlSchemaTypeObjectByPath(pSchema,pDefPath)
{
    //debug('Search <'+pDefPath+'> in definitions of common Schema: '+pCommonSchemaUrl);
    var lDefinitionsSection=pSchema['definitions'];
    var lDefPathArray=pDefPath.split('/');
    var lDef=lDefinitionsSection;
    for(lDefPathStep of lDefPathArray)
    {
       var  lSubDef=lDef[lDefPathStep];
       if(lSubDef==undefined)
       {
           //debug(' Error Out: '+lDefPathStep);
           return lDef;
       }
       else
       {
           //debug(' Next: '+lDefPathStep);
           lDef=lSubDef;
           lDef['itemsType']=lDefPathStep;
       }
    }
    //debug(' Out: '+JSON.stringify(lDef));
    return lDef;
}
