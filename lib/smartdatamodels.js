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
                 //debug('Found <'+pEntityType+'> / <'+pPropName+'> in '+lUrlRef+'     '+JSON.stringify(lTypeObj));
                 return lTypeObj.type;
           }
           //debug('Will be handled by another Schema: <'+pEntityType+'> / <'+pPropName+'> in '+lUrlRef+' :: '+lDefRef);
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
    var lSchemas=gSchemas[pEntityType];
    if(lSchemas==undefined)
    {
       debug('Entity: '+pEntityType+' no schemas');
       return undefined;
    }
    var lSchema=lSchemas[lUrlSchema];
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
                   if(lSchemas[lUrlRef]==undefined && needLoadingCommonSchema(lUrlRef))
                   {
                       //console.log('<'+lUrlRef+'> :: '+JSON.stringify(lSchemas,null,2));
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
                   else if(lSchemas[lUrlRef]!=undefined)
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
           parseSchema(pEntityType,lSchema,lEntitySchema);
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
   for(var lAttrib of pSchema.allOf)
   {
       if(lAttrib['properties']!=undefined)
       {
          parseProperties(pEntityType,lAttrib.properties,pEntitySchema);
       }
       else if(lAttrib['$ref']!=undefined)
       {
          var lRef=lAttrib['$ref'];
          var lUrlRef=lRef.split('#')[0];
          var lDefRef=lRef.split('#')[1];

          var lEntitySchema=getSchema(pEntityType,lUrlRef);
          if(lEntitySchema==null)
          {
             // All Entity's Schemas are parsed in getEntitySchema(),
             // This may be a common Schema. The 
             var lCommonSchema=getCommonSchema(lUrlRef);
             if(lCommonSchema==undefined)
             {
                debug('Add this Schema to your Common schemas config: '+lUrlRef);
             }
             else if(lDefRef.startsWith('/definitions'))
             {
                var lDefRefGroup=lDefRef.replace('/definitions/','');
                parseRefDefinitionsGroup(pEntityType,lCommonSchema,lDefRefGroup,pEntitySchema);
             }
             else
             {
                debug('TODO: $ref='+lUrlRef+'  DefRef='+lDefRef);
             }
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
function parseProperties(pEntityType,pSchemaProperties,pEntitySchema)
{
     var lParcialSchema=toEntityProperties(pEntityType,pSchemaProperties);
     for(var lKey in lParcialSchema)
     {
          var lAttr=lParcialSchema[lKey];
          pEntitySchema.addAttribute(lKey,lAttr.type,lAttr.itemsType,lAttr.oneOf,lAttr.object,lAttr.description);
     }
}
/**
 * Parses the Definition Group of a $ref url
 */
function parseRefDefinitionsGroup(pEntityType,pSchema,pDefRefGroup,pEntitySchema)
{
   lDefinitionsGroup=pSchema.definitions[pDefRefGroup];
   //debug('parseRefDefinitionsGroup: '+pDefRefGroup+':\n'+JSON.stringify(lDefinitionsGroup,null,2));
   if(lDefinitionsGroup['type']=='object' && lDefinitionsGroup['properties']!=undefined)
   {
       parseProperties(pEntityType,lDefinitionsGroup.properties,pEntitySchema);
   }
   else
   {
       debug('TODO: parseRefDefinitionsGroup :: type='+lDefinitionsGroup['type']);
   }
}
/**
 * Converts the SDM structure to EntitySchema's structure
 */
function toEntityProperties(pEntityType,pProperties)
{
   var lPropsList={};
   for(var lPropName in pProperties)
   {
         var lPropType=toEntityProperty(pEntityType,lPropName,pProperties[lPropName]);
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
function toEntityProperty(pEntityType,pPropName,pProperty)
{
    var lProp={};
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
            var lType=parseDefinitionOf(lItem['$ref']);
            if(lType!=null)
            {
               lProp['type']='array';
               lProp['itemsType']=lType;
               return  lProp;
            }
         }
         else if(lItem['anyOf']!=undefined)
         {
            lProp['type']='array';
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
         lProp['description']=pProperty['description'];
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
        var lType=parseDefinitionOf(pProperty['$ref']);
        if(lType!=undefined)
        {
           lProp['type']=lType;
           return  lProp;
        }
        else
        {
           debug('TODO: '+pPropName+' :: '+JSON.stringify(lProp));
        }
    }
    else if(pProperty['oneOf']!=undefined)
    {
         lProp['type']='oneOf';
         lProp['oneOf']=pProperty['oneOf'];
         return  lProp;
    }
    else if(pProperty['anyOf']!=undefined)
    {
         lProp['type']='anyOf';
         lProp['oneOf']=pProperty['anyOf'];
         return  lProp;
    }
    else
    {
       debug('TODO:  '+pPropName+' = '+JSON.stringify(pProperty));
    }
}
/**
 * Get the Type from its $ref URL
 */
function parseDefinitionOf(pRef)
{
   var lUrlParts=pRef.split("#");
   if(lUrlParts.length!=2)
   {
      return undefined;
   }
   var lUrlSchema=lUrlParts[0];
   var lDefType=lUrlParts[1];
   if(lDefType.startsWith('/definitions/'))
   {
       return lDefType.replaceAll('/definitions/','');
   }
   return undefined;
}


