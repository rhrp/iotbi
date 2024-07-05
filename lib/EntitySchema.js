/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This class implements all services related to Entities' Schemas
 * It follows a very similar approach of SDM schemas, the main diference is about $ref statements.
 * In place of $ref, provides those refered schemas
 *
 */
var sdm = require('./smartdatamodels.js');
var debug = require('debug')('iotbi.EntitySchema');

class EntitySchema
{
  constructor(pName)
  {
      this.name = pName;
      this.schema={};
      this.required=[];
  }
  // Defines the required fields
  setRequired(pRequired)
  {
      this.required=pRequired;
  }
  addRequired(pRequired)
  {
      if(pRequired!=undefined)
      {
         for(var c of pRequired)
         {
           if(!this.required.includes(c))
           {
              this.required.push(c);
           }
           else
           {
              debug('Warn: '+c+' is already included in required list'); 
           }
         }
      }
      else
      {
         debug('Warn: trying to set a undefined list');
      }
  }
  // Add an attribute definitions
  addAttribute(pName,pType,pItemType,pOneOf,pObject,pDescription) 
  {
      this.schema[pName]={};
      this.schema[pName].type = pType;
      if(pItemType!=undefined)
      {
         this.schema[pName].itemType = pItemType;
      }
      if(pOneOf!=undefined)
      {
        this.schema[pName].oneOf = pOneOf;
      }
      if(pObject!=undefined)
      {
        this.schema[pName].object = pObject;
      }
      if(pDescription!=undefined)
      {
        this.schema[pName].description = pDescription;
      }

  };
 
  // Show Schema
  showSchema()
  {
      console.log('Schema of '+this.name+':\n'+JSON.stringify(this.schema,null,2));
  };
  /**
   * Return an array with all Relationships field name
   */
  getRelationShipFields()
  {
     var lRelationShipList=[];
     for(var lAttribName in this.schema)
     {
       var lSchemaAttr=this.schema[lAttribName];
       if(lSchemaAttr.type==sdm.TYPE_ENTITY_IDENTIFIER || (lSchemaAttr.itemType!=undefined && lSchemaAttr.itemType==sdm.TYPE_ENTITY_IDENTIFIER))
       {
          lRelationShipList.push(lAttribName);
       }
    }
    return lRelationShipList
  }
  getRequired()
  {
     return this.required;
  }
 /**
  * Returns an array with all attributes
  */
  getAllFields()
  {
     var lList=[];
     for(var lAttribName in this.schema)
     {
       var lSchemaAttr=this.schema[lAttribName];
       lList.push(lAttribName);
    }
    return lList;
  }  
}
module.exports = EntitySchema;
