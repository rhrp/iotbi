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
      this.title= undefined;
      this.description='xxx';
      this.schema={};
      this.required=[];
  }
  getSchema()
  {
     return this.schema;
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
  setTitle(pTitle)
  {
     debug('setTitle('+pTitle+')');
     this.title=pTitle;
  }
  setDescription(pDescription) 
  {
     debug('setDescription('+pDescription+')');
     this.description=pDescription;
  }
  getName()
  {
     return this.name;
  }
  getTitle()
  {
     return this.title;
  }
  getDescription()
  {
     return this.description;
  }
  // Add an attribute definitions
  addAttribute(pName,pType,pItemType,pOneOf,pAnyOf,pObject,pDescription,pRef,pRefType) 
  {
      this.schema[pName]={};
      if(pType=='array')
      {
         this.schema[pName].type = pType;
         this.schema[pName].itemType = pRefType!=undefined?pRefType:pItemType;
      }
      else
      {   
         this.schema[pName].type = pRefType!=undefined?pRefType:pType;
         if(pItemType!=undefined)
         {
            this.schema[pName].itemType = pItemType;
         }
      }
      if(pOneOf!=undefined)
      {
        this.schema[pName].oneOf = pOneOf;
      }
      if(pAnyOf!=undefined)
      {
        this.schema[pName].anyOf = pAnyOf;
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
      console.log('Schema of '+this.name+': '+JSON.stringify(this,null,2));
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
       lList.push(lSchemaAttr);
    }
    return lList;
  }  
  getAllNameFields()
  {
     var lList=[];
     for(var lAttribName in this.schema)
     {
       lList.push(lAttribName);
    }
    return lList;
  } 
  getFieldByName(pAttribName)
  {
    //debug('getFieldByName('+pAttribName+')');
    var lValue=this.schema[pAttribName];
    //debug('getFieldByName('+pAttribName+')='+JSON.stringify(lValue));
    return lValue;
  }
  isArrayByName(pAttribName)
  {
    var lField=this.getFieldByName(pAttribName);
    return lField.type=='array';
  }
  isOneOfByName(pAttribName)
  {
    var lField=this.getFieldByName(pAttribName);
    return lField.type=='oneOf' || lField.oneOf!=undefined;
  }
  isAnyOfByName(pAttribName)
  {
    var lField=this.getFieldByName(pAttribName);
    return lField.type=='anyOf' || lField.anyOf!=undefined;
  }
  getItemTypeByName(pAttribName)
  {
    var lField=this.getFieldByName(pAttribName);
    return lField.itemType;
  }
}
module.exports = EntitySchema;
