/*!
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the MCP route.
 * 
 */
var z = require('zod/v4');
var express = require('express');
var debug = require('debug')('iotbi.mcp');
var configsys = require('./configsys.js');
var metadatasys = require('../lib/metadatasys.js');


/**
 * Schema of a EntityType
 */
const zNgsiEntity = z.object({
    entityType: z.string().describe('Entity type'),
    urlSchema: z.string().describe('URL of the Entity\'Schema'),
    title: z.string(),
    description: z.string().describe('Description of the entity')
  });
/**
 * Schema of a relationship among two EntityType
 */
const zNgsiEntityRelationship = z.object({
    fiwareService: z.string(),
    entityType: z.string(),
    attribute: z.string(),
    relEntityType: z.string()
  });

/**
 *
 */
exports.getAvailableEntitiesMetadata =  {
        title: 'Available Entities Tool ',
        description: 'Provides a list of all avalilable Entities',
        inputSchema: {},
        outputSchema: {entities:z.array(zNgsiEntity)}
    };

/**
 *
 */
exports.getAvailableEntities = async function getAvailableEntities()
{
     debug('getAvailableEntities');
     var lEntitiesAsTableAndSchema=metadatasys.toTableListOfEntities();
     var lEntitiesList=lEntitiesAsTableAndSchema[0];
     var output={entities:lEntitiesList}
     debug('Output: '+JSON.stringify(output,null,2));
     return {
            content: [{ type: 'text', text: JSON.stringify(output) }],
            structuredContent: output
     } 
} 


exports.getRelatedEntitiesMetadata =  {
        title: 'Related entities Tool ',
        description: 'List all the relationships between entities that share a common atribute.\nUse the values in `entity_foregin_id` to find which Ids to search in the related entities.',
        inputSchema: {entitiesTypes:z.array(z.string())},
        outputSchema: {relatedEntities:z.array(zNgsiEntityRelationship)}
    };
exports.getRelatedEntities = async function getRelatedEntities(zEntitiesTypes)
{
     var lListEntityTypes=zEntitiesTypes.entitiesTypes;
     debug('getRelatedEntities('+lListEntityTypes+')');
     var lServices=getAllowedServices();
     debug(JSON.stringify(lServices));
     lRelationshipsAsTableAndSchema=await metadatasys.toTableEntityRelationships(lServices,lListEntityTypes)
     debug('TableAndSchema: '+JSON.stringify(lRelationshipsAsTableAndSchema,null,2));
     var lRelationshipsList=lRelationshipsAsTableAndSchema[0];
     var output={relatedEntities:lRelationshipsList}
     debug('Output: '+JSON.stringify(output,null,2));
     return {
                content: [{ type: 'text', text: JSON.stringify(output) }],
                structuredContent: output
            };
}


function getAllowedServices()
{
     var lUser=configsys.getMcpAppKey();
     var lAliasArray=configsys.getBrokersAliasList();
     var lServices=[];
     for(var i in lAliasArray)
     {
         if(configsys.allowScopeToUser(lUser,lAliasArray[i]) && configsys.isConfigOk(lAliasArray[i]) && configsys.isVersionLDV1(lAliasArray[i]))
         {
             lServices.push(lAliasArray[i]);
         }
     }
     debug('User: '+lUser+' Allowed Services: '+JSON.stringify(lServices));
     return lServices
}
