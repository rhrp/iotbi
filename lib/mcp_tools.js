/*!
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the MCP route.
 * 
 */
var z = require('zod/v4');
var express = require('express');
var debug = require('debug')('iotbi.mcp_tools');
var configsys = require('./configsys.js');
var metadatasys = require('./metadatasys.js');
var graph = require('./graphbuilder.js');
var outputtable = require('./outputTable.js');

const graphmodel=graph.getGraph('Teste');

/**
 * Schema of a EntityType
 */
const zNgsiEntity = z.object({
    entityType: z.string().describe('Entity type'),
    urlSchema: z.string().describe('URL of the Entity\'Schema'),
    title: z.string().describe('Title of the entity'),
    description: z.string().describe('Description of the entity')
  });
/**
 * Schema of a relationship among two EntityType
 */
const zNgsiEntityRelationship = z.object({
    fiwareService: z.string().describe('A name that identifies the NGSI broker'),
    entityType: z.string().describe('Entity type'),
    attribute: z.string().describe('Entity attribute'),
    relEntityType: z.string()
  });
/**
 * Schema of a geolocation cluster of EntityType
 */
const zNgsiEntityGeoCluster = z.object({
    fiwareService: z.string().describe('A name that identifies the NGSI broker'),
    entityType: z.string().describe('Entity type'),
    cluster: z.number().describe('Id of the cluster'),
    centroid_location_coordinates_lon: z.number().describe('Longitude of cluster\'s centroid'),
    centroid_location_coordinates_lat: z.number().describe('Latitude of cluster\'s centroid'),
    points: z.number(),
    max_distance: z.number()
  });


/**
 * Schema of Result of Query 
 */
const zNgsiQueryResult = z.object({
    rowsCount:z.number().describe('Number of rows in the result'),
    content:z.string().optional().describe('The result of the query in textual format'),
    format:z.string().describe('Format of the result TXT, CSV, JSON, etc'),
    info:z.string().optional().describe('A complementary message'),
   });


/**
 * Metadata of getAvailableEntities
 */
exports.getAvailableEntitiesMetadata =  {
        title: 'Available Entities Tool ',
        description: 'Provides a list of all avalilable Entities',
        inputSchema: {},
        outputSchema: {entities:z.array(zNgsiEntity).describe('List of all avalilable Entities')}
    };

/**
 * Returns a list of all available NGSI Entities
 */
exports.getAvailableEntities = async function getAvailableEntities()
{
     debug('getAvailableEntities');
     var lEntitiesAsTableAndSchema=metadatasys.toTableListOfEntities();
     var lEntitiesList=lEntitiesAsTableAndSchema.getRows();
     var output={entities:lEntitiesList}
     //debug('Output: '+JSON.stringify(output,null,2));
     return {
            content: [{ type: 'text', text: JSON.stringify(output) }],
            structuredContent: output
     } 
} 

/**
 * Metadata of getRelatedEntities
 */
exports.getRelatedEntitiesMetadata =  {
        title: 'Related entities Tool ',
        description: 'Lists all the relationships between entities that share a common atribute.\nUse the values in `entity_foregin_id` to find which Ids to search in the related entities.',
        inputSchema: {entitiesTypes:z.array(z.string().describe('A list of NGSI Entities(provided by the `getAvailableEntities` tool'))},
        outputSchema: {relatedEntities:z.array(zNgsiEntityRelationship).optional(),error:z.string().optional().describe('Error description')}
    };
/**
 * Returns a tabular data structure providing related entities as One To Many
 */
exports.getRelatedEntities = async function getRelatedEntities(zEntitiesTypes)
{
     var lListEntityTypes=zEntitiesTypes.entitiesTypes;
     debug('getRelatedEntities('+JSON.stringify(lListEntityTypes)+') (size='+lListEntityTypes.length+')');
     for(var lEntityType of lListEntityTypes)
     {
          if(!isExistsEntity(lEntityType))
          {
             var output={error:msgEntityNotFound(lEntityType)};
             return {
                content: [{ type: 'text', text: JSON.stringify(output) }],
                structuredContent:  output
            };
          }
     }
     var lServices=getAllowedServices();
     debug(JSON.stringify(lServices));
     lRelationshipsAsTableAndSchema=await metadatasys.toTableEntityRelationships(lServices,lListEntityTypes)
     //debug('TableAndSchema: '+JSON.stringify(lRelationshipsAsTableAndSchema,null,2));
     var lRelationshipsList=lRelationshipsAsTableAndSchema.getRows();
     var output={relatedEntities:lRelationshipsList}
     //debug('Output: '+JSON.stringify(output,null,2));
     return {
                content: [{ type: 'text', text: JSON.stringify(output) }],
                structuredContent: output
            };
}
/**
 * Metadata of getGeoMetadata
 */
exports.getGeoMetadataMetadata =  {
        title: 'Geo metadata Tool',
        description: 'List all geographical clusters where there is data about a NGSI Entity. It  considers a geographical central point and the radius parameter in kilometers.',
        inputSchema: {entityType:z.string().describe('NGSI Entity (provided by the `getAvailableEntities` tool'),
                      radius:z.number().describe('The radius in kms')
                     },
        outputSchema: {geoMetadataEntities:z.array(zNgsiEntityGeoCluster),error:z.string().optional().describe('Error description')}
    };
/**
 * Returns a tabular data structure providing geometadata
 */
exports.getGeoMetadata = async function getGeoMetadata(pParameters,pxxx)
{
     var lEntityType=pParameters.entityType;
     if(!isExistsEntity(lEntityType))
     {
          return mcpStructuredError(msgEntityNotFound(lEntityType));
     }
     var lRadius=pParameters.radius;
     debug('getGeoMetadata ::Parameters: '+JSON.stringify(pParameters))
//TODO: what is this second param?
console.log('RequestInfo?: '+JSON.stringify(pxxx))
     debug('getGeoMetadata('+lEntityType+','+lRadius+')');
     var lBrokerParallel=getBroker();
     lGeoMetadataAsTableAndSchema=await lBrokerParallel.getGeoMetadata(lEntityType,lRadius);
     //debug('TableAndSchema: '+JSON.stringify(lGeoMetadataAsTableAndSchema,null,2));
     var lGeoMetadataAsList=lGeoMetadataAsTableAndSchema[0];
     var output={geoMetadataEntities:lGeoMetadataAsList}
     //debug('Output: '+JSON.stringify(output,null,2));
     return {
                content: [{ type: 'text', text: JSON.stringify(output) }],
                structuredContent: output
            };
}

/**
 * Metadata of getGraphEntities
 */
exports.getGraphEntitiesMetadata =  {
        title: 'Graph of NGSI entities Tool ',
        description: 'Provides a graph of NGSI entities describing their structure and attributes',
        inputSchema: {entityType:z.string()},
        outputSchema: {graphOfEntities:graphmodel.getSchemaZ()}
    };

/**
 * Returns the neighborhood Graph of a NGSI Entity 
 */
exports.getGraphEntities = async function getGraphEntities(zEntityType)
{
    var lEntityType=zEntityType.entityType;
    if(!isExistsEntity(lEntityType))
    {
       return mcpStructuredError(msgEntityNotFound(lEntityType));
    }
    debug('getGraphEntities('+lEntityType+')');
    var output={graphOfEntities:graphmodel.neighborhood(lEntityType)};
    return {
                content: [{ type: 'text', text: JSON.stringify(output) }],
                structuredContent: output
            };
};

/**
 * Metadata of execQueryNGSI
 */
exports.execQueryNGSIMetadata =  {
        title: 'Tool for quering NGSI entities',
        description: 'Retrieves data from the NGSI broker by means of geospatial parameters and  NGSI query',
        inputSchema: {entityType:z.string().describe('An entityType provided by the `getAvailableEntities` tool'),
                      attribList:z.string().describe('Comma separated list of attribute values, provided by the `getGraphEntities` tool'),
                      georel:z.string().optional().describe('The geospatial relationship. Examples: `near;maxDistance==10000` or `near;minDistance==10000` or `within` using meter as unit of distance'),
                      geometry:z.string().optional().describe('The type of geometry. Possible value:  `Point` or `Polygon`'),
                      coordinates:z.string().optional().describe('The coordinates using square brackets. In case of Point [longitude, latitude] and Polygon [[longitude, latitude],[longitude, latitude],..].'),
                      geoproperty:z.string().optional().describe('The name of property to be queried. e.g.: `location`'),
                      queryNGSI:z.string().optional().describe('A query string conforming to the NGSI-LD specification ETSI GS CIM 009 V1.4.1 (2021-02) using the pipe `|` for `or` operator and the `;` for the `and` operator. Apply only in numerical fields.'),
                      fullTextSearch:z.string().optional().describe('A string to be used in a full text search'),
                      entityId:z.string().optional().describe('The Id of the entity to search'), 
                      },
        outputSchema: {
                        result:zNgsiQueryResult.optional(),error:z.string().optional().describe('Error description')
                      }
    };
/**
 * Returns the neighborhood Graph of a NGSI Entity 
 */
exports.execQueryNGSI = async function execQueryNGSI(pParameters,pxxx)
{
    var lTopK=100;
    var lEntityType=pParameters.entityType;
    var lAttribList=pParameters.attribList;
    var lGeorel=pParameters.georel;
    var lGeometry=pParameters.geometry;
    var lCoordinates=pParameters.coordinates;
    var lGeoproperty=pParameters.geoproperty;
    var lQuery=pParameters.queryNGSI;
    var lFullTextSearch=pParameters.fullTextSearch;
    var lEntityId=pParameters.entityId;
    var lQueryDetails='execQueryNGSI('+lEntityType+',<'+lAttribList+'>,<'+lGeorel+'>,'+lGeometry+',<'+lCoordinates+'>,'+lGeoproperty+',<'+lQuery+'>,'+lFullTextSearch+','+lEntityId+')';
    debug(lQueryDetails);
    if(!isExistsEntity(lEntityType))
    {
       debug('Invalid Entity Type:'+lEntityType);
       return mcpStructuredError(msgEntityNotFound(lEntityType));
    }
    var lBrokerParallel=getBroker();
    if(lQuery!=undefined && lQuery.length>0)
    {
       var lQueryValidation=lBrokerParallel.setQuery(lQuery,lEntityType);
       debug('Query Validation='+(lQueryValidation!=undefined?lQueryValidation:'Ok'));
       if(lQueryValidation!=undefined)
       {
         // This avoids errors at the broker
         return mcpStructuredError(lQueryValidation);
       }
    }
    if(lFullTextSearch!=undefined && lFullTextSearch.length>0)
    {
      if(lFullTextSearch=='*' || lFullTextSearch=='%')
      {
          debug('Wildcard');
      }
      else
      {
          lBrokerParallel.setTextPattern(lFullTextSearch);
      }
    }
    //Limit of rows in the result
    lBrokerParallel.setTopK(lTopK);
    var lGeoQueryValidation=lBrokerParallel.setGeoQuery(lGeorel,lGeometry,lCoordinates,lGeoproperty,lEntityType);
    if(lGeoQueryValidation!=undefined)
    {
       // This avoids errors at the broker
       return mcpStructuredError(lGeoQueryValidation);
    }

    var outout={};
    var lError=undefined;
    await lBrokerParallel.getCurrentData(lEntityType,lAttribList,lEntityId)
        .then((lSortedResult) => {
           let lTable=lSortedResult.table;
           debug(lQueryDetails);
           debug('Tool::Data is loaded Rows:'+lTable.countRows()+'  Schema:'+JSON.stringify(lTable.getSchema()));
           if(lSortedResult.excludedOutTopK>0)
           {
               debug('SortedResult.excludedOutTopK='+lSortedResult.excludedOutTopK);
               lError='The result with '+(lTable.countRows()+lSortedResult.excludedOutTopK)+' rows is too long. Of these, '+lSortedResult.excludedOutTopK+' rows exceed  the limit to be presented. Use the getGeoMetadata tool to get spatial metadata and then apply spatial contraints. Or provide a text pattern.'; 
           }
           else
           {
               var lCsv=outputtable.toCSV(lTable);
               output={result:{content:lCsv,rowsCount:lTable.countRows(),format:'csv',info:lSortedResult.message}};
           }
           //debug(JSON.stringify(output,null,2));
        })
        .catch((err) => {
            debug(err);
            lError='Exception: '+JSON.stringify(err);
        });
     if(lError!=undefined)
     {
          return mcpStructuredError(lError);
     }
     else
     {
        return {
              content: [{ type: 'text', text: JSON.stringify(output) }],
              structuredContent: output
            };
     }
};


/**
 * Resource ngsiQuery
 * georel, to express the desired geospatial relationship;
 * geometry, to express the type of the reference geometry;
 * coordinates, to express the reference geometry;
 * geoproperty, to express the target geometry of an Entity. This parameter is optional, location is the default
 */
exports.ngsiQuery=async function(uri,entityType,attribList,b64georel,b64geometry,b64coordinates,geoproperty,b64query,b64textPattern,entityId)
{
  debug('Resource :: EntityType: '+entityType+'  Attrs: '+attribList);
  debug('  Query (base64): '+b64query);
  debug('  textPattern(base64):'+b64textPattern);
  debug(`  GeoQuery ${b64georel} ${b64geometry} ${b64coordinates} ${geoproperty}`);
  debug(`  EntityId: {entityId}`);
  var lEntity=entityType;
  if(!isExistsEntity(entityType))
  {
     return mcpTextError(msgEntityNotFound(entityType),uri);
  }
  var lEntityId=undefined;
  if(entityId!=undefined)
  {
      debug('TODO : validate id '+entityId)
      lEntityId=entityId;
  }
 
  var lAttribs=attribList!=undefined && attribList.length>0?attribList.replaceAll('%2C',','):undefined;
  var lGeorel=undefined;
  if(b64georel!=undefined && b64georel.length>0)
  {
     var buff = Buffer.from(b64georel,'base64');
     lGeorel = buff.toString('ascii'); 
     lGeorel=lGeorel=='None'?undefined:lGeorel;
     debug('Georel:'+lGeorel);
  }
  var lGeometry=undefined;
  if(b64geometry!=undefined && b64geometry.length>0)
  {
     var buff = Buffer.from(b64geometry,'base64');
     lGeometry = buff.toString('ascii'); 
     lGeometry=lGeometry=='None'?undefined:lGeometry;
     debug('Geometry:'+lGeometry);
  }
  var lCoordinates=undefined;
  if(b64coordinates!=undefined && b64coordinates.length>0)
  {
     var buff = Buffer.from(b64coordinates,'base64');
     lCoordinates = buff.toString('ascii'); 
     lCoordinates=lCoordinates=='None'?undefined:lCoordinates;
     debug('Cordinates:'+lCoordinates);
  }
  var lGeoproperty=geoproperty;
  
  var lQuery=undefined;
  if(b64query!=undefined && b64query.length>0)
  {
     var buff = Buffer.from(b64query,'base64');
     lQuery = buff.toString('ascii'); 
     debug('Query:'+lQuery);
  }
  var lTextPattern=undefined;
  if(b64textPattern!=undefined && b64textPattern.length>0)
  {
     var buff = Buffer.from(b64textPattern,'base64');
     lTextPattern= buff.toString('ascii'); 
     debug('TextPattern='+lTextPattern);
  }

  var lOut={};
  var lBrokerParallel=getBroker();
  var lQueryValidation=lBrokerParallel.setQuery(lQuery,lEntity);
  debug('Query Validation='+(lQueryValidation!=undefined?lQueryValidation:'Ok'));
  if(lQueryValidation!=undefined)
  {
     // This avoids errors at the broker
     return mcpTextError(lQueryValidation,uri);
  }
  var lGeoQueryValidation=lBrokerParallel.setGeoQuery(lGeorel,lGeometry,lCoordinates,lGeoproperty,lEntity);
  if(lGeoQueryValidation!=undefined)
  {
     // This avoids errors at the broker
     return mcpTextError(lGeoQueryValidation,uri);
  }
  lBrokerParallel.setTextPattern(lTextPattern);

  await lBrokerParallel.getCurrentData(lEntity,lAttribs,lEntityId)
        .then((lSortedResult) => {
           let lTable=lSortedResult.table;
           debug('Resource::Data is loaded Rows:'+lTable.countRows()+'  Schema:'+JSON.stringify(lTable.getSchema()));
           var lCsv=outputtable.toCSV(lTable);
           lOut={
                    contents: [{
                        uri: uri.href,
                        text: lCsv
                        }]
                }

        })
        .catch((err) => {
           debug(err);
           lOut=mcpTextError(err,uri);
        });
  //debug('Out='+JSON.stringify(lOut,null,2));
  return lOut;
}

function getBroker()
{
  var BrokerParallelize=require('../lib/brokerParallelize.js');
  return new BrokerParallelize(getAllowedServices());
}

/**
 *
 */
function getAllowedServices()
{
     var lUser=configsys.getMcpAppKey();
     var lAliasArray=configsys.getBrokersAliasList();
     var lServices=[];
     for(var i in lAliasArray)
     {
         if(configsys.allowScopeToUser(lUser,lAliasArray[i]) && configsys.isConfigOk(lAliasArray[i]) && 
            (configsys.isVersionLDV1(lAliasArray[i]) || configsys.isVersionV2(lAliasArray[i]))
           )
         {
             lServices.push(lAliasArray[i]);
         }
     }
     debug('User: '+lUser+' Allowed Services: '+JSON.stringify(lServices));
     return lServices
}
function isExistsEntity(pEntityType)
{
    return configsys.isExistsEntity(pEntityType);
}
function mcpTextError(pMsg,pUri)
{
     debug('Return mcpTextError('+pMsg+')');
     var lOut;
     if(pUri==undefined)
     {
       lOut={contents: [{
                        text: JSON.stringify({error:pMsg})
                        }]
             }
     }
     else
     {
       lOut={contents: [{
                        uri: pUri.href,
                        text: JSON.stringify({error:pMsg})
                        }]
            }
     }
     debug(lOut);
     return lOut;
}
function mcpStructuredError(pMsg)
{
     debug('Return mcpStructuredError('+pMsg+')');
     var output={error:pMsg};
     return {
                content: [{ type: 'text', text: JSON.stringify(output) }],
                structuredContent:  output
            };
}
function msgEntityNotFound(pEntityType)
{
   return 'The entity `'+pEntityType+'` does not exists. Call the `getAvailableEntities` tool to check if it exists.';
}
 
