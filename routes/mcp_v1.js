/*!
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the MCP route.
 * 
 */
var { McpServer, ResourceTemplate } = require('@modelcontextprotocol/sdk/server/mcp.js');
var { StreamableHTTPServerTransport }  = require('@modelcontextprotocol/sdk/server/streamableHttp.js');
var z = require('zod/v4');
var tools = require('../lib/mcp_tools.js');
var configsys = require('../lib/configsys.js');
var apiSystem = require('../lib/apiSystem.js');
var express = require('express');
var router = express.Router();
var debug = require('debug')('iotbi.route.mcp');

/**
 * List of Fiware Services allowed to MCP
 */
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



function bootServer()
{
   // Create an MCP server
   const server = new McpServer({
     name: 'iotbi-mcp',
     version: '1.0.0'
   });

   server.registerTool(
     'getAvailableEntities',
     tools.getAvailableEntitiesMetadata,
     tools.getAvailableEntities
   );

   server.registerTool(
     'getRelatedEntities',
     tools.getRelatedEntitiesMetadata,
     tools.getRelatedEntities
   );

   server.registerTool(
     'getGeoMetadata',
     tools.getGeoMetadataMetadata,
     tools.getGeoMetadata
   );

   server.registerTool(
     'getGraphEntities',
     tools.getGraphEntitiesMetadata,
     tools.getGraphEntities
   );
   server.registerTool(
     'execQueryNGSI',
     tools.execQueryNGSIMetadata,
     tools.execQueryNGSI
   );
   // Static resource
   server.resource(
       "info",
       "info://app",
        async (uri) => ({
          contents: [{
           uri: uri.href,
           text: "This MCP server provides an interface to a federated NGSI ecosystem."
           }]
        })
   );


   // Add a dynamic greeting resource
   /*
   server.registerResource(
     'greeting',
      new ResourceTemplate('greeting://{name}', { list: undefined }),
      {
        title: 'Greeting Resource', // Display name for UI
        description: 'Dynamic greeting generator'
      },
      async (uri, { name }) => ({
        contents: [
            {
                uri: uri.href,
                text: `Hello, ${name}!`
            }
        ]
      })
    );
    */

    /**
     * function(uri,entityType,attribList,b64georel,b64geometry,b64coordinates,geoproperty,b64query,b64textPattern,entityId)
     */
     server.resource(
       "ngsi-entity-textPattern",
       new ResourceTemplate("ngsildv1://{entityType}/{georel}/{geometry}/{coordinates}/{geoproperty}/{b64textPattern}/textPattern", { list: undefined }),
       async (uri, { entityType,georel,geometry,coordinates,geoproperty,b64textPattern }) => (tools.ngsiQuery(uri,entityType,undefined,georel,geometry,coordinates,geoproperty,undefined,b64textPattern,undefined))
     );

     server.resource(
       "ngsi-entity-attrs-query",
       new ResourceTemplate("ngsildv1://{entityType}/{attribList}/{georel}/{geometry}/{coordinates}/{geoproperty}/{b64query}/query", { list: undefined }),
       async (uri, { entityType,attribList,georel,geometry,coordinates,geoproperty,b64query}) => (tools.ngsiQuery(uri,entityType,attribList,georel,geometry,coordinates,geoproperty,b64query,undefined,undefined))
     );

     server.resource(
       "ngsi-entity-attrs",
       new ResourceTemplate("ngsildv1://{entityType}/{attribList}/{georel}/{geometry}/{coordinates}/{geoproperty}/data", { list: undefined }),
       async (uri, { entityType,attribList,georel,geometry,coordinates,geoproperty}) => (tools.ngsiQuery(uri,entityType,attribList,georel,geometry,coordinates,geoproperty))
     );

     server.resource(
       "ngsi-entity-by-id",
       new ResourceTemplate("ngsildv1://{entityType}/{entityId}/data", { list: undefined }),
       async (uri, { entityType,entityId }) => (tools.ngsiQuery(uri,entityType,undefined,undefined,undefined,undefined,undefined,undefined,undefined,entityId))
     );

     server.resource(
        "ngsi-entity",
        new ResourceTemplate("ngsildv1://{entityType}/data", { list: undefined }),
        async (uri, { entityType}) => (tools.ngsiQuery(uri,entityType))
     );
     
     router.post('*',[serviceDebug,serviceAuth,apiSystem.serviceLoadAllMetadata,serviceMcp]);
     module.exports = router;

     return server;
}

function serviceMcp(req,res,next)
{
    // Create a new transport for each request to prevent request ID collisions
    const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: undefined,
        enableJsonResponse: true
    });

    res.on('close', () => {
        debug('MCP Server connection closed');
        transport.close();
    });
    server.connect(transport).then(() => {
           debug('MCP Server connection is UP');
           debug('appKey='+getRequestAuthAppKey(req)+'  Method: '+req.body.method );
           transport.handleRequest(req, res, req.body);
        })
        .catch((err) => {
           debug(err);
           sendError(res,500,err)
        });
};
function serviceDebug(req,res,next)
{
   debug('Request Debug::URL:'+req.url+'   UrlPath:'+req.path);
   debug('Headers:');
   for(header in req.headers)
   {
      debug('  '+header+'='+req.headers[header]);
   }
   next();
   debug('Request Debug::Waiting for MCP Server...');
}

function serviceAuth(req,res,next)
{
   var lHeaderAuthorization=req.headers['authorization'];
   var lToken=lHeaderAuthorization!=undefined?lHeaderAuthorization.split(' ')[1]:undefined;
   if(lToken==undefined)
   {
       sendError(res,401,'Unauthorized');
       return;
   }
   var lAppKey=configsys.findAppKeyBySha256(lToken);
   if(lAppKey==undefined || lAppKey!=configsys.getMcpAppKey())
   {
       //TODO: 403?
       debug('lAppKey='+lAppKey+'    mcpAppKey='+configsys.getMcpAppKey());
       sendError(res,403,'Unauthorized');
       return;
   }

   if(!configsys.allowMcpToUser(lAppKey))
   {
      debug('Your account does not have permissions for using MCP :: Mcp User='+lAppKey);
      sendError(res,403,'Your account does not have permissions for using this MCP server');
      return;
   }

   saveAuthData(req,lAppKey)
   //debug('appKey: '+lAppKey);
   next();
   debug('Request Auth::Waiting for MCP Server...');
}

function saveAuthData(req,appKey)
{
   req.locals={};
   req.locals.appKey=appKey;
}
function getRequestAuthAppKey(req)
{
   return req.locals!=undefined?req.locals.appKey:undefined;
}
/**
 * Send a error
 */
function sendError(res,pCode,pMsg)
{
   var lCode=pCode;
   if(pCode==undefined)
   {
      debug('The error code is not defined. Sending error 500');
      lCode=500;
   }
   debug('Send ['+lCode+']:'+pMsg);
   res.status(lCode).send(pMsg);
}


const server=bootServer();
