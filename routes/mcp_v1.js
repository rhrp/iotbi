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


// Add a dynamic greeting resource
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
           debug('appKey='+getRequestAuthAppKey(req));
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
   //authorization=Bearer 01583265018ff95cdc418f26da309f756a9e42948e5dce899fc07a7016430f0b
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
   saveAuthData(req,lAppKey)
   debug('appKey: '+lAppKey);
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


router.post('*',[serviceDebug,serviceAuth,apiSystem.serviceLoadAllMetadata,serviceMcp]);
module.exports = router;
