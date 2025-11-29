# iotbi
**Lightweight data bridge for connecting self-service end-user analytic tools to NGSI-based IoT systems**


## Start and stop
### Start the service in foreground mode
```sh
$ npm run startfg
```

### Start the service in background mode
```
./scripts/service.sh start
```

### Start the service in background mode (deprecated)
```sh
$ npm run startbg 
```

### Show logs   
```
./scripts/service.sh logs
```

## Endpoints
### API Endpoint
http://127.0.0.1:5000/v1/:firwareService/:entityType[/:entityId]

### WebHDFS API Endpoint
http://127.0.0.1:5000/webhdfs/v1/

### MCP API Endpoint
http://127.0.0.1:5000/mcp

## Environment variables
IOTBI_PORT		- Bind port number, by default is 5000

IOTBI_USE_HTTPS	- Enables HTTPS security, by default is false. Requires additional file certificates

DEBUG=iotbi.*		- Namespace for debugging 

MCP_APPKEY              - AppKey allowed to use MCP endpoint
