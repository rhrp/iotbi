# iotbi
Lightweight data bridge for connecting self-service end-user analytic tools to NGSI-based IoT systems



## Start the service in foreground mode
```sh
$ npm run start
```


## Start the service in background mode
```sh
$ npm run startbg 
```



## Environment variables
IOTBI_PORT   	- Bind port number, by default is 5000 
IOTBI_USE_HTTPS	- Enables HTTPS security, by default is false. Requires additional file certificates

DEBUG=iotbi.*   - Namespace for debugging 
