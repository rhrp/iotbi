/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the booting process
 */


/**
 * Module dependencies.
 */
var debug = require('debug')('iotbi.boot');
var version = require('../lib/version.js');
var cache = require('../lib/cache.js');
var fs=require('fs');
var ngsildcontext = require('../lib/ngsildcontext.js');
var configsys = require('../lib/configsys.js'); 


debug('Starting IoTBI ',version.version)
/**
 * Get port from environment and store in Express.
 */
var port = normalizePort(process.env.IOTBI_PORT || '5000');
var use_https = normalizeBoolean(process.env.IOTBI_USE_HTTPS || false);

/**
 * Create HTTP/HTTPS server.
 */
function startServer(app)
{
  app.set('port', port);
  if(use_https) {
    var options = {
      key: fs.readFileSync('/home/rhp/certs/api.iotbi.tech/privkey.pem'),
      cert: fs.readFileSync('/home/rhp/certs/api.iotbi.tech/cert.pem'),
      ca: fs.readFileSync('/home/rhp/certs/api.iotbi.tech/chain.pem')
    };
    var https = require('https');
    server = https.createServer(options,app);
  } else {
    var http = require('http');
    server = http.createServer(app);
  }
  return server;
}


/**
 * Normalize a port into a number, string, or false.
 */
function normalizePort(val) {
  var port = parseInt(val, 10);

  if (isNaN(port)) {
    // named pipe
    return val;
  }

  if (port >= 0) {
    // port number
    return port;
  }

  return false;
}
/**
 * Normalize a string into a boolean
 */
function normalizeBoolean(val) {
  if(val!=undefined && typeof val === 'string')
  {
    return val.toLowerCase() === 'true';
  }
  else
  {
    return false;
  }
}


/**
 * Event listener for HTTP server "error" event.
 */
function onError(error) {
  if (error.syscall !== 'listen') {
    throw error;
  }

  var bind = typeof port === 'string'
    ? 'Pipe ' + port
    : 'Port ' + port;

  // handle specific listen errors with friendly messages
  switch (error.code) {
    case 'EACCES':
      debug(bind + ' requires elevated privileges');
      process.exit(1);
      break;
    case 'EADDRINUSE':
      debug(bind + ' is already in use');
      process.exit(1);
      break;
    default:
      throw error;
  }
}

/**
 * Event listener for HTTP server "listening" event.
 */
function onListening() {
  var addr = server.address();
  var bind = typeof addr === 'string'
    ? 'pipe ' + addr
    : 'port ' + addr.port;
  debug('Listening on ' + bind+'   HTTPS:'+use_https);
  debug('Process id '+`${process.pid}`+'  TimeZone:'+`${process.env.TZ}`);
}



var server=undefined;
/**
 * Init the cache subsystem
 */
cache.initCache();

/**
 *
 */
ngsildcontext.createPromisesloadNgsiContext(configsys.getBrokersAliasList(),configsys.getKnownEntityTypes())
        .then(() => {
           
           debug('All schemas were loaded');
           var app = require('../app');
           server=startServer(app);
          /**
            * Listen on provided port
            */
          server.listen(port);
          server.on('error', onError);
          server.on('listening', onListening);
        })
        .catch((err) => {
           debug(err);
        });

debug('Waiting server...');

