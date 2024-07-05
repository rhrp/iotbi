/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module provides the API services for system utilities, such as presenting to users the current state of cache, schemas and accounting.
 * 
 */
var url  = require('url');
var configsys = require('./configsys.js');
var debug = require('debug')('iotbi.api.service.system');
var ngsildcontext = require('./ngsildcontext.js');
var sdm = require('./smartdatamodels.js');
var accounting = require('./accounting.js');

exports.serviceCacheContexts = function(req,res,next)
{
    debug('cachedContexts');
    res.status(200).json(ngsildcontext.cachedContexts());
}
exports.serviceCacheSchemas = function(req,res,next)
{
    debug('cachedSchemas');
    res.status(200).json(sdm.loadedSchemas());
}

exports.serviceAccountingAccess = function(req,res,next)
{
    debug('accountingAccess');
    res.status(200).json(accounting.accountingAccess());
}
