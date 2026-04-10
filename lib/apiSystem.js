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
var perfanalyser= require('../lib/performenceanalyser.js');

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
exports.servicePerformanceRequestMetrics = function(req,res,next)
{
    let lRequestId=req.params.requestId;
    debug(`PerformanceRequestMetrics(${lRequestId})`);
    res.status(200).json(perfanalyser.getRequestMetrics(lRequestId));
}
exports.servicePerformanceSystemMetrics = function(req,res,next)
{
    debug('PerformanceSystemMetrics ');
    res.status(200).json(perfanalyser.getSystemMetrics(true));
}
exports.servicePerformanceStartTrackingMetrics = function(req,res,next)
{
    debug('servicePerformanceStartTrackingMetrics');
    res.status(200).json(perfanalyser.startTrackingSystemState());
}
exports.servicePerformanceStopTrackingMetrics = function(req,res,next)
{
    debug('servicePerformanceStopTrackingMetrics');
    res.status(200).json(perfanalyser.stopTrackingSystemState());
}
exports.serviceLoadAllMetadata = function(req,res,next)
{
   debug('serviceWebhdfsLoadAllMetadata');
   ngsildcontext.createPromisesloadNgsiContext(configsys.getBrokersAliasList(),configsys.getKnownEntityTypes())
        .then(() => {
           debug('All schemas were loaded');
           next();
        })
        .catch((err) => {
           debug(err);
           sendError(res,500,err)
        });
}
