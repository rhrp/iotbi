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
var perfanalyser= require('./performenceanalyser.js');
const Table = require('./model/tablemodel.js');
const BrokerParallelize=require('./brokerParallelize.js');
const utils = require('./utils.js');
const output = require('./outputTable.js');

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
exports.serviceGeoMetadata = function(req,res,next)
{
    debug('serviceGeoMetadata');
    var lEntityType=req.params.entityType;
    let lFormat=req.params.format;
    let lRadius=req.params.radius;
    let lFiwareServices=utils.getRequestAllowedFiwareServices(res);
    debug('getGeoMetadata('+JSON.stringify(lFiwareServices)+','+lEntityType+','+lRadius+','+lFormat+')');
    if(!output.isValidFormat(lFormat))
    {
        utils.saveOutputError(res,400,'Invalid format',lEntityType)
        next();
        return;
    }
    let lBrokerParallel=new BrokerParallelize(lFiwareServices);
    lBrokerParallel.getGeoMetadata(lEntityType,lRadius).then((lGeoMetadataAsTableAndSchema) => {
       //debug('TableAndSchema: '+JSON.stringify(lGeoMetadataAsTableAndSchema,null,2));
       let lTable=new Table('GeoMetadata',lGeoMetadataAsTableAndSchema[0],lGeoMetadataAsTableAndSchema[1]);
       utils.saveOutputTable(res,lTable,lFormat,lEntityType);
       next();
     })
     .catch((err) => {
           debug(err);
           utils.saveOutputError(res,500,err,lEntityType);
          next();
     });
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
