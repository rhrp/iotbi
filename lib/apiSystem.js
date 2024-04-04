/**
 *
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 */

var url  = require('url');
var apiConfig = require('./apiConfig.js');
var debug = require('debug')('iotbi.api.service.system');
var ngsildcontext = require('./ngsildcontext.js');
var accounting = require('./accounting.js');

exports.serviceCacheContexts = function(req,res,next)
{
    debug('cachedContexts');
    res.status(200).json(ngsildcontext.cachedContexts());
}

exports.serviceAccountingAccess = function(req,res,next)
{
    debug('accountingAccess');
    res.status(200).json(accounting.accountingAccess());
}
