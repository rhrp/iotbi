/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the API routes.
 * 
 */
const express = require('express');
const url  = require('url');
const fs = require('fs');
const router = express.Router();
const configsys = require('../lib/configsys.js');
const apiOrion = require('../lib/apiOrion.js');// Deprecated  module
const apiCurrentState = require('../lib/apiCurrentState.js');
const apiTemporal = require('../lib/apiTemporal.js');
const apiSystem = require('../lib/apiSystem.js');
const apiCache = require('../lib/cache.js');
const accounting = require('../lib/accounting.js')
const debug = require('debug')('iotbi.route.api');

const PARAM_APPKEY	=	'appKey';

var gReqCount=0;

function getParamAppKey(req)
{
   //Get the appKey from query string or header
   var lAppKey=req.query[PARAM_APPKEY];
   if(lAppKey!=null)
   {
      debug('QueryString appKey: '+lAppKey);
   }
   else
   {
      lAppKey=req.headers[PARAM_APPKEY.toLowerCase()];
      if(lAppKey!=null)
      {
         debug('Header appKey: '+lAppKey);
      }
   }
   return lAppKey;
}
function validateToken(req,res,next)
{
   if(!configsys.security.enabled)
   {
	debug('Security is disabled');
   }
   else
   {
        var lAppKey=getParamAppKey(req);
        if(!configsys.allowApiToKey(lAppKey))
        {
            debug('Invalid appKey or you dont have access to this fiware service');
   	    res.status(401).send('Invalid appKey or you dont have access to this fiware service');
            return;
        }
    }
    next();
}
/**
 * At this stage, is request is recorded in to the accouting system, as well as the user's quota is validated
 */
function apiAccounting(req,res,next)
{
   var lUrl=req.path;   //req.originalUrl;
   var lCurrTime = new Date().getTime()
   var lRemoteIP=req.ip;
   var lFiwareService = req.params.fiwareService;
   var lAppKey=getParamAppKey(req);

   var lUnderLimit=accounting.recAccess(lAppKey,lUrl,lFiwareService,lRemoteIP,lCurrTime);
   if(!lUnderLimit)
   {
       res.status(401).send('The daily limit was exceded!');
       return;
   }
   next();
}
function apiReqCounter(req,res,next)
{
   var lCurrTime = new Date().getTime();
   gReqCount++;
   debug('Request ID:'+gReqCount);
   res.locals.iotbi_reqId=gReqCount;
   res.locals.iotbi_reqStarted=lCurrTime;

   next();
   debug('Request Counter::Waiting for API...');
}
/**
 * At this stage, is checkd if the user have access to the requested fiware service
 */
function apiCheckAccessFiwareService(req,res,next)
{
    var lFiwareService = req.params.fiwareService;
    var lAppKey=req.query[PARAM_APPKEY];
    if(!configsys.allowScopeToKey(lAppKey,lFiwareService))
    {
         debug('Your account have access to this fiware service :: appKey='+lAppKey+' FiwareService='+lFiwareService);
         res.status(401).send('Your account have access to this fiware service');
    }
    else
    {
        next();
        debug('Check Access::Waiting for API...');
    }
}
function apiCheckAccessSystem(req,res,next)
{
    var lAppKey=req.query[PARAM_APPKEY];
    if(!configsys.allowScopeToKey(lAppKey,'system'))
    {
         debug('Your account have access to System tenant :: appKey='+lAppKey);
         res.status(401).send('Your account have access to System tenant');
    }
    else
    {
        next();
        debug('Check Access::Waiting for API...');
    }
}

function apiSelector(req,res,next)
{
   //Query params
   var lUrlParts = url.parse(req.url, true);
   var lQuery = lUrlParts.query;
   var lFromDate = lQuery.fromDate;
   var lToDate = lQuery.toDate;
   var lFiwareService = req.params.fiwareService;
   
   if (lFromDate==undefined && lToDate==undefined)
   {
        debug('Broker API - Broker: '+configsys.getBrokerName(lFiwareService));
        apiCurrentState.service(req,res,next); 
   }
   else
   {
        debug('NGSI Temporal/QuantumLeap API - Broker: '+configsys.getBrokerName(lFiwareService)
	  					     +(configsys.isBrokerSuportsTemporalAPI(lFiwareService)?' (Entities and Temporal Data)':' and QuantumLeap for temporal data'));
        apiTemporal.service(req,res,next);
   }
   debug('API Selector::Wait API...');
}

//V0
router.get('/v0/orion/:fiwareService/:entityType',[apiReqCounter,validateToken,apiCheckAccessFiwareService,apiAccounting,apiOrion.service]);
router.get('/v0/orion/:fiwareService/:entityType/:entityId',[apiReqCounter,validateToken,apiCheckAccessFiwareService,apiAccounting,apiOrion.service]);
router.get('/v0/ql/:fiwareService/:entityType',[apiReqCounter,validateToken,apiCheckAccessFiwareService,apiAccounting,apiTemporal.service]);
router.get('/v0/ql/:fiwareService/:entityType/:entityId',[apiReqCounter,validateToken,apiCheckAccessFiwareService,apiAccounting,apiTemporal.service]);
//V1 
router.get('/v1/system/cache/contexts',               [apiReqCounter,validateToken,apiCheckAccessSystem,apiAccounting,apiSystem.serviceCacheContexts]);
router.get('/v1/system/cache/schemas',                [apiReqCounter,validateToken,apiCheckAccessSystem,apiAccounting,apiSystem.serviceCacheSchemas]);
router.get('/v1/system/accounting',                   [apiReqCounter,validateToken,apiCheckAccessSystem,apiAccounting,apiSystem.serviceAccountingAccess]);
router.get('/v1/:fiwareService/:entityType',          [apiReqCounter,validateToken,apiCheckAccessFiwareService,apiAccounting,apiCache.doCacheAPI,apiSelector]);
router.get('/v1/:fiwareService/:entityType/:entityId',[apiReqCounter,validateToken,apiCheckAccessFiwareService,apiAccounting,apiCache.doCacheAPI,apiSelector]);

module.exports = router;
