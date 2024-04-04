const express = require('express');
const url  = require('url');
const router = express.Router();
const apiConfig = require('../lib/apiConfig.js');
const apiOrion = require('../lib/apiOrion.js');// Deprecated  module
const apiCurrentState = require('../lib/apiCurrentState.js');
const apiTemporal = require('../lib/apiTemporal.js');
const apiSystem = require('../lib/apiSystem.js');
const accounting = require('../lib/accounting.js')
const debug = require('debug')('iotbi.route');

const PARAM_APPKEY	=	'appKey';

function validateToken(req,res,next)
{
   var lOk=false;
   if(!apiConfig.security.enabled)
   {
	debug('Security is disabled');
        lOk=true;
   }
   else
   {
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
	   else
           {
             debug('Security is enabled, but no appKey was provided');
           }
        }
        if(lAppKey!=null)
        {
            for(lKey in apiConfig.security.appKeys)
            {
              var k=apiConfig.security.appKeys[lKey];
              if(lAppKey==lKey)
              {
                  var k=apiConfig.security.appKeys[lKey];
                  debug('Found the key '+k.name+' :: daily limit is '+k.limitDay);
                  lOk=true;
                  break;
              }
            }
        }
   }
   if(!lOk)
   {
	res.status(401).send('Invalid appKey');
   }
   else
   {
        var lUrl=req.path;   //req.originalUrl;
        var lCurrTime = new Date().getTime()
        var lRemoteIP=req.ip;
        var lFiwareService = req.params.fiwareService
        accounting.recAccess(lAppKey,lUrl,lFiwareService,lRemoteIP,lCurrTime)

	try
	{
   		next();
	}
	catch(error)
	{
		debug('Error on next'+error);
	} 
    }
}
var gReqCount=0;
function apiReqCounter(req,res,next)
{
   var lCurrTime = new Date().getTime();
   gReqCount++;
   debug('Request ID:'+gReqCount);
   res.locals.iotbi_reqId=gReqCount;
   res.locals.iotbi_reqStarted=lCurrTime;

   next();
}
function apiSelector(req,res,next)
{
   //Query params
   var lUrlParts = url.parse(req.url, true);
   var lQuery = lUrlParts.query;
   var lFromDate = lQuery.fromDate;
   var lToDate = lQuery.toDate;
   var lFiwareService = req.params.fiwareService
   
   if (lFromDate==undefined && lToDate==undefined)
   {
        debug('Broker API - Broker: '+apiConfig.getBrokerName(lFiwareService));
        apiCurrentState.service(req,res,next); 
   }
   else
   {
        debug('NGSI Temporal/QuantumLeap API - Broker: '+apiConfig.getBrokerName(lFiwareService)
	  					     +(apiConfig.isBrokerSuportsTemporalAPI(lFiwareService)?' (Entities and Temporal Data)':' and QuantumLeap for temporal data'));
        apiTemporal.service(req,res,next);
   }
   debug('Wait API...');
}

//V0
router.get('/v0/orion/:fiwareService/:entityType',[apiReqCounter,validateToken,apiOrion.service]);
router.get('/v0/orion/:fiwareService/:entityType/:entityId',[apiReqCounter,validateToken,apiOrion.service]);
router.get('/v0/ql/:fiwareService/:entityType',[apiReqCounter,validateToken,apiTemporal.service]);
router.get('/v0/ql/:fiwareService/:entityType/:entityId',[apiReqCounter,validateToken,apiTemporal.service]);
//V1
router.get('/v1/system/cache/contexts',[apiReqCounter,validateToken,apiSystem.serviceCacheContexts]);
router.get('/v1/system/accounting',[apiReqCounter,validateToken,apiSystem.serviceAccountingAccess]);
router.get('/v1/:fiwareService/:entityType',[apiReqCounter,validateToken,apiSelector]);
router.get('/v1/:fiwareService/:entityType/:entityId',[apiReqCounter,validateToken,apiSelector]);

module.exports = router;
