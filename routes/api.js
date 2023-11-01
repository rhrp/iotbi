var express = require('express');
//var request = require('request');
var url  = require('url');
var router = express.Router();
var apiConfig = require('../lib/apiConfig.js');
var apiOrion = require('../lib/apiOrion.js');
var apiOrionNGSILib = require('../lib/apiOrionNGSILib.js');
var apiQuantumLeap = require('../lib/apiQuantumLeap.js');

const PARAM_APPKEY	=	'appKey';

function validateToken(req,res,next)
{
   var lOk=false;
   if(!apiConfig.security.enabled)
   {
	console.log('Security is disabled');
        lOk=true;
   }
   else
   {
        var lAppKey=req.query[PARAM_APPKEY];
	if(lAppKey!=null)
	{
           console.log('QueryString appKey: '+lAppKey);
        }
        else
        {
           lAppKey=req.headers[PARAM_APPKEY.toLowerCase()];
           if(lAppKey!=null)
           {
	     console.log('Header appKey: '+lAppKey);
           }
	   else
           {
             console.log('Security is enabled, but no appKey was provided');
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
                  console.log('Found the key '+k.name+' :: daily limit is '+k.limitDay);
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
	try
	{
   		next();
	}
	catch(error)
	{
		console.log('Error on next'+error);
	} 
    }
}
var gReqCount=0;
function apiReqCounter(req,res,next)
{
   gReqCount++;
   console.log('Request ID:'+gReqCount);
   next();
}
function apiSelector(req,res,next)
{
   //Query params
   var lUrlParts = url.parse(req.url, true);
   var lQuery = lUrlParts.query;
   var lFromDate = lQuery.fromDate;
   var lToDate = lQuery.toDate;
   if (lFromDate==undefined && lToDate==undefined)
   {
        console.log('Broker API');
        apiOrionNGSILib.service(req,res,next); 
   }
   else
   {
        console.log('NGSI Temporal/QuantumLeap API');
        apiQuantumLeap.service(req,res,next);
   }
   console.log('Wait API...');
}

router.get('/v0/orion/:fiwareService/:entityType',[apiReqCounter,validateToken,apiOrion.service]);
router.get('/v0/orion/:fiwareService/:entityType/:entityId',[apiReqCounter,validateToken,apiOrion.service]);
router.get('/v1/orion/:fiwareService/:entityType',[apiReqCounter,validateToken,apiOrionNGSILib.service]);
router.get('/v1/orion/:fiwareService/:entityType/:entityId',[apiReqCounter,validateToken,apiOrionNGSILib.service]);
router.get('/v1/ql/:fiwareService/:entityType',[apiReqCounter,validateToken,apiQuantumLeap.service]);
router.get('/v1/ql/:fiwareService/:entityType/:entityId',[apiReqCounter,validateToken,apiQuantumLeap.service]);
router.get('/v1/:fiwareService/:entityType',[apiReqCounter,validateToken,apiSelector]);
router.get('/v1/:fiwareService/:entityType/:entityId',[apiReqCounter,validateToken,apiSelector]);



module.exports = router;
