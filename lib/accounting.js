var debug = require('debug')('iotbi.system.accounting');
var apiConfig = require('./apiConfig.js');

accounting={};

exports.accountingAccess = function()
{
  return accounting;
}

exports.recAccess=function recAccess(pAppKey,pUrl,pTenant,pIP,pNow)
{
   var lAppKey;
   var lAppName=apiConfig.getAppName(pAppKey);
   var lIP=pIP==undefined?'0.0.0.0':pIP;
   var lTenant;
   if(pTenant==undefined && pUrl==undefined)
   {
      lTenant='__undefined__';
   }
   else if(pTenant==undefined)
   {
     lTenant='system';
   }
   else if(!apiConfig.isBrokerServerOk(pTenant))
   {
     lTenant='unknown';
   }
   else
   {
     lTenant=pTenant;
   }

   if(lAppName==undefined)
   {
      lAppKey='__undefined__';
      lAppName='__undefined__';
   }
   var lRecord=accounting[lAppName];
   if(lRecord==undefined)
   {
      lRecord={'count':1,'lastAcess':pNow,'ips':{}};
      accounting[lAppName]=lRecord;
   }
   else
   {
      lRecord.count=lRecord.count+1;
      lRecord.lastAcess=pNow;
   }
   //
   var lRecordIP=lRecord.ips[lIP];
   if(lRecordIP==undefined)
   {
      lRecordIP={'count':1,'lastAcess':pNow,'tenants':{}}
      lRecord.ips[lIP]=lRecordIP;
   }
   else
   {
      lRecordIP.count=lRecordIP.count+1;
      lRecordIP.lastAcess=pNow;
   }
   //
   var lRecordTenant=lRecordIP.tenants[lTenant];
   if(lRecordTenant==undefined)
   {
      lRecordTenant={'count':1,'lastAcess':pNow}
      lRecordIP.tenants[lTenant]=lRecordTenant;
   }
   else
   {
      lRecordTenant.count=lRecordTenant.count+1;
      lRecordTenant.lastAcess=pNow;
   }

   debug('Record Access:: IP '+pIP+' :: '+pTenant);
}
