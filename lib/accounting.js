/*!
 * Copyright(c) 2023-2023 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module implements the accounting subsystem.
 */
var debug = require('debug')('iotbi.system.accounting');
var configsys = require('./configsys.js');

accounting={};

exports.accountingAccess = function()
{
  return accounting;
}

exports.recAccess=function recAccess(pAppKey,pUrl,pTenant,pIP,pNow)
{
   var lAppKey;
   var lAppName=configsys.getAppName(pAppKey);
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
   else if(!configsys.isBrokerServerOk(pTenant))
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
      lRecord={'count':1,'lastAcess':pNow,'ips':{},'yyyymmdd':{}};
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
   //Accounting per user
   var lDate = new Date();
   //Mounth 0-11 ;-)
   var lKeyDay=lDate.getFullYear()+(lDate.getMonth()<10?'0':'')+lDate.getMonth()+(lDate.getDay()<10?'0':'')+lDate.getDay();
   var lRecordDay=lRecord.yyyymmdd[lKeyDay];
   if(lRecordDay==undefined)
   {
      lRecord.yyyymmdd[lKeyDay]=1;
      lRecordDay=1;
   }
   else
   {
      lRecordDay=lRecordDay+1;
      lRecord.yyyymmdd[lKeyDay]=lRecordDay;
   }
   var lLimitDay=configsys.getLimitDay(pAppKey);

   debug('Record Access:: IP '+pIP+' :: '+pTenant+'   Daily limit:'+lLimitDay+'  user total requests ('+lKeyDay+'):'+lRecordDay);
   return lLimitDay!=undefined && lLimitDay>lRecordDay;
}
