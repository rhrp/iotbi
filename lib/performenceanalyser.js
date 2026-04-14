/**
 * Performance metrics
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var configsys = require('./configsys.js');
var utils = require('./utils.js');
var debug = require('debug')('iotbi.performenceanalyser');
var { PerformanceObserver, performance } = require('node:perf_hooks');

const OP_GETCD='getCurrentData';
const OP_LP='loadMainTablePage';
const EVENT_PAGE_COLLECT='pageCollect'
const EVENT_START_BROKER_CALL='startInvoke';
const EVENT_START_MERGE_DATA = 'startMergeData';
const EVENT_START_MERGE_TABLE = 'startMergeTable';
const EVENT_END_MERGE = 'endMerge';
const EVENT_END_BROKER_CALL='endBrokerCall'

const MEASURE_MAIN = 'Main';
const MEASURE_PREPARE = 'Prepare';
const MEASURE_CALL_BROKERS = 'CallBrokers';
const MEASURE_MERGE_DATA = 'MergeData';
const MEASURE_MERGE_TABLE = 'MergeTable';
/**
 * Metrics of a Request
 */
class RequestMetrics {
   constructor(requestId,timeStamp,concurrency,batchConcurrency,requestDuration,preparationDuration,callBrokersDuration,callSlowBrokerDuration,transformDuration,mergeDuration,memoryUsage,rowsCount,brokersCallsMetrics) 
   {
      this.requestId=requestId;
      this.timeStamp=timeStamp;
      this.concurrency=concurrency;
      this.batchConcurrency=batchConcurrency!=undefined?parseInt(batchConcurrency):0;
      this.requestDuration=requestDuration;
      this.preparationDuration=preparationDuration;
      this.callBrokersDuration=callBrokersDuration;
      this.callSlowBrokerDuration=callSlowBrokerDuration;
      this.transformDuration=transformDuration;
      this.mergeDuration=mergeDuration;
      this.memoryUsageRss=memoryUsage.rss
      this.memoryUsageHeapTotal=memoryUsage.heapTotal;
      this.memoryUsageHeapUsed=memoryUsage.heapUsed;
      this.memoryUsageExternal=memoryUsage.external;
      this.memoryUsageArrayBuffers=memoryUsage.arrayBuffers;
      this.rowsCount=rowsCount;
      this.brokerCalls=brokersCallsMetrics;
   }
}
class BrokerRequestMetrics {
   constructor(broker,callDuration,transformDuration,rowsCount) 
   {
      this.broker=broker;
      this.callDuration=callDuration;
      this.transformDuration=transformDuration;
      this.rowsCount=rowsCount;
   }
}
/**
 * Manager of performence metrics
 */
class PerformanceAccounter {
  /**
   *
   */
   constructor() 
   {
      this.requestMetrics=[];
   }
   logMetric(pRequestMetric)
   {
     this.requestMetrics.push(pRequestMetric); 
   }
   getRequestMetrics(requestId)
   {
     if(requestId!=undefined)
     {
       for(let r of this.requestMetrics)
       {
          if(r.requestId==requestId)
          {
             return r;
          }
       }
       return {};
     }
     return this.requestMetrics;
   }
}
/**
 * Manager of performence metrics
 */
class SystemState {
   constructor() 
   {
      this.metrics=[];
      this.concurrency=0;
      this.tracking=false;
      this.#loadState();
   }
   #loadState()
   {
      this.timeStamp=performance.now();
      this.memoryUsage=process.memoryUsage();
   }
   setState(concurrency)
   {
      this.concurrency=concurrency;
      this.#loadState();
      return this.saveState();
   }
   updateState()
   {
      this.#loadState();
      return this.saveState();
   }
   saveState()
   {
      if(this.tracking)
      {
        this.metrics.push({
                         timeStamp:this.timeStamp,
                         concurrency:this.concurrency,
                         memoryUsageRss:this.memoryUsage.rss,
                         memoryUsageHeapTotal:this.memoryUsage.heapTotal,
                         memoryUsageHeapUsed:this.memoryUsage.heapUsed,
                         memoryUsageExternal:this.memoryUsage.external,
                         memoryUsageArrayBuffers:this.memoryUsage.arrayBuffers
                        });
        return true;
      }
      return false;
   }
   getConcurrency()
   {
      return this.concurrency;
   }
   getTimeStamp()
   {
      return this.timeStamp;
   }
   getMetrics(shiftToZero)
   {
      if(shiftToZero)
      {
         let lOut=[];
         let lStartTS=undefined;
         for(let m of this.metrics)
         {
            if(m.concurrency>0 && lStartTS==undefined)
            {
               lStartTS=m.timeStamp;
            }
            if(lStartTS!=undefined)
            {
               let n=Object.assign({}, m);
               n.timeStamp=m.timeStamp-lStartTS; 
               lOut.push(n);
            }
         }
         return lOut;
      }
      return this.metrics;
   }
   startTracking(testName)
   {
      //TODO: implement named tests
      this.metrics=[];
      this.tracking=true;
   }
   stopTracking()
   {
      this.tracking=false;
   }
}

/**
 * Metrics Manager initialization
 */
const performanceAccounter=new PerformanceAccounter();
const systemState=new SystemState();

function markName(pRequestId,pOperation,pEvent,pFiwareService,pPartId,pPartLength)
{
    if(pFiwareService!=undefined && pPartId!=undefined && pPartLength==undefined)
    {
      return ''+pRequestId+':'+pFiwareService+':'+pPartId+':'+pOperation+'/'+pEvent;
    }
    else if(pFiwareService!=undefined && pPartId!=undefined && pPartLength!=undefined)
    {
      return ''+pRequestId+':'+pFiwareService+':'+pPartId+':'+pPartLength+':'+pOperation+'/'+pEvent;
    }
    else
    {
      return ''+pRequestId+':'+pOperation+'/'+pEvent;
    }
}
function mark(pRequestId,pOperation,pEvent,pFiwareService,pPartId,pPartLength)
{
    let lMarkName=markName(pRequestId,pOperation,pEvent,pFiwareService,pPartId,pPartLength);
    let lMark=performance.mark(lMarkName);
    debug(`Do Mark ${lMarkName} at ${lMark.startTime} ms`);
    return lMark;
}
function measureOperation(pRequestId,pOperation,pPhase,pMark1,pMark2)
{
   try
   {
      return performance.measure(pRequestId+':'+pOperation+'/'+pPhase,pMark1,pMark2);
   }
   catch(err)
   {
      debug(`Error while getting measure: ${err}`);
      return undefined;
   }
}
function measureBrokerCall(pRequestId,pBroker,pPage,pMark1,pMark2)
{
   try
   {
      return performance.measure(pRequestId+':Broker/'+pBroker+'/'+pPage,pMark1,pMark2);
   }
   catch(err)
   {
      debug(`Error while getting measure: ${err}`);
      return undefined;
   }
}

function match(pRequestId,pObjectName)
{
    return pObjectName.startsWith(pRequestId+':');
}
function matchOperation(pRequestId,pOperation,pObjectName)
{
    return pObjectName.startsWith(pRequestId+':') && pObjectName.indexOf(':'+pOperation+'/')>0;
}
function matchOperationStart(pOperation,pObjectName)
{
    return pObjectName.endsWith(':'+pOperation+'/start');
}
function matchOperationEnd(pOperation,pObjectName)
{
    return pObjectName.endsWith(':'+pOperation+'/end');
}
function parseFiwareService(pObjectName)
{
    return pObjectName.split(':')[1];
}
function parsePartId(pObjectName)
{
    return pObjectName.split(':')[2];
}
function parsePartLength(pObjectName)
{
    return pObjectName.split(':')[3];
}

/**
 Get the event from the object's name
 */
function parseEvent(pObjectName)
{
   return pObjectName.substring(pObjectName.indexOf('/')+1); 
}
function matchMeasureOperation(pRequestId,pObjectName,pOperation,pPhase)
{
    //return pObjectName.startsWith(pRequestId+':') && pObjectName.endsWith(pOperation+'/'+pPhase);
    return pObjectName==(pRequestId+':'+pOperation+'/'+pPhase);
}
function matchMeasureBroker(pRequestId,pObjectName)
{
   return pObjectName.startsWith(pRequestId+':Broker/');
}
function getSystemMetrics(shiftToZero)
{
   return systemState.getMetrics(shiftToZero);
}
function getRequestMetrics(requestId)
{
   return performanceAccounter.getRequestMetrics(requestId);
}
function setState(concurrency)
{
   systemState.setState(concurrency);
}
function stopTrackingSystemState()
{
   systemState.stopTracking();
   return 'stoped';
}
function startTrackingSystemState(testName)
{
   systemState.startTracking(testName);
   return 'started';
}
function hookTimedMetrics()
{
   let lSaved=systemState.updateState();
   let lTime=systemState.getTimeStamp();
   if(lTime%10==0)
   {
      debug(`hookTimedMetrics[${lTime}]::Concurency ${systemState.getConcurrency()} saved ${lSaved}`);
   }
}
/**
 * Hooks the event of a request is starting
 */
function hookStartRequest(req,res,next)
{
   debug('hookStartRequest');
   next();
}
/**
 * Hooks the event of a request is ending
 */
function hookEndRequest(req,res,next)
{
   let lBatchConcurrency=req.query.concurrency;
   let lConcurrencyRequests=utils.getRequestConcurrency(res);
   let lRequestId=utils.getRequestId(req);
   let lMemoryUsage=process.memoryUsage();
   let lTimeStamp=performance.now();  //Now? Or when?

   let marks=performance.getEntriesByType('mark');
   debug(`hookEndRequest -  RequestId: ${lRequestId} Concurency: ${lBatchConcurrency}  Memory Usage: ${JSON.stringify(lMemoryUsage)}`);
   for(let m of marks)
   {
       //console.log(JSON.stringify(m,null,2));
       if(match(lRequestId,m.name))
       {
          debug(`${m.entryType} ${m.name} => \t Start Time ${m.startTime.toFixed(5)} ms`);
       }
   }


   //Collect measures
   let lMarkStart=markName(lRequestId,OP_GETCD,'start');
   let lMarkStartInvoke=markName(lRequestId,OP_GETCD,EVENT_START_BROKER_CALL);
   let lMarkStartMerge=markName(lRequestId,OP_GETCD,EVENT_START_MERGE_TABLE);
   let lMarkEndMerge=markName(lRequestId,OP_GETCD,EVENT_END_MERGE);
   let lMarkEnd=markName(lRequestId,OP_GETCD,'end');
   measureOperation(lRequestId,OP_GETCD,MEASURE_MAIN,lMarkStart,lMarkEnd);
   measureOperation(lRequestId,OP_GETCD,MEASURE_PREPARE,lMarkStart,lMarkStartInvoke);
   measureOperation(lRequestId,OP_GETCD,MEASURE_CALL_BROKERS,lMarkStartInvoke,lMarkStartMerge);
   measureOperation(lRequestId,OP_GETCD,MEASURE_MERGE_TABLE,lMarkStartMerge,lMarkEndMerge);

   //Collect marks related to loadPages
   let lMarksByBrokerPage={}
   for(let m of marks)
   {
       if(matchOperation(lRequestId,OP_LP,m.name))
       {
          let lFiwareService=parseFiwareService(m.name);
          let lPartId=parseInt(parsePartId(m.name));
          let lPartLength=undefined;
          let lEvent=parseEvent(m.name);
          if(lMarksByBrokerPage[lFiwareService]==undefined)
          {
             lMarksByBrokerPage[lFiwareService]={};
             lMarksByBrokerPage[lFiwareService]['parts']={};
             lMarksByBrokerPage[lFiwareService]['markEndBrokerTransform']=undefined;
          }
          if(lMarksByBrokerPage[lFiwareService]['parts'][lPartId]==undefined)
          {
             lMarksByBrokerPage[lFiwareService]['parts'][lPartId]={};
          }
          lMarksByBrokerPage[lFiwareService]['parts'][lPartId][lEvent]={};
          lMarksByBrokerPage[lFiwareService]['parts'][lPartId][lEvent]['mark']=m;
          if(lEvent==undefined)
          {
             debug(`Invalid event  mark=${m.name}`);
          }
          else if(lEvent==EVENT_PAGE_COLLECT)
          {
             lPartLength=parsePartLength(m.name);
             lMarksByBrokerPage[lFiwareService]['parts'][lPartId][lEvent]['size']=parseInt(lPartLength);
          }
          else if(lEvent==EVENT_END_BROKER_CALL)
          {
             lPartLength=parsePartLength(m.name);
             lMarksByBrokerPage[lFiwareService]['markEndBrokerTransform']=m;
          }
          debug(`${m.entryType} ${m.name} => Event ${lEvent} FiwareService ${lFiwareService} PartId ${lPartId} PartLength ${lPartLength}`);
       }
   }
   //Compute measures per Broker
   let lBrokersDuration={};	  // Time consumed by the broker
   let lBrokersTransformTime={};  // Time consumed for data transformations
   let lBrokersRowsCount={};
   let lSlowestBroker=undefined;
   for(let lFiwareService in lMarksByBrokerPage)
   {
       lBrokersDuration[lFiwareService]=0;
       lBrokersRowsCount[lFiwareService]=0;
       lBrokersTransformTime[lFiwareService]=0;
       let lTimeEndLastPage=undefined;
       let lParts=lMarksByBrokerPage[lFiwareService]['parts'];
       for(let lPageId in lParts)
       {
          let ms=lParts[lPageId]['startPage'];
          let me=lParts[lPageId]['endPage'];
          let ml=lParts[lPageId][EVENT_PAGE_COLLECT];
          if(ms!=undefined && me!=undefined && ml!=undefined && ms['mark']!=undefined && me['mark']!=undefined && ml['size']!=undefined)
          {
               let measure=measureBrokerCall(lRequestId,lFiwareService,lPageId,ms['mark'].name,me['mark'].name);
               lParts[lPageId]['measure']=measure.name;
               lBrokersDuration[lFiwareService]=lBrokersDuration[lFiwareService]+measure.duration;
               lBrokersRowsCount[lFiwareService]=lBrokersRowsCount[lFiwareService]+ml['size'];
               if(lTimeEndLastPage==undefined || lTimeEndLastPage<me['mark'].startTime)
               {
                  // Time of last page
                  lTimeEndLastPage=me['mark'].startTime;
               }
               if(lSlowestBroker==undefined || lBrokersDuration[lFiwareService]>lBrokersDuration[lSlowestBroker])
               {
                  lSlowestBroker=lFiwareService;
               }
          }
          else
          {
               debug('Operation inconsistency in '+lFiwareService+'  Page '+lPageId);
          }
       }
       let lMarkEndTransform=lMarksByBrokerPage[lFiwareService]['markEndBrokerTransform'];
       if(lMarkEndTransform && lTimeEndLastPage!=undefined)
       {
          // This includes: time for collect data and transform
          lBrokersTransformTime[lFiwareService]=lMarkEndTransform.startTime-lTimeEndLastPage;
          debug(`Broker[${lFiwareService}] Last page: ${lTimeEndLastPage} \t End transform: ${lMarkEndTransform.startTime}   TransformTime: ${lBrokersTransformTime[lFiwareService]}`);
       }
   }
   // Compute end of transformation
   if(lSlowestBroker!=undefined)
   {
     let lMarkEndTransform=lMarksByBrokerPage[lSlowestBroker]['markEndBrokerTransform'];
     debug(`SlowestBroker: ${lSlowestBroker} mark=${lMarkEndTransform.name}  Time=${lMarkEndTransform.startTime}`)
     // The merge start efectevely when the slowest broker finishes its data transformation
     measureOperation(lRequestId,OP_GETCD,MEASURE_MERGE_DATA,lMarkEndTransform.name,lMarkEndMerge);
   } 
   //Compute metrics	
   let measures=performance.getEntriesByType('measure');
   let lRequestDuration;
   let lPreparationDuration
   let lCallBrokersDuration;
   let lMergeDataDuration;
   for(let m of measures)
   {
       if(match(lRequestId,m.name))
       {
         debug(`${m.entryType} Start Time ${m.startTime.toFixed(5)}   End Time ${(m.startTime+m.duration).toFixed(5)}   Duration: ${m.duration.toFixed(5)} ms :: <${m.name}> `);
         if(matchMeasureOperation(lRequestId,m.name,OP_GETCD,'Main'))
         {
            lRequestDuration=m.duration;
         }
         else if(matchMeasureOperation(lRequestId,m.name,OP_GETCD,MEASURE_PREPARE))
         {
            lPreparationDuration=m.duration;
         }
         else if(matchMeasureOperation(lRequestId,m.name,OP_GETCD,MEASURE_CALL_BROKERS))
         {
            lCallBrokersDuration=m.duration;
         }
         else if(matchMeasureOperation(lRequestId,m.name,OP_GETCD,MEASURE_MERGE_DATA))
         {
            lMergeDataDuration=m.duration;
         }
         else if(matchMeasureOperation(lRequestId,m.name,OP_GETCD,MEASURE_MERGE_TABLE))
         {
            debug(`Merge Table:  lMergeDataDuration=${lMergeDataDuration}  MergeTableDuration=${m.duration}`);
         }
         else if(!matchMeasureBroker(lRequestId,m.name))
         {
            debug('Ignored measure '+m.name);
         }
      }
   }
   //Concurrency
   debug(`Concurrent Requests: ${lConcurrencyRequests} Parameter Concurrency=${lBatchConcurrency}`);
   let lBrokersMaxDuration=0;
   let lTransformDuration=0;
   debug('lCallBrokersDuration='+lCallBrokersDuration);
   let lBrokerMetrics=[];
   for(let lFiwareService in lBrokersDuration)
   {
      debug('\t Broker['+lFiwareService+']='+lBrokersDuration[lFiwareService]);
      if(lBrokersMaxDuration<lBrokersDuration[lFiwareService])
      {
         lBrokersMaxDuration=lBrokersDuration[lFiwareService];
         lTransformDuration=lBrokersTransformTime[lFiwareService];
      }
      lBrokerMetrics.push(new BrokerRequestMetrics(lFiwareService,lBrokersDuration[lFiwareService],lBrokersTransformTime[lFiwareService],lBrokersRowsCount[lFiwareService]));
   }
   debug(`lBrokersMaxDuration=${lBrokersMaxDuration}ms    lTransformDuration=${lTransformDuration}ms`);
   let lTable=utils.getOutputTable(res);
   let lRows=lTable!=undefined?lTable.countRows():0;
   let lMetric=new RequestMetrics(lRequestId,lTimeStamp,lConcurrencyRequests,lBatchConcurrency,lRequestDuration,lPreparationDuration,lCallBrokersDuration,lBrokersMaxDuration,lTransformDuration,lMergeDataDuration,lMemoryUsage,lRows,lBrokerMetrics);
   performanceAccounter.logMetric(lMetric);

   next();
}

/**
 * Exported functions
 */
exports.markName = markName;
exports.getRequestMetrics = getRequestMetrics;
exports.getSystemMetrics = getSystemMetrics;
exports.setState = setState;
exports.startTrackingSystemState = startTrackingSystemState;
exports.stopTrackingSystemState = stopTrackingSystemState;
exports.mark = mark;
exports.OP_GETCD = OP_GETCD;
exports.OP_LP = OP_LP;
exports.EVENT_PAGE_COLLECT = EVENT_PAGE_COLLECT;
exports.EVENT_START_BROKER_CALL = EVENT_START_BROKER_CALL;
exports.EVENT_START_MERGE_DATA = EVENT_START_MERGE_DATA;
exports.EVENT_START_MERGE_TABLE = EVENT_START_MERGE_TABLE;
exports.EVENT_END_MERGE = EVENT_END_MERGE;
exports.EVENT_END_BROKER_CALL = EVENT_END_BROKER_CALL;
exports.hookTimedMetrics = hookTimedMetrics;
exports.hookStartRequest = hookStartRequest;
exports.hookEndRequest = hookEndRequest;

