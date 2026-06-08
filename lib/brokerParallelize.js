/**
 * API for providing Matadata
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var configsys = require('./configsys.js');
var Broker = require('./broker.js')
var TableModel = require('./model/tablemodel.js');
var Rank = require('../lib/topk/rank.js');
var PrintTable=require('../lib/printTable.js');
var schema = require('./schema.js');
var NgsiGeoQueryParser=require('./utils/NgsiGeoQueryParser.js');
var debug = require('debug')('iotbi.brokerparallelize');
var { PerformanceObserver, performance } = require('node:perf_hooks');
var perfanalyser= require('../lib/performenceanalyser.js');

const HARD_LIMIT_TO_RANK = 17000;
const HARD_LIMIT_ROWS_NOSCORED = 50;

module.exports = class BrokerParallelize {
  /**
   *
   */
   constructor(fiwareServices) 
   {
      this.fiwareServices = fiwareServices;
      this.offset=0;
      this.limit=100;
      this.query=undefined;
      this.georel=undefined;
      this.geometry=undefined;
      this.coordinates=undefined;
      this.geoproperty=undefined;
      this.textPattern=undefined;
      this.searchField=undefined;
      this.topK=undefined;
      this.requestId=0;
   }
   setRequestId(requestId)
   {
       this.requestId=requestId;
   }
   setQuery(query,pEntityType)
   {
     var lValidation=Broker.validateQuery(query,pEntityType);
     debug('Query Validation: '+(lValidation==undefined?'Ok':lValidation));
     this.query=query;
     return lValidation;
   }
   setGeoQuery(pGeorel,pGeometry,pCoordinates,pGeoproperty,pEntityType)
   {
      var lValidation=Broker.validateGeoQuery(pGeorel,pGeometry,pCoordinates,pGeoproperty,pEntityType);
      this.georel=pGeorel;
      this.geometry=pGeometry;
      this.coordinates=pCoordinates;
      this.geoproperty=pGeoproperty;
      return lValidation;
   }
   getQuery()
   {
     return this.query;
   }
   setTextPattern(textPattern,searchField)
   {
      this.textPattern=(textPattern!=undefined && textPattern.length>0)?textPattern:undefined;;
      this.searchField=(searchField!=undefined && searchField.length>0)?searchField:undefined;
      //TODO: validate
      return undefined;
   }
   getTextPattern()
   {
      return this.textPattern;
   }
   setTopK(k)
   {
      this.topK=k;
   }
   static calcTopKRows(pTable,pTopKCells)
   {
      if(pTable==undefined || pTopKCells==undefined)
      {
         return undefined;
      }
      else
      {
         let lTopKRows=pTopKCells/pTable.countCols();
         debug(`TopK ::  Rows=${lTopKRows}  Cells=${pTopKCells}`);
         return lTopKRows;
      }
   }
  /**
   * Returns a Result
   */
   getCurrentData(pEntityType,pAttribs,pEntityId,pExtended,pTableNameJoin,pJoinAttribute,pMaxColumns)
   {
      perfanalyser.mark(this.requestId,perfanalyser.OP_GETCD,'start');
      let lRequestId=this.requestId;
      return this.#__getCurrentData(pEntityType,pAttribs,pEntityId,pExtended,pTableNameJoin,pJoinAttribute,pMaxColumns)
           .then(result => new Promise(function(resolve, reject) {
                 resolve(result);
           }))
           .catch(error => {
             //debug('Error:'+JSON.stringify(error));
             throw error;
           })
           .finally(() =>  {
                  perfanalyser.mark(lRequestId,perfanalyser.OP_GETCD,'end');
           });
   }
   #__getCurrentData(pEntityType,pAttribs,pEntityId,pExtended,pTableNameJoin,pJoinAttribute,pMaxColumns)
   {
      debug(this.fiwareServices);
      var lPromises=[];
      var lPromisesService=[];
      var lPromisesBrokers=[];
      var lQuery=this.getQuery();
      var lGeorel=this.georel;
      var lGeometry=this.geometry;
      var lCoordinates=this.coordinates;
      var lGeoproperty=this.geoproperty;
      var lTextPattern=this.textPattern;
      var lSearchField=this.searchField;
      var lTopK=this.topK;
      let lSelectedAttribs=undefined;
      let lMaxColumns=(pMaxColumns==undefined?999999:pMaxColumns);
      for(var lFiwareService of this.fiwareServices)
      {
           var lBroker=new Broker(lFiwareService);
           lBroker.setLimit(this.limit);
           if(pAttribs!=undefined && pAttribs.length>0)
           {
                debug('pAttribs=\"'+pAttribs+'\"');
                var lSplited=pAttribs.split(',');
                if(lSplited.length<70)
                {
                    for(var a of lSplited)
                    {
                       debug('a='+a);
                       if(a!='id' && a!='type')
                       {
                           var lOk=lBroker.validateAttribute(pEntityType,a);
                           if(lOk!=undefined)
                           {
                              // Return a promise of Error
                              return new Promise(function(resolve, reject) { reject(lOk)});
                           }
                           else
                           {
                              lBroker.addAttrib(a);
                           }
                       }
                       else
                       {
                          debug('Param '+a+' is ignored');
                       }
                    }
                }
                else
                {
                   debug('The list of attributes ('+lSplited.length+') is too long. All will be considered!');
                }
           }
           lSelectedAttribs=lBroker.getAttribs();
           lBroker.setQuery(lQuery);
           lBroker.setGeoQuery(lGeorel,lGeometry,lCoordinates,lGeoproperty);
           lBroker.setTextPattern(lTextPattern,lSearchField)
           debug('Create Promise: '+lFiwareService+' '+pEntityType+'  AttribList='+pAttribs+'  query='+lQuery);
           // Get the Query
           lPromises.push(lBroker.createPromiseQueryTableEntities(pEntityType,pEntityId,pExtended,pTableNameJoin,pJoinAttribute,this.requestId));
           lPromisesService.push(lFiwareService);
           lPromisesBrokers.push(lBroker);
      }
      let lSucessCall=false;
      let lRequestId=this.requestId;
      perfanalyser.mark(lRequestId,perfanalyser.OP_GETCD,perfanalyser.EVENT_START_BROKER_CALL);
      return new Promise(function(resolve, reject) {
         Promise.all(lPromises)
            .then(results => {
               lSucessCall=true;
               perfanalyser.mark(lRequestId,perfanalyser.OP_GETCD,perfanalyser.EVENT_START_MERGE_TABLE);
               var lTableAcumulator=undefined;
               for(var lResultIdx in results)
               {
                  var lTable=results[lResultIdx];
                  if(lTable==undefined)
                  {
                      debug('Invalid Result at '+lResultIdx);
                  }
                  else if(lTable.countRows()==0)
                  {
                      debug('Empty Result at '+lResultIdx);
                  }
                  else if(lTableAcumulator==undefined)
                  {
                     lTableAcumulator=lTable;
                     debug('Init Table :: Cols: '+lTableAcumulator.countCols()+' Rows:'+lTableAcumulator.countRows());
                  }
                  else
                  {
                      var lResultService=lPromisesService[lResultIdx];
                      var lOk=lTableAcumulator.merge(lTable);
                      debug('Merge Table :: '+lOk+' Rows:'+lTable.countRows()+' ->  '+lTableAcumulator.countRows());
                  }
               }
               if(lTableAcumulator==undefined)
               {
                   lTableAcumulator=new TableModel('NGSI-LD-v1 empty table',[],{});
               }
               if(lTableAcumulator.countCols()>lMaxColumns)
               {
                      if(lSelectedAttribs!=undefined && lSelectedAttribs.length>lTableAcumulator.countCols()-1)
                      {
                         debug(`Too many columns to load: ${lSelectedAttribs.length}`);
                         reject('In the query, select less attributes.');
                      }
                      else if(lSelectedAttribs==undefined || lSelectedAttribs.length==0)
                      {
                         debug(`Too many columns to load: ${lTableAcumulator.countCols()}`);
                         reject('In the query, you must select the attributes to load.');
                      }
                      else
                      {
                         debug(`Too many columns to load: ${lTableAcumulator.countCols()}, despite only ${lSelectedAttribs.length} are selected!`);
                      }
               }

               //coordinates: [longitude, latitude]
               debug('Local Geoquery='+configsys.isLocalGeoQueryOn()+' georel='+lGeorel+'    geometry='+lGeometry+'    coordinates='+lCoordinates);
               if(configsys.isLocalGeoQueryOn() && lGeorel!=undefined)
               {
                   var parser = new NgsiGeoQueryParser(lGeorel,lGeometry,lCoordinates);
		   var geoquery=parser.getParsedData();
                   debug('Do local GeoQuery georel='+geoquery.georel+
                                           '  Geometry='+geoquery.geometry+
                                           '  Long: '+geoquery.coordinates[0]+'   Lat:'+geoquery.coordinates[1]+
                                           '  MaxDistance='+geoquery.maxDistance+
                                           '  MinDistance='+geoquery.minDistance+
                                           '  GeoProperty='+lGeoproperty);
                   // The geoquery is local
                   //pointLat,pointLon,pMinDistance,pMaxDistance,pGeopropert
                   lTableAcumulator=lTableAcumulator.filterGeorelNear(geoquery.coordinates[1],geoquery.coordinates[0],geoquery.minDistance,geoquery.maxDistance,lGeoproperty);
               }
               if(lTextPattern!=undefined && lSearchField!=undefined && lTextPattern.length>40)
               {
                      //Our transformer dont like long strings :-(
                      reject('The full-text string is too long. Divide it on multiple searchs');
               }
               else if(lTextPattern!=undefined && lSearchField==undefined && lTableAcumulator.countCols()>10)
               {
                      reject('Specify the column in which to apply full-text search');
               }
               else if(lTextPattern!=undefined && (lTableAcumulator.countRows()*lTableAcumulator.countCols()>HARD_LIMIT_TO_RANK))
               {
                      debug('Long data. Return without filter/sort due to long result. Rows: '+lTableAcumulator.countRows()+' and Cols: '+lTableAcumulator.countCols());
                      //debug(JSON.stringify(lTableAcumulator,null,2));
                      //let lRank=new Rank(lTableAcumulator,lSelectedAttribs,undefined,undefined,undefined);
                      //let lUnsortedResult=lRank.unsorted(lTopK,'The is too long. Apply geospatial restritions using the parameter geoquery or reduce the number of columns.');
                      //lUnsortedResult.debugResult();
                      //resolve(lUnsortedResult);
                      reject('The result is too long. Apply geospatial restritions using the parameter geoquery or reduce the number of columns.');
               }
               else if(lTextPattern!=undefined && lTableAcumulator.countRows()>0)
               {
                      let lTransformer=configsys.getTransformerSentenceSimilarity();
                      let lTopKRows=BrokerParallelize.calcTopKRows(lTableAcumulator,lTopK);
                      debug('Do local filter topK '+lTopK+' cells ('+lTopKRows+' rows) by pattern:"'+lTextPattern+'" in field(s): '+lSearchField);
                      let lRank=new Rank(lTableAcumulator,lSelectedAttribs,lTextPattern,lSearchField,lTransformer);
                      lRank.sortByRelevance(lTopKRows,undefined,HARD_LIMIT_ROWS_NOSCORED)
                          .then(function (lSortResult) {
                                      lTableAcumulator=lSortResult.table;
                                      //Clone table for debug
                                      let lTableDebug=lTableAcumulator.clone();
                                      //Add column Score
                                      let lNewCol=[];
                                      let n=0;
                                      lSortResult.scores.forEach((s) => {lNewCol[n]=s.score.similarity.toFixed(3);n=n+1})
                                      lTableDebug.addColumn('score',schema.STRING,lNewCol);

                                      //Mark the text segments
                                      n=0;
                                      lSortResult.scores.forEach((s) => {
                                           PrintTable.paintRow(lTableDebug,s.score.colName,n,s.score.sentence);
                                           n=n+1;
                                      });
                                      PrintTable.printTable(lTableDebug,debug);
                                      debug('Searched Text: '+lTextPattern+' in field(s):'+lSearchField);
                                      lSortResult.debugResult();
                                      resolve(lSortResult);
                                   });
               }
               else
               {
                      let lMsg;
                      if(lTableAcumulator.countRows()>0)
                      {
                         lMsg='Success';
                      }
                      else if(lGeorel!=undefined)
                      {
                         lMsg='Empty result. Using geometadata tool, obtain all cluster in which your may search data to answer the user\'s questions.';
                      }
                      else
                      {
                         Msg='Empty result. Check if your query is adjusted to the user\' questions.';
                      }
                    
                      let lTopKRows=BrokerParallelize.calcTopKRows(lTableAcumulator,lTopK);
                      debug('Return without filter/sort :: TopK Rows='+lTopKRows+'  (cells='+lTopK+')');
                      //debug(JSON.stringify(lTableAcumulator,null,2));
                      let lRank=new Rank(lTableAcumulator,lSelectedAttribs,undefined,undefined,undefined);
                      let lUnsortedResult=lRank.unsorted(lTopKRows,lMsg);
                      lUnsortedResult.debugResult();
                      resolve(lUnsortedResult);
               }
           })
          .catch(error => {
             debug('Error using brokers '+JSON.stringify(lPromisesService)+':'+JSON.stringify(error));
             reject(error);
           })
          .finally(() => {
             if(lSucessCall)
             {
                perfanalyser.mark(lRequestId,perfanalyser.OP_GETCD,perfanalyser.EVENT_END_MERGE);
             }
           });
       });
   }
  /**
   *
   */
   getGeoMetadata(pEntityType,pRadius)
   {
      debug(this.fiwareServices);
      var lPromises=[];
      var lPromisesService=[];
      var lPromisesBrokers=[];
      for(var lFiwareService of this.fiwareServices)
      {
           var lBroker=new Broker(lFiwareService);
           lBroker.setLimit(this.limit);
           debug('Create Promise: '+lFiwareService+' '+pEntityType+'  Radius: '+pRadius);
           // Get the Query
           lPromises.push(lBroker.createPromiseGeoMetadataEntities(pEntityType,pRadius));
           lPromisesService.push(lFiwareService);
           lPromisesBrokers.push(lBroker);
      }
     return new Promise(function(resolve, reject) {
         Promise.all(lPromises)
            .then(results => {
               var lResultOut=[];
               //debug(JSON.stringify(results,null,2))
               var lTable=[];
               var lSchema={};
               for(var lResultIdx in results)
               {
                  var lSubTableSchema=results[lResultIdx];
                  if(lSubTableSchema==undefined)
                  {
                      debug('Invalid tableSchema at '+lResultIdx);
                  }
                  else
                  {
                      var lResultService=lPromisesService[lResultIdx];
                      debug('Fiware Service: '+lResultService);
                      var lSubTable=lSubTableSchema.getRows();
                      if(lSubTable.length>0)
                      {
                         for(var lRow of lSubTable)
                         {
                           lTable.push(lRow);
                         }
                      }
                   }
               }
               //Must be equal to results
               lSchema['fiwareService']=schema.STRING;
               lSchema['entityType']=schema.STRING;
               lSchema['cluster']=schema.INTEGER;
               lSchema['centroid_location_coordinates_lon']=schema.DOUBLE;
               lSchema['centroid_location_coordinates_lat']=schema.DOUBLE;
               lSchema['points']=schema.INTEGER;
               lSchema['max_distance']=schema.DOUBLE;
               debug('Table:'+JSON.stringify(lTable,null,2));
               resolve([lTable,lSchema]);
           })
          .catch(error => {
             debug(error);
             reject(error);
           });
       }); 
   }
}
