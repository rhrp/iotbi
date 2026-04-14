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
//var EntitySchema = require('./EntitySchema.js');
var NgsiGeoQueryParser=require('./utils/NgsiGeoQueryParser.js');
var debug = require('debug')('iotbi.brokerparallelize');
var { PerformanceObserver, performance } = require('node:perf_hooks');
var perfanalyser= require('../lib/performenceanalyser.js');


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
   setTextPattern(textPattern)
   {
      this.textPattern=textPattern;
   }
   getTextPattern()
   {
      return this.textPattern;
   }
   setTopK(k)
   {
      this.topK=k;
   }
  /**
   * Returns a Result
   */
   getCurrentData(pEntityType,pAttribs,pEntityId,pExtended,pTableNameJoin,pJoinAttribute)
   {
      perfanalyser.mark(this.requestId,perfanalyser.OP_GETCD,'start');
      let lRequestId=this.requestId;
      return this.#__getCurrentData(pEntityType,pAttribs,pEntityId,pExtended,pTableNameJoin,pJoinAttribute)
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
   #__getCurrentData(pEntityType,pAttribs,pEntityId,pExtended,pTableNameJoin,pJoinAttribute)
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
      var lTopK=this.topK;

      for(var lFiwareService of this.fiwareServices)
      {
           var lBroker=new Broker(lFiwareService);
           lBroker.setLimit(this.limit);
           if(pAttribs!=undefined && pAttribs.length>0)
           {
                debug('pAttribs=\"'+pAttribs+'\"');
                var lSplited=pAttribs.split(',');
                if(lSplited.length<7)
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
           lBroker.setQuery(lQuery);
           lBroker.setGeoQuery(lGeorel,lGeometry,lCoordinates,lGeoproperty);
           lBroker.setTextPattern(lTextPattern)
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
               if(lTextPattern!=undefined && lTableAcumulator.countRows()>2000)
               {
                      debug('Long data. Return without filter/sort:');
                      //debug(JSON.stringify(lTableAcumulator,null,2));
                      let lRank=new Rank(lTableAcumulator,undefined,undefined);
                      resolve(lRank.unsorted(lTopK,'The is too long. Apply geospatial restritions.'));
               }
               else if(lTextPattern!=undefined)
               {
                      let lTransformer=configsys.getTransformerSentenceSimilarity();
                      debug('Do local filter topK '+lTopK+' rows by pattern:"'+lTextPattern+'"');
                      let lRank=new Rank(lTableAcumulator,lTextPattern,lTransformer);
                      lRank.sortByRelevance(lTopK)
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
                                      debug('Searched Text: '+lTextPattern);
                                      debug('Return Rows: '+lTableAcumulator.countRows());
                                      resolve(lSortResult);
                                   });
               }
               else
               {
                      debug('Return without filter/sort:');
                      //debug(JSON.stringify(lTableAcumulator,null,2));
                      let lRank=new Rank(lTableAcumulator,undefined);
                      resolve(lRank.unsorted(lTopK));
               }
           })
          .catch(error => {
             debug('Error:'+JSON.stringify(error));
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
