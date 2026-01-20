/**
 * API for providing Matadata
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var configsys = require('./configsys.js');
var Broker = require('./broker.js')
var TableModel = require('./model/tablemodel.js');
var schema = require('./schema.js');
//var EntitySchema = require('./EntitySchema.js');
var NgsiGeoQueryParser=require('./utils/NgsiGeoQueryParser.js');
var debug = require('debug')('iotbi.brokerparallelize');

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
   getCurrentData(pEntityType,pAttribs,pEntityId)
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
                          lBroker.addAttrib(a);
                       else
                          debug('Param '+a+' is ignored');
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
           lPromises.push(lBroker.createPromiseQueryTableEntities(pEntityType));
           lPromisesService.push(lFiwareService);
           lPromisesBrokers.push(lBroker);
      }
      return new Promise(function(resolve, reject) {
         Promise.all(lPromises)
            .then(results => {
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
               if(configsys.isLocalSearchTextPatternOn() && lTextPattern!=undefined)
               {
                  debug('Do local filter rows by pattern:"'+lTextPattern+'"');
                  lTableAcumulator=lTableAcumulator.filterTextPattern(lTextPattern);
               }
               resolve(lTableAcumulator);
           })
          .catch(error => {
             debug(error);
             reject(error);
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
