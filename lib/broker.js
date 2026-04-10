/**
 * API for providing Matadata
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var configsys = require('./configsys.js');
var ngsildv1 = require('./ngsildv1.js');
var ngsiv2 = require('./ngsiv2.js');
var NGSI = require('ngsijs');
//var ngsildcontext = require('./ngsildcontext.js');
//var schema = require('./schema.js');
var EntitySchema = require('./EntitySchema.js');
var sdm = require('./smartdatamodels.js');
NgsiQueryParser=require('./utils/NgsiQueryParser.js');

var debug = require('debug')('iotbi.broker');

module.exports = class Broker {
  /**
   *
   */
   constructor(fiwareService) 
   {
      this.fiwareService = fiwareService;
      this.isNsgiLD=configsys.isVersionLDV1(fiwareService);
      this.offset=0;
      this.limit=20;
      this.attribs=[];
      this.query=undefined;
      this.georel=undefined;
      this.geometry=undefined;
      this.coordinates=undefined;
      this.geoproperty=undefined;
      //local filter
      this.textPattern=undefined;
   }
   static GEOMETRY_POINT = 'Point';
   static GEOMETRY_POLYGON = 'Polygon';
   setQuery(query)
   {
      this.query=query;
   }
   validateAttribute(pEntityType,pAttributeName)
   {
       if(configsys.isVersionV2(this.fiwareService))
       {
          //This validation is no possible in NGSI-V2
          return undefined;
       }
       var lSchema=sdm.getEntitySchema(pEntityType);
       if(lSchema==undefined)
       {
          return 'The Schemas are not loaded or the entity '+pEntityType+' does not exists';
       }
       var lValidFields=lSchema.getAllNameFields();
       return lValidFields.includes(pAttributeName)?undefined:'The Entity `'+pEntityType+'` does not have a property `'+pAttributeName+'`';
   }
   static validateQuery(pQuery,pEntityType)
   {
       if(pQuery==undefined)
       {
          return undefined;
       }
       const parser = new NgsiQueryParser();
       var lAnalise = parser.analisarExpressao(pQuery.replaceAll(' ','_SPACE_'));
       //debug(JSON.stringify(lAnalise,null,2));
       if(!lAnalise.valida)
       {
           debug('The query does not have valid NGSI syntax: '+pQuery);
           debug('Erro:'+lAnalise.erro);
           return 'The query does not have valid NGSI syntax';
       }
       var lSchema=sdm.getEntitySchema(pEntityType);
       if(lSchema==undefined)
       {
          return 'The Schemas are not loaded or the entity '+pEntityType+' does not exists';
       }
       var lValidFields=lSchema.getAllNameFields();
       for(var lFieldName of lAnalise.parametros)
       {
            var lField=lSchema.getFieldByName(lFieldName);
            if(lField==undefined)
            {
                 return 'The field `'+lFieldName+'` does not exists in entity `'+pEntityType+'`';
            }
            else if(lFieldName==lSchema.getNgsiLocation())
            {
                 return 'The field `'+lFieldName+'`  is a GeoProperty.  Apply a geoquery using the tool `setGeoQuery`.';
            }
            else if(lSchema.isTypeStringByName(lFieldName))
            {
                 return 'By doing a query using the field `'+lFieldName+'` ('+lSchema.getTypeByName(lFieldName)+') will limit the search result. Use the search based on text patterns.';
            }
            else if(!lSchema.isTypeNumberByName(lFieldName) && !lSchema.isTypeStringByName(lFieldName))
            {
                 return 'The field `'+lFieldName+'` ('+lSchema.getTypeByName(lFieldName)+') is not allowed in a query';
            }
            else
            {
                 debug('The field `'+lFieldName+'` ('+lSchema.getTypeByName(lFieldName)+') is ok');
            }
       }
   }
   static validateGeoQuery(pGeorel,pGeometry,pCoordinates,pGeoproperty,pEntityType)
   {
       if(pGeorel==undefined)
       {
          return undefined;
       }
       if(!pGeorel.startsWith('near') && pGeorel!='within')
       {
          debug('The georel '+pGeorel+' is not allowed');
          return 'The allowed georel are `near` combined with `minDistance` oe `maxDistance` and `within`';
       }
       if(pGeometry==undefined)
       {
          return 'The geometry is required';
       }
       if(pGeometry!=Broker.GEOMETRY_POINT && pGeometry!=Broker.GEOMETRY_POLYGON)
       {
          debug('The geometry '+pGeometry+' is not allowed');
          return 'The allowed geometry are '+Broker.GEOMETRY_POINT+' and '+Broker.GEOMETRY_POLYGON;
       }
       if(pCoordinates==undefined)
       {
          return 'The coordinates are required';
       }
       if(!pCoordinates.startsWith('[') || !pCoordinates.endsWith(']'))
       {
          return 'The coordinates must be in format [longitude, latitude]';
       }
       //this.isNsgiLD
       return undefined;
   }
   setGeoQuery(pGeorel,pGeometry,pCoordinates,pGeoproperty,pEntityType)
   {
      this.georel=pGeorel;
      this.geometry=pGeometry;
      this.coordinates=pCoordinates;
      this.geoproperty=pGeoproperty;
   }
   setTextPattern(textPattern)
   {
      this.textPattern=textPattern;
   }
   setLimit(limit)
   {
      this.limit=limit;
   }
   countSelectedAttributes()
   {
       return this.attribs.length;
   }
   addAttrib(attibute)
   {
      if(attibute!=undefined)
      {
         var lAttrib=attibute.toLowerCase();
         if(this.attribs.includes(lAttrib))
         {
            // Check using lowerCase
            debug('The attribute '+attibute+' is already included');
         }
         else
         {
            //But, adds the original 
            this.attribs.push(attibute);
         }
      }
      else
      {
         debug('Invalid attribut name');
      }
   }
  /**
   * Not used
   *
   createPromiseQueryNgsiEntities(pEntityType,pEntityId)
   {
       if(configsys.isVersionLDV1(this.fiwareService))
       {
           return this.#createPromiseQueryNgsiEntitiesLDV1(pEntityType,pEntityId)
       }
       else if(configsys.isVersionV2(this.fiwareService))
       {
           return this.#createPromiseQueryNgsiEntitiesV2(pEntityType,pEntityId);
       }
       else
       {
           debug('Invalid Broker:'+this.fiwareService);
           throw new Error('Invalid Broker: '+this.fiwareService); 
       }
   }
   */
   createPromiseQueryTableEntities(pEntityType,pEntityId,pExtended,pTableNameJoin,pJoinAttribute,pRequestId)
   {
       if(configsys.isVersionLDV1(this.fiwareService))
       {
           return this.#createPromiseQueryTableEntitiesLDV1(pEntityType,pEntityId,pExtended,pTableNameJoin,pJoinAttribute,pRequestId);
       }
       else if(configsys.isVersionV2(this.fiwareService))
       {
           return this.#createPromiseQueryTableEntitiesV2(pEntityType,pEntityId,pExtended,pRequestId);
       }
       else
       {
           debug('Invalid Broker:'+this.fiwareService);
           throw new Error('Invalid Broker: '+this.fiwareService); 
       }
   }

  /**
   *
   */
   createPromiseGeoMetadataEntities(pEntityType,pRadius)
   {
       if(configsys.isVersionLDV1(this.fiwareService))
       {
           return this.#createPromiseGeoMetadataEntitiesLDV1(pEntityType,undefined,pRadius);
       }
       else if(configsys.isVersionV2(this.fiwareService))
       {
           return this.#createPromiseGeoMetadataEntitiesV2(pEntityType,undefined,pRadius);
       }
       else
       {
           debug('Invalid Broker: '+this.fiwareService);
           throw new Error('Invalid Broker: '+this.fiwareService); 
       }
   }
   /**
       	NGSI-LD 		NGSI v2
	georel=within		georel=coveredBy
	geometry=Polygon|Point	geometry=polygon|point
	coordinates=[...]	coords=...
   */
   static #translateParamGeorel(pParam,pIsNsgiLD)
   {
       if(!pIsNsgiLD)
       {
	 if(pParam=='within')
         {
             return 'coveredBy';
         }
         else if(pParam.startsWith('near'))
         {
             return pParam.replaceAll('==',':');
         }
         else 
         {
             return pParam;
         }
       }
       else
       {
          return pParam;
       }
   }
   static #translateParamCoordinatesPoint(pParam,pIsNsgiLD)
   {
       if(pIsNsgiLD)
       {
           // [long,lat]
           return pParam;
       }
       else
       {
           // lat,long
           let lCoords=JSON.parse(pParam);
           return ''+lCoords[1]+','+lCoords[0];
       }
   }
   static #translateParamCoordinatesPolygon(pParam,pIsNsgiLD)
   {
       if(pIsNsgiLD)
       {
           // [[long,lat],[long,lat],..]
           return pParam;
       }
       else
       {
           // lat,long;lat,long;..
           let lCoords=JSON.parse(pParam);
           let lOut='';
           for(let lIdx in lCoords)
           {
               let lPoint=lCoords[lIdx];
               lOut=lOut+(lIdx==0?'':';')+lPoint[1]+','+lPoint[0];
           }
           return lOut;
       }
   }

   static #translateParamGeometry(pParam,pIsNsgiLD)
   {
       if(!pIsNsgiLD && pParam==Broker.GEOMETRY_POINT)
       {
           return 'point';
       }
       if(!pIsNsgiLD && pParam==Broker.GEOMETRY_POLYGON)
       {
           return 'polygon';
       }
       else
       {
           return pParam;
       }
   }
   addParams(pOptions,pEntityType,pEntityId)
   {
      if(pEntityId!=undefined)
      {
         pOptions["id"]=pEntityId;
      }
      if(this.query!=undefined)
      {
         pOptions["q"]=this.query;
      }
      if(!configsys.isLocalGeoQueryOn())
      {
         //Geoquery at the broker
         if(this.georel!=undefined)
         {
           pOptions["georel"]=Broker.#translateParamGeorel(this.georel,this.isNsgiLD);
         }
         if(this.geometry!=undefined)
         {
              pOptions["geometry"]=Broker.#translateParamGeometry(this.geometry,this.isNsgiLD);
         }
         if(this.coordinates!=undefined)
         {
           if(this.isNsgiLD)
           {
              pOptions["coordinates"]=this.coordinates;
           }
           else if(this.geometry=='Point')
           {
              //TODO: melhorar
              pOptions["coords"]=Broker.#translateParamCoordinatesPoint(this.coordinates,this.isNsgiLD);
           }
           else if(this.geometry=='coveredBy')
           {
              //TODO: melhorar
              pOptions["coords"]=Broker.#translateParamCoordinatesPolygon(this.coordinates,this.isNsgiLD);
           }
           else
           {
              debug('Invalid geometry');
           }
         }
         if(this.geoproperty!=undefined)
         {
           pOptions["geoproperty"]=this.geoproperty;
         }
      }
      if(this.countSelectedAttributes()>0)
      {
          if(this.textPattern!=undefined)
          {
              //The SearchTextPattern is done localy
              debug('Dont apply attrs ('+this.#getAttribListString()+') selection to enable local filtering on all attributes.');
          }
          else
          {
              pOptions["attrs"]=this.#getAttribListString();
          }
      }
      var lSchema=sdm.getEntitySchema(pEntityType);
      var lLocation=undefined
      if(lSchema==undefined)
      {
         debug('The Schemas are not loaded or the entity '+pEntityType+' does not exists');
      }
      else
      {
         lLocation=lSchema.getNgsiLocation();
         //TODO: Only location?
         if(lLocation!=undefined && configsys.isLocalGeoQueryOn() && !this.attribs.includes(lLocation))
         {
            // If other attributes are selected, add the lGeoproperty to enable the local filter by location
            if(this.countSelectedAttributes()>0)
            {
              pOptions["attrs"]=pOptions["attrs"]+','+lLocation;
            }
            else
            {
              pOptions["attrs"]='location';
            }
        }
      }

      debug('Options:'+JSON.stringify(pOptions,null,2));
      return pOptions;
   }
  /**
   * Query for Entities in NGSI-LD
   */
   #createPromiseQueryNgsiEntitiesLDV1(pEntityType,pEntityId)
   {
      var lBrokerURL=configsys.getBrokerURL(this.fiwareService);
      var lTenant=configsys.getBrokerTenant(this.fiwareService);
      var lContext=ngsildv1.getNgsiLdEntityContext(this.fiwareService,pEntityType);
      var lConnection = new NGSI.Connection(lBrokerURL);
      var lOptions = {
                   "tenant":lTenant,
                   "@context":lContext,
                   "type":pEntityType,
                   "offset":this.offset,
                   "limit":this.limit
                  };
      lOptions=this.addParams(lOptions,pEntityType,pEntityId);
      debug('FiwareService: '+this.fiwareService);
      debug('Type: '+pEntityType);
      debug('Broker URL: '+lBrokerURL);
      debug('Options: '+JSON.stringify(lOptions));

      return lConnection.ld.queryEntities(lOptions);
   }
  /**
   * Query for Entities as a Table
   */
   #createPromiseQueryTableEntitiesLDV1(pEntityType,pEntityId,pExtended,pTableNameJoin,pJoinAttribute,pRequestId)
   {
      var lEndpoint=configsys.getBrokerEndpoint(this.fiwareService);
      var lBrokerURL=configsys.getBrokerURL(this.fiwareService);
      var lTenant=configsys.getBrokerTenant(this.fiwareService);
      var lFiwareServicePath=undefined;
      var lExtended=pExtended==undefined?false:pExtended;
      var lTableNameJoin=pTableNameJoin;
      var lJoinAttribute=pJoinAttribute;
      var lOptions = {
                   "type":pEntityType,
                   "offset":this.offset,
                   "limit":this.limit
                  };
      lOptions=this.addParams(lOptions,pEntityType,pEntityId);
      debug('FiwareService: '+this.fiwareService+'(Tenant: '+lTenant+')');
      debug('Type: '+pEntityType+'   Extended='+lExtended+'  TableNameJoin='+pTableNameJoin+'   pJoinAttribute='+pJoinAttribute);
      debug('EndPoint: '+lEndpoint+' (Broker URL: '+lBrokerURL+')');
      debug('Options: '+JSON.stringify(lOptions));

      return ngsildv1.listEntities(lEndpoint,lOptions,this.fiwareService,lFiwareServicePath,lExtended,lTableNameJoin,lJoinAttribute,pRequestId);
   }
   #createPromiseQueryNgsiEntitiesV2(pEntityType,pEntityId)
   {
      var NGSI = require('ngsijs');
      var lBrokerURL=configsys.getBrokerURL(this.fiwareService);
      var lTenant=configsys.getBrokerTenant(this.fiwareService);
      var lContext=ngsildv1.getNgsiLdEntityContext(this.fiwareService,pEntityType);
      var lConnection = new NGSI.Connection(lBrokerURL);
      var lOptions = {
                   "tenant":lTenant,
                   "type":pEntityType,
                   "offset":this.offset,
                   "limit":this.limit
                  };
      lOptions=this.addParams(lOptions,pEntityType,pEntityId);
      debug('FiwareService: '+this.fiwareService);
      debug('Type: '+pEntityType);
      debug('Broker URL: '+lBrokerURL);
      debug('Options: '+JSON.stringify(lOptions));

      return lConnection.v2.queryEntities(lOptions);
   }
   #createPromiseQueryTableEntitiesV2(pEntityType,pEntityId,pExtended,pRequestId)
   {
      var lEndpoint=configsys.getBrokerEndpoint(this.fiwareService);
      var lBrokerURL=configsys.getBrokerURL(this.fiwareService);
      var lTenant=configsys.getBrokerTenant(this.fiwareService);
      var lFiwareServicePath=undefined;
      var lExtended=pExtended==undefined?false:pExtended;
      var lTableNameJoin=undefined;
      var lJoinAttribute=undefined;
      var lOptions = {
                   "type":pEntityType,
                   "offset":this.offset,
                   "limit":this.limit
                  };
      lOptions=this.addParams(lOptions,pEntityType,pEntityId);
      debug('FiwareService: '+this.fiwareService+'(Tenant: '+lTenant+')');
      debug('Type: '+pEntityType);
      debug('EndPoint: '+lEndpoint+' (Broker URL: '+lBrokerURL+')');
      debug('Options: '+JSON.stringify(lOptions));
      return ngsiv2.listEntities(lEndpoint,lOptions,this.fiwareService,lFiwareServicePath,lExtended,pEntityType,pRequestId);
   }
   #createPromiseGeoMetadataEntitiesLDV1(pEntityType,pFiwareServicePath,pRadius)
   {
      return ngsildv1.listGeoMetadataEntities(this.fiwareService,pFiwareServicePath,pEntityType,pRadius);
   }
   #createPromiseGeoMetadataEntitiesV2(pEntityType,pFiwareServicePath,pRadius)
   {
      return ngsiv2.listGeoMetadataEntities(this.fiwareService,pFiwareServicePath,pEntityType,pRadius)
   }
   #getAttribListString()
   {
      var lOut=undefined;
      for(var a of this.attribs)
      {
         lOut=(lOut==undefined?a:lOut+','+a);
      }
      return lOut;
   }
}
