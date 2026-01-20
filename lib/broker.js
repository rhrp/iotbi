/**
 * API for providing Matadata
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var configsys = require('./configsys.js');
var ngsildv1 = require('./ngsildv1.js');
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
   setQuery(query)
   {
      this.query=query;
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
       var lSchema=sdm.getEntitySchema(pEntityType);
       if(lSchema==undefined)
       {
          return 'The Schemas are not loaded or the entity '+pEntityType+' does not exists';
       }
       var lType=lSchema.getTypeByName(pGeoproperty);
       if(lType==undefined)
       {
          return 'The attribute '+pGeoproperty+' does not exists in '+pEntityType;
       }
       debug(pGeoproperty+' is type '+lType);
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
   *
   */
   createPromiseQueryNgsiEntities(pEntityType)
   {
       if(configsys.isVersionLDV1(this.fiwareService))
       {
           return this.#createPromiseQueryNgsiEntitiesLDV1(pEntityType)
       }
       else if(configsys.isVersionV2(this.fiwareService))
       {
           return this.#createPromiseQueryNgsiEntitiesV2(pEntityType);
       }
       else
       {
           debug('Invalid Broker:'+this.fiwareService);
           throw new Error('Invalid Broker: '+this.fiwareService); 
       }
   }
   createPromiseQueryTableEntities(pEntityType)
   {
       if(configsys.isVersionLDV1(this.fiwareService))
       {
           return this.#createPromiseQueryTableEntitiesLDV1(pEntityType)
       }
       else if(configsys.isVersionV2(this.fiwareService))
       {
           return this.#createPromiseQueryTableEntitiesV2(pEntityType);
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
   addParams(pOptions,pEntityType)
   {
      if(this.query!=undefined)
      {
         pOptions["q"]=this.query;
      }
      if(!configsys.isLocalGeoQueryOn())
      {
         //Geoquery at the broker
         if(this.georel!=undefined)
         {
           pOptions["georel"]=this.georel;
         }
         if(this.geometry!=undefined)
         {
           pOptions["geometry"]=this.geometry;
         }
         if(this.coordinates!=undefined)
         {
           pOptions["coordinates"]=this.coordinates;
         }
         if(this.geoproperty!=undefined)
         {
           pOptions["geoproperty"]=this.geoproperty;
         }
      }
      if(!configsys.isLocalSearchTextPatternOn() && this.textPattern!=undefined)
      {
          if(this.query!=undefined)
          {
              debug('WARNING: The query is already defined will be replcaced by a textPattern search!');
          }
          var lSchema=sdm.getEntitySchema(pEntityType);
          if(lSchema==undefined)
          {
              debug('The Schemas are not loaded or the entity '+pEntityType+' does not exists');
          }
          else
          {
             var lTmp=undefined;
             var lValidFields=lSchema.getAllNameFields();
             for(var lFieldName of lValidFields)
             {
                if(lSchema.isTypeStringByName(lFieldName) && lFieldName!=lSchema.getNgsiType())
                {
                   lTmp=(lTmp==undefined?'':lTmp+'|')+lFieldName+'~="(?i)'+this.textPattern+'"';
                   debug('Add to query ::  '+lFieldName+' of type='+lSchema.getTypeByName(lFieldName))
                }
                if(lTmp!=undefined)
                {
                   debug('Replace the query '+lTmp)
                   pOptions["q"]=lTmp;
                }
             }
          }
      }
      if(this.countSelectedAttributes()>0)
      {
         pOptions["attrs"]=this.#getAttribListString();
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

      debug(JSON.stringify(pOptions,null,2));
      return pOptions;
   }
  /**
   * Query for Entities in NGSI-LD
   */
   #createPromiseQueryNgsiEntitiesLDV1(pEntityType)
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
      lOptions=this.addParams(lOptions,pEntityType);
      debug('FiwareService: '+this.fiwareService);
      debug('Type: '+pEntityType);
      debug('Broker URL: '+lBrokerURL);
      debug('Options: '+JSON.stringify(lOptions));

      return lConnection.ld.queryEntities(lOptions);
   }
  /**
   * Query for Entities as a Table
   */
   #createPromiseQueryTableEntitiesLDV1(pEntityType)
   {
      var lEndpoint=configsys.getBrokerEndpoint(this.fiwareService);
      var lBrokerURL=configsys.getBrokerURL(this.fiwareService);
      var lTenant=configsys.getBrokerTenant(this.fiwareService);
      var lFiwareServicePath=undefined;
      var lExtended=false;
      var lTableNameJoin=undefined;
      var lJoinAttribute=undefined;
      var lOptions = {
                   "type":pEntityType,
                   "offset":this.offset,
                   "limit":this.limit
                  };
      lOptions=this.addParams(lOptions,pEntityType);
      debug('FiwareService: '+this.fiwareService+'(Tenant: '+lTenant+')');
      debug('Type: '+pEntityType);
      debug('EndPoint: '+lEndpoint+' (Broker URL: '+lBrokerURL+')');
      debug('Options: '+JSON.stringify(lOptions));

      return ngsildv1.listEntities(lEndpoint,lOptions,this.fiwareService,lFiwareServicePath,lExtended,lTableNameJoin,lJoinAttribute);
   }
   #createPromiseQueryNgsiEntitiesV2(pEntityType)
   {
      var NGSI = require('ngsijs');
      var lConnection = new NGSI.Connection(lOrionURL);
      return lConnection.v2.listEntities({"service":pFiwareService,"type":pEntityType,"limit":this.limit})
   }
   #createPromiseQueryTableEntitiesV2(pEntityType)
   {
      var NGSI = require('ngsijs');
      var lConnection = new NGSI.Connection(lOrionURL);
      return lConnection.v2.listEntities({"service":pFiwareService,"type":pEntityType,"limit":this.limit})
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
