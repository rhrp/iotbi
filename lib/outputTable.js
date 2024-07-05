/*!
 * API QuantumLeap 
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var formats = require('./formats.js');
var  { Parser, StreamParser } = require('@json2csv/plainjs');
var parquet = require('@dsnp/parquetjs');
const { v4: uuidv4 } = require('uuid');
var schema=require('./schema.js');
var cache=require('./cache.js');
var fs = require('fs');
var fsPromises = require('fs').promises; 
var debug = require('debug')('iotbi.outputTable');


exports.sendTable = sendTable;
exports.createPromiseSaveInCache = createPromiseSaveInCache;
/**
 * Response Table
 */
function sendTable(res,pTableAndSchema,pFormat,pEntityName)
{
   var lTable=pTableAndSchema[0];
   var lSchema=pTableAndSchema[1];
   //debug('Table: '+JSON.stringify(lTable,null,2));
   //debug('Schema: '+JSON.stringify(lSchema,null,2));
   var lCurrTime = new Date().getTime();
   var lTotalTime=lCurrTime-res.locals.iotbi_reqStarted;
   debug('Request ID: '+res.locals.iotbi_reqId+'  Total time: '+lTotalTime+'ms   Format:'+pFormat);
   try
   {
        if(formats.isCSV(pFormat))
        {
           var lOut=sendAsCSV(lTable);
           res.status(200).attachment(toAttachFilename(pEntityName)+'.'+formats.CSV).type('text/csv').send(lOut);
        }
        else if(formats.isParquet(pFormat))
        {
           if(lTable.length==0)
           {
              debug('Is not possible the generate a empty Parquet table');
              res.status(404).json({'description':'Empty table'});              
           }
           else
           {
             sendAsParquet(res,lTable,pEntityName,lSchema);
           }
        }
        else
        {
           var lOut=sendAsJSON(lTable);
           res.status(200).json(lOut);
        }
   }
   catch(ex)
   {
      debug(ex);
      var stack = ex.stack;
      console.log( stack );

      res.status(500).json({'description':ex});
   }
}
/**
 * Response Error
 */
exports.sendError = function(res,code,err,at_place)
{
   var lError={};
   lError.code=code;
   lError.description=JSON.stringify(err);  // cast to string
   //ToDO console.log(typeof err);

   if(at_place==undefined)
   {
     debug('sendError: Code='+code+'  Error='+JSON.stringify(err));
   }
   else
   {
     debug('sendError: Code='+code+'  Error='+JSON.stringify(err)+'  at place '+at_place);
     lError.atPlace=at_place;
   }
   res.status(code).json(lError);
}

function sendAsJSON(pTableValues)
{
    return pTableValues;
}
function sendAsCSV(pTableValues) 
{
   const opts = {};
   const parser = new Parser(opts);
   var lCsv = parser.parse(pTableValues);
   //console.log('----------------------');
   //console.log(lCsv);
   //console.log('----------------------');
   return lCsv;
}
/**
 * Infer the Schema 
 * First, compare with type in NGSI-V2 metadata and NGSI-LD Schemas
 * Then, infer by value
 */
function inferSchema(pTableValues,pSchemaHelper)
{
  var r=pTableValues[0];
  var lSchemaJson={};
  var n=0;
  for (var key in r)
  {
     var lValue=r[key];
      var lType=typeof lValue;
      if(pSchemaHelper!=undefined && pSchemaHelper[key]!=undefined)
      {
          lType=pSchemaHelper[key];
      }
      if(lType === 'string' || lType===schema.STRING)
      {
         //debug('Col: '+key+' inferSchema: Ok string ',key);
         lSchemaJson[key]=parquet.ParquetFieldBuilder.createStringField();
      }
      else if(lType === 'number' || lType===schema.DOUBLE)
      {
         //debug('Col: '+key+' inferSchema: Ok number',key);
         lSchemaJson[key]=parquet.ParquetFieldBuilder.createDoubleField();
      }
      else if(lType === 'boolean')
      {
         //debug('Col: '+key+' inferSchema: Ok boolean',key);
         lSchemaJson[key]=parquet.ParquetFieldBuilder.createBooleanField();
      }
      else if(lType === 'DateTime' || lType===schema.DATETIME)
      {
         //debug('Col: '+key+' inferSchema: Ok DateTime',key);
         lSchemaJson[key]=parquet.ParquetFieldBuilder.createTimestampField();
      }
      else if(lType===schema.TIMESTAMP || lType===schema.INTEGER)
      {
         //debug('Col: '+key+' inferSchema: Ok TimeStamp',key);
         lSchemaJson[key]=parquet.ParquetFieldBuilder.createIntField(64);
      }
      else if(lType === 'point')
      {
         debug('Col: '+key+' inferSchema: Ignore Point ',key);
      }
      else
      {
         debug('Col: '+key+' inferSchema: Ignore ',key+' (Type:'+lType+')   '+JSON.stringify(r));
      }
      n=n+1;
  }
  //debug('Schema with '+n+' cols:\n'+JSON.stringify(lSchemaJson,null,2));
  debug('Schema with '+n+' cols');
  return lSchemaJson;
}
function getRowJson(pSchema,pTableValues,pRowId)
{
  //debug('Schema: '+JSON.stringify(pSchema))
  var r=pTableValues[pRowId];
  var lRowJson={};
  for (var key in pSchema)
  {
      var lType=pSchema[key];
      //debug('Row:'+pRowId+' :: '+key+' =  '+r[key]+'    Type='+lType);
      if(lType==schema.DATETIME)
      {
        lValue=Date.parse(r[key]);
      }
      else
      {
        lValue=r[key];
      }
      lRowJson[key]=lValue;
  }
  return lRowJson;
}

function createPromiseSaveInCache(pFilename,pTableValues,pEntityName,pSchema,pFormat)
{
    if(formats.isParquet(pFormat))
    {
      return createPromiseSaveInCacheAsParquet(pFilename,pTableValues,pEntityName,pSchema);
    }
    else if(formats.isJSON(pFormat))
    {
      return createPromiseSaveInCacheAsJSON(pFilename,pTableValues,pEntityName,pSchema);
    }
    else if(formats.isCSV(pFormat))
    {
      return createPromiseSaveInCacheAsCSV(pFilename,pTableValues,pEntityName,pSchema);
    }
    else
    {
      debug('Invalid format: '+pFormat)
      return undefined;
    }
}
function createPromiseSaveInCacheAsParquet(pFilename,pTableValues,pEntityName,pSchema)
{
  var lSchemaParquetJson=inferSchema(pTableValues,pSchema);
  //debug('Infered Schema: '+JSON.stringify(lSchemaParquetJson,null,2));
  //debug('Real Schema: '+JSON.stringify(pSchema,null,2));
  //debug('Table: '+JSON.stringify(pTableValues,null,2));
  return new Promise(async function(resolve, reject) {
      var lSchema = new parquet.ParquetSchema(lSchemaParquetJson);
      var writer = await parquet.ParquetWriter.openFile(lSchema,pFilename);
      // append a few rows to the file
      for(var i=0;i<pTableValues.length;i++)
      {
          var lRow=getRowJson(pSchema,pTableValues,i);
          await writer.appendRow(lRow);
      }
      await writer.close();
      resolve(pFilename);
  });
}
function createPromiseSaveInCacheAsJSON(pFilename,pTableValues,pEntityName,pSchema)
{
   return fsPromises.writeFile(pFilename,JSON.stringify(pTableValues),{flag: "w",mode: 0o700 });
}
function createPromiseSaveInCacheAsCSV(pFilename,pTableValues,pEntityName,pSchema)
{
   const opts = {};
   const parser = new Parser(opts);
   var lCsv = parser.parse(pTableValues);
   debug(lCsv);
return fsPromises.writeFile(pFilename,lCsv,{flag: "w",mode: 0o700 });
}

function sendAsParquet(res,pTableValues,pEntityName,pSchema)
{
  var lFile;
  if(res.locals.iotbi_cache_key!=undefined)
  {
      lFile=cache.genCacheFile(res.locals.iotbi_cache_key,formats.PARQUET);
  }
  else
  {
      lFile='/tmp/iotbi/'+uuidv4()+'.parquet';
  }
  createPromiseSaveInCacheAsParquet(lFile,pTableValues,pEntityName,pSchema)
        .then(lFile => {
              //debug('Sending: '+lFile);
               cache.sendCachedFile(res,toAttachFilename(pEntityName)+'.parquet',lFile,'parquet')
         })
        .catch(err => {
           debug(JSON.stringify(err))
           res.status(500).json({'description':err});
        })
}
function toAttachFilename(pEntityName)
{
   if(pEntityName==undefined)
   {
       return 'entityData';
   }
   else
   {
       return pEntityName; 
   }
}
