/*!
 * Copyright(c) 2023-2024 Rui Humberto Pereira
 * MIT Licensed
 *
 * This module defines constants and utilities for using data formats.
 */

exports.PARQUET='parquet';
exports.CSV='csv';
exports.JSON='json';

/**
 * Check if the format is known/valid
 */
exports.isValid = function(pFormat)
{
   return this.isParquet(pFormat) || this.isCSV(pFormat) || this.isJSON(pFormat);
}
exports.isParquet = function(pFormat)
{
   return isEqual(pFormat,this.PARQUET);
}
exports.isJSON = function(pFormat)
{
   return isEqual(pFormat,this.JSON);
}
exports.isCSV = function(pFormat)
{
   return isEqual(pFormat,this.CSV);
}
function isEqual(pFormatTest,pFormat)
{
  return pFormatTest!=undefined && pFormatTest.toLowerCase()==pFormat;
}
