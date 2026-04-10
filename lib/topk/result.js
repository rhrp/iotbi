/*!
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.topk.result');


module.exports = class Result
{
  constructor(table,scores,ignoredLowScore,excludedOutTopK,msg)
  {
      this.table=table;
      this.scores=scores;
      this.ignoredLowScore=ignoredLowScore;
      this.excludedOutTopK=excludedOutTopK;
      this.message=msg;
  }
}
