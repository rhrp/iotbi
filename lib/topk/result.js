/*!
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.topk.result');


module.exports = class Result
{
  constructor(table,selectedAttrs,scores,ignoredLowScore,excludedOutTopK,acceptedLowScore,msg)
  {
      this.table=table;
      this.selectedAttrs=selectedAttrs;
      this.scores=scores;
      this.ignoredLowScore=ignoredLowScore;
      this.excludedOutTopK=excludedOutTopK;
      this.acceptedLowScore=acceptedLowScore;
      this.message=msg;
  }
  getTable(pOnlySelectedCols)
  {
      return pOnlySelectedCols?this.table.selectByAttribs(this.selectedAttrs):this.table;
  }
  debugResult()
  {
      debug('Result:');
      debug('  Selected Attribs: '+this.selectedAttrs);
      debug('  Selected Cols: '+this.table.findColNamesByAttribs(this.selectedAttrs));
      debug('  Cols: '+this.table.getColsList());
      debug(`  Rows: ${this.table.countRows()}`);
      debug(`  Message: ${this.message}`);
      debug(`  Ignored due to Low Score; ${this.ignoredLowScore}`);
      debug(`  Excluded Out of TopK: ${this.excludedOutTopK}`);
      debug(`  Accepted but Low Scored:  ${this.acceptedLowScore}`);
  }
}
