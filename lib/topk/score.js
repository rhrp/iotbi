/*!
 * Copyright(c) 2023-2025 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.topk.score');


const NO_SIMILARITY=-1;


module.exports = class Score
{
  constructor(sentence,similarity=NO_SIMILARITY,colName=undefined)
  {
      this.colName=colName;
      this.sentence=sentence;
      this.similarity=similarity;
  }
  setColName(pColName)
  {
     this.colName=pColName;
  }
  hasScore()
  {
     return this.score!=undefined && this.score>NO_SIMILARITY;
  }
  isGreaterThan(score)
  {
      return score!=undefined && this.similarity>score.similarity;
  }
  get()
  {
      return {
               colName:this.colName,
               token:this.sentence,
               similarity:this.similarity
             };
  }
}
