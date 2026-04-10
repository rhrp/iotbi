/**
 * Provides similarity services based on fast-fuzzy
 *
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.topk.transformer.fast-fuzzy');
var Base  = require('./base.js');
var Score = require('../score.js');
var {Searcher} = require("fast-fuzzy");


const THRESHOLD_FASTFUZZY=0.85;

module.exports = class Transformer extends Base
{
  /**
   *
   */
   constructor() 
   {
      super('No Model','fast-fuzzy');
      debug(super.about());
   }
  /**
   * Generates the embeddings of all text segments in the table and save them in the cache
   *  
   */
   async embeddings()
   {
       debug('Fast Fuzzy does not use embeddings');
       return [];
   }
   similarityBestScore(pText,pSentences)
   {
       //debug('Sentences: '+pSentences.length);
       let searcher = new Searcher(pSentences);
       let lMatchs=searcher.search(pText,{returnMatchData: true,threshold:THRESHOLD_FASTFUZZY});
       let lBestScore=new Score(undefined,undefined);
       if(lMatchs.length>0)
       {
           for(let lMatch of lMatchs)
           {
               let lMatchPart=lMatch.item.substring(lMatch.match.index,lMatch.match.index+lMatch.match.length);
               debug('similarityText('+this.textPattern+') ('+lMatch.item+') match part value: "'+lMatchPart+'" (index='+lMatch.match.index+',length='+lMatch.match.length+')  Score='+lMatch.score);
               if(lMatch.score>lBestScore.similarity)
               {
                   lBestScore=new Score(lMatchPart,lMatch.score);
               }
            }
       }
       //debug('FastFuzzy Score: '+JSON.stringify(lBestScore));
       return lBestScore;
   }
}
