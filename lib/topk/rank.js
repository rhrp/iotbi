/**
 * Rank - Provides ranking service
 *
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.topk.rank');
var Table = require('../model/tablemodel.js');
var schema=require('../schema.js');
var Score = require('./score.js');
var Result = require('./result.js');
var winkTokenize = require('wink-tokenizer');


const WINDOW_EXTENSION=0;
const RELEVANT_TOKENS=['word','number','emoticon'];

module.exports = class Rank {
  /**
   *
   */
   constructor(pTable,pSelectedAttribs,textPattern,searchField,transformer) 
   {
      this.table=pTable;
      this.selectedAttribs=pSelectedAttribs;
      this.textPattern=this.normalizeText(textPattern);	// Text to searched in searchField
      this.searchField=searchField;
      this.searchCols=searchField!=undefined?this.table.findColNamesByAttribs([searchField]):undefined;
      this.transformer=transformer;
      this.tokenizer = winkTokenize();
      this.textPatternTokens=textPattern!=undefined?this.tokenize(textPattern,true):[];
      this.windowSize=this.textPatternTokens.length==0?0:this.textPatternTokens.length+WINDOW_EXTENSION;
      debug('Window Size: '+this.windowSize+'  textPattern:'+textPattern+' searchField: '+searchField+' ('+JSON.stringify(this.searchCols)+')  Tokens:'+this.textPatternTokens.length);
   }
  /**
   * Get the tokens of text segment
   */
   tokenize(pText,pOnlyTheRelevant)
   {
       let lTokens = this.tokenizer.tokenize(pText, { type: 'word' });
       //debug('Tokens of \"'+pText+'\": '+JSON.stringify(lWords,null,2));
       let lWords=[];
       for(let lToken of lTokens)
       {
          if(RELEVANT_TOKENS.includes(lToken['tag']))
          {
             lWords.push(lToken);
          }
       }
       return lWords;
   }
  /**
   * Normalize the text to be converted to embeddings.
   * The char Euro  crashes the Transformar
   */
   normalizeText(text)
   {
       if(text!=undefined)
       {
         if(typeof text === 'string')
         {
            return text.replace(/\u20AC/g, '');
         }
         debug('Not String: '+text);
      } 
      return undefined;
   }
   normalizeSentences(sentences)
   {
       if(sentences==undefined)
       {
          return sentences;
       }
       let lOut=[];
       for(let lText of sentences)
       {
          lOut.push(this.normalizeText(lText));
       }
       return lOut;
   }
  /**
   * Generates the embeddings of all text segments in the table and save them in the cache
   *  
   */
   async makeEmbeddings()
   {
      // Find all sentences to be cached
      let lSentences=[this.textPattern];
      for(let lRow of this.table.getRows())
      {
        for(let lColName in this.table.getSchema())
        {
           if(this.searchCols==undefined || this.searchCols.includes(lColName))
           {
             if(this.table.isTypeString(lColName))
             {
               let lCell=lRow[lColName];
               if(lCell!=undefined && lCell.length>0)
               {
                  let lFieldSentences=this.overlappedTokens(this.normalizeText(lCell));
                  for(let lVal of lFieldSentences)
                  {
                      if(!lSentences.includes(lVal))
                      {
                          lSentences.push(lVal);
                      }
                  }
               }
             }
           }
        }
      }
      return await this.transformer.embeddings(lSentences);
   }
   /**
    *  Return the window of tokens at the pPos position
    */
   tokenWindow(pPos,pTokens)
   {
      let lOut='';
      let pLastPos=pPos+this.windowSize;
      pLastPos=pLastPos>=pTokens.length?pTokens.length:pLastPos;
      let lSize=0;
      for(let i=pPos;i<pLastPos;i++)
      {
         lOut=lOut+' '+pTokens[i]['value'];
         lSize=lSize+1;
      }
      return {
               text:lOut,   // Text 
               size:lSize   // Number of selected tokens
             };
   }
   async bestScore(pSentences)
   {
       return this.transformer.similarityBestScore(this.textPattern,this.normalizeSentences(pSentences));
   }
  /**
   * Return all text segments (tokens) of a sliding window 
   */
   overlappedTokens(pText)
   {
       if(pText==undefined || pText.length==0)
       {
          return [];
       }
       let n=0;
       let lTokens=this.tokenize(pText,true);
       if(lTokens.length==0)
       {
          return [];
       }
       let lWindow=this.tokenWindow(n,lTokens);
       // Get all overlaped tokens 
       //debug(pText);
       let lSentences=[];
       do
       {
          lSentences.push(lWindow['text'].trim());
          //debug('\t['+n+'] Size: '+lWindow['size']+' Window: '+lWindow['text']);
          n=n+1;
          lWindow=this.tokenWindow(n,lTokens);
       } while(lWindow['size']==this.windowSize);
       return lSentences;
   }
  /**
   * Return score of similarity of the text segment 
   * Splits the text in tokens and finds the best score of a sliding window 
   */
   async similarityText(pColName,pText)
   {
       if(pText==undefined || pText.length==0)
       {
          return new Score(undefined,undefined,pColName);
       }
       let lSentences=this.overlappedTokens(pText);
       if(lSentences.length==0)
       {
          return new Score(undefined,undefined,pColName);
       }
       let lScore=await this.bestScore(lSentences);
       lScore.setColName(pColName);
       //debug(`Calculate similarity of ColName: "${pColName}" with TextPattern "${this.textPattern}"`); 
       /**
       if(lScore.hasScore())
       {
          debug(`TextPattern "${this.textPattern}" ColName: "${pColName}" Sentences: ${JSON.stringify(lSentences)} Text "${pText}" ::  ${JSON.stringify(lScore)}`);
       }
       else
       {
          debug(`TextPattern "${this.textPattern}" ColName: "${pColName}" Sentences: ${JSON.stringify(lSentences)} Text "${pText}" ::  no Score`);
       }
       */
       return lScore;
   }   
  /**
   * Returns the best similarity of a Row
   */
   async similarityRow(pRow)
   {
       var lVals=[];
       let lBestScore=new Score(undefined,undefined);
       for(let lColName in pRow)
       {
          if(this.searchCols==undefined || this.searchCols.includes(lColName))
          {
            if(this.table.isTypeString(lColName))
            {
               let lCell=pRow[lColName];
               if(lCell!=undefined && lCell.length>0)
               {
                  let lTmp=this.normalizeText(lCell);
                  lVals.push(lTmp);
                  let lScore=await this.similarityText(lColName,lTmp);
                  if(lScore.isGreaterThan(lBestScore))
                  {
                      lBestScore=lScore;
                  }
               }
            }
          }
       }
       //debug('Best of row '+JSON.stringify(lBestScore));
       return lBestScore;    
   }
  /**
   *
   */
   async getIndexSortedBySimilarity()
   {
       let lScores={};
       let lTextPattern=this.textPattern.toLowerCase();
       for(let lRowIdx in this.table.getRows())
       {
           let lRow=this.table.getRows()[lRowIdx];
           lScores[lRowIdx]=await this.similarityRow(lRow);
           //debug(lRowIdx+'   score='+JSON.stringify(lScores[lRowIdx]));
       }
       let lSortedKeys = Object.keys(lScores).sort(function(a,b){return lScores[b].similarity-lScores[a].similarity})
       let lSortedScores=[];
       for(let lIdx in lSortedKeys)
       {
           let lKey=parseInt(lSortedKeys[lIdx]);
           let lScore=lScores[lKey];
           let lTopKSimilarity=0;
           let lNoSimilarity=0;
           if(lScore.hasScore())
           {
              //debug(lIdx+' key='+lKey+'   score='+JSON.stringify(lScore));
              lTopKSimilarity=lTopKSimilarity+1;
           }
           else
           {
              lNoSimilarity=lNoSimilarity+1;
           }
           lSortedScores.push({key:lKey,score:lScore});
       }
       return lSortedScores;
    }
    async sortByRelevance(topk,threshold,maxRowsUnScored) 
    {
       //Update embeddings cache
       await this.makeEmbeddings();
      
       let lSortedTable=new Table('Sort by '+this.textPattern,[],this.table.getSchema());
       let lScores=await this.getIndexSortedBySimilarity();
       let lScoresTable=[];
       let lIgnoredLowScore=0;
       let lAcceptedTopK=0;
       let lAcceptedLowScore=0;
       let lExcludedOutTopK=0;
       let lThresholdScore=threshold==undefined?0:threshold;
       for(var lScore of lScores)
       {
          let lOk=false;
          if(topk!=undefined && lScore.score.similarity>lThresholdScore && lAcceptedTopK<topk)
          {
             //debug('add '+lScore.key+'   score='+lScore.score.similarity);
             lAcceptedTopK=lAcceptedTopK+1;
             lOk=true;
          }
          else if(topk!=undefined && lScore.score.similarity>lThresholdScore)
          {
             lExcludedOutTopK=lExcludedOutTopK+1;
          }
          else if(topk!=undefined && lAcceptedLowScore<maxRowsUnScored)
          {
             lAcceptedLowScore=lAcceptedLowScore+1;
             lOk=true;
          }
          else
          {
             lIgnoredLowScore=lIgnoredLowScore+1;
          }
          if(lOk)
          {
             var lRow=this.table.getRowByPos(lScore.key);
             lSortedTable.rows.push(lRow);
             lScoresTable.push(lScore);
          }
       }
       debug('Initial table '+this.table.countRows()+' rows, topk='+topk+' maxRowsUnScored='+maxRowsUnScored+'  sorted/filtred table '+lSortedTable.countRows()+' rows (topk '+lAcceptedTopK+' and low score '+lAcceptedLowScore+'). IgnoredLowScore='+lIgnoredLowScore+' ExcludedOutTopK='+lExcludedOutTopK);
       let lMsg;
       if(lExcludedOutTopK>0)
       {
           lMsg='Sucess, but '+lExcludedOutTopK+' rows were excluded due to a long result. Try to apply geospatial restrictions';
       }
       else if(lAcceptedTopK==0 && lAcceptedLowScore>0)
       {
           lMsg='Sucess, but is possible that you search in the wrong properties. Try to search in other properties such as "description","title" or "aleternateNAme".';
       }
       else
       {
           lMsg='Sucess';
       }
       return new Result(lSortedTable,this.selectedAttribs,lScoresTable,lIgnoredLowScore,lExcludedOutTopK,lAcceptedLowScore,lMsg);
    }
    #makeEmptyScore(pNumberOfRows)
    {
       let lScores=[];
       for(let i=0;i<pNumberOfRows;i++)
       {
          lScores[i]={
                      key:i,
                      score:new Score(undefined,undefined)
                     };
       }
       return lScores;
    }
   /**
    * A utility method for generating an unordered result.
    */
    unsorted(topk,pMsg)
    {
        let lUnsortedTable=this.table;
        let lUnsortedScores=this.#makeEmptyScore(lUnsortedTable.countRows());
        let lExcludedOutTopK=0;

        if(topk!=undefined && lUnsortedTable.countRows()>topk)
        {
           lUnsortedTable=new Table(lUnsortedTable.name,lUnsortedTable.getRows().slice(0,topk),lUnsortedTable.getSchema());
           lUnsortedScores=lUnsortedScores.slice(0,topk)
           lExcludedOutTopK=lUnsortedTable.countRows()-topk;
           debug('Exclude '+lExcludedOutTopK+' rows');
        }
        debug('UnsortedTable::Rows: '+lUnsortedTable.countRows());
        return new Result(lUnsortedTable,this.selectedAttribs,lUnsortedScores,0,lExcludedOutTopK,0,pMsg);
    }
}
