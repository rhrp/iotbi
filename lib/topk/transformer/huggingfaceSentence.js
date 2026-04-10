/**
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.topk.transformer.huggingface');
var Base  = require('./base.js');
var Score = require('../score.js');
//var crypto = require('crypto');

const THRESHOLD_TRANSFORMER=0.80;

/**
      'sentence-transformers/all-MiniLM-L6-v2';  // Very bad
      'Xenova/paraphrase-multilingual-MiniLM-L12-v2';
      'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2';
      'Xenova/bge-m3';                           // Results similar to Xenova/paraphrase-multilingual-MiniLM-L12-v2
      'intfloat/multilingual-e5-base';           // Bad results 
*/
module.exports = class Transformer extends Base
{
  constructor(pModel='sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')
  {
      super(pModel,'@huggingface/transformers');
      debug(super.about());
      this.cache={};
  }
  async getExtractor()
  {
      const { pipeline,env } = await import('@huggingface/transformers');

      // 1. Bloqueia downloads remotos (caso o modelo n  o esteja carregado, tenta descarregar e colocar em:
      //    node_modules/@xenova/transformers/.cache
      //    node_modules/@huggingface/transformers/.cache/
      env.allowRemoteModels = false;
      // 2. Define onde os ficheiros do modelo estao localizados. 
      env.localModelPath = './huggingface_models'; 
      // 3. Inicializar o pipeline de extra    o de caracter  sticas (embeddings)
      // Create a pipeline for feature extraction, using the full-precision model (fp32)
      return pipeline('feature-extraction',this.model,
                      {
                        dtype:'fp32',  // Avoids a system warning.  Options: fp32, fp16, q8, int8, uint8, q4, bnb4, q4f16
                        device: 'cpu'  // Options cpu, cuda
                      });
  }
 /**
  * Creates a promise of get the similarity of two sentences. 
  * Direct implementation without using cache
  */
  async similarityTwoSentencesPromise(sentence1,sentente2) 
  {
     const {dot} = await import('@huggingface/transformers');
  
     var sentences = [sentence1,sentente2];

     // 4. Gerar embeddings para as frases
     // 'pooling' e 'normalize' garantem que os vetores estejam prontos para comparaçã
     extractor=await this.getExtractor();
     var output = await extractor(sentences, { pooling: 'mean', normalize: true });

     // 3. Extrair os vectores (data)
     var embedding1 = output[0].data;
     var embedding2 = output[1].data;

     // 4. Calcular a similaridade (com vetores normalizados, dot product = cosine similarity)
     var similarity = dot(embedding1, embedding2);

     //debug(`Similarity: ${similarity.toFixed(4)}`);
     return similarity;
  }
 /**
  *
  */
  async #similarity(text,sentence)
  {
      let textEmbedding=this.getCachedEmbedding(text);
      let sentenceEmbedding=this.getCachedEmbedding(sentence);
      if(textEmbedding==undefined)
      {
         debug('Not cached: "'+text+'"');
         return undefined;
      }
      if(sentenceEmbedding==undefined)
      {
         debug('Not cached: "'+sentence+'"');
         return undefined;
      }
      let {dot} = await import('@huggingface/transformers');
      var similarity = dot(textEmbedding,sentenceEmbedding);   
      return new Score(sentence,similarity); 

  }
 /**
  * Note: text and sentences embeddings must be previously cached
  */
  async similarityBestScore(text,sentences)
  {
      let lBestScore=undefined;
      for(let sentence of sentences)
      {
         let lScore=await this.#similarity(text,sentence);
         if(lScore.similarity>THRESHOLD_TRANSFORMER && (lBestScore==undefined || lScore.isGreaterThan(lBestScore)))
         {
            // To the moment, the best score upper a Thereshold
            lBestScore=lScore;
         }
      }
      return lBestScore==undefined?new Score(undefined,undefined):lBestScore;
  }

 /**
  *  Generate non cached embeddings and save them in the cache
  */ 
  async embeddingsNotCached(missingSentences)
  {
     // 'pooling' e 'normalize' garantem que os vetores estejam prontos para compara    
     let extractor=await this.getExtractor();
     let output = await extractor(missingSentences, { pooling: 'mean', normalize: true });
     // 3. Extrair os vectores (data)
     for(let i=0;i<missingSentences.length;i++)
     {
       let embedding = output[i].data;
       this.addCache(missingSentences[i],embedding);
     }
  }
 /**
  * Get the sentences' embeddings.
  */
  async embeddings(sentences) 
  {
     debug('Sentences: '+sentences.length);
     var nonCachedSentences=[];
     for(let sentence of sentences)
     {
        if(this.getCachedEmbedding(sentence)==undefined && !nonCachedSentences.includes(sentence))
        {
            //Not cached and not duplicated
            nonCachedSentences.push(sentence);
        }
     }
     // 4. Gerar embeddings para as frases
     debug('Sentences: '+sentences.length+' not in cache '+nonCachedSentences.length);
     if(nonCachedSentences.length>0)
     {
         debug('Sentences: '+sentences.length+' not in cache '+nonCachedSentences.length);
         //Ensure that all embeddings are cached
         await this.embeddingsNotCached(nonCachedSentences);
     }
     else
     {
         debug('Sentences: '+sentences.length+'. All of them are cached!'); 
     }
     let embeddings=[];
     for(let sentence of sentences)
     {
        embeddings.push(this.getCachedEmbedding(sentence));
     }
     debug(`Cache size:${Object.keys(this.cache).length}`);
     return embeddings;
  }

  /**
    Creates a promise for a set of sentences
   */
  async similarityAll(text,sentences) 
  {
     var lSentences=[text];
     lSentences=lSentences.concat(sentences);
     await this.embeddings(lSentences)
     var lSimilarity=[];
     for(let sentence of sentences)
     {
         var similarity = await this.#similarity(text,sentence);   
         lSimilarity.push(similarity); 
     }
     return lSimilarity;
  }
  addCache(sentence,embedding)
  {
     this.cache[sentence]=embedding;
  }
  getCachedEmbedding(sentence)
  {
     return this.cache[sentence];
  }
}


