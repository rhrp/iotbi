/**
 * Memory-based cache for embedings
 *
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.topk.cache.inmemory');

//const cache={}; 

module.exports = class InMemoryCache 
{
  /**
   *
   */
   constructor() 
   {
      this.cache={};
      debug(`Cache inited size=${this.size()}`);
   }
   put(key,obj)
   {
      this.cache[key]=obj;
   }
   get(key)
   {
      return this.cache[key];
   }
   size()
   {
      return Object.keys(this.cache).length;
   }
}
