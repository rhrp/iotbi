/**
 * Provides a super class for a transformer
 *
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.topk.transformer.base');


const THRESHOLD_FASTFUZZY=0.85;

module.exports = class Transformer
{
    constructor(model='no model',type='no type')
    {
      this.model=model;
      this.type=type;
    }
    about()
    {
         return `Transformer: ${this.type}   Model: ${this.model}`;
    }
}

