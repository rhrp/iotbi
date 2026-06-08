/**
 * NgsiQueryParser
 * Copyright(c) 2023-2026 Rui Humberto Pereira
 * MIT Licensed
 *
 */
var debug = require('debug')('iotbi.utils.NgsiQueryParser');

module.exports = class NgsiQueryParser {
  constructor() {
    this.operators = {
      ';': { precedence: 2, associativity: 'left' }, // AND
      '|': { precedence: 1, associativity: 'left' }, // OR
      '==': { precedence: 3, associativity: 'left' },
      '!=': { precedence: 3, associativity: 'left' },
      '>': { precedence: 3, associativity: 'left' },
      '>=': { precedence: 3, associativity: 'left' },
      '<': { precedence: 3, associativity: 'left' },
      '<=': { precedence: 3, associativity: 'left' }
    };
    debug('NgsiQueryParser');
  }

  // ========== MÉTODO PRINCIPAL PARA EXTRAIR PARÂMETROS ==========
  /**
   * Extrai todos os parâmetros (variáveis) de uma expressão
   * @param {string} expression - Expressão a ser analisada
   * @returns {Array} Array com nomes únicos dos parâmetros
   */
  extrairParametros(expression) {
    try {
      const tokens = this.tokenize(expression);
      const ast = this.parseExpression(tokens);
      return this.extrairParametrosDaAST(ast);
    } catch (error) {
      debug(error)
      return [];
    }
  }

  /**
   * Extrai parâmetros de uma AST já processada
   * @param {object} ast - Árvore sintática
   * @returns {Array} Array com nomes únicos dos parâmetros
   */
  extrairParametrosDaAST(ast) {
    const parametros = new Set();
    this._percorrerAST(ast, parametros);
    return Array.from(parametros).sort();
  }

  /**
   * Percorre recursivamente a AST coletando parâmetros
   * @param {object} node - Nó da AST
   * @param {Set} parametros - Set para armazenar parâmetros únicos
   */
  _percorrerAST(node, parametros) {
    if (!node) return;

    switch (node.type) {
      case 'operand':
        // Verifica se é um nome de variável válido
        if (node.value && /^[a-zA-Z_]\w*$/.test(node.value)) {
          parametros.add(node.value);
        }
        break;

      case 'comparison':
        // Adiciona a variável do lado esquerdo da comparação
        if (node.left && /^[a-zA-Z_]\w*$/.test(node.left)) {
          parametros.add(node.left);
        }
        // Verifica se o lado direito é uma variável)
        if (node.right && /^[a-zA-Z_]\w*$/.test(node.right)) {
          parametros.add(node.right);
        }
        break;

      case 'expression':
        this._percorrerAST(node.left, parametros);
        this._percorrerAST(node.right, parametros);
        break;
    }
  }

  // ========== MÉTODO ALTERNATIVO: Extração via Regex ==========
  /**
   * Extrai parâmetros usando expressão regular (mais rápido, menos preciso)
   * @param {string} expression - Expressão a ser analisada
   * @returns {Array} Array com nomes únicos dos parâmetros
   */
  extrairParametrosRegex(expression) {
    const parametros = new Set();
    
    // Regex para capturar variáveis:
    // 1. Em comparações: "variavel>", "variavel==", "variavel<=", etc.
    // 2. Como operandos simples: "variavel" em contextos booleanos
    // 3. Ignorar números e strings
    
    const regexVariaveis = /\b([a-zA-Z_]\w*)\b(?=\s*(?:[;|()]|$|==|!=|>|>=|<|<=))/g;
    const matches = expression.match(regexVariaveis) || [];
    
    // Filtrar palavras reservadas e números
    const palavrasReservadas = new Set(['true', 'false', 'null', 'undefined']);
    
    matches.forEach(param => {
      if (!palavrasReservadas.has(param.toLowerCase()) && isNaN(param)) {
        parametros.add(param);
      }
    });
    
    return Array.from(parametros).sort();
  }

  // ========== MÉTODO COMPLETO: Validar e Extrair ==========
  /**
   * Valida a expressão e extrai parâmetros em uma única chamada
   * @param {string} expression - Expressão a ser analisada
   * @returns {object} Objeto com validação e parâmetros
   */
  analisarExpressao(expression) {
    try {
      const tokens = this.tokenize(expression);
      const ast = this.parseExpression(tokens);
      const parametros = this.extrairParametrosDaAST(ast);
      
      return {
        valida: true,
        ast: ast,
        parametros: parametros,
        parametrosCount: parametros.length,
        expressao: expression
      };
    } catch (error) {
      return {
        valida: false,
        erro: error.message,
        parametros: [],
        parametrosCount: 0,
        expressao: expression
      };
    }
  }

  validate(expression) {
    try {
      const tokens = this.tokenize(expression);
      const ast = this.parseExpression(tokens);
      const parametros = this.extrairParametrosDaAST(ast);
      
      return {
        valid: true,
        ast: ast,
        parameters: parametros,
        expression: expression
      };
    } catch (error) {
      return {
        valid: false,
        error: error.message,
        parameters: [],
        expression: expression
      };
    }
  }

  tokenize(expression) {
    const tokens = [];
    let current = '';
    
    for (let i = 0; i < expression.length; i++) {
      const char = expression[i];
      
      if (/\s/.test(char)) {
        if (current) {
          tokens.push(current);
          current = '';
        }
        continue;
      }
      
      if (i + 1 < expression.length) {
        const twoCharOp = char + expression[i + 1];
        if (twoCharOp === '==' || twoCharOp === '!=' || 
            twoCharOp === '>=' || twoCharOp === '<=') {
          if (current) {
            tokens.push(current);
            current = '';
          }
          tokens.push(twoCharOp);
          i++;
          continue;
        }
      }
      
      if (char === ';' || char === '|' || char === '(' || char === ')' || 
          char === '>' || char === '<' || char === '=') {
        if (current) {
          tokens.push(current);
          current = '';
        }
        tokens.push(char);
        continue;
      }
      
      current += char;
    }
    
    if (current) {
      tokens.push(current);
    }
    
    return tokens;
  }

  parseExpression(tokens) {
    const output = [];
    const operators = [];
    
    for (let i = 0; i < tokens.length; i++) {
      const token = tokens[i];
      
      if (this.isOperand(token)) {
        output.push({ type: 'operand', value: token });
      }
      else if (this.isOperator(token)) {
        while (operators.length > 0 && 
               operators[operators.length - 1] !== '(' &&
               this.hasPrecedence(operators[operators.length - 1], token)) {
          output.push({ type: 'operator', value: operators.pop() });
        }
        operators.push(token);
      }
      else if (token === '(') {
        operators.push(token);
      }
      else if (token === ')') {
        while (operators.length > 0 && operators[operators.length - 1] !== '(') {
          output.push({ type: 'operator', value: operators.pop() });
        }
        if (operators.length === 0 || operators[operators.length - 1] !== '(') {
          throw new Error('Parênteses desbalanceados');
        }
        operators.pop();
      } else {
        throw new Error(`Token inválido: ${token}`);
      }
    }
    
    while (operators.length > 0) {
      const op = operators.pop();
      if (op === '(' || op === ')') {
        throw new Error('Parênteses desbalanceados');
      }
      output.push({ type: 'operator', value: op });
    }
    
    return this.buildAST(output);
  }

  buildAST(rpn) {
    const stack = [];
    
    for (const item of rpn) {
      if (item.type === 'operand') {
        const comparison = this.parseComparison(item.value);
        if (comparison) {
          stack.push(comparison);
        } else {
          stack.push({
            type: 'operand',
            value: item.value
          });
        }
      } else if (item.type === 'operator') {
        if (stack.length < 2) {
          throw new Error('Expressão inválida: operador sem operandos suficientes');
        }
        const right = stack.pop();
        const left = stack.pop();
        
        stack.push({
          type: 'expression',
          operator: item.value,
          left: left,
          right: right
        });
      }
    }
    
    if (stack.length !== 1) {
      throw new Error('Expressão inválida');
    }
    
    return stack[0];
  }

  parseComparison(operand) {
    const comparisonRegex = /^([a-zA-Z_]\w*)(==|!=|>|>=|<|<=)(-?\d+(?:\.\d+)?|\w+)$/;
    const match = operand.match(comparisonRegex);
    
    if (match) {
      return {
        type: 'comparison',
        left: match[1],
        operator: match[2],
        right: match[3]
      };
    }
    
    return null;
  }

  isOperand(token) {
    return !this.isOperator(token) && token !== '(' && token !== ')' && 
           token !== '==' && token !== '!=' && token !== '>=' && token !== '<=';
  }

  isOperator(token) {
    return token in this.operators || token === '>' || token === '<' || 
           token === '=' || token === ';' || token === '|';
  }

  hasPrecedence(op1, op2) {
    const prec1 = this.operators[op1]?.precedence || 0;
    const prec2 = this.operators[op2]?.precedence || 0;
    return prec1 >= prec2;
  }

  evaluate(ast, context = {}) {
    if (ast.type === 'operand') {
      return Boolean(context[ast.value]);
    } else if (ast.type === 'comparison') {
      const leftValue = context[ast.left];
      const rightValue = isNaN(ast.right) ? context[ast.right] : Number(ast.right);
      
      switch (ast.operator) {
        case '==': return leftValue == rightValue;
        case '!=': return leftValue != rightValue;
        case '>': return leftValue > rightValue;
        case '>=': return leftValue >= rightValue;
        case '<': return leftValue < rightValue;
        case '<=': return leftValue <= rightValue;
        default: return false;
      }
    } else if (ast.type === 'expression') {
      const leftResult = this.evaluate(ast.left, context);
      const rightResult = this.evaluate(ast.right, context);
      
      switch (ast.operator) {
        case ';': return leftResult && rightResult;
        case '|': return leftResult || rightResult;
        default: return false;
      }
    }
    
    return false;
  }
}
