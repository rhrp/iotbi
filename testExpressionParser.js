NgsiQueryParser=require('./lib/utils/NgsiQueryParser.js')

// ========== FUNÇÕES DE CONVENIÊNCIA ==========

/**
 * Extrai parâmetros de uma expressão (wrapper simples)
 * @param {string} expression - Expressão a ser analisada
 * @returns {Array} Array com nomes dos parâmetros
 */
function extrairParametros(expression) {
  const parser = new NgsiQueryParser();
  return parser.extrairParametros(expression);
}

/**
 * Analisa expressão completa e retorna tudo
 * @param {string} expression - Expressão a ser analisada
 * @returns {object} Resultado completo da análise
 */
function analisarExpressaoCompleta(expression) {
  const parser = new NgsiQueryParser();
  return parser.analisarExpressao(expression);
}

// ========== EXEMPLOS DE USO ==========

console.log("=== TESTES DE EXTRAÇÃO DE PARÂMETROS ===\n");

const testes = [
  "(a>0|b<=3);c==4",
  "idade>=18;saldo>0|vip==1",
  "(x>10|y<5);(z==20|w!=30);activo==true",
  "status==1;nivel>2",
  "a>b;c==d|e!=f",
  "somenteVariavel",
  "(param1>100);param2==50|param3<=30",
  "complexo;teste|(outro==1);final>0"
];

const parser = new NgsiQueryParser();

testes.forEach((expr, index) => {
  console.log(`Teste ${index + 1}: "${expr}"`);
  
  // Método 1: Análise completa
  const analise = parser.analisarExpressao(expr);
  console.log(`  Válida: ${analise.valida}`);
  console.log(`  Parâmetros (AST): [${analise.parametros.join(', ')}]`);
  console.log(`  Quantidade: ${analise.parametrosCount}`);
  
  // Método 2: Apenas extração
  const params = parser.extrairParametros(expr);
  console.log(`  Parâmetros (extração): [${params.join(', ')}]`);
  
  // Método 3: Extração rápida com Regex
  const paramsRegex = parser.extrairParametrosRegex(expr);
  console.log(`  Parâmetros (Regex): [${paramsRegex.join(', ')}]`);
  
  console.log();
});

// ========== EXEMPLO PRÁTICO ==========
console.log("=== EXEMPLO PRÁTICO ===\n");

const expressaoUtilizador = "(idade>=18|responsavel==true);saldo>=50.00;activo==1";

console.log(`Expressão: ${expressaoUtilizador}`);

// Analise completa
const resultado = analisarExpressaoCompleta(expressaoUtilizador);

if (resultado.valida) {
  console.log("✓ Expressão válida!");
  console.log(`✓ Parâmetros necessários: [${resultado.parametros.join(', ')}]`);
  console.log(`✓ Total de parâmetros: ${resultado.parametrosCount}`);
  
  // Exemplo de contexto com valores
  const contexto = {
    idade: 25,
    responsavel: false,
    saldo: 100.50,
    activo: 1
  };
  
  console.log("\nContexto de exemplo:");
  console.log(contexto);
  
  const avaliacao = parser.evaluate(resultado.ast, contexto);
  console.log(`\nAvaliação da expressão: ${avaliacao}`);
  
  // Verificar se todos os parâmetros estão presentes no contexto
  const parametrosFaltantes = resultado.parametros.filter(p => !(p in contexto));
  if (parametrosFaltantes.length > 0) {
    console.log(`⚠️  Parâmetros em falta no contexto: [${parametrosFaltantes.join(', ')}]`);
  } else {
    console.log("✓ Todos os parâmetros presentes no contexto");
  }
} else {
  console.log(`✗ Expressão inválida: ${resultado.erro}`);
}

// ========== FUNÇÃO DE UTILIDADE ==========
/**
 * Cria um objeto de contexto vazio com base nos parâmetros da expressão
 * @param {string} expression - Expressão a ser analisada
 * @returns {object} Objeto com parâmetros inicializados como null
 */
function criarContextoVazio(expression) {
  const params = extrairParametros(expression);
  const contexto = {};
  params.forEach(param => {
    contexto[param] = null;
  });
  return contexto;
}

// Exemplo de uso
console.log("\n=== CRIANDO CONTEXTO VAZIO ===");
const contextoVazio = criarContextoVazio(expressaoUtilizador);
console.log("Contexto vazio:", contextoVazio);

