var { createHash } = require('crypto');

lAppKey='mcp_user'

let lMcpToken=createHash('sha256').update(lAppKey).digest('hex');

console.log(`${lAppKey} => ${lMcpToken}`);
