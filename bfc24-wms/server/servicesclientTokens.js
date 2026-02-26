// services/clientTokens.js

/**
 * Временно: хардкод токенов клиентов.
 * Потом уберём в БД.
 */
const TOKENS = {
  // client_id: "WB_SUPPLIER_TOKEN"
  1: process.env.WB_TOKEN_CLIENT_1,
  2: process.env.WB_TOKEN_CLIENT_2,
};

function getWbTokenForClient(clientId) {
  return TOKENS[clientId] || null;
}

module.exports = { getWbTokenForClient };
