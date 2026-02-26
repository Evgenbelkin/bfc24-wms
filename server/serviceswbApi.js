// services/wbApi.js
const axios = require("axios");

/**
 * Получить список карточек WB “списком”.
 *
 * clientWbToken — токен поставщика WB (тот, что в ЛК поставщика)
 * params        — объект с параметрами запроса WB (limit, filter и т.п.)
 */
async function fetchWbCards(clientWbToken, params = {}) {
  const url = "https://marketplace-api.wildberries.ru/content/v2/get/cards/list";

  const res = await axios.post(url, params, {
    headers: {
      Authorization: clientWbToken,
      "Content-Type": "application/json",
    },
    timeout: 30000,
  });

  // У WB структура может отличаться, но почти всегда есть cards
  if (Array.isArray(res.data.cards)) {
    return res.data.cards;
  }

  // fallback: если cards нет, но тело — массив
  if (Array.isArray(res.data)) {
    return res.data;
  }

  // если пришло что-то иное — отладим отдельно
  console.log("Unexpected WB response:", res.data);
  return [];
}

module.exports = {
  fetchWbCards,
};
