// services/wbSync.js
const { pool } = require("../db");
const { fetchWbCards } = require("./wbApi");
const { normalizeWbItem } = require("./wbNormalize");

/**
 * Синк карточек WB в masterdata.items для одного клиента.
 *
 * clientId      — ID клиента в твоей БД
 * clientWbToken — токен поставщика WB для этого клиента
 */
async function syncWbItemsForClient(clientId, clientWbToken) {
  // 1. Тянем карточки
  const cards = await fetchWbCards(clientWbToken, {
    // здесь подставишь реальные параметры WB (например, без фильтров, limit и т.п.)
    settings: {
      cursor: {
        limit: 100,
      },
    },
  });

  console.log(`Получено карточек от WB для client_id=${clientId}:`, cards.length);

  // 2. Пробегаем по всем карточкам
  for (const card of cards) {
    const norm = normalizeWbItem(card, clientId);

    if (!norm.wb_nm_id) {
      // без nmID смысла нет, пропускаем
      console.log("Пропуск карточки без nmID:", card);
      continue;
    }

    // 3. UPSERT в masterdata.items
    await pool.query(
      `
      INSERT INTO masterdata.items (
        client_id,
        wb_nm_id,
        vendor_code,
        brand,
        item_name,
        is_active
      ) VALUES ($1, $2, $3, $4, $5, $6)
      ON CONFLICT (client_id, wb_nm_id)
      DO UPDATE SET
        vendor_code = COALESCE(NULLIF(EXCLUDED.vendor_code, ''), masterdata.items.vendor_code),
        brand       = COALESCE(NULLIF(EXCLUDED.brand, ''), masterdata.items.brand),
        item_name   = COALESCE(NULLIF(EXCLUDED.item_name, ''), masterdata.items.item_name),
        is_active   = EXCLUDED.is_active
      ;
      `,
      [
        norm.client_id,
        norm.wb_nm_id,
        norm.vendor_code,
        norm.brand,
        norm.item_name,
        norm.is_active,
      ]
    );
  }

  console.log(`Синк карточек WB для client_id=${clientId} завершён`);
}

module.exports = {
  syncWbItemsForClient,
};
