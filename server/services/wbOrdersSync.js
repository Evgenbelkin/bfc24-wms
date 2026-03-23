// services/wbOrdersSync.js
const axios = require('axios');
const { pool } = require('../db');

// Базовый URL WB, по умолчанию marketplace-api
const WB_API_BASE =
  process.env.WB_API_BASE || 'https://marketplace-api.wildberries.ru';

/**
 * Синхронизация заказов WB в таблицу public.mp_wb_orders
 * mpAccountId — id в mp_accounts (у тебя для "Мой кабинет WB" это 2)
 */
async function syncWbOrdersForAccount(mpAccountId) {
  // 1. Берём api_token WB для этого mp_account_id
  const tokenRes = await pool.query(
    `
      SELECT api_token
      FROM mp_accounts
      WHERE id = $1
        AND marketplace = 'wb'
        AND is_active = true
    `,
    [mpAccountId]
  );

  if (!tokenRes.rowCount) {
    throw new Error(
      `WB api_token not found for mp_account_id=${mpAccountId}`
    );
  }

  const apiToken = tokenRes.rows[0].api_token;

  // 2. Запрашиваем новые заказы у WB за последние 2 дня
  const TWO_DAYS_MS = 2 * 24 * 60 * 60 * 1000;
  const dateFrom = new Date(Date.now() - TWO_DAYS_MS).toISOString();

  // ВАЖНО: используем marketplace-api
  const url = `${WB_API_BASE}/api/v3/orders/new`;

  const resp = await axios.get(url, {
    headers: {
      Authorization: apiToken, // сюда WB-токен из mp_accounts
    },
    params: {
      dateFrom,
    },
    timeout: 30000,
  });

  const data = resp.data;

  // В норме WB отдаёт { orders: [...] }
  let orders = [];
  if (data && Array.isArray(data.orders)) {
    orders = data.orders;
  } else if (Array.isArray(data)) {
    // fallback: если вдруг тело — просто массив
    orders = data;
  } else {
    console.log('[syncWbOrders] Unexpected WB response structure:', data);
    orders = [];
  }

  console.log(
    `[syncWbOrders] получено заказов от WB для mp_account_id=${mpAccountId}:`,
    orders.length
  );

  // 3. Кладём/обновляем заказы в public.mp_wb_orders
  for (const o of orders) {
    if (!o) {
      console.log('[syncWbOrders] skip empty order payload:', o);
      continue;
    }

    // У этого эндпоинта заказ — это оbject с полем id
    // На всякий случай поддерживаем и orderId, если WB что-то поменяет
    const wbOrderId = o.orderId || o.id;

    if (!wbOrderId) {
      console.log('[syncWbOrders] skip payload without id/orderId:', o);
      continue;
    }

    // createdAt — дата создания заказа у WB
    const createdAt =
      o.createdAt ||
      o.createdDate ||
      new Date().toISOString();

    // статус приводим к строке (на случай, если в таблице status TEXT)
    const status =
      o.status !== undefined && o.status !== null
        ? String(o.status)
        : 'new';

    // barcode: WB отдаёт массив skus, берём первый
    const barcode = Array.isArray(o.skus) && o.skus.length > 0
      ? o.skus[0]
      : (o.barcode || null);

    // warehouse_name: пробуем взять из warehouseName, иначе из offices
    const officesArray = Array.isArray(o.offices) ? o.offices : [];
    const warehouseName =
      o.warehouseName ||
      officesArray[1] ||
      officesArray[0] ||
      null;

    const regionName = o.regionName || null;

    await pool.query(
      `
      INSERT INTO public.mp_wb_orders (
        client_mp_account_id,
        wb_order_id,
        nm_id,
        chrt_id,
        article,
        barcode,
        warehouse_id,
        warehouse_name,
        region_name,
        status,
        created_at,
        raw
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12
      )
      ON CONFLICT (client_mp_account_id, wb_order_id)
      DO UPDATE SET
        nm_id          = EXCLUDED.nm_id,
        chrt_id        = EXCLUDED.chrt_id,
        article        = EXCLUDED.article,
        barcode        = EXCLUDED.barcode,
        warehouse_id   = EXCLUDED.warehouse_id,
        warehouse_name = EXCLUDED.warehouse_name,
        region_name    = EXCLUDED.region_name,
        status         = EXCLUDED.status,
        created_at     = EXCLUDED.created_at,
        raw            = EXCLUDED.raw;
      `,
      [
        mpAccountId,                // client_mp_account_id
        wbOrderId,                  // wb_order_id
        o.nmId || null,
        o.chrtId || null,
        o.article || null,
        barcode,
        o.warehouseId || null,
        warehouseName,
        regionName,
        status,
        createdAt,
        JSON.stringify(o),          // raw: всегда JSON, не NULL
      ]
    );
  }

  return { imported: orders.length };
}

module.exports = { syncWbOrdersForAccount };
