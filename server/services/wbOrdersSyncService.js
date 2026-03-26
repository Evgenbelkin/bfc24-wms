const axios = require('axios');
const { pool } = require('../db');

const WB_MARKETPLACE_API_BASE =
  process.env.WB_API_BASE || 'https://marketplace-api.wildberries.ru';

function isValidDateOnly(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
}

function normalizePositiveInt(value, fieldName) {
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) {
    throw new Error(`Поле ${fieldName} должно быть положительным целым числом`);
  }
  return num;
}

function resolveClientAccess(reqUser, requestedClientId) {
  if (!reqUser) {
    throw new Error('Пользователь не авторизован');
  }

  if (!['owner', 'admin'].includes(reqUser.role)) {
    throw new Error('Недостаточно прав доступа');
  }

  if (!requestedClientId) {
    throw new Error('Обязателен параметр client_id');
  }

  return normalizePositiveInt(requestedClientId, 'client_id');
}

async function getMpAccount(clientId, mpAccountId) {
  const res = await pool.query(
    `
    SELECT id, wms_client_id, api_token, marketplace, is_active
    FROM public.mp_accounts
    WHERE id = $1
    LIMIT 1
    `,
    [mpAccountId]
  );

  if (!res.rowCount) {
    throw new Error(`Аккаунт mp_accounts.id=${mpAccountId} не найден`);
  }

  const row = res.rows[0];

  if (Number(row.wms_client_id) !== Number(clientId)) {
    throw new Error(`Аккаунт mp_accounts.id=${mpAccountId} не принадлежит client_id=${clientId}`);
  }

  if (!row.api_token) {
    throw new Error(`У mp_accounts.id=${mpAccountId} пустой api_token`);
  }

  return row;
}

function buildDateList(dateFrom, dateTo) {
  const days = [];
  const current = new Date(`${dateFrom}T00:00:00Z`);
  const end = new Date(`${dateTo}T00:00:00Z`);

  while (current <= end) {
    const yyyy = current.getUTCFullYear();
    const mm = String(current.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(current.getUTCDate()).padStart(2, '0');
    days.push(`${yyyy}-${mm}-${dd}`);
    current.setUTCDate(current.getUTCDate() + 1);
  }

  return days;
}

async function fetchOrdersForDate(apiToken, dateStr) {
  const dateFrom = `${dateStr}T00:00:00Z`;
  const url = `${WB_MARKETPLACE_API_BASE}/api/v3/orders/new`;

  const resp = await axios.get(url, {
    headers: {
      Authorization: apiToken,
    },
    params: {
      dateFrom,
    },
    timeout: 60000,
  });

  const data = resp.data;

  if (data && Array.isArray(data.orders)) {
    return data.orders;
  }

  if (Array.isArray(data)) {
    return data;
  }

  return [];
}

async function upsertOrderRaw({
  clientId,
  mpAccountId,
  order,
}) {
  const sourceOrderId = order.orderId || order.id || null;
  const sourceRid = order.rid || null;

  const skus = Array.isArray(order.skus) ? order.skus : [];
  const barcode = skus.length ? String(skus[0]) : (order.barcode || null);

  const offices = Array.isArray(order.offices) ? order.offices : [];
  const warehouseName =
    order.warehouseName ||
    offices[1] ||
    offices[0] ||
    null;

  const orderDatetime =
    order.createdAt ||
    order.createdDate ||
    null;

  const statusRaw =
    order.status !== undefined && order.status !== null
      ? String(order.status)
      : (order.deliveryType ? String(order.deliveryType) : null);

  const priceRaw =
    order.price !== undefined && order.price !== null
      ? Number(order.price)
      : null;

  const convertedPriceRaw =
    order.convertedPrice !== undefined && order.convertedPrice !== null
      ? Number(order.convertedPrice)
      : null;

  const finalPriceRaw =
    order.finalPrice !== undefined && order.finalPrice !== null
      ? Number(order.finalPrice)
      : null;

  const convertedFinalPriceRaw =
    order.convertedFinalPrice !== undefined && order.convertedFinalPrice !== null
      ? Number(order.convertedFinalPrice)
      : null;

  await pool.query(
    `
    INSERT INTO analytics.wb_orders_raw (
      client_id,
      client_mp_account_id,
      report_type,
      source_order_id,
      source_rid,
      source_nm_id,
      source_chrt_id,
      article,
      barcode,
      warehouse_name,
      region_name,
      status_raw,
      event_datetime,
      order_datetime,
      price_raw,
      converted_price_raw,
      final_price_raw,
      converted_final_price_raw,
      raw
    )
    VALUES (
      $1,$2,'orders',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18
    )
    ON CONFLICT (client_mp_account_id, COALESCE(source_order_id, ''), COALESCE(source_rid, ''))
    DO UPDATE SET
      source_nm_id = EXCLUDED.source_nm_id,
      source_chrt_id = EXCLUDED.source_chrt_id,
      article = EXCLUDED.article,
      barcode = EXCLUDED.barcode,
      warehouse_name = EXCLUDED.warehouse_name,
      region_name = EXCLUDED.region_name,
      status_raw = EXCLUDED.status_raw,
      event_datetime = EXCLUDED.event_datetime,
      order_datetime = EXCLUDED.order_datetime,
      price_raw = EXCLUDED.price_raw,
      converted_price_raw = EXCLUDED.converted_price_raw,
      final_price_raw = EXCLUDED.final_price_raw,
      converted_final_price_raw = EXCLUDED.converted_final_price_raw,
      raw = EXCLUDED.raw
    `,
    [
      clientId,
      mpAccountId,
      sourceOrderId ? String(sourceOrderId) : null,
      sourceRid ? String(sourceRid) : null,
      order.nmId || null,
      order.chrtId || null,
      order.article || null,
      barcode,
      warehouseName,
      order.regionName || null,
      statusRaw,
      orderDatetime,
      orderDatetime,
      priceRaw,
      convertedPriceRaw,
      finalPriceRaw,
      convertedFinalPriceRaw,
      JSON.stringify(order),
    ]
  );
}

async function syncWbOrders(reqUser, body) {
  const clientId = resolveClientAccess(reqUser, body.client_id);

  if (!body.mp_account_id) {
    throw new Error('Обязателен параметр mp_account_id');
  }
  const mpAccountId = normalizePositiveInt(body.mp_account_id, 'mp_account_id');

  if (!body.date_from || !isValidDateOnly(body.date_from)) {
    throw new Error('Обязателен параметр date_from в формате YYYY-MM-DD');
  }

  if (!body.date_to || !isValidDateOnly(body.date_to)) {
    throw new Error('Обязателен параметр date_to в формате YYYY-MM-DD');
  }

  if (body.date_from > body.date_to) {
    throw new Error('date_from не может быть больше date_to');
  }

  const account = await getMpAccount(clientId, mpAccountId);
  const days = buildDateList(body.date_from, body.date_to);

  let fetchedDays = 0;
  let fetchedRows = 0;

  for (const day of days) {
    const orders = await fetchOrdersForDate(account.api_token, day);
    fetchedDays += 1;
    fetchedRows += orders.length;

    for (const order of orders) {
      await upsertOrderRaw({
        clientId,
        mpAccountId,
        order,
      });
    }
  }

  const countRes = await pool.query(
    `
    SELECT COUNT(*)::int AS cnt
    FROM analytics.wb_orders_raw
    WHERE client_id = $1
      AND client_mp_account_id = $2
      AND order_datetime >= $3::date
      AND order_datetime < ($4::date + INTERVAL '1 day')
    `,
    [clientId, mpAccountId, body.date_from, body.date_to]
  );

  return {
    ok: true,
    message: 'WB orders sync completed',
    stats: {
      fetched_days: fetchedDays,
      fetched_rows_total: fetchedRows,
      raw_rows_in_range: Number(countRes.rows[0]?.cnt || 0),
    },
    filters: {
      client_id: clientId,
      mp_account_id: mpAccountId,
      date_from: body.date_from,
      date_to: body.date_to,
    },
  };
}

module.exports = {
  syncWbOrders,
};