const axios = require('axios');
const { pool } = require('../db');

const WB_STATISTICS_API_BASE = 'https://statistics-api.wildberries.ru';

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

  if (row.marketplace && String(row.marketplace).toLowerCase() !== 'wb') {
    throw new Error(`Аккаунт mp_accounts.id=${mpAccountId} не относится к WB`);
  }

  if (row.is_active === false) {
    throw new Error(`Аккаунт mp_accounts.id=${mpAccountId} неактивен`);
  }

  return row;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isRateLimitError(error) {
  return Number(error?.response?.status || 0) === 429;
}

function getRetryDelayMs(error, attempt) {
  const retryAfterHeader = error?.response?.headers?.['retry-after'];
  const retryAfterSeconds = Number(retryAfterHeader);

  if (Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0) {
    return retryAfterSeconds * 1000;
  }

  const backoff = Math.min(3000 * Math.pow(2, attempt - 1), 30000);
  const jitter = Math.floor(Math.random() * 1000);
  return backoff + jitter;
}

async function fetchOrdersSince(apiToken, dateFrom) {
  const url = `${WB_STATISTICS_API_BASE}/api/v1/supplier/orders`;

  const resp = await axios.get(url, {
    headers: {
      Authorization: apiToken,
    },
    params: {
      dateFrom,
      flag: 1,
    },
    timeout: 120000,
  });

  if (Array.isArray(resp.data)) {
    return resp.data;
  }

  return [];
}

async function fetchOrdersSinceWithRetry(apiToken, dateFrom) {
  const maxAttempts = 7;
  let attempt = 0;

  while (attempt < maxAttempts) {
    attempt += 1;

    try {
      const rows = await fetchOrdersSince(apiToken, dateFrom);
      return {
        rows,
        attempts: attempt,
      };
    } catch (error) {
      if (!isRateLimitError(error)) {
        throw error;
      }

      const waitMs = getRetryDelayMs(error, attempt);
      console.warn(`[wbOrdersSync] 429 for dateFrom=${dateFrom}, attempt ${attempt}/${maxAttempts}, wait ${waitMs}ms`);
      await sleep(waitMs);
    }
  }

  throw new Error(`WB вернул 429 слишком много раз для dateFrom=${dateFrom}`);
}

function toDateOnly(value) {
  if (!value) return null;

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;

  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(date.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

function isOrderInRange(order, dateFrom, dateTo) {
  const orderDate =
    toDateOnly(order?.date) ||
    toDateOnly(order?.lastChangeDate);

  if (!orderDate) {
    return false;
  }

  return orderDate >= dateFrom && orderDate <= dateTo;
}

function toNumberOrNull(value) {
  if (value === undefined || value === null || value === '') {
    return null;
  }

  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

async function upsertOrderRaw({
  clientId,
  mpAccountId,
  order,
}) {
  const sourceOrderId =
    order.odid ||
    order.orderId ||
    order.srid ||
    order.gNumber ||
    null;

  const sourceRid =
    order.rid ||
    order.srid ||
    null;

  const orderDatetime =
    order.date ||
    order.lastChangeDate ||
    null;

  const eventDatetime =
    order.lastChangeDate ||
    order.date ||
    null;

  const article =
    order.supplierArticle ||
    order.article ||
    null;

  const barcode =
    order.barcode ||
    null;

  const priceRaw =
    toNumberOrNull(order.totalPrice);

  const convertedPriceRaw =
    toNumberOrNull(order.priceWithDisc);

  const finalPriceRaw =
    toNumberOrNull(order.finishedPrice);

  const convertedFinalPriceRaw =
    toNumberOrNull(
      order.finishedPriceWithDisc !== undefined
        ? order.finishedPriceWithDisc
        : order.priceWithDisc
    );

  const statusRaw =
    order.isCancel === true
      ? 'cancelled'
      : 'ordered';

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
      raw = EXCLUDED.raw,
      updated_at = NOW()
    `,
    [
      clientId,
      mpAccountId,
      sourceOrderId ? String(sourceOrderId) : null,
      sourceRid ? String(sourceRid) : null,
      order.nmId || null,
      order.chrtId || null,
      article,
      barcode,
      order.warehouseName || null,
      order.regionName || null,
      statusRaw,
      eventDatetime,
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

  const { rows, attempts } = await fetchOrdersSinceWithRetry(account.api_token, body.date_from);
  const filteredRows = rows.filter((order) => isOrderInRange(order, body.date_from, body.date_to));

  let upsertedRows = 0;

  for (const order of filteredRows) {
    await upsertOrderRaw({
      clientId,
      mpAccountId,
      order,
    });
    upsertedRows += 1;
  }

  const countRes = await pool.query(
    `
    SELECT COUNT(*)::int AS cnt
    FROM analytics.wb_orders_raw
    WHERE client_id = $1
      AND client_mp_account_id = $2
      AND COALESCE(order_datetime, event_datetime, created_at) >= $3::date
      AND COALESCE(order_datetime, event_datetime, created_at) < ($4::date + INTERVAL '1 day')
    `,
    [clientId, mpAccountId, body.date_from, body.date_to]
  );

  return {
    ok: true,
    message: 'WB orders sync completed',
    stats: {
      fetched_rows_total: rows.length,
      rows_in_requested_range: filteredRows.length,
      raw_rows_in_range: Number(countRes.rows[0]?.cnt || 0),
      upserted_rows: upsertedRows,
      total_attempts: attempts,
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