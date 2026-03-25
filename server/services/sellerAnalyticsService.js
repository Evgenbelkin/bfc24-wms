const sellerAnalyticsQueryService = require('./sellerAnalyticsQueryService');

function isValidDateOnly(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
}

function normalizeFulfillmentModel(value) {
  const raw = String(value || 'all').trim().toLowerCase();

  if (['all', 'fbs', 'fbo'].includes(raw)) {
    return raw;
  }

  throw new Error('Некорректный fulfillment_model. Допустимо: all, fbs, fbo');
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

  if (reqUser.role === 'seller') {
    if (!reqUser.client_id) {
      throw new Error('У пользователя seller не заполнен client_id');
    }

    if (requestedClientId && Number(requestedClientId) !== Number(reqUser.client_id)) {
      throw new Error('Нет доступа к запрошенному client_id');
    }

    return Number(reqUser.client_id);
  }

  if (['owner', 'admin'].includes(reqUser.role)) {
    if (!requestedClientId) {
      throw new Error('Для owner/admin обязателен query-параметр client_id');
    }

    return normalizePositiveInt(requestedClientId, 'client_id');
  }

  throw new Error('Недостаточно прав доступа');
}

function validateBaseParams(reqUser, query) {
  const clientId = resolveClientAccess(reqUser, query.client_id);

  if (!query.mp_account_id) {
    throw new Error('Обязателен query-параметр mp_account_id');
  }

  const mpAccountId = normalizePositiveInt(query.mp_account_id, 'mp_account_id');

  if (!query.date_from || !isValidDateOnly(query.date_from)) {
    throw new Error('Обязателен query-параметр date_from в формате YYYY-MM-DD');
  }

  if (!query.date_to || !isValidDateOnly(query.date_to)) {
    throw new Error('Обязателен query-параметр date_to в формате YYYY-MM-DD');
  }

  if (query.date_from > query.date_to) {
    throw new Error('date_from не может быть больше date_to');
  }

  const fulfillmentModel = normalizeFulfillmentModel(query.fulfillment_model);

  return {
    clientId,
    mpAccountId,
    dateFrom: query.date_from,
    dateTo: query.date_to,
    fulfillmentModel,
  };
}

async function getOverview(reqUser, query) {
  const params = validateBaseParams(reqUser, query);

  const summary = await sellerAnalyticsQueryService.getOverviewSummary(params);

  return {
    ok: true,
    filters: {
      client_id: params.clientId,
      mp_account_id: params.mpAccountId,
      date_from: params.dateFrom,
      date_to: params.dateTo,
      fulfillment_model: params.fulfillmentModel,
    },
    summary: {
      orders_count: Number(summary.orders_count || 0),
      revenue_total: Number(summary.revenue_total || 0),
      average_order_value: Number(summary.average_order_value || 0),
      items_sold: Number(summary.items_sold || 0),
    },
  };
}

async function getSalesDaily(reqUser, query) {
  const params = validateBaseParams(reqUser, query);

  const rows = await sellerAnalyticsQueryService.getSalesDaily(params);

  return {
    ok: true,
    filters: {
      client_id: params.clientId,
      mp_account_id: params.mpAccountId,
      date_from: params.dateFrom,
      date_to: params.dateTo,
      fulfillment_model: params.fulfillmentModel,
    },
    rows: rows.map((row) => ({
      date: row.date,
      orders_count: Number(row.orders_count || 0),
      revenue_total: Number(row.revenue_total || 0),
      items_sold: Number(row.items_sold || 0),
    })),
  };
}

async function getTopSkus(reqUser, query) {
  const params = validateBaseParams(reqUser, query);

  let limit = 10;
  if (query.limit) {
    limit = normalizePositiveInt(query.limit, 'limit');
  }

  const rows = await sellerAnalyticsQueryService.getTopSkus({
    ...params,
    limit,
  });

  return {
    ok: true,
    filters: {
      client_id: params.clientId,
      mp_account_id: params.mpAccountId,
      date_from: params.dateFrom,
      date_to: params.dateTo,
      fulfillment_model: params.fulfillmentModel,
      limit,
    },
    rows: rows.map((row) => ({
      sku_id: row.sku_id ? Number(row.sku_id) : null,
      barcode: row.barcode || null,
      vendor_code: row.vendor_code || null,
      wb_vendor_code: row.wb_vendor_code || null,
      item_name: row.item_name || null,
      qty_sold: Number(row.qty_sold || 0),
      revenue_total: Number(row.revenue_total || 0),
      avg_price: Number(row.avg_price || 0),
      orders_count: Number(row.orders_count || 0),
    })),
  };
}

module.exports = {
  getOverview,
  getSalesDaily,
  getTopSkus,
};