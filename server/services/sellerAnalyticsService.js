const sellerAnalyticsQueryService = require('./sellerAnalyticsQueryService');

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

  return {
    clientId,
    mpAccountId,
    dateFrom: query.date_from,
    dateTo: query.date_to,
    fulfillmentModel: 'all',
  };
}

async function getOverview(reqUser, query) {
  const params = validateBaseParams(reqUser, query);
  const summary = await sellerAnalyticsQueryService.getOverviewSummary(params);

  return {
    ok: true,
    source: 'orders=mp_wb_orders; sales=analytics.wb_sales_normalized',
    filters: {
      client_id: params.clientId,
      mp_account_id: params.mpAccountId,
      date_from: params.dateFrom,
      date_to: params.dateTo,
    },
    summary: {
      orders: {
        count: Number(summary.orders_count || 0),
        qty: Number(summary.orders_qty || 0),
        amount: Number(summary.orders_amount || 0),
      },
      sales: {
        count: Number(summary.sales_count || 0),
        qty: Number(summary.sales_qty || 0),
        amount: Number(summary.sales_amount || 0),
      },
      realization: {
        amount: Number(summary.realization_amount || 0),
      },
      returns: {
        count: Number(summary.returns_count || 0),
        amount: Number(summary.returns_amount || 0),
      },
      buyout_percent: Number(summary.buyout_percent || 0),
    },
  };
}

async function getSalesDaily(reqUser, query) {
  const params = validateBaseParams(reqUser, query);
  const rows = await sellerAnalyticsQueryService.getSalesDaily(params);

  return {
    ok: true,
    source: 'orders=mp_wb_orders; sales=analytics.wb_sales_normalized',
    filters: {
      client_id: params.clientId,
      mp_account_id: params.mpAccountId,
      date_from: params.dateFrom,
      date_to: params.dateTo,
    },
    rows: rows.map((row) => ({
      date: row.date,
      orders: {
        count: Number(row.orders_count || 0),
        qty: Number(row.orders_qty || 0),
        amount: Number(row.orders_amount || 0),
      },
      sales: {
        count: Number(row.sales_count || 0),
        qty: Number(row.sales_qty || 0),
        amount: Number(row.sales_amount || 0),
      },
      realization: {
        amount: Number(row.realization_amount || 0),
      },
      returns: {
        count: Number(row.returns_count || 0),
        amount: Number(row.returns_amount || 0),
      },
      buyout_percent: Number(row.buyout_percent || 0),
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
    source: 'analytics.wb_sales_normalized',
    filters: {
      client_id: params.clientId,
      mp_account_id: params.mpAccountId,
      date_from: params.dateFrom,
      date_to: params.dateTo,
      limit,
    },
    rows: rows.map((row) => ({
      sku_id: row.sku_id ? Number(row.sku_id) : null,
      barcode: row.barcode || null,
      vendor_code: row.vendor_code || null,
      wb_vendor_code: row.wb_vendor_code || null,
      item_name: row.item_name || null,
      sales_qty: Number(row.sales_qty || 0),
      sales_amount: Number(row.sales_amount || 0),
      realization_amount: Number(row.realization_amount || 0),
      avg_price: Number(row.avg_price || 0),
      sales_count: Number(row.sales_count || 0),
      returns_count: Number(row.returns_count || 0),
      returns_amount: Number(row.returns_amount || 0),
    })),
  };
}

module.exports = {
  getOverview,
  getSalesDaily,
  getTopSkus,
};