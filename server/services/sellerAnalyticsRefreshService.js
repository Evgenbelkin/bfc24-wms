const wbSalesSyncService = require('./wbSalesSyncService');
const wbSalesNormalizeService = require('./wbSalesNormalizeService');
const wbOrdersSyncService = require('./wbOrdersSyncService');
const wbOrdersNormalizeService = require('./wbOrdersNormalizeService');

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

function validateBody(body) {
  if (!body) {
    throw new Error('Не передано тело запроса');
  }

  if (!body.client_id) {
    throw new Error('Обязателен параметр client_id');
  }

  if (!body.mp_account_id) {
    throw new Error('Обязателен параметр mp_account_id');
  }

  if (!body.date_from || !isValidDateOnly(body.date_from)) {
    throw new Error('Обязателен параметр date_from в формате YYYY-MM-DD');
  }

  if (!body.date_to || !isValidDateOnly(body.date_to)) {
    throw new Error('Обязателен параметр date_to в формате YYYY-MM-DD');
  }

  if (body.date_from > body.date_to) {
    throw new Error('date_from не может быть больше date_to');
  }

  return {
    client_id: normalizePositiveInt(body.client_id, 'client_id'),
    mp_account_id: normalizePositiveInt(body.mp_account_id, 'mp_account_id'),
    date_from: body.date_from,
    date_to: body.date_to,
  };
}

async function refreshAnalyticsForPeriod(reqUser, body) {
  const validated = validateBody(body);

  const startedAt = new Date();
  const steps = {};

  steps.sales_sync = await wbSalesSyncService.syncWbSales(reqUser, validated);
  steps.sales_normalize = await wbSalesNormalizeService.normalizeWbSales(reqUser, validated);
  steps.orders_sync = await wbOrdersSyncService.syncWbOrders(reqUser, validated);
  steps.orders_normalize = await wbOrdersNormalizeService.normalizeWbOrders(reqUser, validated);

  const finishedAt = new Date();

  return {
    ok: true,
    message: 'Аналитика за период обновлена',
    filters: validated,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    steps,
  };
}

module.exports = {
  refreshAnalyticsForPeriod,
};