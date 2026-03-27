const wbSalesSyncService = require('./wbSalesSyncService');
const wbSalesNormalizeService = require('./wbSalesNormalizeService');
const wbOrdersSyncService = require('./wbOrdersSyncService');
const wbOrdersNormalizeService = require('./wbOrdersNormalizeService');

async function refreshAnalyticsForPeriod(reqUser, body) {
  const salesSync = await wbSalesSyncService.syncWbSales(reqUser, body);
  const salesNormalize = await wbSalesNormalizeService.normalizeWbSales(reqUser, body);
  const ordersSync = await wbOrdersSyncService.syncWbOrders(reqUser, body);
  const ordersNormalize = await wbOrdersNormalizeService.normalizeWbOrders(reqUser, body);

  return {
    ok: true,
    message: 'Аналитика за период обновлена',
    filters: {
      client_id: Number(body.client_id),
      mp_account_id: Number(body.mp_account_id),
      date_from: body.date_from,
      date_to: body.date_to,
    },
    steps: {
      sales_sync: salesSync,
      sales_normalize: salesNormalize,
      orders_sync: ordersSync,
      orders_normalize: ordersNormalize,
    },
  };
}

module.exports = {
  refreshAnalyticsForPeriod,
};