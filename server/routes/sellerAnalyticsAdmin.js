const express = require('express');
const router = express.Router();

const { authRequired, requireRole } = require('../authMiddleware');
const sellerAnalyticsIngestService = require('../services/sellerAnalyticsIngestService');
const sellerAnalyticsRefreshService = require('../services/sellerAnalyticsRefreshService');

router.use(authRequired);
router.use(requireRole(['owner', 'admin']));

router.post('/sync-orders', async (req, res) => {
  try {
    const result = await sellerAnalyticsIngestService.syncOrdersToAnalytics(req.user, req.body);
    return res.json(result);
  } catch (error) {
    console.error('[POST /seller-analytics-admin/sync-orders] error:', error);
    return res.status(400).json({
      ok: false,
      error: error.message || 'Ошибка синхронизации аналитики',
    });
  }
});

router.post('/refresh-period', async (req, res) => {
  try {
    const result = await sellerAnalyticsRefreshService.refreshAnalyticsForPeriod(req.user, req.body);
    return res.json(result);
  } catch (error) {
    console.error('[POST /seller-analytics-admin/refresh-period] error:', error);
    return res.status(400).json({
      ok: false,
      error: error.message || 'Ошибка обновления аналитики за период',
    });
  }
});

module.exports = router;