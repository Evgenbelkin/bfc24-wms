const express = require('express');
const router = express.Router();

const sellerAnalyticsService = require('../services/sellerAnalyticsService');
const { authRequired, requireRole } = require('../authMiddleware');

router.use(authRequired);
router.use(requireRole(['owner', 'admin', 'seller']));

router.get('/overview', async (req, res) => {
  try {
    const data = await sellerAnalyticsService.getOverview(req.user, req.query);
    return res.json(data);
  } catch (error) {
    console.error('[GET /seller-analytics/overview] error:', error);
    return res.status(400).json({
      ok: false,
      error: error.message || 'Ошибка получения overview аналитики',
    });
  }
});

router.get('/sales-daily', async (req, res) => {
  try {
    const data = await sellerAnalyticsService.getSalesDaily(req.user, req.query);
    return res.json(data);
  } catch (error) {
    console.error('[GET /seller-analytics/sales-daily] error:', error);
    return res.status(400).json({
      ok: false,
      error: error.message || 'Ошибка получения daily analytics',
    });
  }
});

router.get('/top-skus', async (req, res) => {
  try {
    const data = await sellerAnalyticsService.getTopSkus(req.user, req.query);
    return res.json(data);
  } catch (error) {
    console.error('[GET /seller-analytics/top-skus] error:', error);
    return res.status(400).json({
      ok: false,
      error: error.message || 'Ошибка получения top skus',
    });
  }
});

module.exports = router;