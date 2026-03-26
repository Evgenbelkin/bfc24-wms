const express = require('express');
const router = express.Router();

const { authRequired, requireRole } = require('../authMiddleware');
const wbOrdersNormalizeService = require('../services/wbOrdersNormalizeService');

router.use(authRequired);
router.use(requireRole(['owner', 'admin']));

router.post('/wb-orders-normalize', async (req, res) => {
  try {
    const result = await wbOrdersNormalizeService.normalizeWbOrders(req.user, req.body);
    return res.json(result);
  } catch (error) {
    console.error('[POST /wb/wb-orders-normalize] error:', error);
    return res.status(400).json({
      ok: false,
      error: error.message || 'Ошибка нормализации WB orders',
    });
  }
});

module.exports = router;