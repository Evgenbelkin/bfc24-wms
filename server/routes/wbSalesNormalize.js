const express = require('express');
const router = express.Router();

const { authRequired, requireRole } = require('../authMiddleware');
const wbSalesNormalizeService = require('../services/wbSalesNormalizeService');

router.use(authRequired);
router.use(requireRole(['owner', 'admin']));

router.post('/wb-sales-normalize', async (req, res) => {
  try {
    const result = await wbSalesNormalizeService.normalizeWbSales(req.user, req.body);
    return res.json(result);
  } catch (error) {
    console.error('[POST /wb/wb-sales-normalize] error:', error);
    return res.status(400).json({
      ok: false,
      error: error.message || 'Ошибка нормализации WB sales',
    });
  }
});

module.exports = router;