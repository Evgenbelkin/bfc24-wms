const express = require('express');
const router = express.Router();

const { authRequired, requireRole } = require('../authMiddleware');
const wbSalesSyncService = require('../services/wbSalesSyncService');

router.use(authRequired);
router.use(requireRole(['owner', 'admin']));

/**
 * POST /wb-sales-sync
 * body:
 * {
 *   client_id,
 *   mp_account_id,
 *   date_from,
 *   date_to
 * }
 */
router.post('/wb-sales-sync', async (req, res) => {
  try {
    const result = await wbSalesSyncService.syncWbSales(req.user, req.body);
    return res.json(result);
  } catch (error) {
    console.error('[POST /wb-sales-sync] error:', error);
    return res.status(400).json({
      ok: false,
      error: error.message || 'Ошибка синхронизации WB sales',
    });
  }
});

module.exports = router;