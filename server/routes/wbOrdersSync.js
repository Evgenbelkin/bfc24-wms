const express = require('express');
const router = express.Router();

const { authRequired, requireRole } = require('../authMiddleware');
const wbOrdersSyncService = require('../services/wbOrdersSyncService');

router.use(authRequired);
router.use(requireRole(['owner', 'admin']));

router.post('/wb-orders-sync', async (req, res) => {
  try {
    const result = await wbOrdersSyncService.syncWbOrders(req.user, req.body);
    return res.json(result);
  } catch (error) {
    console.error('[POST /wb/wb-orders-sync] error:', error);
    return res.status(400).json({
      ok: false,
      error: error.message || 'Ошибка синхронизации WB orders',
    });
  }
});

module.exports = router;