const express = require('express');
const router = express.Router();

const { authRequired, requireRole } = require('../authMiddleware');
const { recalcDistributionFromStock } = require('../services/wbStockDistributionFromStockService');

router.post(
  '/wb/stock-distribution/recalc-from-stock',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const body = req.body || {};
      const clientId = Number(body.client_id);
      const mpAccountId = Number(body.mp_account_id);
      const barcode = String(body.barcode || '').trim();

      if (!Number.isInteger(clientId) || clientId <= 0) {
        return res.status(400).json({ error: 'client_id обязателен' });
      }
      if (!Number.isInteger(mpAccountId) || mpAccountId <= 0) {
        return res.status(400).json({ error: 'mp_account_id обязателен' });
      }
      if (!barcode) {
        return res.status(400).json({ error: 'barcode обязателен' });
      }

      const result = await recalcDistributionFromStock({ clientId, mpAccountId, barcode });

      return res.json(result);
    } catch (err) {
      console.error('[POST /wb/stock-distribution/recalc-from-stock] error:', err);
      return res.status(500).json({
        error: 'Не удалось пересчитать распределение из stock',
        details: String(err.message || err)
      });
    }
  }
);

module.exports = router;