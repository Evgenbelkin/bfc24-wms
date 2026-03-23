const express = require('express');
const router = express.Router();

const { authRequired, requireRole } = require('../authMiddleware');
const {
  recalculateDistribution,
  getDistribution
} = require('../services/wbStockDistributionService');

router.post(
  '/wb/stock-distribution/recalculate',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const body = req.body || {};

      const clientId = Number(body.client_id);
      const mpAccountId = Number(body.mp_account_id);
      const barcode = String(body.barcode || '').trim();
      const totalQty = Number(body.total_qty);

      if (!Number.isInteger(clientId) || clientId <= 0) {
        return res.status(400).json({ error: 'client_id обязателен' });
      }

      if (!Number.isInteger(mpAccountId) || mpAccountId <= 0) {
        return res.status(400).json({ error: 'mp_account_id обязателен' });
      }

      if (!barcode) {
        return res.status(400).json({ error: 'barcode обязателен' });
      }

      if (!Number.isInteger(totalQty) || totalQty < 0) {
        return res.status(400).json({ error: 'total_qty должен быть целым числом >= 0' });
      }

      const result = await recalculateDistribution({
        clientId,
        mpAccountId,
        barcode,
        totalQty,
        triggerType: 'manual'
      });

      return res.json(result);
    } catch (err) {
      console.error('[POST /wb/stock-distribution/recalculate] error:', err);
      return res.status(500).json({
        error: 'Не удалось пересчитать распределение остатков',
        details: String(err.message || err)
      });
    }
  }
);

router.get(
  '/wb/stock-distribution',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const clientId = Number(req.query.client_id);
      const mpAccountId = Number(req.query.mp_account_id);
      const barcode = String(req.query.barcode || '').trim();

      if (!Number.isInteger(clientId) || clientId <= 0) {
        return res.status(400).json({ error: 'client_id обязателен' });
      }

      if (!Number.isInteger(mpAccountId) || mpAccountId <= 0) {
        return res.status(400).json({ error: 'mp_account_id обязателен' });
      }

      if (!barcode) {
        return res.status(400).json({ error: 'barcode обязателен' });
      }

      const result = await getDistribution({
        clientId,
        mpAccountId,
        barcode
      });

      return res.json(result);
    } catch (err) {
      console.error('[GET /wb/stock-distribution] error:', err);
      return res.status(500).json({
        error: 'Не удалось получить распределение остатков',
        details: String(err.message || err)
      });
    }
  }
);

module.exports = router;