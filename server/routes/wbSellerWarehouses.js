const express = require('express');
const router = express.Router();

const { authRequired, requireRole } = require('../authMiddleware');

const {
  syncWbSellerWarehouses,
  listClientWbWarehouses,
  updateClientWbWarehouseSettings
} = require('../services/wbSellerWarehousesService');

router.post(
  '/wb/warehouses/sync',
  authRequired,
  requireRole(['owner', 'admin', 'seller']),
  async (req, res) => {
    try {
      const body = req.body || {};
      const clientId = Number(body.client_id);
      const mpAccountId = Number(body.mp_account_id);

      if (!Number.isInteger(clientId) || clientId <= 0) {
        return res.status(400).json({ error: 'client_id обязателен' });
      }

      if (!Number.isInteger(mpAccountId) || mpAccountId <= 0) {
        return res.status(400).json({ error: 'mp_account_id обязателен' });
      }

      // Для seller дополнительно страхуемся:
      // он может синхронизировать только свой client_id
      if (req.user?.role === 'seller') {
        const sellerClientId = Number(req.user?.client_id);
        if (!sellerClientId || sellerClientId !== clientId) {
          return res.status(403).json({
            error: 'Нет доступа к этому client_id'
          });
        }
      }

      const result = await syncWbSellerWarehouses({
        clientId,
        mpAccountId,
        userId: req.user?.id || null
      });

      return res.json(result);
    } catch (err) {
      console.error('[POST /wb/warehouses/sync] error:', err);
      return res.status(500).json({
        error: 'Не удалось синхронизировать склады WB',
        details: String(err.message || err)
      });
    }
  }
);

router.get(
  '/wb/warehouses',
  authRequired,
  requireRole(['owner', 'admin', 'seller']),
  async (req, res) => {
    try {
      const clientId = Number(req.query.client_id);
      const mpAccountId = req.query.mp_account_id ? Number(req.query.mp_account_id) : null;

      if (!Number.isInteger(clientId) || clientId <= 0) {
        return res.status(400).json({ error: 'client_id обязателен' });
      }

      if (req.user?.role === 'seller') {
        const sellerClientId = Number(req.user?.client_id);
        if (!sellerClientId || sellerClientId !== clientId) {
          return res.status(403).json({
            error: 'Нет доступа к этому client_id'
          });
        }
      }

      if (req.query.mp_account_id && (!Number.isInteger(mpAccountId) || mpAccountId <= 0)) {
        return res.status(400).json({ error: 'mp_account_id некорректен' });
      }

      const rows = await listClientWbWarehouses({
        clientId,
        mpAccountId
      });

      return res.json({
        items: rows,
        count: rows.length
      });
    } catch (err) {
      console.error('[GET /wb/warehouses] error:', err);
      return res.status(500).json({
        error: 'Не удалось получить склады WB',
        details: String(err.message || err)
      });
    }
  }
);

router.get(
  '/wb/warehouses/settings',
  authRequired,
  requireRole(['owner', 'admin', 'seller']),
  async (req, res) => {
    try {
      const clientId = Number(req.query.client_id);
      const mpAccountId = Number(req.query.mp_account_id);

      if (!Number.isInteger(clientId) || clientId <= 0) {
        return res.status(400).json({ error: 'client_id обязателен' });
      }

      if (req.user?.role === 'seller') {
        const sellerClientId = Number(req.user?.client_id);
        if (!sellerClientId || sellerClientId !== clientId) {
          return res.status(403).json({
            error: 'Нет доступа к этому client_id'
          });
        }
      }

      if (!Number.isInteger(mpAccountId) || mpAccountId <= 0) {
        return res.status(400).json({ error: 'mp_account_id обязателен' });
      }

      const rows = await listClientWbWarehouses({
        clientId,
        mpAccountId
      });

      return res.json({
        success: true,
        client_id: clientId,
        mp_account_id: mpAccountId,
        items: rows.map(r => ({
          wb_warehouse_id: r.wb_warehouse_id || null,
          warehouse_code: r.warehouse_code,
          warehouse_name: r.warehouse_name,
          is_active: r.is_active,
          is_enabled_for_distribution: r.is_enabled_for_distribution,
          weight: Number(r.weight),
          last_synced_at: r.last_synced_at,
          updated_at: r.updated_at
        })),
        count: rows.length
      });
    } catch (err) {
      console.error('[GET /wb/warehouses/settings] error:', err);
      return res.status(500).json({
        error: 'Не удалось получить настройки складов WB',
        details: String(err.message || err)
      });
    }
  }
);

router.put(
  '/wb/warehouses/settings',
  authRequired,
  requireRole(['owner', 'admin', 'seller']),
  async (req, res) => {
    try {
      const body = req.body || {};

      const clientId = Number(body.client_id);
      const mpAccountId = Number(body.mp_account_id);
      const warehouseCode = body.warehouse_code;

      if (!Number.isInteger(clientId) || clientId <= 0) {
        return res.status(400).json({ error: 'client_id обязателен' });
      }

      if (req.user?.role === 'seller') {
        const sellerClientId = Number(req.user?.client_id);
        if (!sellerClientId || sellerClientId !== clientId) {
          return res.status(403).json({
            error: 'Нет доступа к этому client_id'
          });
        }
      }

      if (!Number.isInteger(mpAccountId) || mpAccountId <= 0) {
        return res.status(400).json({ error: 'mp_account_id обязателен' });
      }

      if (!String(warehouseCode || '').trim()) {
        return res.status(400).json({ error: 'warehouse_code обязателен' });
      }

      const hasWeight = Object.prototype.hasOwnProperty.call(body, 'weight');
      const hasFlag = Object.prototype.hasOwnProperty.call(body, 'is_enabled_for_distribution');

      if (!hasWeight && !hasFlag) {
        return res.status(400).json({
          error: 'Нужно передать хотя бы одно поле: weight или is_enabled_for_distribution'
        });
      }

      const result = await updateClientWbWarehouseSettings({
        clientId,
        mpAccountId,
        warehouseCode,
        weight: hasWeight ? body.weight : undefined,
        isEnabledForDistribution: hasFlag ? body.is_enabled_for_distribution : undefined
      });

      return res.json({
        success: true,
        item: result
      });
    } catch (err) {
      console.error('[PUT /wb/warehouses/settings] error:', err);
      return res.status(500).json({
        error: 'Не удалось обновить настройки склада WB',
        details: String(err.message || err)
      });
    }
  }
);

module.exports = router;