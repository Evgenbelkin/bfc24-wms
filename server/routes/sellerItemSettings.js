const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const { authRequired, requireRole } = require('../authMiddleware');

function resolveSellerScope(req) {
  const user = req.user || {};
  const body = req.body || {};
  const query = req.query || {};

  const client_id =
    user.role === 'seller'
      ? user.client_id
      : Number(body.client_id || query.client_id || 0);

  const mp_account_id = Number(body.mp_account_id || query.mp_account_id || 0) || null;

  return { client_id, mp_account_id };
}

/**
 * GET /seller-item-settings
 * Возвращает настройки товаров селлера
 */
router.get(
  '/',
  authRequired,
  requireRole(['owner', 'admin', 'seller']),
  async (req, res) => {
    try {
      const { client_id, mp_account_id } = resolveSellerScope(req);

      if (!client_id) {
        return res.status(400).json({ error: 'client_id обязателен' });
      }

      const params = [client_id];
      let sql = `
        SELECT
          id,
          client_id,
          mp_account_id,
          barcode,
          low_stock_threshold,
          target_stock,
          is_monitoring_enabled,
          warning_multiplier,
          created_at,
          updated_at
        FROM wms.seller_item_settings
        WHERE client_id = $1
      `;

      if (mp_account_id) {
        params.push(mp_account_id);
        sql += ` AND (mp_account_id = $2 OR mp_account_id IS NULL) `;
      }

      sql += ` ORDER BY barcode `;

      const { rows } = await pool.query(sql, params);

      return res.json({
        ok: true,
        client_id,
        mp_account_id,
        items: rows
      });
    } catch (err) {
      console.error('[seller-item-settings][GET] error:', err);
      return res.status(500).json({ error: 'Ошибка чтения настроек товаров селлера' });
    }
  }
);

/**
 * POST /seller-item-settings/upsert
 * Сохраняет настройки по товару
 */
router.post(
  '/upsert',
  authRequired,
  requireRole(['owner', 'admin', 'seller']),
  async (req, res) => {
    try {
      const { client_id, mp_account_id } = resolveSellerScope(req);
      const barcode = String(req.body.barcode || '').trim();

      const low_stock_threshold = Number(req.body.low_stock_threshold ?? 0);

      const target_stock =
        req.body.target_stock === null || req.body.target_stock === undefined || req.body.target_stock === ''
          ? null
          : Number(req.body.target_stock);

      const is_monitoring_enabled =
        req.body.is_monitoring_enabled === undefined
          ? true
          : Boolean(req.body.is_monitoring_enabled);

      const warning_multiplier =
        req.body.warning_multiplier === null || req.body.warning_multiplier === undefined || req.body.warning_multiplier === ''
          ? null
          : Number(req.body.warning_multiplier);

      if (!client_id) {
        return res.status(400).json({ error: 'client_id обязателен' });
      }

      if (!barcode) {
        return res.status(400).json({ error: 'barcode обязателен' });
      }

      if (!Number.isInteger(low_stock_threshold) || low_stock_threshold < 0) {
        return res.status(400).json({ error: 'low_stock_threshold должен быть целым числом >= 0' });
      }

      if (target_stock !== null && (!Number.isInteger(target_stock) || target_stock < 0)) {
        return res.status(400).json({ error: 'target_stock должен быть целым числом >= 0' });
      }

      if (
        warning_multiplier !== null &&
        (!Number.isFinite(warning_multiplier) || warning_multiplier <= 1)
      ) {
        return res.status(400).json({ error: 'warning_multiplier должен быть числом > 1' });
      }

      const sql = `
        INSERT INTO wms.seller_item_settings (
          client_id,
          mp_account_id,
          barcode,
          low_stock_threshold,
          target_stock,
          is_monitoring_enabled,
          warning_multiplier
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (client_id, barcode)
        DO UPDATE SET
          mp_account_id = EXCLUDED.mp_account_id,
          low_stock_threshold = EXCLUDED.low_stock_threshold,
          target_stock = EXCLUDED.target_stock,
          is_monitoring_enabled = EXCLUDED.is_monitoring_enabled,
          warning_multiplier = EXCLUDED.warning_multiplier,
          updated_at = now()
        RETURNING
          id,
          client_id,
          mp_account_id,
          barcode,
          low_stock_threshold,
          target_stock,
          is_monitoring_enabled,
          warning_multiplier,
          created_at,
          updated_at
      `;

      const params = [
        client_id,
        mp_account_id,
        barcode,
        low_stock_threshold,
        target_stock,
        is_monitoring_enabled,
        warning_multiplier
      ];

      const { rows } = await pool.query(sql, params);

      return res.json({
        ok: true,
        item: rows[0]
      });
    } catch (err) {
      console.error('[seller-item-settings][UPSERT] error:', err);
      return res.status(500).json({ error: 'Ошибка сохранения настроек товара' });
    }
  }
);

/**
 * POST /seller-item-settings/bulk-update-warning
 * Массово задаёт warning_multiplier для списка товаров
 */
router.post(
  '/bulk-update-warning',
  authRequired,
  requireRole(['owner', 'admin', 'seller']),
  async (req, res) => {
    const client = await pool.connect();

    try {
      const { client_id } = resolveSellerScope(req);
      const { barcodes, warning_multiplier, mp_account_id } = req.body;

      if (!client_id) {
        return res.status(400).json({ ok: false, error: 'client_id обязателен' });
      }

      if (!Array.isArray(barcodes) || !barcodes.length) {
        return res.status(400).json({ ok: false, error: 'barcodes обязателен и должен быть непустым массивом' });
      }

      if (!Number.isFinite(Number(warning_multiplier)) || Number(warning_multiplier) <= 1) {
        return res.status(400).json({ ok: false, error: 'warning_multiplier должен быть числом > 1' });
      }

      await client.query('BEGIN');

      for (const rawBarcode of barcodes) {
        const barcode = String(rawBarcode || '').trim();
        if (!barcode) continue;

        await client.query(
          `
          INSERT INTO wms.seller_item_settings (
            client_id,
            mp_account_id,
            barcode,
            warning_multiplier
          )
          VALUES ($1, $2, $3, $4)
          ON CONFLICT (client_id, barcode)
          DO UPDATE SET
            mp_account_id = COALESCE(EXCLUDED.mp_account_id, wms.seller_item_settings.mp_account_id),
            warning_multiplier = EXCLUDED.warning_multiplier,
            updated_at = now()
          `,
          [client_id, mp_account_id || null, barcode, Number(warning_multiplier)]
        );
      }

      await client.query('COMMIT');

      return res.json({ ok: true });
    } catch (err) {
      await client.query('ROLLBACK');
      console.error('[seller-item-settings][BULK-UPDATE-WARNING] error:', err);
      return res.status(500).json({ ok: false, error: err.message || 'Ошибка массового обновления warning_multiplier' });
    } finally {
      client.release();
    }
  }
);

module.exports = router;