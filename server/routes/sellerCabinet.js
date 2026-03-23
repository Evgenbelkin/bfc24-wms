const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const { authRequired, requireRole } = require('../authMiddleware');

const {
  recalcDistributionFromStock,
  recalcAllDistributionFromStock
} = require('../services/wbStockDistributionFromStockService');

const {
  updateClientWbWarehouseSettings
} = require('../services/wbSellerWarehousesService');

const {
  publishDistributionToWb
} = require('../services/wbStockPublishService');

// =====================================================
// helpers: logging / normalize
// =====================================================
function safeJson(value) {
  try {
    return JSON.stringify(
      value,
      (key, val) => {
        if (typeof val === 'bigint') return Number(val);
        if (val instanceof Date) return val.toISOString();
        return val;
      },
      2
    );
  } catch (err) {
    return `[unserializable: ${err.message}]`;
  }
}

function logBlock(title, payload) {
  console.log(`${title} ${safeJson(payload)}`);
}

function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function normalizeWarehouseForLog(row) {
  return {
    id: row.id ?? null,
    client_id: row.client_id ?? null,
    mp_account_id: row.mp_account_id ?? null,
    wb_warehouse_id: row.wb_warehouse_id ? Number(row.wb_warehouse_id) : null,
    warehouse_code: row.warehouse_code || null,
    warehouse_name: row.warehouse_name || null,
    is_active: !!row.is_active,
    is_enabled_for_distribution: !!row.is_enabled_for_distribution,
    weight: toNum(row.weight, 0),
    last_synced_at: row.last_synced_at || null,
    updated_at: row.updated_at || null
  };
}

function normalizePublishWarehouse(entry) {
  return {
    warehouse_id: entry?.warehouse_id ?? entry?.wb_warehouse_id ?? null,
    warehouse_name: entry?.warehouse_name ?? entry?.name ?? null,
    warehouse_code: entry?.warehouse_code ?? null,
    items_count:
      toNum(entry?.items_count, NaN) ||
      (Array.isArray(entry?.stocks) ? entry.stocks.length : 0) ||
      (Array.isArray(entry?.items) ? entry.items.length : 0),
    status: entry?.status ?? null,
    dry_run: entry?.dry_run ?? null,
    stocks: Array.isArray(entry?.stocks)
      ? entry.stocks.map((s) => ({
          barcode: String(s?.barcode || '').trim() || null,
          quantity: toNum(s?.quantity, 0)
        }))
      : undefined,
    items: Array.isArray(entry?.items)
      ? entry.items.map((s) => ({
          barcode: String(s?.barcode || '').trim() || null,
          quantity: toNum(s?.quantity, 0)
        }))
      : undefined,
    response: entry?.response ?? entry?.result ?? null,
    error: entry?.error ?? null
  };
}

function summarizeRecalcResult(result) {
  const summary = {
    ok: result?.ok ?? true,
    total_items: null,
    total_fact_qty: 0,
    total_distributed_qty: 0,
    total_diff: 0,
    items: []
  };

  const candidateArrays = [
    result?.items,
    result?.rows,
    result?.distribution,
    result?.results
  ].filter(Array.isArray);

  if (candidateArrays.length) {
    const items = candidateArrays[0];
    summary.total_items = items.length;

    summary.items = items.slice(0, 100).map((row) => {
      const factQty =
        toNum(row?.fact_qty, NaN) ||
        toNum(row?.fact_wms_qty, NaN) ||
        toNum(row?.stock_qty, NaN) ||
        0;

      const distributedQty =
        toNum(row?.distributed_qty, NaN) ||
        toNum(row?.distributed_total, NaN) ||
        toNum(row?.qty_total, NaN) ||
        0;

      const diff =
        Number.isFinite(Number(row?.diff))
          ? Number(row.diff)
          : factQty - distributedQty;

      summary.total_fact_qty += factQty;
      summary.total_distributed_qty += distributedQty;
      summary.total_diff += diff;

      return {
        barcode: String(row?.barcode || '').trim() || null,
        item_name: row?.item_name || row?.name || null,
        fact_qty: factQty,
        distributed_qty: distributedQty,
        diff,
        distribution:
          row?.distribution ||
          row?.warehouse_qty ||
          row?.warehouses ||
          null
      };
    });
  }

  summary.total_fact_qty = toNum(summary.total_fact_qty, 0);
  summary.total_distributed_qty = toNum(summary.total_distributed_qty, 0);
  summary.total_diff = toNum(summary.total_diff, 0);

  return summary;
}

function summarizePublishResult(result) {
  const summary = {
    ok: result?.ok ?? true,
    dry_run: result?.dry_run ?? null,
    warehouses: [],
    warehouses_total: 0,
    total_positions: 0,
    zero_qty_lines: 0,
    response: result?.response ?? null,
    error: result?.error ?? null
  };

  const warehouseArrays = [
    result?.warehouses,
    result?.results,
    result?.publish_results,
    result?.data
  ].filter(Array.isArray);

  if (warehouseArrays.length) {
    const arr = warehouseArrays[0];
    summary.warehouses = arr.map(normalizePublishWarehouse);
    summary.warehouses_total = summary.warehouses.length;

    for (const wh of summary.warehouses) {
      const lines = Array.isArray(wh.stocks)
        ? wh.stocks
        : Array.isArray(wh.items)
        ? wh.items
        : [];

      summary.total_positions += lines.length;

      for (const line of lines) {
        if (toNum(line?.quantity, 0) === 0) {
          summary.zero_qty_lines += 1;
        }
      }
    }
  }

  return summary;
}

// =====================================================
// Получить client_id
// =====================================================
async function resolveAllowedClientId(req, requestedClientId) {
  const role = req.user?.role;
  const userId = req.user?.id;

  if (role === 'owner' || role === 'admin') {
    if (requestedClientId && Number(requestedClientId) > 0) {
      return Number(requestedClientId);
    }
    return null;
  }

  if (role === 'seller') {
    if (Number(req.user?.client_id) > 0) {
      return Number(req.user.client_id);
    }

    const q1 = await pool.query(
      `
      SELECT client_id
      FROM auth.users
      WHERE id = $1
      LIMIT 1
      `,
      [userId]
    );

    if (q1.rows.length && Number(q1.rows[0].client_id) > 0) {
      return Number(q1.rows[0].client_id);
    }
  }

  return null;
}

// =====================================================
// Получить mp_account_id по клиенту
// =====================================================
async function resolveMpAccountId(client, clientId, requestedMpAccountId) {
  if (requestedMpAccountId && Number(requestedMpAccountId) > 0) {
    return Number(requestedMpAccountId);
  }

  const q = await client.query(
    `
    SELECT mp_account_id
    FROM wms.client_wb_warehouses
    WHERE client_id = $1
      AND mp_account_id IS NOT NULL
    ORDER BY mp_account_id ASC
    LIMIT 1
    `,
    [clientId]
  );

  if (!q.rows.length) {
    return null;
  }

  return Number(q.rows[0].mp_account_id);
}

// =====================================================
function parseOptionalBoolean(value) {
  if (value === undefined || value === null) return undefined;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;

  const s = String(value).trim().toLowerCase();

  if (['true', '1', 'yes', 'y', 'on'].includes(s)) return true;
  if (['false', '0', 'no', 'n', 'off'].includes(s)) return false;

  return undefined;
}

// =====================================================
// Главный хендлер кабинета
// =====================================================
async function getFbsStockHandler(req, res) {
  const client = await pool.connect();

  try {
    const requestedClientId = Number(req.query.client_id);
    const requestedMpAccountId = Number(req.query.mp_account_id);

    const allowedClientId = await resolveAllowedClientId(req, requestedClientId);

    if (!allowedClientId) {
      return res.status(403).json({
        ok: false,
        error: 'Нет доступа к client_id'
      });
    }

    const mpAccountId = await resolveMpAccountId(client, allowedClientId, requestedMpAccountId);

    if (!mpAccountId) {
      return res.status(400).json({
        ok: false,
        error: 'Не найден mp_account_id для клиента'
      });
    }

    // =====================================================
    // 1) Склады WB
    // =====================================================
    const warehousesRes = await client.query(
      `
      SELECT
        id,
        client_id,
        mp_account_id,
        wb_warehouse_id,
        warehouse_code,
        warehouse_name,
        is_active,
        COALESCE(is_enabled_for_distribution, false) AS is_enabled_for_distribution,
        COALESCE(weight, 0) AS weight,
        last_synced_at,
        updated_at
      FROM wms.client_wb_warehouses
      WHERE client_id = $1
        AND mp_account_id = $2
      ORDER BY
        is_enabled_for_distribution DESC,
        weight DESC,
        warehouse_name ASC
      `,
      [allowedClientId, mpAccountId]
    );

    const warehouses = warehousesRes.rows.map((row) => ({
      id: row.id,
      client_id: row.client_id,
      mp_account_id: row.mp_account_id,
      wb_warehouse_id: row.wb_warehouse_id ? Number(row.wb_warehouse_id) : null,
      warehouse_code: row.warehouse_code,
      warehouse_name: row.warehouse_name,
      is_active: !!row.is_active,
      is_enabled_for_distribution: !!row.is_enabled_for_distribution,
      weight: Number(row.weight || 0),
      last_synced_at: row.last_synced_at || null,
      updated_at: row.updated_at || null
    }));

    // =====================================================
    // 2) Фактический остаток из WMS + обогащение из masterdata.items
    // =====================================================
    const stockRes = await client.query(
      `
      WITH stock_base AS (
        SELECT
          s.client_id,
          COALESCE(NULLIF(TRIM(s.barcode), ''), NULLIF(TRIM(sku.barcode), '')) AS barcode,
          SUM(s.qty)::int AS fact_qty
        FROM wms.stock s
        LEFT JOIN wms.sku sku
          ON sku.id = s.sku_id
        WHERE s.client_id = $1
          AND COALESCE(NULLIF(TRIM(s.barcode), ''), NULLIF(TRIM(sku.barcode), '')) IS NOT NULL
        GROUP BY
          s.client_id,
          COALESCE(NULLIF(TRIM(s.barcode), ''), NULLIF(TRIM(sku.barcode), ''))
      )
      SELECT
        sb.client_id,
        sb.barcode,
        sb.fact_qty,
        i.id AS item_id,
        i.item_name,
        i.vendor_code,
        i.wb_vendor_code,
        i.brand
      FROM stock_base sb
      LEFT JOIN masterdata.items i
        ON i.client_id = sb.client_id
       AND i.barcode = sb.barcode
      ORDER BY
        i.item_name ASC NULLS LAST,
        sb.barcode ASC
      `,
      [allowedClientId]
    );

    // =====================================================
    // 2.1) Продажи за 14 дней по выполненным picking_tasks
    // =====================================================
    const salesRes = await client.query(
      `
      WITH sales_agg AS (
        SELECT
          pt.client_id,
          TRIM(pt.barcode) AS barcode,
          SUM(pt.qty)::numeric(12,2) AS sold_qty_14d,
          ROUND((SUM(pt.qty)::numeric / 14.0), 2) AS avg_daily_sales
        FROM wms.picking_tasks pt
        WHERE pt.client_id = $1
          AND pt.status = 'done'
          AND pt.finished_at IS NOT NULL
          AND pt.finished_at >= now() - interval '14 days'
          AND TRIM(COALESCE(pt.barcode, '')) <> ''
        GROUP BY
          pt.client_id,
          TRIM(pt.barcode)
      )
      SELECT
        client_id,
        barcode,
        sold_qty_14d,
        avg_daily_sales
      FROM sales_agg
      ORDER BY barcode
      `,
      [allowedClientId]
    );

    const salesMap = new Map();

    for (const row of salesRes.rows) {
      const barcode = String(row.barcode || '').trim();
      if (!barcode) continue;

      salesMap.set(barcode, {
        sold_qty_14d: Number(row.sold_qty_14d || 0),
        avg_daily_sales: Number(row.avg_daily_sales || 0)
      });
    }

    // =====================================================
    // 3) Распределение по складам WB + обогащение из masterdata.items
    // =====================================================
    const distributionRes = await client.query(
      `
      SELECT
        d.client_id,
        d.mp_account_id,
        d.barcode,
        d.warehouse_code,
        d.qty,
        d.calculated_at,
        d.updated_at AS distribution_updated_at,
        i.id AS item_id,
        i.item_name,
        i.vendor_code,
        i.wb_vendor_code,
        i.brand
      FROM wms.client_stock_distribution d
      LEFT JOIN masterdata.items i
        ON i.client_id = d.client_id
       AND i.barcode = d.barcode
      WHERE d.client_id = $1
        AND d.mp_account_id = $2
      ORDER BY
        i.item_name ASC NULLS LAST,
        d.barcode ASC,
        d.warehouse_code ASC
      `,
      [allowedClientId, mpAccountId]
    );

    // =====================================================
    // 3.1) Настройки селлера по товарам
    // =====================================================
    const settingsRes = await client.query(
      `
      SELECT
        client_id,
        barcode,
        low_stock_threshold,
        target_stock,
        is_monitoring_enabled,
        warning_multiplier
      FROM wms.seller_item_settings
      WHERE client_id = $1
      `,
      [allowedClientId]
    );

    const settingsMap = new Map();

    for (const row of settingsRes.rows) {
      const barcode = String(row.barcode || '').trim();
      if (!barcode) continue;

      settingsMap.set(barcode, {
        low_stock_threshold: Number(row.low_stock_threshold || 0),
        target_stock: row.target_stock !== null ? Number(row.target_stock) : null,
        is_monitoring_enabled: row.is_monitoring_enabled !== false,
        warning_multiplier:
          row.warning_multiplier !== null ? Number(row.warning_multiplier) : null
      });
    }

    // =====================================================
    // 4) Собираем единую карту товаров
    // =====================================================
    const itemsMap = new Map();

    function ensureItem(seed) {
      const barcode = String(seed?.barcode || '').trim();
      if (!barcode) return null;

      if (!itemsMap.has(barcode)) {
        const warehouseQty = {};
        for (const wh of warehouses) {
          warehouseQty[String(wh.warehouse_code)] = 0;
        }

        itemsMap.set(barcode, {
          item_id: seed.item_id || null,
          item_name: seed.item_name || null,
          vendor_code: seed.vendor_code || null,
          wb_vendor_code: seed.wb_vendor_code || null,
          brand: seed.brand || null,
          barcode,
          fact_qty: 0,
          distributed_qty: 0,
          sold_qty_14d: 0,
          avg_daily_sales: 0,
          days_left: null,
          warehouse_qty: warehouseQty,
          calculated_at: null,
          distribution_updated_at: null,
          low_stock_threshold: 0,
          target_stock: null,
          is_monitoring_enabled: true,
          warning_multiplier: null,
          stock_status: 'normal'
        });
      }

      const item = itemsMap.get(barcode);

      if (!item.item_name && seed.item_name) item.item_name = seed.item_name;
      if (!item.vendor_code && seed.vendor_code) item.vendor_code = seed.vendor_code;
      if (!item.wb_vendor_code && seed.wb_vendor_code) item.wb_vendor_code = seed.wb_vendor_code;
      if (!item.brand && seed.brand) item.brand = seed.brand;
      if (!item.item_id && seed.item_id) item.item_id = seed.item_id;

      return item;
    }

    for (const row of stockRes.rows) {
      const item = ensureItem(row);
      if (!item) continue;
      item.fact_qty = Number(row.fact_qty || 0);
    }

    // =====================================================
// подтягиваем справочник для товаров из продаж
// =====================================================
const salesBarcodes = Array.from(salesMap.keys());

let salesItemsMap = new Map();

if (salesBarcodes.length > 0) {
  const salesItemsRes = await client.query(
    `
    SELECT
      barcode,
      item_name,
      vendor_code,
      wb_vendor_code,
      brand,
      id AS item_id
    FROM masterdata.items
    WHERE client_id = $1
      AND barcode = ANY($2)
    `,
    [allowedClientId, salesBarcodes]
  );

  for (const row of salesItemsRes.rows) {
    const barcode = String(row.barcode || '').trim();
    if (!barcode) continue;

    salesItemsMap.set(barcode, row);
  }
}

// =====================================================
// применяем продажи + обогащаем справочником
// =====================================================
for (const [barcode, sales] of salesMap.entries()) {
  const ref = salesItemsMap.get(barcode) || {};

  const item = ensureItem({
    barcode,
    item_name: ref.item_name,
    vendor_code: ref.vendor_code,
    wb_vendor_code: ref.wb_vendor_code,
    brand: ref.brand,
    item_id: ref.item_id
  });

  if (!item) continue;

  item.sold_qty_14d = Number(sales.sold_qty_14d || 0);
  item.avg_daily_sales = Number(sales.avg_daily_sales || 0);
}

    for (const row of distributionRes.rows) {
      const item = ensureItem(row);
      if (!item) continue;

      const warehouseCode = String(row.warehouse_code || '').trim();
      const qty = Number(row.qty || 0);

      item.distributed_qty += qty;

      if (warehouseCode) {
        item.warehouse_qty[warehouseCode] = (item.warehouse_qty[warehouseCode] || 0) + qty;
      }

      if (row.calculated_at) {
        if (!item.calculated_at || new Date(row.calculated_at) > new Date(item.calculated_at)) {
          item.calculated_at = row.calculated_at;
        }
      }

      if (row.distribution_updated_at) {
        if (!item.distribution_updated_at || new Date(row.distribution_updated_at) > new Date(item.distribution_updated_at)) {
          item.distribution_updated_at = row.distribution_updated_at;
        }
      }
    }

    // =====================================================
    // 4.1) Применяем настройки селлера
    // =====================================================
    for (const item of itemsMap.values()) {
      const settings = settingsMap.get(item.barcode);

      if (settings) {
        item.low_stock_threshold = settings.low_stock_threshold;
        item.target_stock = settings.target_stock;
        item.is_monitoring_enabled = settings.is_monitoring_enabled;

        if (settings.warning_multiplier !== null) {
          item.warning_multiplier = settings.warning_multiplier;
        }
      }

      if (Number(item.avg_daily_sales || 0) > 0) {
        item.days_left = Number((Number(item.fact_qty || 0) / Number(item.avg_daily_sales || 0)).toFixed(1));
      } else {
        item.days_left = null;
      }
    }

    // =====================================================
    // 4.2) Рассчитываем статус остатка
    // =====================================================
    for (const item of itemsMap.values()) {
      const qty = Number(item.fact_qty || 0);
      const threshold = Number(item.low_stock_threshold || 0);
      const multiplier = Number(item.warning_multiplier || 1.5);

      if (!item.is_monitoring_enabled) {
        item.stock_status = 'monitoring_off';
        continue;
      }

      if (threshold > 0 && qty <= threshold) {
        item.stock_status = 'critical';
      } else if (threshold > 0 && qty <= Math.ceil(threshold * multiplier)) {
        item.stock_status = 'warning';
      } else {
        item.stock_status = 'normal';
      }
    }

    const items = Array.from(itemsMap.values()).sort((a, b) => {
      const order = {
        critical: 0,
        warning: 1,
        normal: 2,
        monitoring_off: 3
      };

      const statusDiff = (order[a.stock_status] ?? 99) - (order[b.stock_status] ?? 99);
      if (statusDiff !== 0) return statusDiff;

      const aDays = a.days_left === null ? 999999 : Number(a.days_left);
      const bDays = b.days_left === null ? 999999 : Number(b.days_left);
      if (aDays !== bDays) return aDays - bDays;

      const nameA = String(a.item_name || '');
      const nameB = String(b.item_name || '');

      return (
        nameA.localeCompare(nameB, 'ru') ||
        String(a.barcode || '').localeCompare(String(b.barcode || ''), 'ru')
      );
    });

    // =====================================================
    // 5) totals / stats
    // =====================================================
    const totalsByWarehouse = {};
    for (const wh of warehouses) {
      totalsByWarehouse[String(wh.warehouse_code)] = 0;
    }

    let factGrandTotal = 0;
    let distributedGrandTotal = 0;
    let criticalItemsCount = 0;
    let warningItemsCount = 0;
    let monitoringOffItemsCount = 0;
    let soldQty14dTotal = 0;
    let avgDailySalesTotal = 0;

    for (const item of items) {
      factGrandTotal += Number(item.fact_qty || 0);
      distributedGrandTotal += Number(item.distributed_qty || 0);
      soldQty14dTotal += Number(item.sold_qty_14d || 0);
      avgDailySalesTotal += Number(item.avg_daily_sales || 0);

      if (item.stock_status === 'critical') criticalItemsCount += 1;
      if (item.stock_status === 'warning') warningItemsCount += 1;
      if (item.stock_status === 'monitoring_off') monitoringOffItemsCount += 1;

      for (const wh of warehouses) {
        const code = String(wh.warehouse_code);
        totalsByWarehouse[code] += Number(item.warehouse_qty?.[code] || 0);
      }
    }

    const warehousesEnabled = warehouses.filter((w) => w.is_enabled_for_distribution).length;
    const weightsSum = warehouses
      .filter((w) => w.is_enabled_for_distribution)
      .reduce((sum, w) => sum + Number(w.weight || 0), 0);

    let lastSyncAt = null;
    for (const w of warehouses) {
      const dt = w.last_synced_at || null;
      if (!dt) continue;
      if (!lastSyncAt || new Date(dt) > new Date(lastSyncAt)) {
        lastSyncAt = dt;
      }
    }

    logBlock('[seller-cabinet/fbs-stock][summary]', {
      user: {
        id: req.user?.id || null,
        username: req.user?.username || null,
        role: req.user?.role || null,
        client_id: req.user?.client_id || null
      },
      requested: {
        client_id: requestedClientId || null,
        mp_account_id: requestedMpAccountId || null
      },
      resolved: {
        client_id: allowedClientId,
        mp_account_id: mpAccountId
      },
      warehouses_total: warehouses.length,
      warehouses_enabled: warehousesEnabled,
      weights_sum: weightsSum,
      items_total: items.length,
      fact_grand_total: factGrandTotal,
      distributed_grand_total: distributedGrandTotal,
      sold_qty_14d_total: soldQty14dTotal,
      avg_daily_sales_total: Number(avgDailySalesTotal.toFixed(2)),
      diff_grand_total: factGrandTotal - distributedGrandTotal,
      critical_items_count: criticalItemsCount,
      warning_items_count: warningItemsCount,
      monitoring_off_items_count: monitoringOffItemsCount,
      totals_by_warehouse: totalsByWarehouse,
      warehouses: warehouses.map(normalizeWarehouseForLog)
    });

    return res.json({
      ok: true,
      client_id: allowedClientId,
      mp_account_id: mpAccountId,
      warehouses,
      items,
      totals: {
        by_warehouse: totalsByWarehouse,
        grand_total: distributedGrandTotal,
        fact_grand_total: factGrandTotal,
        distributed_grand_total: distributedGrandTotal,
        diff_grand_total: factGrandTotal - distributedGrandTotal,
        sold_qty_14d_total: soldQty14dTotal,
        avg_daily_sales_total: Number(avgDailySalesTotal.toFixed(2))
      },
      stats: {
        warehouses_total: warehouses.length,
        warehouses_enabled: warehousesEnabled,
        weights_sum: weightsSum,
        last_sync_at: lastSyncAt,
        critical_items_count: criticalItemsCount,
        warning_items_count: warningItemsCount,
        monitoring_off_items_count: monitoringOffItemsCount
      }
    });
  } catch (err) {
    console.error('[seller-cabinet/fbs-stock] error:', err);
    return res.status(500).json({
      ok: false,
      error: err.message || 'Ошибка загрузки кабинета'
    });
  } finally {
    client.release();
  }
}

// =====================================================
router.get(
  '/fbs-stock',
  authRequired,
  requireRole(['owner', 'admin', 'seller']),
  getFbsStockHandler
);

router.get(
  '/wb-stock-distribution',
  authRequired,
  requireRole(['owner', 'admin', 'seller']),
  getFbsStockHandler
);

// =====================================================
router.patch(
  '/update-distribution',
  authRequired,
  requireRole(['owner', 'admin', 'seller']),
  async (req, res) => {
    try {
      const requestedClientId = Number(req.body.client_id);
      const allowedClientId = await resolveAllowedClientId(req, requestedClientId);

      if (!allowedClientId) {
        return res.status(403).json({
          ok: false,
          error: 'Нет доступа к client_id'
        });
      }

      const bodyMpAccountId = Number(req.body.mp_account_id);

      const client = await pool.connect();
      let mpAccountId = null;

      try {
        mpAccountId = await resolveMpAccountId(client, allowedClientId, bodyMpAccountId);
      } finally {
        client.release();
      }

      if (!mpAccountId) {
        return res.status(400).json({
          ok: false,
          error: 'Не найден mp_account_id для клиента'
        });
      }

      const warehouseCode = String(req.body.warehouse_code || '').trim();
      const weight = Number(req.body.weight);
      const isEnabled = parseOptionalBoolean(req.body.is_enabled_for_distribution);

      if (!warehouseCode) {
        return res.status(400).json({
          ok: false,
          error: 'warehouse_code обязателен'
        });
      }

      if (!Number.isFinite(weight) || weight < 0) {
        return res.status(400).json({
          ok: false,
          error: 'weight должен быть числом >= 0'
        });
      }

      if (typeof isEnabled !== 'boolean') {
        return res.status(400).json({
          ok: false,
          error: 'is_enabled_for_distribution должен быть boolean'
        });
      }

      logBlock('[seller-cabinet/update-distribution][request]', {
        user: {
          id: req.user?.id || null,
          username: req.user?.username || null,
          role: req.user?.role || null,
          client_id: req.user?.client_id || null
        },
        resolved: {
          client_id: allowedClientId,
          mp_account_id: mpAccountId
        },
        payload: {
          warehouse_code: warehouseCode,
          weight,
          is_enabled_for_distribution: isEnabled
        }
      });

      const result = await updateClientWbWarehouseSettings({
        clientId: allowedClientId,
        mpAccountId,
        warehouseCode,
        weight,
        isEnabledForDistribution: isEnabled
      });

      logBlock('[seller-cabinet/update-distribution][result]', {
        ok: true,
        warehouse: {
          ...result,
          weight: Number(result.weight || 0),
          is_enabled_for_distribution: !!result.is_enabled_for_distribution
        }
      });

      return res.json({
        ok: true,
        warehouse: {
          ...result,
          weight: Number(result.weight || 0),
          is_enabled_for_distribution: !!result.is_enabled_for_distribution
        }
      });
    } catch (err) {
      console.error('[seller-cabinet/update-distribution] error:', err);
      return res.status(500).json({
        ok: false,
        error: err.message || 'Ошибка сохранения настроек'
      });
    }
  }
);

// =====================================================
router.post(
  '/auto-distribute',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const requestedClientId = Number(req.body.client_id);
      const allowedClientId = await resolveAllowedClientId(req, requestedClientId);

      if (!allowedClientId) {
        return res.status(403).json({
          ok: false,
          error: 'Нет доступа к client_id'
        });
      }

      const client = await pool.connect();
      let mpAccountId = null;
      let warehousesForLog = [];

      try {
        mpAccountId = await resolveMpAccountId(client, allowedClientId, Number(req.body.mp_account_id));

        if (mpAccountId) {
          const qWh = await client.query(
            `
            SELECT
              id,
              client_id,
              mp_account_id,
              wb_warehouse_id,
              warehouse_code,
              warehouse_name,
              is_active,
              COALESCE(is_enabled_for_distribution, false) AS is_enabled_for_distribution,
              COALESCE(weight, 0) AS weight,
              last_synced_at,
              updated_at
            FROM wms.client_wb_warehouses
            WHERE client_id = $1
              AND mp_account_id = $2
            ORDER BY
              is_enabled_for_distribution DESC,
              weight DESC,
              warehouse_name ASC
            `,
            [allowedClientId, mpAccountId]
          );

          warehousesForLog = qWh.rows.map(normalizeWarehouseForLog);
        }
      } finally {
        client.release();
      }

      if (!mpAccountId) {
        return res.status(400).json({
          ok: false,
          error: 'Не найден mp_account_id для клиента'
        });
      }

      const barcode = String(req.body.barcode || '').trim();

      logBlock('[seller-cabinet/auto-distribute][request]', {
        user: {
          id: req.user?.id || null,
          username: req.user?.username || null,
          role: req.user?.role || null,
          client_id: req.user?.client_id || null
        },
        resolved: {
          client_id: allowedClientId,
          mp_account_id: mpAccountId
        },
        mode: barcode ? 'single' : 'bulk',
        barcode: barcode || null,
        warehouses: warehousesForLog
      });

      if (barcode) {
        const result = await recalcDistributionFromStock({
          clientId: allowedClientId,
          mpAccountId,
          barcode,
          triggerType: 'manual_single'
        });

        logBlock('[seller-cabinet/auto-distribute][result][single]', {
          resolved: {
            client_id: allowedClientId,
            mp_account_id: mpAccountId
          },
          barcode,
          warehouses: warehousesForLog,
          recalc_summary: summarizeRecalcResult(result),
          raw_result: result
        });

        return res.json({
          ok: true,
          mode: 'single',
          ...result
        });
      }

      const result = await recalcAllDistributionFromStock({
        clientId: allowedClientId,
        mpAccountId,
        triggerType: 'manual_bulk'
      });

      logBlock('[seller-cabinet/auto-distribute][result][bulk]', {
        resolved: {
          client_id: allowedClientId,
          mp_account_id: mpAccountId
        },
        warehouses: warehousesForLog,
        recalc_summary: summarizeRecalcResult(result),
        raw_result: result
      });

      return res.json({
        ok: true,
        mode: 'bulk',
        ...result
      });
    } catch (err) {
      console.error('[seller-cabinet/auto-distribute] error:', err);
      return res.status(500).json({
        ok: false,
        error: err.message || 'Ошибка автораспределения'
      });
    }
  }
);

// =====================================================
// dry_run по умолчанию = true
// чтобы боевой publish нельзя было запустить случайно
// =====================================================
router.post(
  '/publish-distribution',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const requestedClientId = Number(req.body.client_id);
      const allowedClientId = await resolveAllowedClientId(req, requestedClientId);

      if (!allowedClientId) {
        return res.status(403).json({
          ok: false,
          error: 'Нет доступа к client_id'
        });
      }

      const client = await pool.connect();
      let mpAccountId = null;
      let warehousesForLog = [];
      let distributionPreviewRows = [];

      try {
        mpAccountId = await resolveMpAccountId(client, allowedClientId, Number(req.body.mp_account_id));

        if (mpAccountId) {
          const qWh = await client.query(
            `
            SELECT
              id,
              client_id,
              mp_account_id,
              wb_warehouse_id,
              warehouse_code,
              warehouse_name,
              is_active,
              COALESCE(is_enabled_for_distribution, false) AS is_enabled_for_distribution,
              COALESCE(weight, 0) AS weight,
              last_synced_at,
              updated_at
            FROM wms.client_wb_warehouses
            WHERE client_id = $1
              AND mp_account_id = $2
            ORDER BY
              is_enabled_for_distribution DESC,
              weight DESC,
              warehouse_name ASC
            `,
            [allowedClientId, mpAccountId]
          );

          warehousesForLog = qWh.rows.map(normalizeWarehouseForLog);

          const qDist = await client.query(
            `
            SELECT
              d.client_id,
              d.mp_account_id,
              d.barcode,
              d.warehouse_code,
              d.qty,
              i.item_name
            FROM wms.client_stock_distribution d
            LEFT JOIN masterdata.items i
              ON i.client_id = d.client_id
             AND i.barcode = d.barcode
            WHERE d.client_id = $1
              AND d.mp_account_id = $2
            ORDER BY
              d.warehouse_code ASC,
              i.item_name ASC NULLS LAST,
              d.barcode ASC
            `,
            [allowedClientId, mpAccountId]
          );

          distributionPreviewRows = qDist.rows.map((r) => ({
            barcode: String(r.barcode || '').trim() || null,
            item_name: r.item_name || null,
            warehouse_code: r.warehouse_code || null,
            qty: toNum(r.qty, 0)
          }));
        }
      } finally {
        client.release();
      }

      if (!mpAccountId) {
        return res.status(400).json({
          ok: false,
          error: 'Не найден mp_account_id для клиента'
        });
      }

      const dryRunParsed = parseOptionalBoolean(req.body.dry_run);
      const dryRun = dryRunParsed === undefined ? true : dryRunParsed;

      logBlock('[seller-cabinet/publish-distribution][request]', {
        user: {
          id: req.user?.id || null,
          username: req.user?.username || null,
          role: req.user?.role || null,
          client_id: req.user?.client_id || null
        },
        resolved: {
          client_id: allowedClientId,
          mp_account_id: mpAccountId
        },
        dry_run: dryRun,
        warehouses: warehousesForLog,
        distribution_preview: {
          total_rows: distributionPreviewRows.length,
          zero_qty_rows: distributionPreviewRows.filter((x) => toNum(x.qty, 0) === 0).length,
          rows: distributionPreviewRows
        }
      });

      const result = await publishDistributionToWb({
        clientId: allowedClientId,
        mpAccountId,
        dryRun
      });

      logBlock('[seller-cabinet/publish-distribution][result]', {
        resolved: {
          client_id: allowedClientId,
          mp_account_id: mpAccountId
        },
        dry_run: dryRun,
        publish_summary: summarizePublishResult(result),
        raw_result: result
      });

      return res.json(result);
    } catch (err) {
      console.error('[seller-cabinet/publish-distribution] error:', err);
      return res.status(500).json({
        ok: false,
        error: err.message || 'Ошибка публикации распределения в WB'
      });
    }
  }
);

module.exports = router;