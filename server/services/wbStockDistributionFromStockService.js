const { pool } = require('../db');

async function safeInsertDistributionRun({
  client,
  clientId,
  mpAccountId,
  triggerType,
  status,
  itemsCount,
  note
}) {
  try {
    await client.query(
      `
      INSERT INTO wms.client_stock_distribution_runs (
        client_id,
        mp_account_id,
        trigger_type,
        status,
        items_count,
        note,
        created_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, NOW())
      `,
      [
        clientId,
        mpAccountId,
        triggerType,
        status,
        itemsCount,
        String(note || '').slice(0, 1000)
      ]
    );
  } catch (e) {
    console.warn('[safeInsertDistributionRun] skipped:', e.message);
  }
}

async function getTotalQtyFromStock(clientId, barcode) {
  const barcodeNormalized = String(barcode || '').trim();
  if (!barcodeNormalized) return 0;

  const { rows } = await pool.query(
    `
    SELECT COALESCE(SUM(s.qty), 0)::int AS total_qty
    FROM wms.stock s
    LEFT JOIN wms.sku sku
      ON sku.id = s.sku_id
    WHERE s.client_id = $1
      AND COALESCE(NULLIF(TRIM(s.barcode), ''), NULLIF(TRIM(sku.barcode), '')) = $2
    `,
    [clientId, barcodeNormalized]
  );

  return Number(rows[0]?.total_qty || 0);
}

async function getAllClientStockTotals(clientId) {
  const { rows } = await pool.query(
    `
    SELECT
      COALESCE(NULLIF(TRIM(s.barcode), ''), NULLIF(TRIM(sku.barcode), '')) AS barcode,
      COALESCE(SUM(s.qty), 0)::int AS total_qty
    FROM wms.stock s
    LEFT JOIN wms.sku sku
      ON sku.id = s.sku_id
    WHERE s.client_id = $1
    GROUP BY COALESCE(NULLIF(TRIM(s.barcode), ''), NULLIF(TRIM(sku.barcode), ''))
    HAVING COALESCE(NULLIF(TRIM(s.barcode), ''), NULLIF(TRIM(sku.barcode), '')) IS NOT NULL
    ORDER BY barcode
    `,
    [clientId]
  );

  return rows.map((r) => ({
    barcode: String(r.barcode),
    total_qty: Number(r.total_qty || 0)
  }));
}

async function getExistingDistributionBarcodes(clientId, mpAccountId) {
  const { rows } = await pool.query(
    `
    SELECT DISTINCT barcode
    FROM wms.client_stock_distribution
    WHERE client_id = $1
      AND mp_account_id = $2
      AND COALESCE(barcode, '') <> ''
    ORDER BY barcode
    `,
    [clientId, mpAccountId]
  );

  return rows.map((r) => String(r.barcode));
}

async function getEnabledWarehouses(clientId, mpAccountId) {
  const { rows } = await pool.query(
    `
    SELECT
      warehouse_code,
      warehouse_name,
      weight
    FROM wms.client_wb_warehouses
    WHERE client_id = $1
      AND mp_account_id = $2
      AND is_active = TRUE
      AND is_enabled_for_distribution = TRUE
      AND COALESCE(weight, 0) > 0
    ORDER BY warehouse_code
    `,
    [clientId, mpAccountId]
  );

  return rows.map((r) => ({
    warehouse_code: String(r.warehouse_code || '').trim(),
    warehouse_name: r.warehouse_name || null,
    weight: Number(r.weight || 0)
  }));
}

function buildFairDistribution(totalQty, warehouses) {
  const qty = Number(totalQty || 0);
  const normalized = (warehouses || [])
    .map((w) => ({
      warehouse_code: String(w.warehouse_code || '').trim(),
      warehouse_name: w.warehouse_name || null,
      weight: Number(w.weight || 0)
    }))
    .filter((w) => w.warehouse_code && w.weight > 0);

  if (!Number.isFinite(qty) || qty < 0) {
    throw new Error(`Некорректный totalQty: ${totalQty}`);
  }

  if (qty === 0) {
    return [];
  }

  if (!normalized.length) {
    throw new Error('Нет активных складов с weight > 0 для распределения');
  }

  const totalWeight = normalized.reduce((sum, w) => sum + w.weight, 0);

  if (!Number.isFinite(totalWeight) || totalWeight <= 0) {
    throw new Error('Сумма весов должна быть > 0');
  }

  const rows = normalized.map((w) => {
    const exactQty = qty * (w.weight / totalWeight);
    const baseQty = Math.floor(exactQty);
    const remainder = exactQty - baseQty;

    return {
      warehouse_code: w.warehouse_code,
      warehouse_name: w.warehouse_name,
      weight: w.weight,
      exact_qty: exactQty,
      base_qty: baseQty,
      remainder,
      qty: baseQty
    };
  });

  let distributed = rows.reduce((sum, r) => sum + r.base_qty, 0);
  let left = qty - distributed;

  rows.sort((a, b) => {
    if (b.remainder !== a.remainder) return b.remainder - a.remainder;
    if (b.weight !== a.weight) return b.weight - a.weight;
    return String(a.warehouse_code).localeCompare(String(b.warehouse_code), 'ru');
  });

  for (let i = 0; i < rows.length && left > 0; i += 1) {
    rows[i].qty += 1;
    left -= 1;
  }

  rows.sort((a, b) => {
    return String(a.warehouse_code).localeCompare(String(b.warehouse_code), 'ru');
  });

  return rows.map((r) => ({
    warehouse_code: r.warehouse_code,
    warehouse_name: r.warehouse_name,
    weight: r.weight,
    qty: r.qty
  }));
}

async function replaceDistributionRows({
  client,
  clientId,
  mpAccountId,
  barcode,
  distributionRows
}) {
  await client.query(
    `
    DELETE FROM wms.client_stock_distribution
    WHERE client_id = $1
      AND mp_account_id = $2
      AND barcode = $3
    `,
    [clientId, mpAccountId, barcode]
  );

  for (const row of distributionRows) {
    const qty = Number(row.qty || 0);
    if (qty <= 0) continue;

    await client.query(
      `
      INSERT INTO wms.client_stock_distribution (
        client_id,
        mp_account_id,
        barcode,
        warehouse_code,
        qty,
        calculated_at,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NOW())
      `,
      [
        clientId,
        mpAccountId,
        barcode,
        row.warehouse_code,
        qty
      ]
    );
  }
}

async function clearDistribution(clientId, mpAccountId, barcode, triggerType) {
  const barcodeNormalized = String(barcode || '').trim();
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await client.query(
      `
      DELETE FROM wms.client_stock_distribution
      WHERE client_id = $1
        AND mp_account_id = $2
        AND barcode = $3
      `,
      [clientId, mpAccountId, barcodeNormalized]
    );

    await safeInsertDistributionRun({
      client,
      clientId,
      mpAccountId,
      triggerType,
      status: 'success',
      itemsCount: 0,
      note: `barcode=${barcodeNormalized}; total_qty=0`
    });

    await client.query('COMMIT');

    return {
      success: true,
      client_id: clientId,
      mp_account_id: mpAccountId,
      barcode: barcodeNormalized,
      total_qty: 0,
      distributed_qty: 0,
      items: []
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function recalcDistributionFromStock({
  clientId,
  mpAccountId,
  barcode,
  triggerType = 'stock_adjust'
}) {
  const barcodeNormalized = String(barcode || '').trim();

  if (!barcodeNormalized) {
    throw new Error('barcode обязателен');
  }

  const totalQty = await getTotalQtyFromStock(clientId, barcodeNormalized);

  if (!Number.isInteger(totalQty) || totalQty <= 0) {
    return clearDistribution(clientId, mpAccountId, barcodeNormalized, triggerType);
  }

  const warehouses = await getEnabledWarehouses(clientId, mpAccountId);

  if (!warehouses.length) {
    throw new Error(
      `Нет активных складов для распределения: client_id=${clientId}, mp_account_id=${mpAccountId}`
    );
  }

  const distributionRows = buildFairDistribution(totalQty, warehouses);
  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await replaceDistributionRows({
      client,
      clientId,
      mpAccountId,
      barcode: barcodeNormalized,
      distributionRows
    });

    await safeInsertDistributionRun({
      client,
      clientId,
      mpAccountId,
      triggerType,
      status: 'success',
      itemsCount: distributionRows.length,
      note: `barcode=${barcodeNormalized}; total_qty=${totalQty}; distributed_qty=${distributionRows.reduce((sum, x) => sum + Number(x.qty || 0), 0)}`
    });

    await client.query('COMMIT');

    return {
      success: true,
      client_id: clientId,
      mp_account_id: mpAccountId,
      barcode: barcodeNormalized,
      total_qty: totalQty,
      distributed_qty: distributionRows.reduce((sum, x) => sum + Number(x.qty || 0), 0),
      items: distributionRows
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function recalcAllDistributionFromStock({
  clientId,
  mpAccountId,
  triggerType = 'manual_bulk'
}) {
  const stockTotals = await getAllClientStockTotals(clientId);
  const existingBarcodes = await getExistingDistributionBarcodes(clientId, mpAccountId);

  const stockMap = new Map(
    stockTotals.map((row) => [String(row.barcode), Number(row.total_qty || 0)])
  );

  const allBarcodesSet = new Set([
    ...existingBarcodes.map((x) => String(x)),
    ...stockTotals.map((x) => String(x.barcode))
  ]);

  const allBarcodes = Array.from(allBarcodesSet).sort((a, b) => a.localeCompare(b, 'ru'));

  const results = [];
  let itemsProcessed = 0;
  let itemsWithStock = 0;
  let itemsCleared = 0;
  let totalFactQty = 0;
  let totalDistributedQty = 0;

  for (const barcode of allBarcodes) {
    const factQty = Number(stockMap.get(barcode) || 0);

    const result = await recalcDistributionFromStock({
      clientId,
      mpAccountId,
      barcode,
      triggerType
    });

    results.push(result);
    itemsProcessed += 1;
    totalFactQty += factQty;
    totalDistributedQty += Number(result.distributed_qty || 0);

    if (factQty > 0) {
      itemsWithStock += 1;
    } else {
      itemsCleared += 1;
    }
  }

  return {
    success: true,
    client_id: clientId,
    mp_account_id: mpAccountId,
    trigger_type: triggerType,
    items_processed: itemsProcessed,
    items_with_stock: itemsWithStock,
    items_cleared: itemsCleared,
    total_fact_qty: totalFactQty,
    total_distributed_qty: totalDistributedQty,
    results
  };
}

module.exports = {
  getTotalQtyFromStock,
  getAllClientStockTotals,
  recalcDistributionFromStock,
  recalcAllDistributionFromStock
};