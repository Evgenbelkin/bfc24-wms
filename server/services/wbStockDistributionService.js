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

async function getEligibleWarehouses(clientId, mpAccountId) {
  const sql = `
    SELECT
      warehouse_code,
      warehouse_name,
      weight
    FROM wms.client_wb_warehouses
    WHERE client_id = $1
      AND mp_account_id = $2
      AND is_active = TRUE
      AND is_enabled_for_distribution = TRUE
      AND weight > 0
    ORDER BY warehouse_name ASC, warehouse_code ASC
  `;

  const { rows } = await pool.query(sql, [clientId, mpAccountId]);

  return rows.map((r) => ({
    warehouse_code: String(r.warehouse_code),
    warehouse_name: r.warehouse_name,
    weight: Number(r.weight)
  }));
}

function allocateByWeights(totalQty, warehouses) {
  if (!Number.isInteger(totalQty) || totalQty < 0) {
    throw new Error('total_qty должен быть целым числом >= 0');
  }

  if (!Array.isArray(warehouses) || warehouses.length === 0) {
    throw new Error('Нет складов для распределения');
  }

  const totalWeight = warehouses.reduce((sum, w) => sum + Number(w.weight || 0), 0);
  if (!(totalWeight > 0)) {
    throw new Error('Сумма весов должна быть больше 0');
  }

  const prepared = warehouses.map((w) => {
    const exact = (totalQty * Number(w.weight || 0)) / totalWeight;
    const baseQty = Math.floor(exact);
    const remainderPart = exact - baseQty;

    return {
      warehouse_code: String(w.warehouse_code),
      warehouse_name: w.warehouse_name,
      weight: Number(w.weight || 0),
      qty: baseQty,
      remainder_part: remainderPart
    };
  });

  let distributed = prepared.reduce((sum, x) => sum + x.qty, 0);
  let remainder = totalQty - distributed;

  prepared.sort((a, b) => {
    if (b.remainder_part !== a.remainder_part) {
      return b.remainder_part - a.remainder_part;
    }
    if (b.weight !== a.weight) {
      return b.weight - a.weight;
    }
    return String(a.warehouse_code).localeCompare(String(b.warehouse_code), 'ru');
  });

  for (let i = 0; i < prepared.length && remainder > 0; i += 1) {
    prepared[i].qty += 1;
    remainder -= 1;

    if (i === prepared.length - 1 && remainder > 0) {
      i = -1;
    }
  }

  prepared.sort((a, b) =>
    String(a.warehouse_name || '').localeCompare(String(b.warehouse_name || ''), 'ru') ||
    String(a.warehouse_code).localeCompare(String(b.warehouse_code), 'ru')
  );

  return prepared.map((x) => ({
    warehouse_code: x.warehouse_code,
    warehouse_name: x.warehouse_name,
    weight: x.weight,
    qty: x.qty
  }));
}

async function recalculateDistribution({
  clientId,
  mpAccountId,
  barcode,
  totalQty,
  triggerType = 'manual'
}) {
  const barcodeNormalized = String(barcode || '').trim();

  if (!barcodeNormalized) {
    throw new Error('barcode обязателен');
  }

  if (!Number.isInteger(totalQty) || totalQty < 0) {
    throw new Error('total_qty должен быть целым числом >= 0');
  }

  const warehouses = await getEligibleWarehouses(clientId, mpAccountId);
  const allocation = allocateByWeights(totalQty, warehouses);

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

    for (const row of allocation) {
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
          barcodeNormalized,
          row.warehouse_code,
          row.qty
        ]
      );
    }

    await safeInsertDistributionRun({
      client,
      clientId,
      mpAccountId,
      triggerType,
      status: 'success',
      itemsCount: allocation.length,
      note: `barcode=${barcodeNormalized}; total_qty=${totalQty}`
    });

    await client.query('COMMIT');

    return {
      success: true,
      client_id: clientId,
      mp_account_id: mpAccountId,
      barcode: barcodeNormalized,
      total_qty: totalQty,
      distributed_qty: allocation.reduce((sum, x) => sum + x.qty, 0),
      items: allocation
    };
  } catch (err) {
    await client.query('ROLLBACK');

    try {
      const fallbackClient = await pool.connect();
      try {
        await safeInsertDistributionRun({
          client: fallbackClient,
          clientId,
          mpAccountId,
          triggerType,
          status: 'error',
          itemsCount: 0,
          note: String(err.message || err)
        });
      } finally {
        fallbackClient.release();
      }
    } catch (_) {}

    throw err;
  } finally {
    client.release();
  }
}

async function getDistribution({ clientId, mpAccountId, barcode }) {
  const barcodeNormalized = String(barcode || '').trim();

  if (!barcodeNormalized) {
    throw new Error('barcode обязателен');
  }

  const { rows } = await pool.query(
    `
    SELECT
      d.client_id,
      d.mp_account_id,
      d.barcode,
      d.warehouse_code,
      w.warehouse_name,
      w.weight,
      w.is_active,
      w.is_enabled_for_distribution,
      d.qty,
      d.calculated_at,
      d.updated_at
    FROM wms.client_stock_distribution d
    LEFT JOIN wms.client_wb_warehouses w
      ON w.client_id = d.client_id
     AND w.mp_account_id = d.mp_account_id
     AND w.warehouse_code = d.warehouse_code
    WHERE d.client_id = $1
      AND d.mp_account_id = $2
      AND d.barcode = $3
    ORDER BY w.warehouse_name ASC, d.warehouse_code ASC
    `,
    [clientId, mpAccountId, barcodeNormalized]
  );

  return {
    success: true,
    client_id: clientId,
    mp_account_id: mpAccountId,
    barcode: barcodeNormalized,
    total_qty: rows.reduce((sum, r) => sum + Number(r.qty || 0), 0),
    items: rows.map((r) => ({
      warehouse_code: r.warehouse_code,
      warehouse_name: r.warehouse_name,
      weight: Number(r.weight || 0),
      qty: Number(r.qty || 0),
      is_active: r.is_active,
      is_enabled_for_distribution: r.is_enabled_for_distribution,
      calculated_at: r.calculated_at,
      updated_at: r.updated_at
    }))
  };
}

module.exports = {
  getEligibleWarehouses,
  allocateByWeights,
  recalculateDistribution,
  getDistribution
};