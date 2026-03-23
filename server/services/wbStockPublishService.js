const { pool } = require('../db');

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

async function getMpAccountForPublish(clientId, mpAccountId) {
  const { rows } = await pool.query(
    `
    SELECT
      id,
      marketplace,
      api_token,
      is_active,
      wms_client_id
    FROM public.mp_accounts
    WHERE id = $1
      AND wms_client_id = $2
    LIMIT 1
    `,
    [mpAccountId, clientId]
  );

  if (!rows[0]) {
    throw new Error(`mp_account_id=${mpAccountId} не найден для client_id=${clientId}`);
  }

  const acc = rows[0];

  if (
    String(acc.marketplace || '').toLowerCase() !== 'wildberries' &&
    String(acc.marketplace || '').toLowerCase() !== 'wb'
  ) {
    throw new Error(`mp_account_id=${mpAccountId} не является аккаунтом WB`);
  }

  if (acc.is_active === false) {
    throw new Error(`mp_account_id=${mpAccountId} неактивен`);
  }

  if (!String(acc.api_token || '').trim()) {
    throw new Error(`У mp_account_id=${mpAccountId} отсутствует api_token`);
  }

  return acc;
}

async function getWarehouseMap(clientId, mpAccountId) {
  const { rows } = await pool.query(
    `
    SELECT
      warehouse_code,
      warehouse_name,
      wb_warehouse_id,
      is_active,
      COALESCE(is_enabled_for_distribution, false) AS is_enabled_for_distribution,
      COALESCE(weight, 0) AS weight
    FROM wms.client_wb_warehouses
    WHERE client_id = $1
      AND mp_account_id = $2
    `,
    [clientId, mpAccountId]
  );

  const map = new Map();

  for (const row of rows) {
    const warehouseCode = String(row.warehouse_code || '').trim();
    if (!warehouseCode) continue;

    map.set(warehouseCode, {
      warehouse_code: warehouseCode,
      warehouse_name: row.warehouse_name || null,
      wb_warehouse_id: row.wb_warehouse_id ? Number(row.wb_warehouse_id) : null,
      is_active: !!row.is_active,
      is_enabled_for_distribution: !!row.is_enabled_for_distribution,
      weight: Number(row.weight || 0)
    });
  }

  return map;
}

async function getChrtMap(clientId, mpAccountId) {
  const { rows } = await pool.query(
    `
    SELECT
      ib.barcode,
      ib.chrt_id,
      ib.nm_id,
      COALESCE(i.item_name, i.title, i.vendor_code, ib.barcode) AS item_name
    FROM public.mp_wb_items_barcodes ib
    JOIN public.mp_client_accounts mca
      ON mca.id = ib.client_mp_account_id
    JOIN public.mp_accounts ma
      ON ma.supplier_id = mca.wb_supplier_id
     AND LOWER(ma.marketplace) IN ('wb', 'wildberries')
    LEFT JOIN public.mp_wb_items i
      ON i.client_mp_account_id = ib.client_mp_account_id
     AND i.nm_id = ib.nm_id
    WHERE ma.id = $1
      AND ma.wms_client_id = $2
      AND ib.barcode IS NOT NULL
      AND ib.chrt_id IS NOT NULL
    ORDER BY ib.nm_id, ib.chrt_id
    `,
    [mpAccountId, clientId]
  );

  const map = new Map();

  for (const row of rows) {
    const barcode = String(row.barcode || '').trim();
    if (!barcode) continue;

    if (!map.has(barcode)) {
      map.set(barcode, {
        barcode,
        chrt_id: Number(row.chrt_id),
        nm_id: row.nm_id ? Number(row.nm_id) : null,
        item_name: row.item_name || null
      });
    }
  }

  return map;
}

/**
 * Берём ВСЕ строки распределения, включая qty = 0.
 * Это важно для сценария, когда склад надо обнулить.
 */
async function getDistributionRows(clientId, mpAccountId) {
  const { rows } = await pool.query(
    `
    SELECT
      d.barcode,
      d.warehouse_code,
      d.qty,
      d.calculated_at,
      i.item_name
    FROM wms.client_stock_distribution d
    LEFT JOIN masterdata.items i
      ON i.client_id = d.client_id
     AND i.barcode = d.barcode
    WHERE d.client_id = $1
      AND d.mp_account_id = $2
    ORDER BY d.barcode, d.warehouse_code
    `,
    [clientId, mpAccountId]
  );

  return rows.map((r) => ({
    barcode: String(r.barcode || '').trim(),
    warehouse_code: String(r.warehouse_code || '').trim(),
    qty: Number(r.qty || 0),
    calculated_at: r.calculated_at || null,
    item_name: r.item_name || null
  }));
}

/**
 * Получаем список всех barcode, которые вообще участвуют в текущем распределении клиента.
 * Даже если по конкретному складу qty=0, SKU должен попасть в payload для обнуления.
 */
function getAllBarcodesFromDistribution(distributionRows) {
  const set = new Set();

  for (const row of distributionRows) {
    const barcode = String(row.barcode || '').trim();
    if (!barcode) continue;
    set.add(barcode);
  }

  return Array.from(set.values()).sort((a, b) => a.localeCompare(b, 'ru'));
}

/**
 * Строим индекс:
 * key = `${barcode}__${warehouse_code}` -> qty
 */
function buildDistributionIndex(distributionRows) {
  const map = new Map();

  for (const row of distributionRows) {
    const barcode = String(row.barcode || '').trim();
    const warehouseCode = String(row.warehouse_code || '').trim();
    if (!barcode || !warehouseCode) continue;

    map.set(`${barcode}__${warehouseCode}`, {
      qty: Number(row.qty || 0),
      item_name: row.item_name || null,
      calculated_at: row.calculated_at || null
    });
  }

  return map;
}

/**
 * Формируем payload по всем складам:
 * - активный склад с weight>0 и is_enabled=true получает рассчитанные значения
 * - неактивный для распределения / weight=0 получает amount=0 по всем SKU
 *
 * Таким образом мы умеем "обнулять" отключённые склады.
 */
function buildStocksPayload({ warehouseMap, chrtMap, distributionRows }) {
  const skipped = [];
  const payloads = [];

  const allBarcodes = getAllBarcodesFromDistribution(distributionRows);
  const distributionIndex = buildDistributionIndex(distributionRows);

  for (const [warehouseCode, wh] of warehouseMap.entries()) {
    if (!wh.wb_warehouse_id) {
      skipped.push({
        warehouse_code: warehouseCode,
        warehouse_name: wh.warehouse_name || null,
        reason: 'у склада отсутствует wb_warehouse_id'
      });
      continue;
    }

    const warehouseId = Number(wh.wb_warehouse_id);
    const warehouseIsEnabled =
      !!wh.is_active &&
      !!wh.is_enabled_for_distribution &&
      Number(wh.weight || 0) > 0;

    const stocks = [];

    for (const barcode of allBarcodes) {
      const item = chrtMap.get(barcode);

      if (!item) {
        skipped.push({
          barcode,
          warehouse_code: warehouseCode,
          qty: 0,
          reason: 'barcode не найден в public.mp_wb_items_barcodes или отсутствует chrt_id'
        });
        continue;
      }

      let amount = 0;
      let itemName = item.item_name || null;

      if (warehouseIsEnabled) {
        const hit = distributionIndex.get(`${barcode}__${warehouseCode}`);
        amount = hit ? Number(hit.qty || 0) : 0;
        if (hit?.item_name) {
          itemName = hit.item_name;
        }
      } else {
        amount = 0;
      }

      stocks.push({
        chrtId: Number(item.chrt_id),
        amount,
        barcode,
        item_name: itemName
      });
    }

    payloads.push({
      warehouse_id: warehouseId,
      warehouse_code: wh.warehouse_code,
      warehouse_name: wh.warehouse_name,
      is_active: !!wh.is_active,
      is_enabled_for_distribution: !!wh.is_enabled_for_distribution,
      weight: Number(wh.weight || 0),
      zero_mode: !warehouseIsEnabled,
      body: {
        stocks: stocks.map((s) => ({
          chrtId: s.chrtId,
          amount: s.amount
        }))
      },
      preview: stocks
    });
  }

  payloads.sort((a, b) => {
    return (
      String(a.warehouse_name || '').localeCompare(String(b.warehouse_name || ''), 'ru') ||
      String(a.warehouse_code || '').localeCompare(String(b.warehouse_code || ''), 'ru')
    );
  });

  return { payloads, skipped };
}

async function wbPutStocks({ apiToken, warehouseId, body }) {
  const url = `https://marketplace-api.wildberries.ru/api/v3/stocks/${warehouseId}`;

  const response = await fetch(url, {
    method: 'PUT',
    headers: {
      Authorization: String(apiToken).trim(),
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });

  const text = await response.text();
  let parsed = null;

  try {
    parsed = text ? JSON.parse(text) : null;
  } catch {
    parsed = text || null;
  }

  if (!response.ok) {
    throw new Error(
      `WB stocks publish failed for warehouseId=${warehouseId}: ${response.status} ${response.statusText}. Body: ${
        typeof parsed === 'string' ? parsed : JSON.stringify(parsed)
      }`
    );
  }

  return {
    ok: true,
    warehouse_id: warehouseId,
    response: parsed
  };
}

async function publishDistributionToWb({
  clientId,
  mpAccountId,
  dryRun = true
}) {
  const account = await getMpAccountForPublish(clientId, mpAccountId);

  const [warehouseMap, chrtMap, distributionRows] = await Promise.all([
    getWarehouseMap(clientId, mpAccountId),
    getChrtMap(clientId, mpAccountId),
    getDistributionRows(clientId, mpAccountId)
  ]);

  const { payloads, skipped } = buildStocksPayload({
    distributionRows,
    warehouseMap,
    chrtMap
  });

  const zeroModeWarehouses = payloads.filter((p) => p.zero_mode).length;
  const totalLines = payloads.reduce((sum, p) => sum + p.body.stocks.length, 0);
  const zeroAmountLines = payloads.reduce(
    (sum, p) => sum + p.body.stocks.filter((x) => Number(x.amount || 0) === 0).length,
    0
  );

  const summary = {
    client_id: Number(clientId),
    mp_account_id: Number(mpAccountId),
    dry_run: !!dryRun,
    warehouses_total: payloads.length,
    warehouses_zero_mode: zeroModeWarehouses,
    rows_total: distributionRows.length,
    rows_skipped: skipped.length,
    rows_ready: totalLines,
    zero_amount_lines: zeroAmountLines
  };

  logBlock('[wbStockPublishService][build]', {
    summary,
    payloads_preview: payloads.map((p) => ({
      warehouse_id: p.warehouse_id,
      warehouse_code: p.warehouse_code,
      warehouse_name: p.warehouse_name,
      is_enabled_for_distribution: p.is_enabled_for_distribution,
      weight: p.weight,
      zero_mode: p.zero_mode,
      lines_total: p.body.stocks.length,
      zero_lines: p.body.stocks.filter((x) => Number(x.amount || 0) === 0).length
    })),
    skipped
  });

  if (dryRun) {
    return {
      ok: true,
      mode: 'dry_run',
      summary,
      payloads,
      skipped
    };
  }

  const results = [];

  for (const payload of payloads) {
    logBlock('[wbStockPublishService][publish][request]', {
      warehouse_id: payload.warehouse_id,
      warehouse_code: payload.warehouse_code,
      warehouse_name: payload.warehouse_name,
      zero_mode: payload.zero_mode,
      body: payload.body,
      preview: payload.preview
    });

    const result = await wbPutStocks({
      apiToken: account.api_token,
      warehouseId: payload.warehouse_id,
      body: payload.body
    });

    logBlock('[wbStockPublishService][publish][response]', {
      warehouse_id: payload.warehouse_id,
      warehouse_code: payload.warehouse_code,
      warehouse_name: payload.warehouse_name,
      zero_mode: payload.zero_mode,
      response: result.response
    });

    results.push({
      warehouse_id: payload.warehouse_id,
      warehouse_code: payload.warehouse_code,
      warehouse_name: payload.warehouse_name,
      zero_mode: payload.zero_mode,
      rows_sent: payload.body.stocks.length,
      response: result.response
    });
  }

  return {
    ok: true,
    mode: 'publish',
    summary,
    results,
    skipped
  };
}

module.exports = {
  publishDistributionToWb
};