const { pool } = require('../db');

function normalizeBoolean(value) {
  return value === true;
}

function normalizeWarehouseRow(row) {
  const wbWarehouseId = row.id ?? row.warehouseId ?? null;

  return {
    warehouse_id: wbWarehouseId,
    wb_warehouse_id: wbWarehouseId,
    warehouse_code: String(
      row.id ??
      row.warehouseId ??
      row.warehouseCode ??
      ''
    ).trim(),
    warehouse_name: String(
      row.name ??
      row.warehouseName ??
      row.officeName ??
      row.address ??
      'Без названия'
    ).trim(),
    is_active: !normalizeBoolean(row.isDeleting),
    is_deleting: normalizeBoolean(row.isDeleting),
    is_processing: normalizeBoolean(row.isProcessing),
    raw_json: row
  };
}

async function fetchWbSellerWarehouses(apiToken) {
  const url = 'https://marketplace-api.wildberries.ru/api/v3/warehouses';

  const response = await fetch(url, {
    method: 'GET',
    headers: {
      Authorization: apiToken,
      'Content-Type': 'application/json'
    }
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `WB warehouses fetch failed: ${response.status} ${response.statusText}. Body: ${text}`
    );
  }

  let data;
  try {
    data = JSON.parse(text);
  } catch (err) {
    throw new Error(`WB warehouses response is not valid JSON: ${text}`);
  }

  if (!Array.isArray(data)) {
    throw new Error(`WB warehouses response is not an array: ${JSON.stringify(data)}`);
  }

  return data.map(normalizeWarehouseRow).filter(x => x.warehouse_code);
}

async function getMpAccountById(mpAccountId) {
  const sql = `
    SELECT
      id,
      marketplace,
      label,
      api_token,
      is_active,
      wms_client_id
    FROM public.mp_accounts
    WHERE id = $1
    LIMIT 1
  `;
  const { rows } = await pool.query(sql, [mpAccountId]);
  return rows[0] || null;
}

async function checkAccountLinkedToClient(clientId, mpAccountId) {
  try {
    const sql1 = `
      SELECT 1
      FROM wms.client_mp_accounts
      WHERE client_id = $1
        AND mp_account_id = $2
      LIMIT 1
    `;
    const res1 = await pool.query(sql1, [clientId, mpAccountId]);
    if (res1.rows.length > 0) return true;
  } catch (err) {
    console.log('[checkAccountLinkedToClient] skip wms.client_mp_accounts:', err.message);
  }

  try {
    const sql2 = `
      SELECT 1
      FROM public.mp_client_accounts
      WHERE client_id = $1
        AND mp_account_id = $2
      LIMIT 1
    `;
    const res2 = await pool.query(sql2, [clientId, mpAccountId]);
    if (res2.rows.length > 0) return true;
  } catch (err) {
    console.log('[checkAccountLinkedToClient] skip public.mp_client_accounts:', err.message);
  }

  return false;
}

async function ensureAccountAccess(clientId, mpAccountId) {
  const account = await getMpAccountById(mpAccountId);

  if (!account) {
    throw new Error(`mp_account_id=${mpAccountId} не найден`);
  }

  if (String(account.marketplace || '').toLowerCase() !== 'wildberries' &&
      String(account.marketplace || '').toLowerCase() !== 'wb') {
    throw new Error(`mp_account_id=${mpAccountId} не является аккаунтом WB`);
  }

  if (account.is_active === false) {
    throw new Error(`mp_account_id=${mpAccountId} неактивен`);
  }

  if (Number(account.wms_client_id) !== Number(clientId)) {
    const isLinked = await checkAccountLinkedToClient(clientId, mpAccountId);
    if (!isLinked) {
      throw new Error(`mp_account_id=${mpAccountId} не привязан к client_id=${clientId}`);
    }
  }

  return account;
}

async function syncWbSellerWarehouses({ clientId, mpAccountId, userId }) {
  const account = await ensureAccountAccess(clientId, mpAccountId);

  if (!account.api_token || !String(account.api_token).trim()) {
    throw new Error(`У mp_account_id=${mpAccountId} отсутствует api_token`);
  }

  const wbWarehouses = await fetchWbSellerWarehouses(account.api_token.trim());

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const existingRes = await client.query(
      `
      SELECT id, warehouse_code, wb_warehouse_id
      FROM wms.client_wb_warehouses
      WHERE client_id = $1
        AND mp_account_id = $2
      `,
      [clientId, mpAccountId]
    );

    const existingMap = new Map(
      existingRes.rows.map(r => [String(r.warehouse_code), r])
    );

    const incomingCodes = new Set();

    for (const wh of wbWarehouses) {
      incomingCodes.add(wh.warehouse_code);

      const existing = existingMap.get(wh.warehouse_code);

      if (existing) {
        await client.query(
          `
          UPDATE wms.client_wb_warehouses
          SET
            wb_warehouse_id = $1,
            warehouse_name = $2,
            is_active = $3,
            source = 'wb_api',
            last_synced_at = NOW(),
            updated_at = NOW()
          WHERE id = $4
          `,
          [
            wh.wb_warehouse_id,
            wh.warehouse_name,
            wh.is_active,
            existing.id
          ]
        );
      } else {
        await client.query(
          `
          INSERT INTO wms.client_wb_warehouses (
            client_id,
            mp_account_id,
            wb_warehouse_id,
            warehouse_code,
            warehouse_name,
            is_active,
            is_enabled_for_distribution,
            weight,
            source,
            last_synced_at,
            created_at,
            updated_at
          )
          VALUES ($1, $2, $3, $4, $5, $6, TRUE, 1.0000, 'wb_api', NOW(), NOW(), NOW())
          `,
          [
            clientId,
            mpAccountId,
            wh.wb_warehouse_id,
            wh.warehouse_code,
            wh.warehouse_name,
            wh.is_active
          ]
        );
      }
    }

    for (const row of existingRes.rows) {
      if (!incomingCodes.has(String(row.warehouse_code))) {
        await client.query(
          `
          UPDATE wms.client_wb_warehouses
          SET
            is_active = FALSE,
            last_synced_at = NOW(),
            updated_at = NOW()
          WHERE id = $1
          `,
          [row.id]
        );
      }
    }

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
      VALUES ($1, $2, 'manual_sync_warehouses', 'success', $3, $4, NOW())
      `,
      [
        clientId,
        mpAccountId,
        wbWarehouses.length,
        `sync wb warehouses by user_id=${userId || 'null'}`
      ]
    );

    await client.query('COMMIT');

    return {
      success: true,
      client_id: Number(clientId),
      mp_account_id: Number(mpAccountId),
      synced_count: wbWarehouses.length,
      warehouses: wbWarehouses.map(x => ({
        wb_warehouse_id: x.wb_warehouse_id,
        warehouse_code: x.warehouse_code,
        warehouse_name: x.warehouse_name,
        is_active: x.is_active,
        is_deleting: x.is_deleting,
        is_processing: x.is_processing
      }))
    };
  } catch (err) {
    await client.query('ROLLBACK');

    await pool.query(
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
      VALUES ($1, $2, 'manual_sync_warehouses', 'error', 0, $3, NOW())
      `,
      [
        clientId,
        mpAccountId,
        String(err.message || err).slice(0, 1000)
      ]
    ).catch(() => {});

    throw err;
  } finally {
    client.release();
  }
}

async function listClientWbWarehouses({ clientId, mpAccountId }) {
  const { rows } = await pool.query(
    `
    SELECT
      id,
      client_id,
      mp_account_id,
      wb_warehouse_id,
      warehouse_code,
      warehouse_name,
      is_active,
      is_enabled_for_distribution,
      weight,
      source,
      last_synced_at,
      created_at,
      updated_at
    FROM wms.client_wb_warehouses
    WHERE client_id = $1
      AND ($2::bigint IS NULL OR mp_account_id = $2)
    ORDER BY is_active DESC, warehouse_name ASC, warehouse_code ASC
    `,
    [clientId, mpAccountId || null]
  );

  return rows;
}

async function updateClientWbWarehouseSettings({
  clientId,
  mpAccountId,
  warehouseCode,
  weight,
  isEnabledForDistribution
}) {
  await ensureAccountAccess(clientId, mpAccountId);

  const warehouseCodeNormalized = String(warehouseCode || '').trim();
  if (!warehouseCodeNormalized) {
    throw new Error('warehouse_code обязателен');
  }

  const updates = [];
  const params = [];
  let idx = 1;

  if (weight !== undefined) {
    const weightNum = Number(weight);
    if (!Number.isFinite(weightNum) || weightNum < 0) {
      throw new Error('weight должен быть числом >= 0');
    }
    updates.push(`weight = $${idx++}`);
    params.push(weightNum);
  }

  if (isEnabledForDistribution !== undefined) {
    if (typeof isEnabledForDistribution !== 'boolean') {
      throw new Error('is_enabled_for_distribution должен быть boolean');
    }
    updates.push(`is_enabled_for_distribution = $${idx++}`);
    params.push(isEnabledForDistribution);
  }

  if (updates.length === 0) {
    throw new Error('Нет данных для обновления');
  }

  updates.push(`updated_at = NOW()`);

  params.push(clientId);
  const clientIdParam = idx++;
  params.push(mpAccountId);
  const mpAccountIdParam = idx++;
  params.push(warehouseCodeNormalized);
  const warehouseCodeParam = idx++;

  const sql = `
    UPDATE wms.client_wb_warehouses
    SET ${updates.join(', ')}
    WHERE client_id = $${clientIdParam}
      AND mp_account_id = $${mpAccountIdParam}
      AND warehouse_code = $${warehouseCodeParam}
    RETURNING
      id,
      client_id,
      mp_account_id,
      wb_warehouse_id,
      warehouse_code,
      warehouse_name,
      is_active,
      is_enabled_for_distribution,
      weight,
      source,
      last_synced_at,
      created_at,
      updated_at
  `;

  const { rows } = await pool.query(sql, params);

  if (!rows[0]) {
    throw new Error(
      `Склад warehouse_code=${warehouseCodeNormalized} не найден для client_id=${clientId}, mp_account_id=${mpAccountId}`
    );
  }

  return rows[0];
}

module.exports = {
  fetchWbSellerWarehouses,
  syncWbSellerWarehouses,
  listClientWbWarehouses,
  updateClientWbWarehouseSettings
};