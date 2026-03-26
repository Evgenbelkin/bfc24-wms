const { pool } = require('../db');

function isValidDateOnly(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''));
}

function normalizePositiveInt(value, fieldName) {
  const num = Number(value);
  if (!Number.isInteger(num) || num <= 0) {
    throw new Error(`Поле ${fieldName} должно быть положительным целым числом`);
  }
  return num;
}

function resolveClientAccess(reqUser, requestedClientId) {
  if (!reqUser) {
    throw new Error('Пользователь не авторизован');
  }

  if (!['owner', 'admin'].includes(reqUser.role)) {
    throw new Error('Недостаточно прав доступа');
  }

  if (!requestedClientId) {
    throw new Error('Обязателен параметр client_id');
  }

  return normalizePositiveInt(requestedClientId, 'client_id');
}

async function normalizeWbOrders(reqUser, body) {
  const clientId = resolveClientAccess(reqUser, body.client_id);

  if (!body.mp_account_id) {
    throw new Error('Обязателен параметр mp_account_id');
  }
  const mpAccountId = normalizePositiveInt(body.mp_account_id, 'mp_account_id');

  if (!body.date_from || !isValidDateOnly(body.date_from)) {
    throw new Error('Обязателен параметр date_from в формате YYYY-MM-DD');
  }

  if (!body.date_to || !isValidDateOnly(body.date_to)) {
    throw new Error('Обязателен параметр date_to в формате YYYY-MM-DD');
  }

  if (body.date_from > body.date_to) {
    throw new Error('date_from не может быть больше date_to');
  }

  const client = await pool.connect();

  try {
    await client.query('BEGIN');

    await client.query(
      `
      DELETE FROM analytics.wb_orders_normalized
      WHERE client_id = $1
        AND client_mp_account_id = $2
        AND order_date >= $3::date
        AND order_date <= $4::date
      `,
      [clientId, mpAccountId, body.date_from, body.date_to]
    );

    const insertSql = `
      INSERT INTO analytics.wb_orders_normalized (
        raw_id,
        client_id,
        client_mp_account_id,
        event_datetime,
        event_date,
        order_datetime,
        order_date,
        wb_order_id,
        rid,
        nm_id,
        chrt_id,
        article,
        barcode,
        warehouse_name,
        region_name,
        status_raw,
        qty,
        price_raw,
        converted_price_raw,
        final_price_raw,
        converted_final_price_raw,
        order_amount,
        is_order,
        raw
      )
      SELECT
        r.id AS raw_id,
        r.client_id,
        r.client_mp_account_id,

        COALESCE(r.event_datetime, r.order_datetime, r.created_at) AS event_datetime,
        COALESCE(r.event_datetime, r.order_datetime, r.created_at)::date AS event_date,

        COALESCE(r.order_datetime, r.event_datetime, r.created_at) AS order_datetime,
        COALESCE(r.order_datetime, r.event_datetime, r.created_at)::date AS order_date,

        r.source_order_id AS wb_order_id,
        r.source_rid AS rid,

        r.source_nm_id AS nm_id,
        r.source_chrt_id AS chrt_id,
        r.article,
        r.barcode,
        r.warehouse_name,
        r.region_name,
        r.status_raw,

        1 AS qty,

        r.price_raw,
        r.converted_price_raw,
        r.final_price_raw,
        r.converted_final_price_raw,

        COALESCE(
          r.converted_final_price_raw,
          r.final_price_raw,
          r.converted_price_raw,
          r.price_raw,
          0
        ) AS order_amount,

        TRUE AS is_order,
        r.raw
      FROM analytics.wb_orders_raw r
      WHERE r.client_id = $1
        AND r.client_mp_account_id = $2
        AND COALESCE(r.order_datetime, r.event_datetime, r.created_at)::date >= $3::date
        AND COALESCE(r.order_datetime, r.event_datetime, r.created_at)::date <= $4::date
    `;

    const insertRes = await client.query(insertSql, [
      clientId,
      mpAccountId,
      body.date_from,
      body.date_to,
    ]);

    await client.query('COMMIT');

    return {
      ok: true,
      normalized_rows: Number(insertRes.rowCount || 0),
      filters: {
        client_id: clientId,
        mp_account_id: mpAccountId,
        date_from: body.date_from,
        date_to: body.date_to,
      },
    };
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackError) {
      console.error('[wbOrdersNormalizeService] rollback error:', rollbackError);
    }
    throw error;
  } finally {
    client.release();
  }
}

module.exports = {
  normalizeWbOrders,
};