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

async function normalizeWbSales(reqUser, body) {
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
      DELETE FROM analytics.wb_sales_normalized
      WHERE client_id = $1
        AND client_mp_account_id = $2
        AND event_date >= $3::date
        AND event_date <= $4::date
      `,
      [clientId, mpAccountId, body.date_from, body.date_to]
    );

    const insertSql = `
      INSERT INTO analytics.wb_sales_normalized (
        raw_id,
        client_id,
        client_mp_account_id,
        report_type,
        event_type,
        status_raw,
        status_normalized,
        event_datetime,
        event_date,
        sale_datetime,
        cancel_datetime,
        return_datetime,
        wb_order_id,
        wb_sale_id,
        rid,
        nm_id,
        chrt_id,
        article,
        barcode,
        subject,
        brand,
        warehouse_name,
        region_name,
        country_name,
        qty,
        price_raw,
        sale_price_raw,
        final_price_raw,
        discount_percent_raw,
        spp_raw,
        for_pay_raw,
        finished_price_raw,
        amount_gross,
        amount_net,
        is_sale,
        is_return,
        is_cancel,
        is_order,
        raw
      )
      SELECT
        r.id AS raw_id,
        r.client_id,
        r.client_mp_account_id,
        r.report_type,

        CASE
          WHEN COALESCE((r.raw->>'saleID'), '') ILIKE 'R%%' THEN 'return'
          WHEN r.report_type = 'sales' THEN 'sale'
          ELSE 'unknown'
        END AS event_type,

        r.status_raw,

        CASE
          WHEN COALESCE((r.raw->>'saleID'), '') ILIKE 'R%%' THEN 'return'
          WHEN r.report_type = 'sales' THEN 'sale'
          ELSE 'unknown'
        END AS status_normalized,

        COALESCE(
          r.sale_datetime,
          r.event_datetime,
          NULLIF(r.raw->>'lastChangeDate', '')::timestamptz,
          NULLIF(r.raw->>'date', '')::timestamptz,
          r.created_at
        ) AS event_datetime,

        (
          COALESCE(
            r.sale_datetime,
            r.event_datetime,
            NULLIF(r.raw->>'lastChangeDate', '')::timestamptz,
            NULLIF(r.raw->>'date', '')::timestamptz,
            r.created_at
          )
        )::date AS event_date,

        COALESCE(
          r.sale_datetime,
          NULLIF(r.raw->>'saleDate', '')::timestamptz,
          NULLIF(r.raw->>'date', '')::timestamptz
        ) AS sale_datetime,

        NULL::timestamptz AS cancel_datetime,

        CASE
          WHEN COALESCE((r.raw->>'saleID'), '') ILIKE 'R%%'
            THEN COALESCE(
              r.event_datetime,
              NULLIF(r.raw->>'lastChangeDate', '')::timestamptz,
              NULLIF(r.raw->>'date', '')::timestamptz,
              r.created_at
            )
          ELSE NULL::timestamptz
        END AS return_datetime,

        COALESCE(
          NULLIF(r.source_order_id, ''),
          NULLIF(r.raw->>'odid', ''),
          NULLIF(r.raw->>'srid', ''),
          NULLIF(r.raw->>'gNumber', '')
        ) AS wb_order_id,

        COALESCE(
          NULLIF(r.source_sale_id, ''),
          NULLIF(r.raw->>'saleID', '')
        ) AS wb_sale_id,

        COALESCE(
          NULLIF(r.source_rid, ''),
          NULLIF(r.raw->>'srid', '')
        ) AS rid,

        COALESCE(
          r.source_nm_id,
          NULLIF(r.raw->>'nmId', '')::bigint
        ) AS nm_id,

        COALESCE(
          r.source_chrt_id,
          NULLIF(r.raw->>'chrtId', '')::bigint
        ) AS chrt_id,

        COALESCE(
          NULLIF(r.article, ''),
          NULLIF(r.raw->>'supplierArticle', ''),
          NULLIF(r.raw->>'saName', '')
        ) AS article,

        COALESCE(
          NULLIF(r.barcode, ''),
          NULLIF(r.raw->>'barcode', '')
        ) AS barcode,

        COALESCE(
          NULLIF(r.subject, ''),
          NULLIF(r.raw->>'subject', '')
        ) AS subject,

        COALESCE(
          NULLIF(r.brand, ''),
          NULLIF(r.raw->>'brand', '')
        ) AS brand,

        COALESCE(
          NULLIF(r.warehouse_name, ''),
          NULLIF(r.raw->>'warehouseName', '')
        ) AS warehouse_name,

        COALESCE(
          NULLIF(r.region_name, ''),
          NULLIF(r.raw->>'regionName', '')
        ) AS region_name,

        COALESCE(
          NULLIF(r.country_name, ''),
          NULLIF(r.raw->>'countryName', '')
        ) AS country_name,

        1 AS qty,

        r.price_raw,

        COALESCE(
          NULLIF(r.raw->>'salePrice', '')::numeric(14,2),
          NULLIF(r.raw->>'totalPrice', '')::numeric(14,2)
        ) AS sale_price_raw,

        r.final_price_raw,

        NULLIF(r.raw->>'discountPercent', '')::numeric(10,2) AS discount_percent_raw,

        NULLIF(r.raw->>'spp', '')::numeric(10,2) AS spp_raw,

        r.for_pay_raw,

        COALESCE(
          r.finished_price_raw,
          NULLIF(r.raw->>'finishedPrice', '')::numeric(14,2),
          NULLIF(r.raw->>'priceWithDisc', '')::numeric(14,2)
        ) AS finished_price_raw,

        COALESCE(
          NULLIF(r.raw->>'salePrice', '')::numeric(14,2),
          NULLIF(r.raw->>'totalPrice', '')::numeric(14,2),
          r.price_raw,
          0
        ) AS amount_gross,

        COALESCE(
          r.for_pay_raw,
          r.final_price_raw,
          NULLIF(r.raw->>'finishedPrice', '')::numeric(14,2),
          NULLIF(r.raw->>'priceWithDisc', '')::numeric(14,2),
          0
        ) AS amount_net,

        CASE
          WHEN COALESCE((r.raw->>'saleID'), '') ILIKE 'R%%' THEN FALSE
          WHEN r.report_type = 'sales' THEN TRUE
          ELSE FALSE
        END AS is_sale,

        CASE
          WHEN COALESCE((r.raw->>'saleID'), '') ILIKE 'R%%' THEN TRUE
          ELSE FALSE
        END AS is_return,

        FALSE AS is_cancel,
        FALSE AS is_order,

        r.raw
      FROM analytics.wb_sales_raw r
      WHERE r.client_id = $1
        AND r.client_mp_account_id = $2
        AND (
          COALESCE(
            r.sale_datetime,
            r.event_datetime,
            NULLIF(r.raw->>'lastChangeDate', '')::timestamptz,
            NULLIF(r.raw->>'date', '')::timestamptz,
            r.created_at
          )
        )::date >= $3::date
        AND (
          COALESCE(
            r.sale_datetime,
            r.event_datetime,
            NULLIF(r.raw->>'lastChangeDate', '')::timestamptz,
            NULLIF(r.raw->>'date', '')::timestamptz,
            r.created_at
          )
        )::date <= $4::date
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
      console.error('[wbSalesNormalizeService] rollback error:', rollbackError);
    }
    throw error;
  } finally {
    client.release();
  }
}

module.exports = {
  normalizeWbSales,
};