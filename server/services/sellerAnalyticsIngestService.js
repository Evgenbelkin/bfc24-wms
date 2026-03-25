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

function normalizeFulfillmentModel(value) {
  const raw = String(value || 'all').trim().toLowerCase();
  if (['all', 'fbs', 'fbo', 'unknown'].includes(raw)) {
    return raw;
  }
  throw new Error('Некорректный fulfillment_model. Допустимо: all, fbs, fbo, unknown');
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

function buildFulfillmentSqlFilter(fulfillmentModel, params, tableAlias = 'src') {
  if (!fulfillmentModel || fulfillmentModel === 'all') {
    return '';
  }

  params.push(fulfillmentModel);
  return ` AND (
    CASE
      WHEN LOWER(COALESCE(${tableAlias}.raw->>'fulfillment_model', '')) IN ('fbs', 'fbo')
        THEN LOWER(${tableAlias}.raw->>'fulfillment_model')
      WHEN LOWER(COALESCE(${tableAlias}.raw->>'fulfilmentType', '')) IN ('fbs', 'fbo')
        THEN LOWER(${tableAlias}.raw->>'fulfilmentType')
      WHEN LOWER(COALESCE(${tableAlias}.raw->>'deliveryType', '')) IN ('fbs', 'fbo')
        THEN LOWER(${tableAlias}.raw->>'deliveryType')
      ELSE 'unknown'
    END
  ) = $${params.length} `;
}

async function syncOrdersToAnalytics(reqUser, body) {
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

  const fulfillmentModel = normalizeFulfillmentModel(body.fulfillment_model || 'all');

  const client = await pool.connect();
  let syncRunId = null;

  try {
    await client.query('BEGIN');

    const insertSyncRunSql = `
      INSERT INTO analytics.sync_runs (
        client_id,
        mp_account_id,
        marketplace,
        fulfillment_model,
        sync_type,
        date_from,
        date_to,
        status,
        rows_loaded,
        started_at
      )
      VALUES (
        $1, $2, 'wb', $3, 'orders_ingest', $4::date, $5::date, 'running', 0, NOW()
      )
      RETURNING id
    `;

    const syncRunRes = await client.query(insertSyncRunSql, [
      clientId,
      mpAccountId,
      fulfillmentModel === 'all' ? null : fulfillmentModel,
      body.date_from,
      body.date_to,
    ]);

    syncRunId = syncRunRes.rows[0].id;

    const baseParams = [clientId, mpAccountId, body.date_from, body.date_to];
    const deleteOrdersParams = [...baseParams];

    const deleteOrdersFulfillmentFilter = buildFulfillmentSqlFilter(
      fulfillmentModel,
      deleteOrdersParams,
      'src'
    );

    const deleteSql = `
      DELETE FROM analytics.sales_orders ao
      WHERE ao.client_id = $1
        AND ao.mp_account_id = $2
        AND ao.order_date >= $3::date
        AND ao.order_date < ($4::date + INTERVAL '1 day')
        AND ao.source_system = 'mp_wb_orders'
        AND EXISTS (
          SELECT 1
          FROM public.mp_wb_orders src
          INNER JOIN public.mp_client_accounts mca
            ON mca.id = src.client_mp_account_id
          WHERE mca.client_id = $1
            AND src.client_mp_account_id = $2
            AND src.created_at >= $3::date
            AND src.created_at < ($4::date + INTERVAL '1 day')
            AND src.wb_order_id::text = ao.external_order_id
            ${deleteOrdersFulfillmentFilter}
        )
    `;

    await client.query(deleteSql, deleteOrdersParams);

    const insertOrdersParams = [clientId, mpAccountId, body.date_from, body.date_to];
    const insertOrdersFulfillmentFilter = buildFulfillmentSqlFilter(
      fulfillmentModel,
      insertOrdersParams,
      'src'
    );

    const insertOrdersSql = `
      INSERT INTO analytics.sales_orders (
        client_id,
        mp_account_id,
        marketplace,
        fulfillment_model,
        external_order_id,
        order_code,
        order_date,
        sale_date,
        status,
        currency_code,
        items_count,
        total_amount,
        source_payload,
        source_system
      )
      SELECT
        mca.client_id AS client_id,
        src.client_mp_account_id AS mp_account_id,
        'wb' AS marketplace,
        CASE
          WHEN LOWER(COALESCE(MAX(src.raw->>'fulfillment_model'), '')) IN ('fbs', 'fbo')
            THEN LOWER(MAX(src.raw->>'fulfillment_model'))
          WHEN LOWER(COALESCE(MAX(src.raw->>'fulfilmentType'), '')) IN ('fbs', 'fbo')
            THEN LOWER(MAX(src.raw->>'fulfilmentType'))
          WHEN LOWER(COALESCE(MAX(src.raw->>'deliveryType'), '')) IN ('fbs', 'fbo')
            THEN LOWER(MAX(src.raw->>'deliveryType'))
          ELSE 'unknown'
        END AS fulfillment_model,
        src.wb_order_id::text AS external_order_id,
        src.wb_order_id::text AS order_code,
        MIN(src.created_at) AS order_date,
        NULL::timestamptz AS sale_date,
        COALESCE(MAX(src.status), 'unknown') AS status,
        COALESCE(MAX(src.currency_code), 'RUB') AS currency_code,
        COUNT(*)::int AS items_count,
        COALESCE(SUM(COALESCE(src.converted_price, src.price, 0)), 0)::numeric(14,2) AS total_amount,
        jsonb_build_object(
          'source_table', 'public.mp_wb_orders',
          'grouped_rows', COUNT(*),
          'wb_order_id', src.wb_order_id
        ) AS source_payload,
        'mp_wb_orders' AS source_system
      FROM public.mp_wb_orders src
      INNER JOIN public.mp_client_accounts mca
        ON mca.id = src.client_mp_account_id
      WHERE mca.client_id = $1
        AND src.client_mp_account_id = $2
        AND src.created_at >= $3::date
        AND src.created_at < ($4::date + INTERVAL '1 day')
        AND src.wb_order_id IS NOT NULL
        ${insertOrdersFulfillmentFilter}
      GROUP BY
        mca.client_id,
        src.client_mp_account_id,
        src.wb_order_id
    `;

    const insertOrdersRes = await client.query(insertOrdersSql, insertOrdersParams);

    const insertLinesParams = [clientId, mpAccountId, body.date_from, body.date_to];
    const insertLinesFulfillmentFilter = buildFulfillmentSqlFilter(
      fulfillmentModel,
      insertLinesParams,
      'src'
    );

    const insertLinesSql = `
      INSERT INTO analytics.sales_order_lines (
        sales_order_id,
        client_id,
        mp_account_id,
        marketplace,
        fulfillment_model,
        external_line_id,
        barcode,
        sku_id,
        nm_id,
        vendor_code,
        wb_vendor_code,
        item_name,
        quantity,
        unit_price,
        final_price,
        line_amount,
        order_date,
        source_payload
      )
      SELECT
        ao.id AS sales_order_id,
        mca.client_id AS client_id,
        src.client_mp_account_id AS mp_account_id,
        'wb' AS marketplace,
        CASE
          WHEN LOWER(COALESCE(src.raw->>'fulfillment_model', '')) IN ('fbs', 'fbo')
            THEN LOWER(src.raw->>'fulfillment_model')
          WHEN LOWER(COALESCE(src.raw->>'fulfilmentType', '')) IN ('fbs', 'fbo')
            THEN LOWER(src.raw->>'fulfilmentType')
          WHEN LOWER(COALESCE(src.raw->>'deliveryType', '')) IN ('fbs', 'fbo')
            THEN LOWER(src.raw->>'deliveryType')
          ELSE 'unknown'
        END AS fulfillment_model,
        CONCAT(
          COALESCE(src.wb_order_id::text, 'no_order'),
          ':',
          COALESCE(NULLIF(src.barcode, ''), NULLIF(src.article, ''), src.id::text)
        ) AS external_line_id,
        NULLIF(src.barcode, '') AS barcode,
        NULL::integer AS sku_id,
        src.nm_id::bigint AS nm_id,
        NULLIF(src.article, '') AS vendor_code,
        NULLIF(src.article, '') AS wb_vendor_code,
        COALESCE(
          NULLIF(src.raw->>'subject', ''),
          NULLIF(src.raw->>'subjectName', ''),
          NULLIF(src.raw->>'productName', ''),
          NULLIF(src.article, ''),
          NULLIF(src.barcode, ''),
          'Без названия'
        ) AS item_name,
        1::int AS quantity,
        COALESCE(src.price, 0)::numeric(14,2) AS unit_price,
        COALESCE(src.converted_price, src.price, 0)::numeric(14,2) AS final_price,
        COALESCE(src.converted_price, src.price, 0)::numeric(14,2) AS line_amount,
        src.created_at AS order_date,
        src.raw AS source_payload
      FROM public.mp_wb_orders src
      INNER JOIN public.mp_client_accounts mca
        ON mca.id = src.client_mp_account_id
      INNER JOIN analytics.sales_orders ao
        ON ao.client_id = mca.client_id
       AND ao.mp_account_id = src.client_mp_account_id
       AND ao.external_order_id = src.wb_order_id::text
       AND ao.source_system = 'mp_wb_orders'
      WHERE mca.client_id = $1
        AND src.client_mp_account_id = $2
        AND src.created_at >= $3::date
        AND src.created_at < ($4::date + INTERVAL '1 day')
        AND src.wb_order_id IS NOT NULL
        ${insertLinesFulfillmentFilter}
    `;

    const insertLinesRes = await client.query(insertLinesSql, insertLinesParams);

    await client.query(
      `
      UPDATE analytics.sync_runs
      SET
        status = 'success',
        rows_loaded = $2,
        finished_at = NOW()
      WHERE id = $1
      `,
      [syncRunId, Number(insertLinesRes.rowCount || 0)]
    );

    await client.query('COMMIT');

    return {
      ok: true,
      message: 'Синхронизация аналитики завершена успешно',
      sync_run_id: Number(syncRunId),
      filters: {
        client_id: clientId,
        mp_account_id: mpAccountId,
        date_from: body.date_from,
        date_to: body.date_to,
        fulfillment_model: fulfillmentModel,
      },
      stats: {
        sales_orders_inserted: Number(insertOrdersRes.rowCount || 0),
        sales_order_lines_inserted: Number(insertLinesRes.rowCount || 0),
      },
    };
  } catch (error) {
    try {
      await client.query('ROLLBACK');
    } catch (rollbackError) {
      console.error('[sellerAnalyticsIngestService] rollback error:', rollbackError);
    }

    if (syncRunId) {
      try {
        await pool.query(
          `
          UPDATE analytics.sync_runs
          SET
            status = 'failed',
            finished_at = NOW(),
            error_text = $2
          WHERE id = $1
          `,
          [syncRunId, String(error.message || 'Unknown ingest error').slice(0, 4000)]
        );
      } catch (syncRunUpdateError) {
        console.error('[sellerAnalyticsIngestService] sync_runs update error:', syncRunUpdateError);
      }
    }

    throw error;
  } finally {
    client.release();
  }
}

module.exports = {
  syncOrdersToAnalytics,
};