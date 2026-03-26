const { pool } = require('../db');

async function getOverviewSummary({
  clientId,
  mpAccountId,
  dateFrom,
  dateTo,
}) {
  const params = [clientId, mpAccountId, dateFrom, dateTo];

  const sql = `
    WITH orders_agg AS (
      SELECT
        COUNT(DISTINCT o.wb_order_id)::int AS orders_count,
        COALESCE(SUM(o.qty), 0)::int AS orders_qty,
        COALESCE(SUM(o.order_amount), 0)::numeric(14,2) AS orders_amount
      FROM analytics.wb_orders_normalized o
      WHERE o.client_id = $1
        AND o.client_mp_account_id = $2
        AND o.order_date >= $3::date
        AND o.order_date <= $4::date
    ),
    sales_agg AS (
      SELECT
        COUNT(*) FILTER (WHERE n.is_sale = TRUE)::int AS sales_count,
        COALESCE(SUM(n.qty) FILTER (WHERE n.is_sale = TRUE), 0)::int AS sales_qty,
        COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_sale = TRUE), 0)::numeric(14,2) AS sales_amount,
        COALESCE(SUM(n.amount_gross) FILTER (WHERE n.is_sale = TRUE), 0)::numeric(14,2) AS realization_amount,
        COUNT(*) FILTER (WHERE n.is_return = TRUE)::int AS returns_count,
        COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_return = TRUE), 0)::numeric(14,2) AS returns_amount
      FROM analytics.wb_sales_normalized n
      WHERE n.client_id = $1
        AND n.client_mp_account_id = $2
        AND n.event_date >= $3::date
        AND n.event_date <= $4::date
    )
    SELECT
      o.orders_count,
      o.orders_qty,
      o.orders_amount,

      s.sales_count,
      s.sales_qty,
      s.sales_amount,
      s.realization_amount,

      s.returns_count,
      s.returns_amount,

      CASE
        WHEN COALESCE(o.orders_qty, 0) = 0 THEN 0::numeric(10,2)
        ELSE ROUND((COALESCE(s.sales_qty, 0)::numeric / o.orders_qty::numeric) * 100, 2)
      END AS buyout_percent

    FROM orders_agg o
    CROSS JOIN sales_agg s
  `;

  const { rows } = await pool.query(sql, params);
  return rows[0] || {
    orders_count: 0,
    orders_qty: 0,
    orders_amount: '0.00',
    sales_count: 0,
    sales_qty: 0,
    sales_amount: '0.00',
    realization_amount: '0.00',
    returns_count: 0,
    returns_amount: '0.00',
    buyout_percent: '0.00',
  };
}

async function getSalesDaily({
  clientId,
  mpAccountId,
  dateFrom,
  dateTo,
}) {
  const params = [dateFrom, dateTo, clientId, mpAccountId];

  const sql = `
    WITH days AS (
      SELECT generate_series(
        $1::date,
        $2::date,
        INTERVAL '1 day'
      )::date AS day
    ),
    orders_agg AS (
      SELECT
        o.order_date AS day,
        COUNT(DISTINCT o.wb_order_id)::int AS orders_count,
        COALESCE(SUM(o.qty), 0)::int AS orders_qty,
        COALESCE(SUM(o.order_amount), 0)::numeric(14,2) AS orders_amount
      FROM analytics.wb_orders_normalized o
      WHERE o.client_id = $3
        AND o.client_mp_account_id = $4
        AND o.order_date >= $1::date
        AND o.order_date <= $2::date
      GROUP BY o.order_date
    ),
    sales_agg AS (
      SELECT
        n.event_date AS day,
        COUNT(*) FILTER (WHERE n.is_sale = TRUE)::int AS sales_count,
        COALESCE(SUM(n.qty) FILTER (WHERE n.is_sale = TRUE), 0)::int AS sales_qty,
        COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_sale = TRUE), 0)::numeric(14,2) AS sales_amount,
        COALESCE(SUM(n.amount_gross) FILTER (WHERE n.is_sale = TRUE), 0)::numeric(14,2) AS realization_amount,
        COUNT(*) FILTER (WHERE n.is_return = TRUE)::int AS returns_count,
        COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_return = TRUE), 0)::numeric(14,2) AS returns_amount
      FROM analytics.wb_sales_normalized n
      WHERE n.client_id = $3
        AND n.client_mp_account_id = $4
        AND n.event_date >= $1::date
        AND n.event_date <= $2::date
      GROUP BY n.event_date
    )
    SELECT
      d.day::text AS date,

      COALESCE(o.orders_count, 0)::int AS orders_count,
      COALESCE(o.orders_qty, 0)::int AS orders_qty,
      COALESCE(o.orders_amount, 0)::numeric(14,2) AS orders_amount,

      COALESCE(s.sales_count, 0)::int AS sales_count,
      COALESCE(s.sales_qty, 0)::int AS sales_qty,
      COALESCE(s.sales_amount, 0)::numeric(14,2) AS sales_amount,
      COALESCE(s.realization_amount, 0)::numeric(14,2) AS realization_amount,

      COALESCE(s.returns_count, 0)::int AS returns_count,
      COALESCE(s.returns_amount, 0)::numeric(14,2) AS returns_amount,

      CASE
        WHEN COALESCE(o.orders_qty, 0) = 0 THEN 0::numeric(10,2)
        ELSE ROUND((COALESCE(s.sales_qty, 0)::numeric / o.orders_qty::numeric) * 100, 2)
      END AS buyout_percent

    FROM days d
    LEFT JOIN orders_agg o
      ON o.day = d.day
    LEFT JOIN sales_agg s
      ON s.day = d.day
    ORDER BY d.day ASC
  `;

  const { rows } = await pool.query(sql, params);
  return rows;
}

async function getTopSkus({
  clientId,
  mpAccountId,
  dateFrom,
  dateTo,
  limit = 10,
}) {
  const params = [clientId, mpAccountId, dateFrom, dateTo, limit];

  const sql = `
    SELECT
      MAX(n.nm_id) AS sku_id,
      n.barcode,
      MAX(n.article) AS vendor_code,
      MAX(n.article) AS wb_vendor_code,
      COALESCE(
        MAX(NULLIF(n.subject, '')),
        MAX(NULLIF(n.brand, '')),
        MAX(NULLIF(n.article, '')),
        MAX(NULLIF(n.barcode, '')),
        'Без названия'
      ) AS item_name,

      COALESCE(SUM(n.qty) FILTER (WHERE n.is_sale = TRUE), 0)::int AS sales_qty,
      COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_sale = TRUE), 0)::numeric(14,2) AS sales_amount,
      COALESCE(SUM(n.amount_gross) FILTER (WHERE n.is_sale = TRUE), 0)::numeric(14,2) AS realization_amount,

      CASE
        WHEN COALESCE(SUM(n.qty) FILTER (WHERE n.is_sale = TRUE), 0) = 0 THEN 0::numeric(14,2)
        ELSE ROUND(
          COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_sale = TRUE), 0)
          / SUM(n.qty) FILTER (WHERE n.is_sale = TRUE),
          2
        )
      END AS avg_price,

      COUNT(*) FILTER (WHERE n.is_sale = TRUE)::int AS sales_count,
      COUNT(*) FILTER (WHERE n.is_return = TRUE)::int AS returns_count,
      COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_return = TRUE), 0)::numeric(14,2) AS returns_amount

    FROM analytics.wb_sales_normalized n
    WHERE n.client_id = $1
      AND n.client_mp_account_id = $2
      AND n.event_date >= $3::date
      AND n.event_date <= $4::date
      AND (n.is_sale = TRUE OR n.is_return = TRUE)
    GROUP BY n.barcode
    ORDER BY sales_amount DESC, sales_qty DESC
    LIMIT $5
  `;

  const { rows } = await pool.query(sql, params);
  return rows;
}

module.exports = {
  getOverviewSummary,
  getSalesDaily,
  getTopSkus,
};