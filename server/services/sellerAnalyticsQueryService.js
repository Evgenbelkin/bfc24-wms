const { pool } = require('../db');

async function getOverviewSummary({
  clientId,
  mpAccountId,
  dateFrom,
  dateTo,
}) {
  const params = [clientId, mpAccountId, dateFrom, dateTo];

  const sql = `
    SELECT
      COUNT(*) FILTER (WHERE n.is_sale = TRUE)::int AS orders_count,
      COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_sale = TRUE), 0)::numeric(14,2) AS revenue_total,
      CASE
        WHEN COUNT(*) FILTER (WHERE n.is_sale = TRUE) = 0 THEN 0::numeric(14,2)
        ELSE ROUND(
          COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_sale = TRUE), 0)
          / COUNT(*) FILTER (WHERE n.is_sale = TRUE),
          2
        )
      END AS average_order_value,
      COALESCE(SUM(n.qty) FILTER (WHERE n.is_sale = TRUE), 0)::int AS items_sold,

      COUNT(*) FILTER (WHERE n.is_return = TRUE)::int AS returns_count,
      COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_return = TRUE), 0)::numeric(14,2) AS returns_total

    FROM analytics.wb_sales_normalized n
    WHERE n.client_id = $1
      AND n.client_mp_account_id = $2
      AND n.event_date >= $3::date
      AND n.event_date <= $4::date
  `;

  const { rows } = await pool.query(sql, params);
  return rows[0] || {
    orders_count: 0,
    revenue_total: '0.00',
    average_order_value: '0.00',
    items_sold: 0,
    returns_count: 0,
    returns_total: '0.00',
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
    sales_agg AS (
      SELECT
        n.event_date AS day,
        COUNT(*) FILTER (WHERE n.is_sale = TRUE)::int AS orders_count,
        COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_sale = TRUE), 0)::numeric(14,2) AS revenue_total,
        COALESCE(SUM(n.qty) FILTER (WHERE n.is_sale = TRUE), 0)::int AS items_sold,
        COUNT(*) FILTER (WHERE n.is_return = TRUE)::int AS returns_count,
        COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_return = TRUE), 0)::numeric(14,2) AS returns_total
      FROM analytics.wb_sales_normalized n
      WHERE n.client_id = $3
        AND n.client_mp_account_id = $4
        AND n.event_date >= $1::date
        AND n.event_date <= $2::date
      GROUP BY n.event_date
    )
    SELECT
      d.day::text AS date,
      COALESCE(a.orders_count, 0)::int AS orders_count,
      COALESCE(a.revenue_total, 0)::numeric(14,2) AS revenue_total,
      COALESCE(a.items_sold, 0)::int AS items_sold,
      COALESCE(a.returns_count, 0)::int AS returns_count,
      COALESCE(a.returns_total, 0)::numeric(14,2) AS returns_total
    FROM days d
    LEFT JOIN sales_agg a
      ON a.day = d.day
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

      COALESCE(SUM(n.qty) FILTER (WHERE n.is_sale = TRUE), 0)::int AS qty_sold,
      COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_sale = TRUE), 0)::numeric(14,2) AS revenue_total,

      CASE
        WHEN COALESCE(SUM(n.qty) FILTER (WHERE n.is_sale = TRUE), 0) = 0 THEN 0::numeric(14,2)
        ELSE ROUND(
          COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_sale = TRUE), 0)
          / SUM(n.qty) FILTER (WHERE n.is_sale = TRUE),
          2
        )
      END AS avg_price,

      COUNT(*) FILTER (WHERE n.is_sale = TRUE)::int AS orders_count,
      COUNT(*) FILTER (WHERE n.is_return = TRUE)::int AS returns_count,
      COALESCE(SUM(n.amount_net) FILTER (WHERE n.is_return = TRUE), 0)::numeric(14,2) AS returns_total

    FROM analytics.wb_sales_normalized n
    WHERE n.client_id = $1
      AND n.client_mp_account_id = $2
      AND n.event_date >= $3::date
      AND n.event_date <= $4::date
      AND (n.is_sale = TRUE OR n.is_return = TRUE)
    GROUP BY n.barcode
    ORDER BY revenue_total DESC, qty_sold DESC
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