const { pool } = require('../db');

function buildFulfillmentFilter(fulfillmentModel, params, tableAlias = 'so') {
  if (!fulfillmentModel || fulfillmentModel === 'all') {
    return '';
  }

  params.push(fulfillmentModel);
  return ` AND ${tableAlias}.fulfillment_model = $${params.length} `;
}

async function getOverviewSummary({
  clientId,
  mpAccountId,
  dateFrom,
  dateTo,
  fulfillmentModel = 'all',
}) {
  const params = [clientId, mpAccountId, dateFrom, dateTo];

  const fulfillmentFilter = buildFulfillmentFilter(fulfillmentModel, params, 'so');

  const sql = `
    SELECT
      COUNT(DISTINCT so.id)::int AS orders_count,
      COALESCE(SUM(sol.line_amount), 0)::numeric(14,2) AS revenue_total,
      CASE
        WHEN COUNT(DISTINCT so.id) = 0 THEN 0::numeric(14,2)
        ELSE ROUND(COALESCE(SUM(sol.line_amount), 0) / COUNT(DISTINCT so.id), 2)
      END AS average_order_value,
      COALESCE(SUM(sol.quantity), 0)::int AS items_sold
    FROM analytics.sales_orders so
    LEFT JOIN analytics.sales_order_lines sol
      ON sol.sales_order_id = so.id
    WHERE so.client_id = $1
      AND so.mp_account_id = $2
      AND so.order_date >= $3::date
      AND so.order_date < ($4::date + INTERVAL '1 day')
      ${fulfillmentFilter}
  `;

  const { rows } = await pool.query(sql, params);
  return rows[0] || {
    orders_count: 0,
    revenue_total: '0.00',
    average_order_value: '0.00',
    items_sold: 0,
  };
}

async function getSalesDaily({
  clientId,
  mpAccountId,
  dateFrom,
  dateTo,
  fulfillmentModel = 'all',
}) {
  const params = [dateFrom, dateTo, clientId, mpAccountId];

  const fulfillmentFilter = buildFulfillmentFilter(fulfillmentModel, params, 'so');

  const sql = `
    WITH days AS (
      SELECT generate_series(
        $1::date,
        $2::date,
        INTERVAL '1 day'
      )::date AS day
    ),
    agg AS (
      SELECT
        so.order_date::date AS day,
        COUNT(DISTINCT so.id)::int AS orders_count,
        COALESCE(SUM(sol.line_amount), 0)::numeric(14,2) AS revenue_total,
        COALESCE(SUM(sol.quantity), 0)::int AS items_sold
      FROM analytics.sales_orders so
      LEFT JOIN analytics.sales_order_lines sol
        ON sol.sales_order_id = so.id
      WHERE so.client_id = $3
        AND so.mp_account_id = $4
        AND so.order_date >= $1::date
        AND so.order_date < ($2::date + INTERVAL '1 day')
        ${fulfillmentFilter}
      GROUP BY so.order_date::date
    )
    SELECT
      d.day::text AS date,
      COALESCE(a.orders_count, 0)::int AS orders_count,
      COALESCE(a.revenue_total, 0)::numeric(14,2) AS revenue_total,
      COALESCE(a.items_sold, 0)::int AS items_sold
    FROM days d
    LEFT JOIN agg a ON a.day = d.day
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
  fulfillmentModel = 'all',
  limit = 10,
}) {
  const params = [clientId, mpAccountId, dateFrom, dateTo];

  const fulfillmentFilter = buildFulfillmentFilter(fulfillmentModel, params, 'sol');

  params.push(limit);

  const sql = `
    SELECT
      MAX(sol.sku_id) AS sku_id,
      sol.barcode,
      MAX(sol.vendor_code) AS vendor_code,
      MAX(sol.wb_vendor_code) AS wb_vendor_code,
      MAX(sol.item_name) AS item_name,
      COALESCE(SUM(sol.quantity), 0)::int AS qty_sold,
      COALESCE(SUM(sol.line_amount), 0)::numeric(14,2) AS revenue_total,
      CASE
        WHEN COALESCE(SUM(sol.quantity), 0) = 0 THEN 0::numeric(14,2)
        ELSE ROUND(COALESCE(SUM(sol.line_amount), 0) / SUM(sol.quantity), 2)
      END AS avg_price,
      COUNT(DISTINCT sol.sales_order_id)::int AS orders_count
    FROM analytics.sales_order_lines sol
    WHERE sol.client_id = $1
      AND sol.mp_account_id = $2
      AND sol.order_date >= $3::date
      AND sol.order_date < ($4::date + INTERVAL '1 day')
      ${fulfillmentFilter}
    GROUP BY sol.barcode
    ORDER BY revenue_total DESC, qty_sold DESC
    LIMIT $${params.length}
  `;

  const { rows } = await pool.query(sql, params);
  return rows;
}

module.exports = {
  getOverviewSummary,
  getSalesDaily,
  getTopSkus,
};