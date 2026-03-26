const axios = require('axios');
const { pool } = require('../db');

const WB_API_BASE = 'https://statistics-api.wildberries.ru';

function validate(body) {
  if (!body.client_id) throw new Error('client_id обязателен');
  if (!body.mp_account_id) throw new Error('mp_account_id обязателен');
  if (!body.date_from) throw new Error('date_from обязателен');
  if (!body.date_to) throw new Error('date_to обязателен');
}

async function getToken(mpAccountId) {
  const res = await pool.query(
    `SELECT api_token FROM mp_accounts WHERE id = $1`,
    [mpAccountId]
  );

  if (!res.rowCount) {
    throw new Error('api_token не найден');
  }

  return res.rows[0].api_token;
}

async function fetchSales(token, dateFrom) {
  const url = `${WB_API_BASE}/api/v1/supplier/sales`;

  const resp = await axios.get(url, {
    headers: {
      Authorization: token,
    },
    params: {
      dateFrom,
    },
    timeout: 60000,
  });

  return resp.data;
}

async function syncWbSales(user, body) {
  validate(body);

  const clientId = Number(body.client_id);
  const mpAccountId = Number(body.mp_account_id);

  const token = await getToken(mpAccountId);

  console.log('[wbSalesSync] fetching sales...');

  const sales = await fetchSales(token, body.date_from);

  console.log('[wbSalesSync] rows:', sales.length);

  let inserted = 0;

  for (const row of sales) {
    await pool.query(
      `
      INSERT INTO analytics.wb_sales_raw (
        client_id,
        client_mp_account_id,
        report_type,
        source_order_id,
        source_sale_id,
        source_nm_id,
        source_chrt_id,
        barcode,
        article,
        status_raw,
        event_datetime,
        sale_datetime,
        price_raw,
        final_price_raw,
        for_pay_raw,
        raw
      )
      VALUES (
        $1,$2,'sales',
        $3,$4,$5,$6,$7,$8,
        $9,$10,$11,$12,$13,$14,$15
      )
      ON CONFLICT DO NOTHING
      `,
      [
        clientId,
        mpAccountId,
        row.orderId || null,
        row.saleID || null,
        row.nmId || null,
        row.chrtId || null,
        row.barcode || null,
        row.supplierArticle || null,
        row.status || null,
        row.date || null,
        row.saleDate || null,
        row.price || null,
        row.finishedPrice || null,
        row.forPay || null,
        JSON.stringify(row),
      ]
    );

    inserted++;
  }

  return {
    ok: true,
    inserted,
  };
}

module.exports = {
  syncWbSales,
};