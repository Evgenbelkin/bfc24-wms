const express = require('express');
const router = express.Router();
const { pool } = require('../db');

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

async function sendTelegramMessage(text) {
  const botToken = process.env.TELEGRAM_BOT_TOKEN || '';
  const chatId = process.env.TELEGRAM_LEADS_CHAT_ID || '';

  if (!botToken || !chatId) {
    return { ok: false, skipped: true, reason: 'telegram env not set' };
  }

  const response = await fetch(`https://api.telegram.org/bot${botToken}/sendMessage`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      chat_id: chatId,
      text,
      parse_mode: 'HTML',
      disable_web_page_preview: true
    })
  });

  const data = await response.json().catch(() => ({}));

  if (!response.ok || !data.ok) {
    throw new Error(data.description || 'Telegram send failed');
  }

  return { ok: true };
}

router.post('/api/site-leads', async (req, res) => {
  try {
    const name = String(req.body?.name || '').trim();
    const contact = String(req.body?.contact || '').trim();
    const ordersVolume = String(req.body?.orders_volume || '').trim();
    const skuCount = String(req.body?.sku_count || '').trim();
    const comment = String(req.body?.comment || '').trim();

    if (!name) {
      return res.status(400).json({ error: 'Имя обязательно' });
    }
    if (!contact) {
      return res.status(400).json({ error: 'Телефон или Telegram обязателен' });
    }
    if (!ordersVolume) {
      return res.status(400).json({ error: 'Объём заказов обязателен' });
    }
    if (!skuCount) {
      return res.status(400).json({ error: 'Количество SKU обязательно' });
    }

    const insertSql = `
      INSERT INTO public.site_leads (
        name,
        contact,
        orders_volume,
        sku_count,
        comment,
        source
      )
      VALUES ($1, $2, $3, $4, $5, 'landing')
      RETURNING id, created_at
    `;

    const insertResult = await pool.query(insertSql, [
      name,
      contact,
      ordersVolume,
      skuCount,
      comment || null
    ]);

    const lead = insertResult.rows[0];

    const siteUrl = process.env.PUBLIC_SITE_URL || 'https://bfc-24.ru';

    const telegramText =
      `<b>Новая заявка с сайта BFC24</b>\n` +
      `ID: <b>${escapeHtml(lead.id)}</b>\n` +
      `Имя: <b>${escapeHtml(name)}</b>\n` +
      `Контакт: <b>${escapeHtml(contact)}</b>\n` +
      `Объём заказов: <b>${escapeHtml(ordersVolume)}</b>\n` +
      `SKU: <b>${escapeHtml(skuCount)}</b>\n` +
      `Комментарий: <b>${escapeHtml(comment || '—')}</b>\n` +
      `Источник: <b>landing</b>\n` +
      `Сайт: <b>${escapeHtml(siteUrl)}</b>`;

    try {
      await sendTelegramMessage(telegramText);
    } catch (telegramError) {
      console.error('[site-leads] telegram error:', telegramError.message);
    }

    return res.status(201).json({
      ok: true,
      lead_id: lead.id,
      created_at: lead.created_at
    });
  } catch (error) {
    console.error('[site-leads] error:', error);
    return res.status(500).json({ error: 'Ошибка сервера при сохранении заявки' });
  }
});

module.exports = router;