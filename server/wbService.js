// wbService.js

const WB_ORDERS_URL = 'https://marketplace-api.wildberries.ru/api/v3/orders/new';

// Базовые урлы для маркетплейс-API WB
const WB_MARKETPLACE_BASE = 'https://marketplace-api.wildberries.ru';
const WB_MARKETPLACE_SANDBOX_BASE = 'https://marketplace-api-sandbox.wildberries.ru';

/**
 * Вспомогательная функция: определить базовый URL по supplyId.
 * В sandbox GI имеют вид WB-GI-SAND-123...
 */
function getWbBaseBySupplyId(supplyId) {
  if (typeof supplyId === 'string' && /^WB-GI-SAND-/i.test(supplyId)) {
    return WB_MARKETPLACE_SANDBOX_BASE;
  }
  return WB_MARKETPLACE_BASE;
}

/**
 * Универсальный вызов WB API для заказов.
 * Ничего не знает про структуру ответа — просто возвращает data как есть.
 */
async function fetchOrders(wbToken, { dateFrom, dateTo, limit = 100 } = {}) {
  const params = new URLSearchParams();

  if (dateFrom) params.append('dateFrom', dateFrom);
  if (dateTo)   params.append('dateTo', dateTo);
  if (limit)    params.append('limit', String(limit));

  const query = params.toString();
  const url   = query ? `${WB_ORDERS_URL}?${query}` : WB_ORDERS_URL;

  console.log('[WB FETCH] URL =', url, 'params =', { dateFrom, dateTo, limit });

  const res = await fetch(url, {
    method: 'GET',
    headers: {
      // В docs WB токен передаётся просто в Authorization без Bearer
      'Authorization': wbToken,
    },
  });

  const text = await res.text();

  if (!res.ok) {
    console.error('[WB FETCH] HTTP error', res.status, text);
    throw new Error('WB HTTP ' + res.status);
  }

  let data = null;

  if (text) {
    try {
      data = JSON.parse(text);
    } catch (e) {
      console.error('[WB FETCH] JSON parse error:', e);
      console.error('[WB FETCH] raw text:', text);
      throw new Error('WB JSON parse error');
    }
  }

  if (data && typeof data === 'object') {
    console.log('[WB FETCH] top-level keys =', Object.keys(data));
  }
  console.log('[WB FETCH] response type =', Array.isArray(data) ? 'array' : typeof data);

  return data;
}

/**
 * Подтверждение FBS-поставки и получение QR-кода поставки.
 *
 * shipmentCode = supplyId вида WB-GI-219280886.
 * token        = WB API token (из mp_accounts.api_token).
 *
 * /api/v3/supplies/{supplyId}/barcode?type=png
 * сейчас отдаёт JSON вида:
 *   {
 *     "barcode": "WB-GI-21928...",
 *     "file": "<base64 PNG без префикса>"
 *   }
 */
async function confirmShipmentAndGetQr({ token, shipmentCode }) {
  if (!token) {
    throw new Error('WB token is empty');
  }
  if (!shipmentCode) {
    throw new Error('shipmentCode (supplyId) is empty');
  }

  const base = getWbBaseBySupplyId(shipmentCode);

  const headers = {
    'Authorization': token,
  };

  // 1) Передаём поставку в доставку
  const deliverUrl = `${base}/api/v3/supplies/${encodeURIComponent(shipmentCode)}/deliver`;
  console.log('[WB] PATCH deliver supply:', deliverUrl);

  try {
    const resDeliver = await fetch(deliverUrl, {
      method: 'PATCH',
      headers,
    });

    const text = await resDeliver.text().catch(() => '');

    if (!resDeliver.ok && resDeliver.status !== 409) {
      console.error('[WB] deliver error:', resDeliver.status, text);
      throw new Error(`WB deliver HTTP ${resDeliver.status}`);
    }

    if (!resDeliver.ok) {
      console.warn('[WB] deliver non-ok but acceptable status:', resDeliver.status, text);
    }
  } catch (err) {
    console.error('[WB] deliver call failed:', err);
    // Не падаем, всё равно пробуем получить QR.
  }

  // 2) Получаем QR-код поставки
  const barcodeUrl =
    `${base}/api/v3/supplies/${encodeURIComponent(shipmentCode)}/barcode?type=png`;

  console.log('[WB] GET supply barcode:', barcodeUrl);

  const resQr = await fetch(barcodeUrl, {
    method: 'GET',
    headers,
  });

  const text = await resQr.text();
  if (!resQr.ok) {
    console.error('[WB] barcode error:', resQr.status, text);
    throw new Error(`WB barcode HTTP ${resQr.status}`);
  }

  let qrBase64 = null;

  try {
    const payload = JSON.parse(text);
    console.log('[WB] barcode payload keys =', Object.keys(payload || {}));

    // ВАЖНО: WB сейчас кладёт картинку именно в "file"
    const rawBase64 = payload && payload.file;
    if (!rawBase64 || typeof rawBase64 !== 'string') {
      throw new Error('WB barcode: field "file" is empty or missing');
    }

    // Если вдруг WB начнёт отдавать уже data:URL — не трогаем.
    if (/^data:image\//i.test(rawBase64)) {
      qrBase64 = rawBase64;
    } else {
      qrBase64 = 'data:image/png;base64,' + rawBase64;
    }
  } catch (e) {
    console.error('[WB] barcode JSON parse error:', e);
    console.error('[WB] barcode raw text prefix:', text.slice(0, 200));
    throw new Error('WB barcode parse error');
  }

  console.log(
    '[WB] barcode OK, qrBase64Len =',
    qrBase64 ? qrBase64.length : 0,
    'prefix =',
    qrBase64 ? qrBase64.slice(0, 40) : null
  );

  return { qrBase64 };
}

module.exports = {
  fetchOrders,
  WB_ORDERS_URL,
  confirmShipmentAndGetQr,
};