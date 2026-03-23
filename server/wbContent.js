// wbContent.js
require('dotenv').config();

/**
 * Загрузка карточек товаров через WB Content API
 * WB_CONTENT_API_TOKEN — токен из .env
 */
async function fetchWbCardsForAccount({ client_mp_account_id, limit = 100 }) {
  const wbContentToken = process.env.WB_CONTENT_API_TOKEN;
  if (!wbContentToken) {
    throw new Error('WB_CONTENT_API_TOKEN не задан в .env');
  }

  const perPageLimit = Math.min(Math.max(Number(limit) || 100, 1), 100);
  const maxPages = 50;
  const url = 'https://content-api.wildberries.ru/content/v2/get/cards/list';

  console.log(
    '[WB_ITEMS] START pagination with perPageLimit =',
    perPageLimit,
    'maxPages =',
    maxPages
  );

  const allCards = [];
  let cursor = null;
  let page = 0;

  while (page < maxPages) {
    page += 1;

    console.log(
      `[WB_ITEMS] PAGE ${page}: request to ${url} with cursor =`,
      cursor
    );

    const body = {
      settings: {
        cursor: {
          limit: perPageLimit,
          nmID: cursor?.nmID || 0,
          updatedAt: cursor?.updatedAt || '1970-01-01T00:00:00Z',
        },
        filter: {
          withPhoto: -1, // без фильтра по фото, берём всё
        },
      },
    };

    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: wbContentToken,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!resp.ok) {
      const txt = await resp.text();
      console.error(
        `[WB_ITEMS] HTTP error on page ${page}:`,
        resp.status,
        txt
      );
      throw new Error('WB_ITEMS HTTP ' + resp.status);
    }

    const data = await resp.json();

    const cards = Array.isArray(data.cards) ? data.cards : [];
    cursor = data.cursor || null;

    console.log(
      `[WB_ITEMS] PAGE ${page}: received cards =`,
      cards.length,
      ', cursor =',
      cursor
    );

    allCards.push(...cards);

    const total = cursor?.total ? Number(cursor.total) : null;
    if (total && allCards.length >= total) {
      console.log(
        '[WB_ITEMS] Reached total from cursor: total =',
        total,
        ', collected =',
        allCards.length
      );
      break;
    }
  }

  console.log(
    '[WB_ITEMS] FINISH pagination, total cards collected =',
    allCards.length
  );

  return allCards;
}

module.exports = { fetchWbCardsForAccount };
