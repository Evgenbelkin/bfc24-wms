// wbItemsService.js
const axios = require('axios');

/**
 * Пагинированная загрузка карточек WB через Content API.
 *
 * wbToken  — токен WB (категория Promotion / Content)
 * options:
 *   - limit    — сколько карточек за один запрос (по умолчанию 100)
 *   - maxPages — максимум страниц (по умолчанию 50)
 *   - filter   — объект filter из WB (по умолчанию { withPhoto: -1 })
 *   - sortAscending — сортировка по updatedAt (true = от старых к новым)
 */
async function fetchWbItems(wbToken, options = {}) {
  const limit = Number(options.limit) || 100;
  const maxPages = Number(options.maxPages) || 50;
  const filter = options.filter || { withPhoto: -1 };
  const sortAscending = options.sortAscending ?? true;

  const url = 'https://content-api.wildberries.ru/content/v2/get/cards/list';

  let allCards = [];
  // первый запрос – только limit, как в доке
  let cursor = { limit };

  for (let page = 1; page <= maxPages; page++) {
    const body = {
      settings: {
        sort: { ascending: sortAscending },
        filter,
        cursor,
      },
    };

    console.log(`[WB_ITEMS] PAGE ${page} : request to ${url} cursor =`, cursor);

    const res = await axios.post(url, body, {
      headers: {
        Authorization: wbToken,
        'Content-Type': 'application/json',
      },
      timeout: 60000,
    });

    const data = res.data || {};
    const cards = Array.isArray(data.cards) ? data.cards : [];
    const respCursor = data.cursor || null;

    allCards.push(...cards);

    console.log(
      `[WB_ITEMS] PAGE ${page}: received ${cards.length}, collected=${allCards.length}, cursor=`,
      respCursor
    );

    // Если нет cursor или total – дальше идти некуда
    if (!respCursor || typeof respCursor.total !== 'number') {
      console.log('[WB_ITEMS] stop: no cursor or no total in cursor');
      break;
    }

    // Если страница пустая – тоже стоп
    if (cards.length === 0) {
      console.log('[WB_ITEMS] stop: empty page');
      break;
    }

    // Ключевая логика из доки:
    // "Повторяйте, пока total в ответе >= limit запроса.
    //  Когда total < limit — карточки закончились."
    if (respCursor.total < limit) {
      console.log(
        '[WB_ITEMS] stop: cursor.total < limit => last page, total =',
        respCursor.total,
        'limit =',
        limit
      );
      break;
    }

    // Готовим курсор для следующей страницы
    cursor = {
      limit,
      updatedAt: respCursor.updatedAt,
      nmID: respCursor.nmID,
    };
  }

  console.log(
    '[WB_ITEMS] FINISH pagination, total cards collected =',
    allCards.length
  );

  return allCards;
}

/**
 * Достаём баркоды из карточки WB.
 * Если у тебя уже была рабочая версия — можешь оставить её,
 * но здесь даю стандартный вариант.
 */
function extractCardBarcodes(card) {
  const result = [];
  if (!card || !Array.isArray(card.sizes)) return result;

  for (const size of card.sizes) {
    const chrtId = size.chrtID || size.chrtId || null;
    const skus = Array.isArray(size.skus) ? size.skus : [];
    for (const sku of skus) {
      result.push({
        nm_id: card.nmID,
        chrt_id: chrtId,
        barcode: sku,
      });
    }
  }

  return result;
}

module.exports = {
  fetchWbItems,
  extractCardBarcodes,
};
