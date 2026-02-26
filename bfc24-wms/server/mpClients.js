// Поиск карточки WB по клиенту WMS и штрихкоду.
//
// Поддерживает два варианта вызова:
//   findWbItemByClientAndBarcode(client, 7, '2037...')
//   findWbItemByClientAndBarcode(client, { clientId: 7, client_id: 7, wms_client_id: 7, barcode: '2037...' })
//
// Логика:
// 1) Находим список WB-аккаунтов (mp_client_accounts + mp_accounts) для client_id ВМС.
//    ВАЖНО: client_mp_account_id во всех wb-таблицах = id из mp_client_accounts.
// 2) Ищем barcode в mp_wb_items_barcodes по этим client_mp_account_id.
// 3) По найденному nm_id (а не chrt_id) подтягиваем карточку из mp_wb_items.
//    chrt_id используем только для логов.

async function findWbItemByClientAndBarcode(pgClient, arg1, arg2) {
  let wmsClientId;
  let barcode;

  // Вариант 1: объект { clientId / client_id / wms_client_id, barcode }
  if (arg2 === undefined && arg1 && typeof arg1 === 'object') {
    const obj = arg1;
    wmsClientId =
      obj.clientId ??
      obj.client_id ??
      obj.wms_client_id ??
      obj.client_mp_account_id; // на всякий случай
    barcode = obj.barcode;
  } else {
    // Вариант 2: позиционные аргументы (clientId, barcode)
    wmsClientId = arg1;
    barcode = arg2;
  }

  const barcodeStr = String(barcode || '').trim();
  if (!barcodeStr) {
    const err = new Error('EMPTY_BARCODE');
    err.code = 'EMPTY_BARCODE';
    throw err;
  }

  const clientIdNum = Number(wmsClientId);
  if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
    const err = new Error('INVALID_CLIENT_ID');
    err.code = 'INVALID_CLIENT_ID';
    throw err;
  }

  console.log('[mpClients] findWbItemByClientAndBarcode:', {
    wmsClientId: clientIdNum,
    barcode: barcodeStr,
  });

  // 0. Определяем WB-аккаунты, привязанные к этому клиенту ВМС.
  // БЕРЁМ id из mp_client_accounts (это именно client_mp_account_id в wb-таблицах!)
  const accRes = await pgClient.query(
    `
    SELECT DISTINCT mca.id AS mp_account_id
    FROM mp_client_accounts mca
    JOIN mp_accounts ma
      ON ma.supplier_id = mca.wb_supplier_id
    WHERE mca.client_id = $1
      AND LOWER(mca.marketplace) = 'wb'
      AND mca.is_active = true
      AND LOWER(ma.marketplace) = 'wb'
      AND ma.is_active = true
    `,
    [clientIdNum]
  );

  const accountIds = accRes.rows.map((r) => r.mp_account_id);
  console.log('[mpClients] WB accounts for client:', {
    wmsClientId: clientIdNum,
    accountIds,
  });

  // 1. Ищем баркод
  let barRow = null;

  if (accountIds.length > 0) {
    const barRes = await pgClient.query(
      `
      SELECT
        b.client_mp_account_id,
        b.nm_id,
        b.chrt_id,
        b.barcode
      FROM mp_wb_items_barcodes b
      WHERE b.barcode = $1
        AND b.client_mp_account_id = ANY($2::int[])
      ORDER BY b.client_mp_account_id
      LIMIT 1
      `,
      [barcodeStr, accountIds]
    );
    if (barRes.rowCount > 0) {
      barRow = barRes.rows[0];
    }
  }

  // Если по привязанным аккаунтам не нашли — ищем глобально
  if (!barRow) {
    const barResAny = await pgClient.query(
      `
      SELECT
        b.client_mp_account_id,
        b.nm_id,
        b.chrt_id,
        b.barcode
      FROM mp_wb_items_barcodes b
      WHERE b.barcode = $1
      ORDER BY b.client_mp_account_id
      LIMIT 1
      `,
      [barcodeStr]
    );
    if (barResAny.rowCount > 0) {
      barRow = barResAny.rows[0];
    }
  }

  if (!barRow) {
    const err = new Error('ITEM_NOT_FOUND');
    err.code = 'ITEM_NOT_FOUND';
    console.log('[mpClients] ITEM_NOT_FOUND in mp_wb_items_barcodes for', {
      wmsClientId: clientIdNum,
      barcode: barcodeStr,
    });
    throw err;
  }

  const accountIdForItem = barRow.client_mp_account_id;
  const nmId = barRow.nm_id;
  const chrtId = barRow.chrt_id; // только для логов/отладки

  // 2. Подтягиваем карточку из mp_wb_items
  let item;

  // 2.1. Основной кейс — по nm_id (ID карточки) и client_mp_account_id
  const itemRes = await pgClient.query(
    `
    SELECT
      i.client_mp_account_id,
      i.nm_id,
      i.chrt_id,
      i.article,
      i.barcode,
      i.item_name,
      i.brand,
      i.subject,
      i.size_name,
      i.preview_url,
      i.vendor_code,
      i.title
    FROM mp_wb_items i
    WHERE i.client_mp_account_id = $1
      AND i.nm_id              = $2
    LIMIT 1
    `,
    [accountIdForItem, nmId]
  );

  if (itemRes.rowCount > 0) {
    item = itemRes.rows[0];
  } else {
    // 2.2. Fallback: пробуем по barcode для того же аккаунта
    const itemByBarcodeRes = await pgClient.query(
      `
      SELECT
        i.client_mp_account_id,
        i.nm_id,
        i.chrt_id,
        i.article,
        i.barcode,
        i.item_name,
        i.brand,
        i.subject,
        i.size_name,
        i.preview_url,
        i.vendor_code,
        i.title
      FROM mp_wb_items i
      WHERE i.client_mp_account_id = $1
        AND i.barcode              = $2
      ORDER BY i.id DESC
      LIMIT 1
      `,
      [accountIdForItem, barcodeStr]
    );

    if (itemByBarcodeRes.rowCount > 0) {
      item = itemByBarcodeRes.rows[0];
      console.log('[mpClients] WB item found by BARCODE fallback:', {
        wmsClientId: clientIdNum,
        accountIdForItem,
        barcode: barcodeStr,
        nmId,
        chrtId,
      });
    } else {
      // 2.3. Глобальный fallback — по nm_id ИЛИ barcode, независимо от аккаунта
      const anyRes = await pgClient.query(
        `
        SELECT
          i.client_mp_account_id,
          i.nm_id,
          i.chrt_id,
          i.article,
          i.barcode,
          i.item_name,
          i.brand,
          i.subject,
          i.size_name,
          i.preview_url,
          i.vendor_code,
          i.title
        FROM mp_wb_items i
        WHERE (i.nm_id = $1 OR i.barcode = $2)
        ORDER BY (i.client_mp_account_id = $3) DESC, i.id
        LIMIT 1
        `,
        [nmId, barcodeStr, accountIdForItem]
      );

      if (anyRes.rowCount === 0) {
        const err = new Error('ITEM_NOT_FOUND');
        err.code = 'ITEM_NOT_FOUND';
        console.log('[mpClients] ITEM_NOT_FOUND in mp_wb_items for', {
          wmsClientId: clientIdNum,
          barcode: barcodeStr,
          nmId,
          chrtId,
          accountIdForItem,
        });
        throw err;
      }

      item = anyRes.rows[0];
      console.log('[mpClients] No exact card for account, GLOBAL fallback used:', {
        wmsClientId: clientIdNum,
        barcode: barcodeStr,
        nmId,
        chrtId,
        accountIdForItem,
        chosenAccountId: item.client_mp_account_id,
      });
    }
  }

  // 3. Возвращаем нормализованный объект
  return {
    mp_account_id: item.client_mp_account_id, // это id из mp_client_accounts
    nm_id: item.nm_id,
    chrt_id: item.chrt_id,
    article: item.article,
    vendor_code: item.vendor_code,
    title: item.title,
    barcode: barcodeStr,      // именно тот штрихкод, который сканировали
    item_name: item.item_name,
    brand: item.brand,
    subject: item.subject,
    size_name: item.size_name,
    preview_url: item.preview_url,
  };
}

module.exports = {
  findWbItemByClientAndBarcode,
};
