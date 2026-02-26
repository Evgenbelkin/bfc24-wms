// services/wbNormalize.js

/**
 * Нормализует сырую карточку WB под нашу таблицу masterdata.items
 *
 * raw     — одна карточка из ответа WB API
 * clientId — ID клиента из нашей БД
 */
function normalizeWbItem(raw, clientId) {
  const safeTrim = (val) =>
    typeof val === "string" ? val.trim() : val == null ? "" : String(val).trim();

  // nmID бывает nmID / nmid / nmId — подстрахуемся
  const nmId =
    raw.nmID ??
    raw.nmid ??
    raw.nmId ??
    raw.nmidId ??
    null;

  const title = safeTrim(raw.title);
  const vendorCode = safeTrim(raw.vendorCode || raw.vendor_code);
  const brand = safeTrim(raw.brand);

  return {
    client_id: clientId,
    wb_nm_id: nmId ? Number(nmId) : null,
    vendor_code: vendorCode || null,
    brand: brand || "NoBrand",
    item_name: title || (vendorCode || "Без названия"),
    is_active: true, // базово считаем, что карточка активна
  };
}

module.exports = {
  normalizeWbItem,
};
