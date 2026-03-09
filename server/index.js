// --- ВЕРХ ФАЙЛА ОСТАВЬ В ТАКОМ ВИДЕ ---

require('dotenv').config();
console.log('=== BFC24 WMS: index.js LOADED ===', __filename);

const express = require('express');
const path = require('path');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');

const { syncWbOrdersForAccount } = require('./services/wbOrdersSync');
const { pool } = require('./db');
const { findWbItemByClientAndBarcode } = require('./mpClients');
const { authRequired, requireRole } = require('./authMiddleware');

// WB сервис один раз
console.log('[index.js] resolved wbService =', require.resolve('./wbService'));
const wbService = require('./wbService');
const { fetchOrders, WB_ORDERS_URL } = wbService;

const { fetchWbItems, extractCardBarcodes } = require('./wbItemsService');

// ⬇️ WB API для складов/остатков
const {
  fetchWbWarehouses,
  fetchWbFbsStocks,
  fetchWbFboStocks,
} = require('./serviceswbApi');

const app = express();


// -------------------------------------------------------
// ГЛОБАЛЬНЫЙ ЛОГГЕР — должен идти первым
// -------------------------------------------------------
app.use((req, res, next) => {
  console.log('INCOMING:', req.method, req.url);
  next();
});

// ==============================
// DEBUG: список зарегистрированных роутов
// GET /debug/routes
// ==============================
app.get('/debug/routes', (req, res) => {
  const list = [];

  const router = req.app && req.app._router;
  if (router && router.stack) {
    router.stack.forEach((layer) => {
      if (layer.route && layer.route.path) {
        const methods = Object.keys(layer.route.methods)
          .filter((m) => layer.route.methods[m])
          .map((m) => m.toUpperCase());

        list.push({
          path: layer.route.path,
          methods,
        });
      }
    });
  }

  res.json({ routes: list });
});


// Статика
const publicDir = path.join(__dirname, 'public');
console.log('Serving static from:', publicDir);
app.use(express.static(publicDir));

// Хелсчек
app.get('/ping-root', (req, res) => {
  res.json({ status: 'ok', from: 'root' });
});

app.get('/debug/wb-service', (req, res) => {
  res.json({ WB_ORDERS_URL });
});

// Явный роут меню
app.get('/menu.html', (req, res) => {
  res.sendFile(path.join(publicDir, 'menu.html'));
});

// Корень → меню
app.get('/', (req, res) => {
  res.redirect('/menu.html');
});

// Основные middleware
app.use(express.json());
app.use(cors());

/**
 * 🔥 Импорт остатков Wildberries (FBS + задел под FBO) для одного MP-аккаунта
 * вызывается из модуля mpClients (/mp-accounts.html)
 * POST /mp/wb/import-stocks?account_id=...
 */
app.post('/mp/wb/import-stocks', authRequired, requireRole('owner'), async (req, res) => {
  const accountId = Number(req.query.account_id);
  if (!accountId) {
    return res.status(400).json({ error: 'account_id required' });
  }

  try {
    // 1. читаем аккаунт из mp_accounts
    const { rows: [acc] } = await pool.query(
      `
      SELECT id, label, marketplace, supplier_id, api_token
      FROM mp_accounts
      WHERE id = $1 AND marketplace = 'wb' AND is_active = true
      `,
      [accountId]
    );

    if (!acc) {
      return res.status(404).json({ error: 'account_not_found' });
    }
    if (!acc.api_token) {
      return res.status(400).json({ error: 'api_token_not_set' });
    }

    const apiToken = acc.api_token;

    // 2. тянем склады продавца (FBS)
    const warehouses = await fetchWbWarehouses(apiToken);

    await pool.query('BEGIN');

    // 3. чистим предыдущие остатки по этому аккаунту
    await pool.query('DELETE FROM mp_wb_stocks_fbs WHERE account_id = $1', [accountId]);
    await pool.query('DELETE FROM mp_wb_stocks_fbo WHERE account_id = $1', [accountId]);

    // 4. обновляем справочник складов
    for (const w of warehouses) {
      const wbWarehouseId = w.id ?? w.warehouseId;
      const wbName = w.name ?? w.nameRu ?? `WB склад ${wbWarehouseId}`;

      await pool.query(
        `
        INSERT INTO mp_wb_warehouses (account_id, wb_warehouse_id, name, warehouse_type, is_active, updated_at)
        VALUES ($1,$2,$3,$4,true,NOW())
        ON CONFLICT (account_id, wb_warehouse_id)
        DO UPDATE SET
          name = EXCLUDED.name,
          warehouse_type = EXCLUDED.warehouse_type,
          is_active = true,
          updated_at = NOW()
        `,
        [
          accountId,
          wbWarehouseId,
          wbName,
          'FBS', // эти склады считаем FBS
        ]
      );
    }

    // 5. тянем остатки по каждому складу FBS
    let fbsCount = 0;

    for (const w of warehouses) {
      const wbWarehouseId = w.id ?? w.warehouseId;
      if (!wbWarehouseId) continue;

      const stocks = await fetchWbFbsStocks(apiToken, wbWarehouseId);

      for (const s of stocks) {
        const nmId = s.nmId ?? s.nm_id;
        const chrtId = s.chrtId ?? s.chrt_id;
        const barcode = s.barcode;
        const qty = s.quantity ?? s.qty ?? 0;

        if (!barcode || !chrtId || !nmId) continue;

        await pool.query(
          `
          INSERT INTO mp_wb_stocks_fbs (account_id, wb_warehouse_id, nm_id, chrt_id, barcode, qty, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,NOW())
          ON CONFLICT (account_id, wb_warehouse_id, chrt_id, barcode)
          DO UPDATE SET
            qty = EXCLUDED.qty,
            updated_at = NOW()
          `,
          [accountId, wbWarehouseId, nmId, chrtId, barcode, qty]
        );

        fbsCount++;
      }
    }

    // 6. FBO — пока не заполняем (fetchWbFboStocks сейчас заглушка)
    let fboCount = 0;

    await pool.query('COMMIT');

    res.json({
      status: 'ok',
      account_id: accountId,
      fbs_rows: fbsCount,
      fbo_rows: fboCount,
    });
  } catch (err) {
    await pool.query('ROLLBACK');
    console.error('mp/wb/import-stocks error:', err);
    res.status(500).json({ error: 'internal_error' });
  }
});


/**
 * 🔹 Список MP-аккаунтов
 * GET /mp/accounts?marketplace=wb
 */
app.get('/mp/accounts', authRequired, requireRole(['owner', 'admin']), async (req, res) => {
  const marketplace = req.query.marketplace || 'wb';

  try {
    const { rows } = await pool.query(
      `
      SELECT
        id,
        label,
        marketplace,
        account_code,
        supplier_id,
        is_active,
        expiry_date,
        wms_client_id,
        (api_token IS NOT NULL AND length(trim(api_token)) > 0) AS has_api_token,
        created_at,
        updated_at
      FROM public.mp_accounts
      WHERE marketplace = $1
      ORDER BY id
      `,
      [marketplace]
    );

    res.json(rows);
  } catch (err) {
    console.error('GET /mp/accounts error:', err);
    res.status(500).json({ error: 'internal_error' });
  }
});


/**
 * 🔹 Создание MP-аккаунта
 * POST /mp/accounts
 * body: { label, marketplace, account_code, supplier_id, api_token, is_active, expiry_date, wms_client_id }
 */
app.post('/mp/accounts', authRequired, requireRole('owner'), async (req, res) => {
  const {
    label,
    marketplace = 'wb',
    account_code = null,
    supplier_id = null,
    api_token = null,
    is_active = true,
    expiry_date = null,
    wms_client_id = null,
  } = req.body || {};

  if (!label) {
    return res.status(400).json({ error: 'label required' });
  }

  try {
    const { rows: [acc] } = await pool.query(
      `
      INSERT INTO public.mp_accounts
        (label, marketplace, account_code, supplier_id, api_token, is_active, expiry_date, wms_client_id, created_at, updated_at)
      VALUES
        ($1,$2,$3,$4,$5,$6,$7,$8,NOW(),NOW())
      RETURNING
        id,
        label,
        marketplace,
        account_code,
        supplier_id,
        is_active,
        expiry_date,
        wms_client_id,
        (api_token IS NOT NULL AND length(trim(api_token)) > 0) AS has_api_token,
        created_at,
        updated_at
      `,
      [label, marketplace, account_code, supplier_id, api_token, is_active, expiry_date, wms_client_id]
    );

    res.status(201).json(acc);
  } catch (err) {
    console.error('POST /mp/accounts error:', err);
    res.status(500).json({ error: 'internal_error' });
  }
});


/**
 * 🔹 Обновление MP-аккаунта
 * PATCH /mp/accounts/:id
 * body: любые из { label, account_code, supplier_id, api_token, is_active, expiry_date, wms_client_id }
 *
 * Важно:
 * - если api_token не передан (undefined) — не трогаем
 * - если api_token передан как null/"" — очищаем
 */
app.patch('/mp/accounts/:id', authRequired, requireRole('owner'), async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ error: 'invalid_id' });

  const {
    label,
    account_code,
    supplier_id,
    api_token,      // может быть undefined / null / ""
    is_active,
    expiry_date,
    wms_client_id,
  } = req.body || {};

  try {
    const { rows: [acc] } = await pool.query(
      `
      UPDATE public.mp_accounts
      SET
        label         = COALESCE($1, label),
        account_code  = COALESCE($2, account_code),
        supplier_id   = COALESCE($3, supplier_id),
        is_active     = COALESCE($4, is_active),
        expiry_date   = COALESCE($5, expiry_date),
        wms_client_id = COALESCE($6, wms_client_id),
        api_token     = CASE
                          WHEN $7::text IS NULL THEN api_token     -- не передали поле -> не трогаем
                          WHEN length(trim($7::text)) = 0 THEN NULL -- передали пусто -> очистка
                          ELSE $7::text
                        END,
        updated_at    = NOW()
      WHERE id = $8
      RETURNING
        id,
        label,
        marketplace,
        account_code,
        supplier_id,
        is_active,
        expiry_date,
        wms_client_id,
        (api_token IS NOT NULL AND length(trim(api_token)) > 0) AS has_api_token,
        created_at,
        updated_at
      `,
      [
        label ?? null,
        account_code ?? null,
        supplier_id ?? null,
        is_active ?? null,
        expiry_date ?? null,
        wms_client_id ?? null,
        (typeof api_token === 'undefined') ? null : String(api_token ?? ''), // см. CASE выше
        id
      ]
    );

    if (!acc) {
      return res.status(404).json({ error: 'account_not_found' });
    }

    res.json(acc);
  } catch (err) {
    console.error('PATCH /mp/accounts/:id error:', err);
    res.status(500).json({ error: 'internal_error' });
  }
});



// -------------------------------------------------------
// Остальной код ниже (роуты, БД и т.д.)
// -------------------------------------------------------

/**
 * Найти активный WB-аккаунт по client_id WMS.
 * Использует public.mp_accounts.
 */
async function getWbAccountForClient(dbClient, wmsClientId) {
  const { rows } = await dbClient.query(
    `
      SELECT id, label, api_token, supplier_id, wms_client_id
      FROM public.mp_accounts
      WHERE marketplace   = 'wb'
        AND is_active     = TRUE
        AND wms_client_id = $1
      ORDER BY id
      LIMIT 1
    `,
    [wmsClientId]
  );

  if (!rows.length) {
    throw new Error(`WB account for wms_client_id=${wmsClientId} not found or inactive`);
  }

  const acc = rows[0];

  if (!acc.api_token) {
    throw new Error(`WB account id=${acc.id} has empty api_token`);
  }

  console.log('[WB] use account', acc.id, acc.label, 'for wms_client_id=', wmsClientId);

  return acc;
}

// ---------------- ВАЛИДАЦИЯ MOVEMENT_TYPE ----------------
// Разрешённые типы движений
const validMovementTypes = [
  'incoming',   // приход
  'move',       // перемещение
  'writeoff',   // списание
  'inventory',  // инвентаризация
  'picking',    // списание при подборе
  'adjust',     // ручная корректировка остатка
];

// Нормализация типа движения без строгой валидации.
// ВАЖНО: эта функция БОЛЬШЕ НИЧЕГО НЕ БРОСАЕТ.
function validateMovementType(rawMovementType) {
  // Приводим к строке, обрезаем пробелы, в нижний регистр
  const mt = String(rawMovementType || '').trim().toLowerCase();
  return mt;
}




// ---------------- REF_TYPE / REF_ID ----------------

// Допустимые типы ссылок на «первичный документ» движения
// manual         — ручная операция (по умолчанию)
// wb_dispatch    — отгрузка WB
// wb_acceptance  — приёмка WB
// inventory_task — задание на инвентаризацию
const validRefTypes = ['manual', 'wb_dispatch', 'wb_acceptance', 'inventory_task'];

/**
 * Нормализуем ref_type:
 *  - если не передан или мусор — вернём 'manual'
 *  - приводим к нижнему регистру и проверяем по списку
 */
function normalizeRefType(rawRefType) {
  if (!rawRefType) return 'manual';

  const val = String(rawRefType).trim().toLowerCase();
  if (validRefTypes.includes(val)) {
    return val;
  }
  // Если прилетела фигня — считаем, что это ручная операция
  return 'manual';
}

/**
 * Нормализуем ref_id:
 *  - если не число или <= 0 — считаем, что ссылки нет -> null
 *  - иначе возвращаем целое число
 */
function normalizeRefId(rawRefId) {
  if (rawRefId === undefined || rawRefId === null || rawRefId === '') {
    return null;
  }
  const n = Number(rawRefId);
  if (!Number.isInteger(n) || n <= 0) {
    return null;
  }
  return n;
}


// Строгая проверка МХ по коду.
// Никаких автосозданий: если нет такой ячейки или она неактивна — ошибка.
async function resolveLocationIdStrict(dbClient, locationCode) {
  const locCode = String(locationCode || '').trim();

  if (!locCode) {
    const err = new Error('LOCATION_CODE_EMPTY');
    err.code = 'LOCATION_CODE_EMPTY';
    throw err;
  }

  console.log('[resolveLocationIdStrict] check location', locCode);

  const res = await dbClient.query(
    `
    SELECT id, code, description, is_active
    FROM masterdata.locations
    WHERE code = $1
    LIMIT 1
    `,
    [locCode]
  );

  console.log('[resolveLocationIdStrict] rowCount =', res.rowCount);

  if (res.rowCount === 0 || res.rows[0].is_active === false) {
    const err = new Error('LOCATION_NOT_FOUND');
    err.code = 'LOCATION_NOT_FOUND';
    throw err;
  }

  return {
    locationId: res.rows[0].id,
    location: res.rows[0],
  };
}


// ---------------- ПОДКЛЮЧЕНИЕ К БД ----------------
// ⛔ здесь больше НЕТ new Pool(...)
// пул берём из ./db (см. верх файла)

// Простой лог запросов
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const ms = Date.now() - start;
    console.log(`${req.method} ${req.originalUrl} ${res.statusCode} - ${ms}ms`);
  });
  next();
});


// Простой лог запросов
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const ms = Date.now() - start;
    console.log(`${req.method} ${req.originalUrl} ${res.statusCode} - ${ms}ms`);
  });
  next();
});

// ---------------- БАЗОВЫЕ ЭНДПОИНТЫ ----------------

app.get('/test-db', async (req, res) => {
  try {
    const result = await pool.query('SELECT NOW() AS now');
    res.json({ status: 'ok', time: result.rows[0].now });
  } catch (err) {
    console.error('DB error:', err);
    res.status(500).json({ error: 'DB connection error' });
  }
});


// ---------------- AUTH: /login, /me ----------------

app.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body || {};

    if (!username || !password) {
      return res.status(400).json({ error: 'username и password обязательны' });
    }

    const q = `
      SELECT
        id,
        username,
        full_name,
        password_hash,
        role,
        active
      FROM public.admin_users
      WHERE username = $1
      LIMIT 1
    `;
    const r = await pool.query(q, [username]);

    if (r.rowCount === 0) {
      return res.status(401).json({ error: 'Неверный логин или пароль' });
    }

    const user = r.rows[0];

    if (!user.active) {
      return res.status(403).json({ error: 'Пользователь отключен' });
    }

    const ok = await bcrypt.compare(password, user.password_hash || '');
    if (!ok) {
      return res.status(401).json({ error: 'Неверный логин или пароль' });
    }

    if (!process.env.JWT_SECRET) {
      return res.status(500).json({ error: 'JWT_SECRET не задан' });
    }

    const payload = {
      id: user.id,
      username: user.username,
      role: user.role
    };

    const token = jwt.sign(payload, process.env.JWT_SECRET, {
      expiresIn: process.env.JWT_EXPIRES || '12h',
    });

    return res.json({
      status: 'ok',
      token,
      user: {
        id: user.id,
        username: user.username,
        full_name: user.full_name,
        role: user.role,
        active: user.active
      },
    });
  } catch (err) {
    console.error('Login error:', err);
    return res.status(500).json({ error: 'Login error' });
  }
});

app.get('/me', authRequired, (req, res) => {
  res.json({ status: 'ok', user: req.user });
});

// ---------------- USERS (AUTH.USERS) ----------------

app.get('/users', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT id, username, role, is_active
       FROM auth.users
       ORDER BY id`
    );
    res.json({ status: 'ok', users: r.rows });
  } catch (err) {
    console.error('Get users error:', err);
    res.status(500).json({ error: 'Get users error' });
  }
});

app.post('/users', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const { username, password, role, is_active } = req.body || {};

    if (!username || !password || !role) {
      return res.status(400).json({ error: 'Нужны username, password и role' });
    }

    if (password.length < 6) {
      return res.status(400).json({ error: 'Пароль должен быть минимум 6 символов' });
    }

    const isActive = is_active === undefined ? true : !!is_active;

    const exist = await pool.query(
      'SELECT id FROM auth.users WHERE username = $1',
      [username]
    );
    if (exist.rowCount > 0) {
      return res.status(409).json({ error: 'Такой username уже есть' });
    }

    const hash = await bcrypt.hash(password, 10);

    const ins = await pool.query(
      `INSERT INTO auth.users (username, password_hash, role, is_active)
       VALUES ($1, $2, $3, $4)
       RETURNING id, username, role, is_active`,
      [username, hash, role, isActive]
    );

    res.status(201).json({
      status: 'ok',
      user: ins.rows[0],
    });
  } catch (err) {
    console.error('Create user error:', err);
    res.status(500).json({ error: 'Create user error' });
  }
});

app.get('/admin-only', authRequired, requireRole('owner', 'admin'), (req, res) => {
  res.json({ status: 'ok', message: 'Доступ есть', user: req.user });
});


// ---------- СПРАВОЧНИК ТОВАРОВ (masterdata.items) ----------
// GET /items
// query:
//   client_id (необязательный; если есть — фильтр по клиенту)
//   barcode   (опционально — строка поиска по штрихкоду ИЛИ части названия)
app.get('/items', authRequired, async (req, res) => {
  try {
    const clientIdRaw = req.query.client_id;
    const search = (req.query.barcode || '').trim();

    const params = [];
    const whereParts = [];

    // Фильтр по client_id, ЕСЛИ он передан
    if (clientIdRaw !== undefined && clientIdRaw !== '') {
      const clientId = Number(clientIdRaw);
      if (!clientId || clientId <= 0) {
        return res.status(400).json({
          error: 'bad_request',
          detail: 'client_id должен быть > 0, если указан',
        });
      }
      params.push(clientId);
      whereParts.push(`i.client_id = $${params.length}`);
    }

    // Поиск по barcode / части названия
    if (search) {
      params.push(`%${search}%`);
      whereParts.push(
        `(i.barcode ILIKE $${params.length} OR i.item_name ILIKE $${params.length})`
      );
    }

    const whereSql = whereParts.length
      ? 'WHERE ' + whereParts.join(' AND ')
      : '';

    const q = `
      SELECT
        i.id,
        i.client_id,
        c.client_name,
        i.barcode,
        i.item_name,
        i.vendor_code,
        i.wb_vendor_code,
        i.brand,
        i.unit,
        i.volume_liters,
        i.needs_packaging,
        i.cost_price,
        i.processing_fee,
        i.is_active
      FROM masterdata.items i
      LEFT JOIN masterdata.clients c
        ON c.id = i.client_id
      ${whereSql}
      ORDER BY i.id
    `;

    const r = await pool.query(q, params);
    return res.json({ items: r.rows });
  } catch (err) {
    console.error('GET /items error', err);
    return res.status(500).json({
      error: 'items_error',
      detail: err.message,
    });
  }
});


// POST /items — создать товар
app.post('/items', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const {
      barcode,
      client_id,
      item_name,
      vendor_code,
      wb_vendor_code,
      brand,
      unit,
      volume_liters,
      length_cm,
      width_cm,
      height_cm,
      weight_grams,
      cost_price,
      processing_fee,
      need_packaging, // API-поле
      is_active,
    } = req.body || {};

    if (!barcode || !client_id || !item_name) {
      return res.status(400).json({
        error: 'Обязательные поля: barcode, client_id, item_name',
      });
    }

    // Валидация литража
    const litersErr = validateLitersRule(unit, volume_liters);
    if (litersErr) {
      return res.status(400).json({ error: litersErr });
    }

    const clientIdNum = Number(client_id);
    if (!clientIdNum) {
      return res.status(400).json({ error: 'client_id должен быть числом' });
    }

    const clientCheck = await pool.query(
      `SELECT id FROM masterdata.clients WHERE id = $1 LIMIT 1`,
      [clientIdNum]
    );
    if (clientCheck.rowCount === 0) {
      return res.status(400).json({ error: 'client_id не существует' });
    }

    // Проверка уникальности штрихкода
    const existsBarcode = await pool.query(
      `SELECT id FROM masterdata.items WHERE barcode = $1 LIMIT 1`,
      [barcode]
    );
    if (existsBarcode.rowCount > 0) {
      return res.status(409).json({ error: 'Такой barcode уже есть' });
    }

    const isActiveVal = is_active === undefined ? true : !!is_active;
    const needPackagingVal = need_packaging === undefined ? false : !!need_packaging;

    const q = `
      INSERT INTO masterdata.items
      (
        barcode,
        client_id,
        item_name,
        vendor_code,
        wb_vendor_code,
        brand,
        unit,
        volume_liters,
        length_cm,
        width_cm,
        height_cm,
        weight_grams,
        cost_price,
        processing_fee,
        needs_packaging,
        is_active,
        created_at
      )
      VALUES
      (
        $1, $2, $3, $4, $5, $6, $7,
        $8, $9, $10, $11, $12,
        $13, $14, $15, $16, NOW()
      )
      RETURNING
        id,
        barcode,
        client_id,
        item_name,
        vendor_code,
        wb_vendor_code,
        brand,
        unit,
        volume_liters,
        length_cm,
        width_cm,
        height_cm,
        weight_grams,
        cost_price,
        processing_fee,
        needs_packaging AS need_packaging,
        created_at,
        is_active
    `;

    const r = await pool.query(q, [
      barcode,
      clientIdNum,
      item_name,
      vendor_code || null,
      wb_vendor_code || null,
      brand || null,
      unit || null,
      volume_liters === undefined ? null : volume_liters,
      length_cm === undefined ? null : length_cm,
      width_cm === undefined ? null : width_cm,
      height_cm === undefined ? null : height_cm,
      weight_grams === undefined ? null : weight_grams,
      cost_price === undefined ? null : cost_price,
      processing_fee === undefined ? null : processing_fee,
      needPackagingVal,
      isActiveVal,
    ]);

    return res.status(201).json({ status: 'ok', item: r.rows[0] });
  } catch (err) {
    console.error('Create item error:', err);
    return res.status(500).json({ error: 'Create item error', detail: err.message });
  }
});

// PATCH /items/:id — обновить товар
app.patch('/items/:id', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) {
      return res.status(400).json({ error: 'Некорректный id' });
    }

    const {
      barcode,
      client_id,
      item_name,
      vendor_code,
      wb_vendor_code,
      brand,
      unit,
      volume_liters,
      length_cm,
      width_cm,
      height_cm,
      weight_grams,
      cost_price,
      processing_fee,
      need_packaging, // API-поле
      is_active,
    } = req.body || {};

    // Валидация литража (unit/volume_liters могут обновляться частично)
    if (unit !== undefined || volume_liters !== undefined) {
      let unitForCheck = unit;
      let volForCheck = volume_liters;

      if (unitForCheck === undefined || volForCheck === undefined) {
        const cur = await pool.query(
          `SELECT unit, volume_liters FROM masterdata.items WHERE id = $1 LIMIT 1`,
          [id]
        );
        if (cur.rowCount === 0) {
          return res.status(404).json({ error: 'Товар не найден' });
        }
        if (unitForCheck === undefined) unitForCheck = cur.rows[0].unit;
        if (volForCheck === undefined) volForCheck = cur.rows[0].volume_liters;
      }

      const litersErr = validateLitersRule(unitForCheck, volForCheck);
      if (litersErr) {
        return res.status(400).json({ error: litersErr });
      }
    }

    const fields = [];
    const values = [];
    let idx = 1;

    if (barcode !== undefined) {
      fields.push(`barcode = $${idx++}`);
      values.push(barcode);
    }

    if (client_id !== undefined) {
      const clientIdNum = Number(client_id);
      if (!clientIdNum) {
        return res.status(400).json({ error: 'client_id должен быть числом' });
      }
      const clientCheck = await pool.query(
        `SELECT id FROM masterdata.clients WHERE id = $1 LIMIT 1`,
        [clientIdNum]
      );
      if (clientCheck.rowCount === 0) {
        return res.status(400).json({ error: 'client_id не существует' });
      }
      fields.push(`client_id = $${idx++}`);
      values.push(clientIdNum);
    }

    if (item_name !== undefined) {
      fields.push(`item_name = $${idx++}`);
      values.push(item_name);
    }
    if (vendor_code !== undefined) {
      fields.push(`vendor_code = $${idx++}`);
      values.push(vendor_code || null);
    }
    if (wb_vendor_code !== undefined) {
      fields.push(`wb_vendor_code = $${idx++}`);
      values.push(wb_vendor_code || null);
    }
    if (brand !== undefined) {
      fields.push(`brand = $${idx++}`);
      values.push(brand || null);
    }
    if (unit !== undefined) {
      fields.push(`unit = $${idx++}`);
      values.push(unit || null);
    }
    if (volume_liters !== undefined) {
      fields.push(`volume_liters = $${idx++}`);
      values.push(volume_liters === null ? null : volume_liters);
    }
    if (length_cm !== undefined) {
      fields.push(`length_cm = $${idx++}`);
      values.push(length_cm === null ? null : length_cm);
    }
    if (width_cm !== undefined) {
      fields.push(`width_cm = $${idx++}`);
      values.push(width_cm === null ? null : width_cm);
    }
    if (height_cm !== undefined) {
      fields.push(`height_cm = $${idx++}`);
      values.push(height_cm === null ? null : height_cm);
    }
    if (weight_grams !== undefined) {
      fields.push(`weight_grams = $${idx++}`);
      values.push(weight_grams === null ? null : weight_grams);
    }
    if (cost_price !== undefined) {
      fields.push(`cost_price = $${idx++}`);
      values.push(cost_price === null ? null : cost_price);
    }
    if (processing_fee !== undefined) {
      fields.push(`processing_fee = $${idx++}`);
      values.push(processing_fee === null ? null : processing_fee);
    }
    if (need_packaging !== undefined) {
      fields.push(`needs_packaging = $${idx++}`);
      values.push(!!need_packaging);
    }
    if (is_active !== undefined) {
      fields.push(`is_active = $${idx++}`);
      values.push(!!is_active);
    }

    if (fields.length === 0) {
      return res.status(400).json({ error: 'Нет полей для обновления' });
    }

    // Проверка дубликата штрихкода при изменении barcode
    if (barcode !== undefined) {
      const existsBarcode = await pool.query(
        `SELECT id FROM masterdata.items WHERE barcode = $1 AND id <> $2 LIMIT 1`,
        [barcode, id]
      );
      if (existsBarcode.rowCount > 0) {
        return res.status(409).json({ error: 'Такой barcode уже занят другим товаром' });
      }
    }

    values.push(id);

    const q = `
      UPDATE masterdata.items
      SET ${fields.join(', ')}
      WHERE id = $${idx}
      RETURNING
        id,
        barcode,
        client_id,
        item_name,
        vendor_code,
        wb_vendor_code,
        brand,
        unit,
        volume_liters,
        length_cm,
        width_cm,
        height_cm,
        weight_grams,
        cost_price,
        processing_fee,
        needs_packaging AS need_packaging,
        created_at,
        is_active
    `;

    const r = await pool.query(q, values);

    if (r.rowCount === 0) {
      return res.status(404).json({ error: 'Товар не найден' });
    }

    return res.json({ status: 'ok', item: r.rows[0] });
  } catch (err) {
    console.error('Update item error:', err);
    return res.status(500).json({ error: 'Update item error', detail: err.message });
  }
});

// DELETE /items/:id — «мягкое» удаление (деактивация)
app.delete('/items/:id', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) {
      return res.status(400).json({ error: 'Некорректный id' });
    }

    const q = `
      UPDATE masterdata.items
      SET is_active = false
      WHERE id = $1
      RETURNING
        id,
        barcode,
        client_id,
        item_name,
        vendor_code,
        wb_vendor_code,
        brand,
        unit,
        volume_liters,
        length_cm,
        width_cm,
        height_cm,
        weight_grams,
        cost_price,
        processing_fee,
        needs_packaging AS need_packaging,
        created_at,
        is_active
    `;

    const r = await pool.query(q, [id]);

    if (r.rowCount === 0) {
      return res.status(404).json({ error: 'Товар не найден' });
    }

    return res.json({ status: 'ok', item: r.rows[0] });
  } catch (err) {
    console.error('Delete item error:', err);
    return res.status(500).json({ error: 'Delete item error', detail: err.message });
  }
});




// ---------------- ITEMS (MASTERDATA.ITEMS) ----------------
// Таблица masterdata.items (актуально):
// id, barcode, client_id, item_name, vendor_code, wb_vendor_code, brand, unit,
// volume_liters, length_cm, width_cm, height_cm, weight_grams,
// cost_price, processing_fee, needs_packaging, created_at, is_active
//
// ВАЖНО: наружу (API) отдаём поле need_packaging (единственное каноническое имя),
// а в БД пишем/читаем needs_packaging.
//
// ДОБАВЛЕНО: базовая валидация "литражных" товаров:
// если unit = 'л'/'L'/'liter'/'litre' (регистр не важен) — volume_liters обязателен (>0).

function isLiterUnit(unit) {
  if (!unit) return false;
  const u = String(unit).trim().toLowerCase();
  return ['л', 'l', 'liter', 'litre'].includes(u);
}

function parsePositiveNumberOrNull(v) {
  if (v === undefined || v === null || v === '') return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return NaN;
  return n;
}

function validateLitersRule(unit, volume_liters) {
  if (!isLiterUnit(unit)) return null;

  const vol = parsePositiveNumberOrNull(volume_liters);
  if (vol === null) {
    return 'Для литражного товара (unit=л/L) поле volume_liters обязательно';
  }
  if (Number.isNaN(vol)) {
    return 'volume_liters должен быть числом';
  }
  if (vol <= 0) {
    return 'volume_liters должен быть больше 0 для литражного товара';
  }
  return null;
}
// ---------- СПРАВОЧНИК КЛИЕНТОВ (masterdata.clients) ----------

// Полный список с фильтрами
app.get('/clients', authRequired, async (req, res) => {
  try {
    const { search, is_active } = req.query;

    const params = [];
    const whereParts = [];

    if (is_active === 'true') {
      params.push(true);
      whereParts.push(`c.is_active = $${params.length}`);
    } else if (is_active === 'false') {
      params.push(false);
      whereParts.push(`c.is_active = $${params.length}`);
    }

    if (search && search.trim() !== '') {
      params.push(`%${search.trim()}%`);
      whereParts.push(
        `(c.client_name ILIKE $${params.length} OR c.client_code ILIKE $${params.length})`
      );
    }

    const whereSql = whereParts.length ? 'WHERE ' + whereParts.join(' AND ') : '';

    const sql = `
      SELECT
        c.id,
        c.client_name,
        c.client_code,
        c.telegram_chat_id,
        c.is_active,
        c.created_at
      FROM masterdata.clients c
      ${whereSql}
      ORDER BY c.id;
    `;

    const result = await pool.query(sql, params);
    return res.json({ clients: result.rows });
  } catch (err) {
    console.error('Get clients error', err);
    return res.status(500).json({
      error: 'Get clients error',
      detail: err.message,
    });
  }
});

// Короткий список для селектов в UI
app.get('/clients-short', authRequired, async (req, res) => {
  try {
    const q = `
      SELECT id, client_name
      FROM masterdata.clients
      WHERE is_active = true
      ORDER BY id
    `;
    const r = await pool.query(q);
    return res.json({ clients: r.rows });
  } catch (err) {
    console.error('clients-short error', err);
    return res.status(500).json({
      error: 'clients-short error',
      detail: err.message,
    });
  }
});

// Создать клиента
app.post('/clients', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const {
      client_name,
      client_code,
      telegram_chat_id,
      is_active,
    } = req.body || {};

    if (!client_name || !client_code) {
      return res.status(400).json({
        error: 'Обязательные поля: client_name, client_code',
      });
    }

    // Код клиента должен быть уникальным
    const existsCode = await pool.query(
      `SELECT id FROM masterdata.clients WHERE client_code = $1 LIMIT 1`,
      [client_code]
    );
    if (existsCode.rowCount > 0) {
      return res.status(409).json({ error: 'Такой client_code уже существует' });
    }

    const isActiveVal = (is_active === undefined ? true : !!is_active);

    const q = `
      INSERT INTO masterdata.clients
      (
        client_name,
        client_code,
        telegram_chat_id,
        is_active,
        created_at
      )
      VALUES ($1, $2, $3, $4, NOW())
      RETURNING
        id,
        client_name,
        client_code,
        telegram_chat_id,
        is_active,
        created_at
    `;

    const r = await pool.query(q, [
      client_name,
      client_code,
      telegram_chat_id || null,
      isActiveVal,
    ]);

    return res.status(201).json({ status: 'ok', client: r.rows[0] });
  } catch (err) {
    console.error('Create client error:', err);
    return res.status(500).json({ error: 'Create client error', detail: err.message });
  }
});

// Обновить клиента
app.patch('/clients/:id', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) {
      return res.status(400).json({ error: 'Некорректный id' });
    }

    const {
      client_name,
      client_code,
      telegram_chat_id,
      is_active,
    } = req.body || {};

    const fields = [];
    const values = [];
    let idx = 1;

    if (client_name !== undefined) {
      fields.push(`client_name = $${idx++}`);
      values.push(client_name);
    }

    if (client_code !== undefined) {
      // проверим уникальность кода
      const existsCode = await pool.query(
        `SELECT id FROM masterdata.clients WHERE client_code = $1 AND id <> $2 LIMIT 1`,
        [client_code, id]
      );
      if (existsCode.rowCount > 0) {
        return res.status(409).json({ error: 'Такой client_code уже занят другим клиентом' });
      }
      fields.push(`client_code = $${idx++}`);
      values.push(client_code);
    }

    if (telegram_chat_id !== undefined) {
      fields.push(`telegram_chat_id = $${idx++}`);
      values.push(telegram_chat_id || null);
    }

    if (is_active !== undefined) {
      fields.push(`is_active = $${idx++}`);
      values.push(!!is_active);
    }

    if (fields.length === 0) {
      return res.status(400).json({ error: 'Нет полей для обновления' });
    }

    values.push(id);

    const q = `
      UPDATE masterdata.clients
      SET ${fields.join(', ')}
      WHERE id = $${idx}
      RETURNING
        id,
        client_name,
        client_code,
        telegram_chat_id,
        is_active,
        created_at
    `;

    const r = await pool.query(q, values);

    if (r.rowCount === 0) {
      return res.status(404).json({ error: 'Клиент не найден' });
    }

    return res.json({ status: 'ok', client: r.rows[0] });
  } catch (err) {
    console.error('Update client error:', err);
    return res.status(500).json({ error: 'Update client error', detail: err.message });
  }
});

// "Удаление" клиента = просто выключаем
app.delete('/clients/:id', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!id) {
      return res.status(400).json({ error: 'Некорректный id' });
    }

    const q = `
      UPDATE masterdata.clients
      SET is_active = false
      WHERE id = $1
      RETURNING
        id,
        client_name,
        client_code,
        telegram_chat_id,
        is_active,
        created_at
    `;

    const r = await pool.query(q, [id]);

    if (r.rowCount === 0) {
      return res.status(404).json({ error: 'Клиент не найден' });
    }

    return res.json({ status: 'ok', client: r.rows[0] });
  } catch (err) {
    console.error('Delete client error:', err);
    return res.status(500).json({ error: 'Delete client error', detail: err.message });
  }
});


// ==============================
// СПРАВОЧНИК МХ С АГРЕГАТАМИ ПО СКЛАДУ
// GET /locations/overview
// Параметры (опц.): search, is_active=true|false
// ==============================
app.get(
  '/locations/overview',
  authRequired,
  requireRole('owner', 'admin', 'supervisor'),
  async (req, res) => {
    try {
      const { search, is_active } = req.query;

      const values = [];
      const whereParts = [];

      if (search) {
        values.push(`%${search}%`);
        whereParts.push(`l.code ILIKE $${values.length}`);
      }

      if (is_active === 'true') {
        whereParts.push('l.is_active = TRUE');
      } else if (is_active === 'false') {
        whereParts.push('l.is_active = FALSE');
      }

      const whereSql = whereParts.length
        ? `WHERE ${whereParts.join(' AND ')}`
        : '';

      const sql = `
        SELECT
          l.id,
          l.code AS location_code,
          l.description,
          l.is_active,
          l.created_at,
          COALESCE(COUNT(DISTINCT s.sku_id), 0) AS total_sku,
          COALESCE(SUM(s.qty), 0)               AS total_qty
        FROM masterdata.locations l
        LEFT JOIN wms.stock s
          ON s.location_id = l.id
        ${whereSql}
        GROUP BY
          l.id, l.code, l.description, l.is_active, l.created_at
        ORDER BY l.code;
      `;

      const { rows } = await pool.query(sql, values);
      return res.json({ rows });
    } catch (err) {
      console.error('GET /locations/overview error:', err);
      return res.status(500).json({ error: 'Internal server error' });
    }
  }
);


// ==============================
// БАЗОВЫЙ СПИСОК МХ
// GET /locations
// Параметры (опц.): id, code|location_code, is_active=true|false
// ==============================
app.get(
  '/locations',
  authRequired,
  requireRole('owner'),
  async (req, res) => {
    try {
      const { id, code, location_code, is_active } = req.query || {};

      const conditions = [];
      const values = [];
      let idx = 1;

      if (id) {
        conditions.push(`id = $${idx++}`);
        values.push(Number(id));
      }

      const codeFilter = location_code || code;
      if (codeFilter) {
        conditions.push(`code ILIKE $${idx++}`);
        values.push(`%${String(codeFilter).trim()}%`);
      }

      if (is_active === 'true') {
        conditions.push('is_active = TRUE');
      } else if (is_active === 'false') {
        conditions.push('is_active = FALSE');
      }

      const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

      const q = `
        SELECT
          id,
          code AS location_code,
          description,
          is_active,
          created_at
        FROM masterdata.locations
        ${where}
        ORDER BY code
        LIMIT 500
      `;

      const r = await pool.query(q, values);

      return res.json({
        status: 'ok',
        locations: r.rows,
      });
    } catch (err) {
      console.error('Get locations error:', err);
      return res.status(500).json({
        error: 'Get locations error',
        detail: err.message,
        code: err.code,
      });
    }
  }
);


// ==============================
// СОЗДАНИЕ НОВОЙ МХ
// POST /locations
// body: { location_code (или code), description?, is_active? }
// ==============================
app.post(
  '/locations',
  authRequired,
  requireRole('owner'),
  async (req, res) => {
    try {
      const {
        location_code,
        code,           // старое имя для совместимости
        description,
        is_active,
      } = req.body || {};

      const codeStr = String(location_code || code || '').trim();
      if (!codeStr) {
        return res.status(400).json({
          error: 'Поле "location_code" (код МХ) обязательно',
        });
      }

      const descVal =
        description != null && String(description).trim() !== ''
          ? String(description).trim()
          : null;

      let isActiveVal = true;
      if (typeof is_active === 'boolean') {
        isActiveVal = is_active;
      } else if (typeof is_active === 'string') {
        isActiveVal = is_active === 'true';
      }

      // Проверяем дубликат
      const exist = await pool.query(
        `
        SELECT id
        FROM masterdata.locations
        WHERE code = $1
        LIMIT 1
        `,
        [codeStr]
      );

      if (exist.rowCount > 0) {
        return res.status(409).json({
          error: 'Место хранения с таким кодом уже существует',
        });
      }

      const ins = await pool.query(
        `
        INSERT INTO masterdata.locations
          (code, description, is_active, created_at)
        VALUES
          ($1,   $2,          $3,        NOW())
        RETURNING
          id,
          code AS location_code,
          description,
          is_active,
          created_at
        `,
        [codeStr, descVal, isActiveVal]
      );

      return res.status(201).json({
        status: 'ok',
        location: ins.rows[0],
      });
    } catch (err) {
      console.error('Create location error:', err);
      return res.status(500).json({
        error: 'Create location error',
        detail: err.message,
        code: err.code,
      });
    }
  }
);


// ==============================
// ОБНОВЛЕНИЕ МХ
// PATCH /locations/:id
// body: { location_code?, description?, is_active? }
// ==============================
app.patch(
  '/locations/:id',
  authRequired,
  requireRole('owner'),
  async (req, res) => {
    try {
      const id = Number(req.params.id);
      if (!id) {
        return res.status(400).json({ error: 'Некорректный id локации' });
      }

      const {
        location_code,
        code,           // для совместимости
        description,
        is_active,
      } = req.body || {};

      const sets = [];
      const values = [];
      let idx = 1;

      if (location_code !== undefined || code !== undefined) {
        const codeStr = String(location_code || code || '').trim();
        if (!codeStr) {
          return res.status(400).json({ error: 'location_code не может быть пустым' });
        }
        sets.push(`code = $${idx++}`);
        values.push(codeStr);
      }

      if (description !== undefined) {
        const descStr =
          description != null && String(description).trim() !== ''
            ? String(description).trim()
            : null;
        sets.push(`description = $${idx++}`);
        values.push(descStr);
      }

      if (is_active !== undefined) {
        let isActVal;
        if (typeof is_active === 'boolean') {
          isActVal = is_active;
        } else if (typeof is_active === 'string') {
          isActVal = is_active === 'true';
        } else {
          isActVal = !!is_active;
        }
        sets.push(`is_active = $${idx++}`);
        values.push(isActVal);
      }

      if (sets.length === 0) {
        return res.status(400).json({ error: 'Нет полей для обновления' });
      }

      values.push(id);
      const q = `
        UPDATE masterdata.locations
        SET ${sets.join(', ')}
        WHERE id = $${idx}
        RETURNING
          id,
          code AS location_code,
          description,
          is_active,
          created_at
      `;

      const r = await pool.query(q, values);

      if (r.rowCount === 0) {
        return res.status(404).json({ error: 'Локация не найдена' });
      }

      return res.json({
        status: 'ok',
        location: r.rows[0],
      });
    } catch (err) {
      console.error('Update location error:', err);
      return res.status(500).json({
        error: 'Update location error',
        detail: err.message,
        code: err.code,
      });
    }
  }
);


// ==============================
// ПРОСМОТР ОСТАТКОВ ПО КОНКРЕТНОЙ МХ
// GET /stock/by-location?location_code=XXX
// ==============================
app.get(
  '/stock/by-location',
  authRequired,
  requireRole('owner', 'admin', 'supervisor'),
  async (req, res) => {
    try {
      const { location_code } = req.query;

      if (!location_code) {
        return res.status(400).json({ error: 'location_code обязателен' });
      }

      // 1. Находим локацию по коду
      const qLoc = `
        SELECT id, code
        FROM masterdata.locations
        WHERE code = $1
        LIMIT 1
      `;
      const rLoc = await pool.query(qLoc, [location_code.trim()]);

      if (rLoc.rowCount === 0) {
        return res.status(404).json({ error: 'Локация не найдена' });
      }

      const locationId = rLoc.rows[0].id;

      // 2. Остатки по этой локации
      const qStock = `
        SELECT
          s.sku_id,
          s.qty,
          s.client_id,
          i.barcode,
          i.item_name,
          i.vendor_code,
          i.unit,
          i.volume_liters,
          i.needs_packaging
        FROM wms.stock s
        LEFT JOIN masterdata.items i
          ON s.sku_id = i.id
        WHERE s.location_id = $1
        ORDER BY i.item_name;
      `;
      const rStock = await pool.query(qStock, [locationId]);

      return res.json({
        location: {
          id: rLoc.rows[0].id,
          code: rLoc.rows[0].code,
        },
        rows: rStock.rows,
      });
    } catch (err) {
      console.error('GET /stock/by-location error:', err);
      return res.status(500).json({
        error: 'Internal server error',
        detail: err.message,
      });
    }
  }
);


// ======================= SKU REGISTRY (WMS.SKU) =======================
// Таблица сейчас: id, client_id, barcode, is_active, created_at
// Здесь держим связку client_id + barcode -> sku_id,
// чтобы sku_id не вводить руками в клиентах/скриптах.


// 1) Строгая проверка товара: он обязан быть в masterdata.items и быть активным.
//    Если НЕ нашли в masterdata.items — пробуем подтянуть карточку из WB
//    через связку client_id (WMS) → mp_client_accounts → mp_wb_items_*,
//    автоматически заводим запись в masterdata.items, и только после этого
//    создаём/находим SKU.
async function resolveSkuIdStrict(dbClient, { client_id, barcode }) {
  const clientIdNum = Number(client_id);
  const barcodeStr = String(barcode || '').trim();

  if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
    throw new Error('resolveSkuIdStrict: invalid client_id');
  }
  if (!barcodeStr) {
    throw new Error('resolveSkuIdStrict: empty barcode');
  }

  console.log('[resolveSkuIdStrict] client_id =', clientIdNum, 'barcode =', barcodeStr);

  let itemRow = null;

  // --- 1. Пытаемся найти товар в masterdata.items
  const itemRes = await dbClient.query(
    `
    SELECT id, is_active
    FROM masterdata.items
    WHERE client_id = $1
      AND barcode   = $2
    LIMIT 1
    `,
    [clientIdNum, barcodeStr]
  );

  if (itemRes.rowCount > 0) {
    itemRow = itemRes.rows[0];

    if (!itemRow.is_active) {
      const err = new Error('ITEM_INACTIVE');
      err.code = 'ITEM_INACTIVE';
      throw err;
    }

    console.log('[resolveSkuIdStrict] item found in masterdata.items id =', itemRow.id);
  } else {
    // --- 2. В masterdata.items не нашли → пробуем подтянуть из WB
    console.log(
      '[resolveSkuIdStrict] item NOT found in masterdata.items, try WB mapping...'
    );

    try {
      const wbItem = await findWbItemByClientAndBarcode(dbClient, {
        client_id: clientIdNum,   // это client_id из WMS (masterdata.clients)
        barcode: barcodeStr,
      });

      console.log(
        '[resolveSkuIdStrict] WB item found:',
        'mp_account_id =', wbItem.mp_account_id,
        'nm_id =', wbItem.nm_id,
        'chrt_id =', wbItem.chrt_id
      );

      // -------- НОРМАЛЬНО ПОДГОТАВЛИВАЕМ ПОЛЯ ДЛЯ masterdata.items --------
      // Наименование: сначала title, потом item_name, потом vendor_code/article, потом fallback "WB товар ..."
      const nameFromWb =
        wbItem.title ||
        wbItem.item_name ||
        wbItem.vendor_code ||
        wbItem.article ||
        `WB товар ${wbItem.nm_id}`;

      // Артикул клиента: сначала vendor_code, потом article
      const vendorCodeFromWb =
        wbItem.vendor_code ||
        wbItem.article ||
        null;

      // 2.1. Заводим (или обновляем) товар в masterdata.items
      const insRes = await dbClient.query(
        `
        INSERT INTO masterdata.items
          (client_id, barcode, item_name, vendor_code, wb_vendor_code, brand, is_active, created_at)
        VALUES
          ($1,       $2,      $3,        $4,          $5,           $6,    true,      NOW())
        ON CONFLICT (client_id, barcode)
        DO UPDATE SET
          -- НЕ затираем уже заполненные поля
          item_name      = COALESCE(items.item_name,      EXCLUDED.item_name),
          vendor_code    = COALESCE(items.vendor_code,    EXCLUDED.vendor_code),
          wb_vendor_code = COALESCE(items.wb_vendor_code, EXCLUDED.wb_vendor_code),
          brand          = COALESCE(items.brand,          EXCLUDED.brand),
          is_active      = true
        RETURNING id, is_active
        `,
        [
          clientIdNum,
          barcodeStr,
          nameFromWb,        // нормальное имя из WB (title/item_name/vendor_code/... или "WB товар nm")
          vendorCodeFromWb,  // артикул продавца из WB (vendor_code/article)
          String(wbItem.nm_id || ''),
          wbItem.brand || null,
        ]
      );

      itemRow = insRes.rows[0];

      if (!itemRow.is_active) {
        const err = new Error('ITEM_INACTIVE');
        err.code = 'ITEM_INACTIVE';
        throw err;
      }

      console.log(
        '[resolveSkuIdStrict] item created/updated from WB in masterdata.items id =',
        itemRow.id
      );
    } catch (err) {
      // Если WB ничего не нашёл — отдаём стандартный ITEM_NOT_FOUND
      if (err.code === 'ITEM_NOT_FOUND') {
        console.log(
          '[resolveSkuIdStrict] WB mapping also not found for client_id =',
          clientIdNum,
          'barcode =',
          barcodeStr
        );
        const e2 = new Error('ITEM_NOT_FOUND');
        e2.code = 'ITEM_NOT_FOUND';
        throw e2;
      }
      // Любая другая ошибка WB — пробрасываем наверх
      throw err;
    }
  }

  // --- 3. Гарантируем наличие SKU (wms.sku) по client_id + barcode
  const skuId = await resolveSkuIdOrCreate(dbClient, {
    client_id: clientIdNum,
    barcode: barcodeStr,
  });

  console.log(
    '[resolveSkuIdStrict] resolved sku_id =',
    skuId,
    'for client_id =',
    clientIdNum,
    'barcode =',
    barcodeStr
  );

  return { skuId, item: itemRow };
}


// -------------------------
// Общий расчёт строк отгрузки/упаковки по поставке WB
// shipmentExternalId = wb_supply_id (WB-GI-...)
// -------------------------
async function loadShipmentLinesForPackingOrShipping(pool, shipmentExternalId) {
  const { rows } = await pool.query(
    `
      WITH agg AS (
        SELECT
          pt.barcode,
          SUM(pt.qty)               AS qty,
          COALESCE(SUM(pt.packed_qty), 0) AS packed_qty
        FROM wms.picking_tasks pt
        WHERE pt.shipment_code = $1
          AND pt.status        = 'done'
        GROUP BY pt.barcode
      )
      SELECT
        a.barcode,
        a.qty,
        a.packed_qty,
        COALESCE(wbi.item_name, wbi.title, a.barcode)       AS item_name,
        COALESCE(wbi.preview_url, wbi.preview_image_url)    AS preview_url,
        MAX(o.wb_sticker)      AS wb_sticker,
        MAX(o.wb_sticker_code) AS wb_sticker_code
      FROM agg a
      -- Берём ОДНУ строку из справочника баркодов (если их несколько)
      LEFT JOIN LATERAL (
        SELECT ib.nm_id, ib.client_mp_account_id
        FROM public.mp_wb_items_barcodes ib
        WHERE ib.barcode = a.barcode
        ORDER BY ib.client_mp_account_id
        LIMIT 1
      ) ib ON true
      LEFT JOIN public.mp_wb_items wbi
        ON wbi.nm_id                = ib.nm_id
       AND wbi.client_mp_account_id = ib.client_mp_account_id
      -- Для стикеров используем реальные строки picking_tasks + mp_wb_orders
      LEFT JOIN wms.picking_tasks pt2
        ON pt2.shipment_code = $1
       AND pt2.barcode       = a.barcode
      LEFT JOIN public.mp_wb_orders o
        ON o.wb_order_id = pt2.wb_order_id
      GROUP BY
        a.barcode,
        a.qty,
        a.packed_qty,
        COALESCE(wbi.item_name, wbi.title, a.barcode),
        COALESCE(wbi.preview_url, wbi.preview_image_url)
      ORDER BY
        COALESCE(wbi.item_name, wbi.title, a.barcode)
    `,
    [shipmentExternalId]
  );

  return rows;
}


// Универсальный хелпер: гарантирует наличие SKU в wms.sku по (client_id, barcode).
// Используется и в /stock/adjust, и в resolveSkuIdStrict (приёмка, списания, движения).
async function resolveSkuIdOrCreate(dbClient, { client_id, barcode }) {
  const clientIdNum = Number(client_id);
  const barcodeStr  = String(barcode || '').trim();

  if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
    throw new Error('resolveSkuIdOrCreate: invalid client_id');
  }
  if (!barcodeStr) {
    throw new Error('resolveSkuIdOrCreate: empty barcode');
  }

  // --- 1. Пытаемся найти существующий SKU ---
  const existingSkuRes = await dbClient.query(
    `
    SELECT id
    FROM wms.sku
    WHERE client_id = $1
      AND barcode   = $2
    LIMIT 1
    `,
    [clientIdNum, barcodeStr]
  );

  if (existingSkuRes.rowCount > 0) {
    return existingSkuRes.rows[0].id;
  }

  // --- 2. Если нет — создаём новый SKU ---
  const insertSkuRes = await dbClient.query(
    `
    INSERT INTO wms.sku (client_id, barcode, is_active)
    VALUES ($1, $2, TRUE)
    ON CONFLICT (client_id, barcode)
    DO UPDATE SET
      is_active = TRUE
    RETURNING id;
    `,
    [clientIdNum, barcodeStr]
  );

  return insertSkuRes.rows[0].id;
}


// ---------------- SKU REGISTRY API (WMS.SKU) ----------------

// GET /sku — поиск SKU + базовые данные товара по штрихкоду
// Фильтры (необязательные): client_id, barcode, sku_id, is_active
app.get('/sku', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const { client_id, barcode, sku_id, is_active } = req.query || {};

    const conditions = [];
    const values = [];
    let idx = 1;

    if (sku_id) {
      conditions.push(`s.id = $${idx++}`);
      values.push(Number(sku_id));
    }
    if (client_id) {
      conditions.push(`s.client_id = $${idx++}`);
      values.push(Number(client_id));
    }
    if (barcode) {
      conditions.push(`s.barcode = $${idx++}`);
      values.push(String(barcode));
    }
    if (is_active !== undefined) {
      conditions.push(`s.is_active = $${idx++}`);
      values.push(is_active === 'true');
    }

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const q = `
      SELECT
        s.id         AS sku_id,
        s.client_id,
        s.barcode,
        s.is_active,
        s.created_at,

        -- данные товара (если есть в masterdata.items)
        i.id         AS item_id,
        i.item_name,
        i.vendor_code,
        i.wb_vendor_code,
        i.unit,
        i.volume_liters,
        i.needs_packaging AS need_packaging
      FROM wms.sku s
      LEFT JOIN masterdata.items i
        ON i.client_id = s.client_id
       AND i.barcode   = s.barcode
      ${where}
      ORDER BY s.client_id, s.barcode, s.id
      LIMIT 500
    `;

    const r = await pool.query(q, values);
    res.json({ status: 'ok', sku: r.rows });
  } catch (err) {
    console.error('Get SKU error:', err);
    res.status(500).json({ error: 'Get SKU error', detail: err.message });
  }
});

// POST /sku — ручное создание/фиксация SKU
app.post('/sku', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const { client_id, barcode, is_active } = req.body || {};

    if (!client_id || !barcode) {
      return res.status(400).json({
        error: 'Обязательные поля: client_id, barcode',
      });
    }

    const clientIdNum = Number(client_id);
    if (!clientIdNum) {
      return res.status(400).json({ error: 'client_id обязателен и должен быть числом' });
    }

    const barcodeStr = String(barcode || '').trim();
    if (!barcodeStr) {
      return res.status(400).json({ error: 'barcode обязателен' });
    }

    // Проверяем, что такой клиент существует
    const clientCheck = await pool.query(
      `SELECT id FROM masterdata.clients WHERE id = $1 LIMIT 1`,
      [clientIdNum]
    );
    if (clientCheck.rowCount === 0) {
      return res.status(400).json({ error: 'client_id не существует' });
    }

    const isActiveVal = is_active === undefined ? true : !!is_active;

    // Если такая пара client_id + barcode уже есть — вернём конфликт
    const exist = await pool.query(
      `SELECT id FROM wms.sku WHERE client_id = $1 AND barcode = $2 LIMIT 1`,
      [clientIdNum, barcodeStr]
    );
    if (exist.rowCount > 0) {
      return res.status(409).json({ error: 'Для этого client_id и barcode SKU уже существует' });
    }

    const ins = await pool.query(
      `
      INSERT INTO wms.sku (client_id, barcode, is_active, created_at)
      VALUES ($1, $2, $3, NOW())
      RETURNING id AS sku_id, client_id, barcode, is_active, created_at
      `,
      [clientIdNum, barcodeStr, isActiveVal]
    );

    res.status(201).json({ status: 'ok', sku: ins.rows[0] });
  } catch (err) {
    console.error('Create SKU error:', err);
    res.status(500).json({ error: 'Create SKU error', detail: err.message });
  }
});


// ---------------- STOCK VIEW (BY LOCATION / BY BARCODE) ----------------
// Просмотр содержимого МХ по коду ячейки
// GET /stock/by-location?location_code=A-01-05
app.get(
  '/stock/by-location',
  authRequired,
  requireRole('owner', 'admin', 'supervisor'),
  async (req, res) => {
    try {
      const { location_code } = req.query;

      if (!location_code) {
        return res.status(400).json({ error: 'Параметр location_code обязателен' });
      }

      const sql = `
        SELECT
          s.sku_id,
          s.qty,
          l.code AS location_code,

          -- ШК: сначала из items, потом из sku, в крайнем случае из stock
          COALESCE(i.barcode, sk.barcode, s.barcode) AS barcode,

          -- Наименование товара: items.item_name -> sku.name
          COALESCE(i.item_name, sk.name) AS item_name,

          -- Клиент
          COALESCE(sk.client_id, s.client_id) AS client_id,
          c.client_name

        FROM wms.stock s
        JOIN masterdata.locations l
          ON l.id = s.location_id

        -- SKU-справочник
        LEFT JOIN masterdata.sku sk
          ON sk.id = s.sku_id

        -- Товар (если есть отдельная карточка)
        LEFT JOIN masterdata.items i
          ON i.id = s.sku_id   -- сейчас id item == sku_id

        -- Клиент: берём client_id из sku, если есть, иначе из stock
        LEFT JOIN masterdata.clients c
          ON c.id = COALESCE(sk.client_id, s.client_id)

        WHERE l.code = $1
          AND s.qty <> 0

        ORDER BY
          COALESCE(i.item_name, sk.name) NULLS LAST,
          COALESCE(i.barcode, sk.barcode, s.barcode) NULLS LAST,
          s.sku_id;
      `;

      const { rows } = await pool.query(sql, [location_code.trim()]);
      return res.json({ rows });
    } catch (err) {
      console.error('GET /stock/by-location error:', err);
      return res.status(500).json({
        error: 'Internal server error',
        detail: err.message,
        code: err.code,
      });
    }
  }
);



// -------------------------
// Получить МХ по коду (для сканера, в т.ч. упаковка)
// -------------------------
app.get(
  '/locations/by-code',
  authRequired,
  requireRole(['owner', 'admin', 'picker', 'packer']),
  async (req, res) => {
    const { code } = req.query;
    const rawCode = (code || '').trim();

    if (!rawCode) {
      return res.status(400).json({ message: 'Не указан код МХ' });
    }

    try {
      const { rows } = await pool.query(
        `
        SELECT
          id,
          client_id,
          code,
          location_code,
          description,
          is_active,
          zone,
          location_type,
          is_pick_location
        FROM wms.locations
        WHERE code = $1 OR location_code = $1
        LIMIT 1
        `,
        [rawCode]
      );

      console.log('[/locations/by-code] code =', JSON.stringify(rawCode));
      console.log('[/locations/by-code] rowCount =', rows.length);

      if (!rows.length) {
        return res.status(404).json({ message: 'МХ не найдена' });
      }

      const location = rows[0];
      const zoneNorm = (location.zone || '').trim().toLowerCase();

      const isPackingZone = zoneNorm === 'упаковка';

      // Чтобы не было проблем с кешем
      res.set('Cache-Control', 'no-store');

      return res.json({
        location,
        is_packing_zone: isPackingZone,
      });
    } catch (err) {
      console.error('locations/by-code error:', err);
      return res.status(500).json({ message: 'Ошибка сервера' });
    }
  }
);


// ---------------- STOCK BY BARCODE ----------------
// client_id НЕ обязателен. Если не передан — ищем по всем клиентам.

app.get(
  '/stock/by-barcode',
  authRequired,
  requireRole('owner'),
  async (req, res) => {
    try {
      const { client_id, barcode } = req.query || {};

      if (!barcode) {
        return res.status(400).json({
          error: 'barcode обязателен',
        });
      }

      const barcodeStr = String(barcode).trim();
      const clientIdNum = client_id ? Number(client_id) : null;

      const values = [];
      let idx = 1;

      // --- фильтры ---
      // всегда фильтруем по barcode и ненулевому остатку
      let where = `WHERE st.barcode = $${idx++} AND st.qty <> 0`;
      values.push(barcodeStr);

      // если client_id передан — добавляем
      if (clientIdNum) {
        where += ` AND st.client_id = $${idx++}`;
        values.push(clientIdNum);
      }

      const q = `
        SELECT
          -- локация и остаток
          l.code              AS location,
          st.qty              AS qty,

          -- идентификаторы
          st.sku_id,
          st.location_id,
          st.client_id,
          st.barcode,

          -- данные товара (по возможности)
          i.item_name,
          i.vendor_code,
          i.wb_vendor_code,
          i.unit,
          i.volume_liters,
          i.needs_packaging   AS need_packaging,

          -- статусы
          i.is_active         AS item_active,
          sku.is_active       AS sku_active,
          l.is_active         AS location_active

        FROM wms.stock st
        LEFT JOIN masterdata.sku sku
          ON sku.id = st.sku_id
        LEFT JOIN masterdata.items i
          ON i.client_id = st.client_id
         AND i.barcode   = st.barcode
        LEFT JOIN masterdata.locations l
          ON l.id = st.location_id
        ${where}
        ORDER BY st.client_id, l.code
      `;

      const r = await pool.query(q, values);

      console.log(
        '[stock/by-barcode] rows:',
        r.rowCount,
        'client_id=',
        clientIdNum,
        'barcode=',
        barcodeStr
      );

      return res.json({
        status: 'ok',
        stock: r.rows,   // фронт уже ждёт поле stock
      });
    } catch (err) {
      console.error('Stock by barcode error:', err);
      return res.status(500).json({
        error: 'Stock by barcode error',
        detail: err.message,
      });
    }
  }
);


/**
 * 🔹 Список WMS клиентов (для селектора в mp-accounts.html)
 * GET /masterdata/clients
 */
app.get('/masterdata/clients', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const r = await pool.query(
      `
      SELECT id, client_name
      FROM masterdata.clients
      ORDER BY client_name ASC, id ASC
      `
    );
    return res.json({ status: 'ok', clients: r.rows });
  } catch (err) {
    console.error('GET /masterdata/clients error:', err);
    return res.status(500).json({ error: 'internal_error' });
  }
});


// ==================== WB → masterdata.items SYNC ====================
//
// ВАЖНО:
// - На вход подаём clientMpAccountId = mp_client_accounts.id
//   (именно его ты передаёшь как clientMpAccountId в импорте)
// - masterdata.items.client_id = mp_client_accounts.client_id (WMS-клиент)
// - ШК берём из mp_wb_items_barcodes, а название/бренд — из mp_wb_items
//
async function syncItemsForClient(clientMpAccountId) {
  const client_mp_account_id = Number(clientMpAccountId);
  if (!client_mp_account_id || Number.isNaN(client_mp_account_id)) {
    throw new Error('Некорректный client_mp_account_id для синхронизации');
  }

  const insertSql = `
    INSERT INTO masterdata.items (
      client_id,
      barcode,
      item_name,
      brand,
      unit,
      is_active
    )
    SELECT
      mca.client_id               AS client_id,     -- WMS-клиент (например, 7)
      bc.barcode                  AS barcode,
      wi.title                    AS item_name,     -- название из mp_wb_items.title
      wi.brand                    AS brand,
      'шт'                        AS unit,
      TRUE                        AS is_active
    FROM mp_client_accounts mca
    JOIN mp_wb_items wi
      ON wi.client_mp_account_id = mca.id
    JOIN mp_wb_items_barcodes bc
      ON bc.client_mp_account_id = wi.client_mp_account_id
     AND bc.nm_id                = wi.nm_id
    WHERE mca.id = $1
      AND bc.barcode IS NOT NULL
  ON CONFLICT (client_id, barcode) DO UPDATE
    SET item_name = EXCLUDED.item_name,
        brand     = EXCLUDED.brand,
        is_active = TRUE
  `;

  const insertRes = await pool.query(insertSql, [client_mp_account_id]);

  return {
    items_upserted: insertRes.rowCount
  };
}



// ---------------- STOCK OVERVIEW (WMS.STOCK + справочник) ----------------
async function stockOverviewHandler(req, res) {
  try {
    const {
      client_id,
      barcode,
      location_code,
      hide_zero,
      only_active_items,
      only_active_mx,
    } = req.query || {};

    const conditions = [];
    const values = [];
    let idx = 1;

    // По умолчанию скрываем нули
    if (hide_zero === 'true' || hide_zero === undefined) {
      conditions.push('st.qty <> 0');
    }

    if (client_id) {
      conditions.push(`st.client_id = $${idx++}`);
      values.push(Number(client_id));
    }

    if (barcode) {
      // 🔧 Фильтруем по штрихкоду из sku, а не из stock
      conditions.push(`sku.barcode = $${idx++}`);
      values.push(String(barcode).trim());
    }

    if (location_code) {
      conditions.push(`l.code = $${idx++}`);
      values.push(String(location_code).trim());
    }

    if (only_active_items === 'true') {
      conditions.push('i.is_active = true');
    }

    if (only_active_mx === 'true') {
      conditions.push('l.is_active = true');
    }

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    const q = `
      SELECT
        st.client_id,
        c.client_name,

        -- 🔧 Штрихкод теперь берём из sku/items
        COALESCE(i.barcode, sku.barcode) AS barcode,

        st.sku_id,
        st.location_id,
        l.code                AS location_code,
        st.qty,

        i.item_name,
        i.unit,
        i.volume_liters,
        i.needs_packaging     AS need_packaging,

        i.is_active           AS item_active,
        l.is_active           AS location_active
      FROM wms.stock st
      LEFT JOIN masterdata.locations l
        ON l.id = st.location_id
      LEFT JOIN masterdata.clients c
        ON c.id = st.client_id

      -- 🔧 Подтягиваем sku по sku_id
      LEFT JOIN wms.sku sku
        ON sku.id = st.sku_id

      -- 🔧 items джойним по client_id + barcode из sku
      LEFT JOIN masterdata.items i
        ON i.client_id = st.client_id
       AND i.barcode   = sku.barcode

      ${where}
      ORDER BY st.client_id, l.code, sku.barcode, st.location_id
    `;

    const r = await pool.query(q, values);

    res.json({
      status: 'ok',
      stock: r.rows,
    });
  } catch (err) {
    console.error('Stock overview error:', err);
    res.status(500).json({
      error: 'Stock overview error',
      detail: err.message,
    });
  }
}

// Алиасы: оба пути работают, права — owner/admin
app.get(
  '/stock',
  authRequired,
  requireRole(['owner', 'admin']),
  stockOverviewHandler
);
app.get(
  '/stock/overview',
  authRequired,
  requireRole(['owner', 'admin']),
  stockOverviewHandler
);


// =========================
// ADMIN USERS API
// =========================

// GET /admin/users - список пользователей с модулями
app.get(
  '/admin/users',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const client = await pool.connect();
    try {
      const usersRes = await client.query(`
        SELECT
          id,
          username,
          full_name,
          role,
          active,
          created_at,
          updated_at
        FROM admin_users
        ORDER BY id ASC
      `);

      const modulesRes = await client.query(`
        SELECT user_id, module_code
        FROM admin_user_modules
        ORDER BY user_id, module_code
      `);

      const modulesMap = {};
      for (const row of modulesRes.rows) {
        if (!modulesMap[row.user_id]) modulesMap[row.user_id] = [];
        modulesMap[row.user_id].push(row.module_code);
      }

      const users = usersRes.rows.map(u => ({
        ...u,
        modules: modulesMap[u.id] || []
      }));

      return res.json({ users });
    } catch (err) {
      console.error('GET /admin/users error:', err);
      return res.status(500).json({ error: 'Ошибка получения пользователей' });
    } finally {
      client.release();
    }
  }
);

// GET /admin/modules - список всех модулей
app.get(
  '/admin/modules',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const client = await pool.connect();
    try {
      const result = await client.query(`
        SELECT code, name, section
        FROM admin_modules
        ORDER BY section, code
      `);

      return res.json({ modules: result.rows });
    } catch (err) {
      console.error('GET /admin/modules error:', err);
      return res.status(500).json({ error: 'Ошибка получения модулей' });
    } finally {
      client.release();
    }
  }
);

// POST /admin/users - создать пользователя
app.post(
  '/admin/users',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const client = await pool.connect();

    try {
      const {
        username,
        full_name,
        password,
        role,
        active,
        modules
      } = req.body || {};

      if (!username || !String(username).trim()) {
        return res.status(400).json({ error: 'Не заполнен username' });
      }

      if (!full_name || !String(full_name).trim()) {
        return res.status(400).json({ error: 'Не заполнен full_name' });
      }

      if (!password || String(password).length < 8) {
        return res.status(400).json({ error: 'Пароль должен быть не короче 8 символов' });
      }

      if (!role || !String(role).trim()) {
        return res.status(400).json({ error: 'Не заполнена роль' });
      }

      const safeModules = Array.isArray(modules) ? modules : [];

      await client.query('BEGIN');

      const existsRes = await client.query(
        `SELECT id FROM admin_users WHERE LOWER(username) = LOWER($1) LIMIT 1`,
        [String(username).trim()]
      );

      if (existsRes.rows.length > 0) {
        await client.query('ROLLBACK');
        return res.status(409).json({ error: 'Пользователь с таким логином уже существует' });
      }

      const password_hash = await bcrypt.hash(String(password), 10);

      const insertUserRes = await client.query(
        `
        INSERT INTO admin_users (
          username,
          full_name,
          password_hash,
          role,
          active
        )
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, username, full_name, role, active, created_at, updated_at
        `,
        [
          String(username).trim(),
          String(full_name).trim(),
          password_hash,
          String(role).trim(),
          typeof active === 'boolean' ? active : true
        ]
      );

      const user = insertUserRes.rows[0];

      if (safeModules.length > 0) {
        for (const moduleCode of safeModules) {
          await client.query(
            `
            INSERT INTO admin_user_modules (user_id, module_code)
            VALUES ($1, $2)
            ON CONFLICT (user_id, module_code) DO NOTHING
            `,
            [user.id, moduleCode]
          );
        }
      }

      await client.query('COMMIT');

      return res.status(201).json({
        message: 'Пользователь создан',
        user: {
          ...user,
          modules: safeModules
        }
      });
    } catch (err) {
      await client.query('ROLLBACK');
      console.error('POST /admin/users error:', err);
      return res.status(500).json({ error: 'Ошибка создания пользователя' });
    } finally {
      client.release();
    }
  }
);

// PATCH /admin/users/:id - редактировать пользователя
app.patch(
  '/admin/users/:id',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const client = await pool.connect();

    try {
      const userId = Number(req.params.id);
      if (!Number.isInteger(userId) || userId <= 0) {
        return res.status(400).json({ error: 'Некорректный ID пользователя' });
      }

      const {
        username,
        full_name,
        password,
        role,
        active,
        modules
      } = req.body || {};

      await client.query('BEGIN');

      const currentRes = await client.query(
        `SELECT * FROM admin_users WHERE id = $1 LIMIT 1`,
        [userId]
      );

      if (currentRes.rows.length === 0) {
        await client.query('ROLLBACK');
        return res.status(404).json({ error: 'Пользователь не найден' });
      }

      if (username && String(username).trim()) {
        const dupRes = await client.query(
          `
          SELECT id
          FROM admin_users
          WHERE LOWER(username) = LOWER($1)
            AND id <> $2
          LIMIT 1
          `,
          [String(username).trim(), userId]
        );

        if (dupRes.rows.length > 0) {
          await client.query('ROLLBACK');
          return res.status(409).json({ error: 'Пользователь с таким логином уже существует' });
        }
      }

      const fields = [];
      const values = [];
      let idx = 1;

      if (username !== undefined) {
        fields.push(`username = $${idx++}`);
        values.push(String(username).trim());
      }

      if (full_name !== undefined) {
        fields.push(`full_name = $${idx++}`);
        values.push(String(full_name).trim());
      }

      if (role !== undefined) {
        fields.push(`role = $${idx++}`);
        values.push(String(role).trim());
      }

      if (typeof active === 'boolean') {
        fields.push(`active = $${idx++}`);
        values.push(active);
      }

      if (password !== undefined && String(password).trim() !== '') {
        if (String(password).length < 8) {
          await client.query('ROLLBACK');
          return res.status(400).json({ error: 'Пароль должен быть не короче 8 символов' });
        }

        const password_hash = await bcrypt.hash(String(password), 10);
        fields.push(`password_hash = $${idx++}`);
        values.push(password_hash);
      }

      fields.push(`updated_at = NOW()`);

      if (fields.length > 0) {
        values.push(userId);

        const updateSql = `
          UPDATE admin_users
          SET ${fields.join(', ')}
          WHERE id = $${idx}
          RETURNING id, username, full_name, role, active, created_at, updated_at
        `;

        await client.query(updateSql, values);
      }

      if (Array.isArray(modules)) {
        await client.query(
          `DELETE FROM admin_user_modules WHERE user_id = $1`,
          [userId]
        );

        for (const moduleCode of modules) {
          await client.query(
            `
            INSERT INTO admin_user_modules (user_id, module_code)
            VALUES ($1, $2)
            ON CONFLICT (user_id, module_code) DO NOTHING
            `,
            [userId, moduleCode]
          );
        }
      }

      const finalUserRes = await client.query(
        `
        SELECT id, username, full_name, role, active, created_at, updated_at
        FROM admin_users
        WHERE id = $1
        `,
        [userId]
      );

      const finalModulesRes = await client.query(
        `
        SELECT module_code
        FROM admin_user_modules
        WHERE user_id = $1
        ORDER BY module_code
        `,
        [userId]
      );

      await client.query('COMMIT');

      return res.json({
        message: 'Пользователь обновлён',
        user: {
          ...finalUserRes.rows[0],
          modules: finalModulesRes.rows.map(r => r.module_code)
        }
      });
    } catch (err) {
      await client.query('ROLLBACK');
      console.error('PATCH /admin/users/:id error:', err);
      return res.status(500).json({ error: 'Ошибка обновления пользователя' });
    } finally {
      client.release();
    }
  }
);

// PATCH /admin/users/:id/status - включить / отключить пользователя
app.patch(
  '/admin/users/:id/status',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const client = await pool.connect();

    try {
      const userId = Number(req.params.id);
      if (!Number.isInteger(userId) || userId <= 0) {
        return res.status(400).json({ error: 'Некорректный ID пользователя' });
      }

      const { active } = req.body || {};

      if (typeof active !== 'boolean') {
        return res.status(400).json({ error: 'Поле active должно быть boolean' });
      }

      const result = await client.query(
        `
        UPDATE admin_users
        SET active = $1,
            updated_at = NOW()
        WHERE id = $2
        RETURNING id, username, full_name, role, active, created_at, updated_at
        `,
        [active, userId]
      );

      if (result.rows.length === 0) {
        return res.status(404).json({ error: 'Пользователь не найден' });
      }

      return res.json({
        message: 'Статус пользователя обновлён',
        user: result.rows[0]
      });
    } catch (err) {
      console.error('PATCH /admin/users/:id/status error:', err);
      return res.status(500).json({ error: 'Ошибка обновления статуса пользователя' });
    } finally {
      client.release();
    }
  }
);

// GET /admin/users/:id/modules - права пользователя
app.get(
  '/admin/users/:id/modules',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const client = await pool.connect();

    try {
      const userId = Number(req.params.id);
      if (!Number.isInteger(userId) || userId <= 0) {
        return res.status(400).json({ error: 'Некорректный ID пользователя' });
      }

      const result = await client.query(
        `
        SELECT module_code
        FROM admin_user_modules
        WHERE user_id = $1
        ORDER BY module_code
        `,
        [userId]
      );

      return res.json({
        user_id: userId,
        modules: result.rows.map(r => r.module_code)
      });
    } catch (err) {
      console.error('GET /admin/users/:id/modules error:', err);
      return res.status(500).json({ error: 'Ошибка получения прав пользователя' });
    } finally {
      client.release();
    }
  }
);

// PUT /admin/users/:id/modules - сохранить права пользователя
app.put(
  '/admin/users/:id/modules',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const client = await pool.connect();

    try {
      const userId = Number(req.params.id);
      if (!Number.isInteger(userId) || userId <= 0) {
        return res.status(400).json({ error: 'Некорректный ID пользователя' });
      }

      const { modules } = req.body || {};

      if (!Array.isArray(modules)) {
        return res.status(400).json({ error: 'modules должен быть массивом' });
      }

      await client.query('BEGIN');

      await client.query(
        `DELETE FROM admin_user_modules WHERE user_id = $1`,
        [userId]
      );

      for (const moduleCode of modules) {
        await client.query(
          `
          INSERT INTO admin_user_modules (user_id, module_code)
          VALUES ($1, $2)
          ON CONFLICT (user_id, module_code) DO NOTHING
          `,
          [userId, moduleCode]
        );
      }

      await client.query('COMMIT');

      return res.json({
        message: 'Права пользователя обновлены',
        user_id: userId,
        modules
      });
    } catch (err) {
      await client.query('ROLLBACK');
      console.error('PUT /admin/users/:id/modules error:', err);
      return res.status(500).json({ error: 'Ошибка сохранения прав пользователя' });
    } finally {
      client.release();
    }
  }
);


// ---------------- СКЛАД: общий обзор ----------------
// GET /stock?client_id=&barcode=&location_code=&hide_zero=&only_active_items=&only_active_sku=&only_active_mx=
app.get('/stock', authRequired, requireRole('owner', 'admin'), async (req, res) => {
  const {
    client_id,
    barcode,
    location_code,
    hide_zero,
    only_active_items,
    only_active_sku,
    only_active_mx,
  } = req.query || {};

  const where = [];
  const params = [];
  let p = 1;

  // client_id — опциональный
  if (client_id) {
    where.push(`s.client_id = $${p}`);
    params.push(Number(client_id));
    p++;
  }

  // фильтр по штрихкоду (фильтруем по ШК из SKU)
  if (barcode && String(barcode).trim()) {
    where.push(`sku.barcode = $${p}`);
    params.push(String(barcode).trim());
    p++;
  }

  // фильтр по МХ
  if (location_code && String(location_code).trim()) {
    where.push(`loc.code = $${p}`);
    params.push(String(location_code).trim());
    p++;
  }

  // флаги "только активные ..."
  if (only_active_items === 'true') {
    where.push(`COALESCE(i.is_active, TRUE) = TRUE`);
  }
  if (only_active_sku === 'true') {
    where.push(`sku.is_active = TRUE`);
  }
  if (only_active_mx === 'true') {
    where.push(`loc.is_active = TRUE`);
  }

  const whereSql  = where.length ? ('WHERE ' + where.join(' AND ')) : '';
  const havingSql = (hide_zero === 'true') ? 'HAVING SUM(s.qty) <> 0' : '';

  const sql = `
    SELECT
      s.client_id,
      c.client_name,

      -- ДВА поля для отладки и фронта
      sku.barcode AS sku_barcode,      -- ШК из wms.sku (главный)
      s.barcode   AS stock_barcode,    -- ШК из wms.stock (на всякий случай)

      s.sku_id,
      s.location_id,
      loc.code     AS location_code,
      SUM(s.qty)   AS qty,

      -- данные товара из masterdata.items
      i.item_name,
      i.unit,
      i.volume_liters,
      i.needs_packaging AS need_packaging,

      -- флаги активности
      i.is_active   AS item_active,
      sku.is_active AS sku_active,
      loc.is_active AS location_active
    FROM wms.stock s
    JOIN wms.sku sku
      ON sku.id = s.sku_id
    JOIN masterdata.clients c
      ON c.id = s.client_id
    JOIN masterdata.locations loc
      ON loc.id = s.location_id
    LEFT JOIN masterdata.items i
      ON i.client_id = s.client_id
     AND i.barcode  = sku.barcode
    ${whereSql}
    GROUP BY
      s.client_id,
      c.client_name,
      sku.barcode,
      s.barcode,
      s.sku_id,
      s.location_id,
      loc.code,
      i.item_name,
      i.unit,
      i.volume_liters,
      i.needs_packaging,
      i.is_active,
      sku.is_active,
      loc.is_active
    ${havingSql}
    ORDER BY sku.barcode, loc.code;
  `;

  try {
    console.log('[/stock] SQL params =', params);
    const { rows } = await pool.query(sql, params);

    console.log('[/stock] rows sample =', rows[0] || null);

    return res.json({
      status: 'ok',
      stock: rows,
    });
  } catch (err) {
    console.error('/stock error:', err);
    return res.status(500).json({
      error: 'stock_query_error',
      detail: err.message,
    });
  }
});



// ---------------- WB: ПРОСМОТР ЗАГРУЖЕННЫХ ТОВАРОВ (mp_wb_items) ----------------
app.get(
  '/mp/wb/items',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const { client_mp_account_id, limit, offset } = req.query || {};

      const clientId = Number(client_mp_account_id || 0);
      if (!clientId) {
        return res.status(400).json({
          error: 'client_mp_account_id_required',
        });
      }

      const pageLimit = Math.min(Math.max(Number(limit) || 50, 1), 100);
      const pageOffset = Math.max(Number(offset) || 0, 0);

      const sql = `
        SELECT
          id,
          client_mp_account_id,
          nm_id,
          imt_id,
          vendor_code,
          brand,
          title,
          pics,
          created_at,
          updated_at,
          COUNT(*) OVER() AS total
        FROM mp_wb_items
        WHERE client_mp_account_id = $1
        ORDER BY nm_id
        LIMIT $2 OFFSET $3
      `;

      const r = await pool.query(sql, [clientId, pageLimit, pageOffset]);
      const rows = r.rows || [];
      const total = rows.length > 0 ? Number(rows[0].total) : 0;

      return res.json({
        status: 'ok',
        total,
        limit: pageLimit,
        offset: pageOffset,
        items: rows.map((row) => ({
          id: row.id,
          client_mp_account_id: row.client_mp_account_id,
          nm_id: row.nm_id,
          imt_id: row.imt_id,
          vendor_code: row.vendor_code,
          brand: row.brand,
          title: row.title,
          pics: row.pics,
          created_at: row.created_at,
          updated_at: row.updated_at,
        })),
      });
    } catch (err) {
      console.error('WB ITEMS LIST ERROR', err);
      return res.status(500).json({
        error: 'internal_error',
        detail: err.message || String(err),
      });
    }
  }
);
 	


// Синхронизация справочника WMS с WB-справочником для конкретного клиента
app.post(
  '/clients/:clientId/sync-items-from-wb',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const clientId = Number(req.params.clientId);
      if (!clientId || Number.isNaN(clientId)) {
        return res.status(400).json({ status: 'error', message: 'Некорректный clientId' });
      }

      const result = await syncItemsForClient(clientId);

      res.json({
        status: 'ok',
        message: 'Синхронизация справочника выполнена',
        details: result
      });
    } catch (err) {
      console.error('sync-items-from-wb error:', err);
      res.status(500).json({
        status: 'error',
        message: 'Ошибка при синхронизации справочника',
        detail: err.message
      });
    }
  }
);




// ---------------- WB: ТЕСТОВАЯ ЗАГРУЗКА ЗАКАЗОВ (PROD) ----------------
app.post(
  '/mp/wb/test-fetch-orders',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      console.log('*** /mp/wb/test-fetch-orders PROD ***', new Date().toISOString());

      const { client_mp_account_id, date_from, date_to, limit } = req.body || {};

      const clientMpAccountId = Number(client_mp_account_id);
      if (!clientMpAccountId) {
        return res.status(400).json({ error: 'client_mp_account_id is required' });
      }

      // 0) WB токен берём из mp_accounts (мульти-аккаунты)
      const acc = await pool.query(
        `
        SELECT api_token
        FROM public.mp_accounts
        WHERE id = $1
          AND marketplace = 'wb'
          AND is_active = true
        LIMIT 1
        `,
        [clientMpAccountId]
      );

      if (!acc.rowCount || !acc.rows[0].api_token) {
        return res.status(400).json({
          error: `WB mp_account_id=${clientMpAccountId} not found/inactive or api_token missing`,
        });
      }

      const wbToken = acc.rows[0].api_token;

      // Забираем сырые данные у WB
      const raw = await fetchOrders(wbToken, {
        dateFrom: date_from,
        dateTo: date_to,
        limit: limit || 100,
      });

      let orders = [];

      if (Array.isArray(raw)) orders = raw;
      else if (raw && Array.isArray(raw.orders)) orders = raw.orders;
      else {
        console.error('[WB FETCH] unexpected payload shape:', raw);
        return res.status(500).json({
          error: 'unexpected_wb_payload',
          rawType: typeof raw,
          hasOrdersField: !!raw && Object.prototype.hasOwnProperty.call(raw, 'orders'),
        });
      }

      console.log(`[WB FETCH] normalized orders length = ${orders.length}`);

      // helpers
      function normalizeStickerBase64(val) {
        if (!val || typeof val !== 'string') return null;
        const s = val.trim();
        if (!s) return null;

        // Если WB вдруг вернул "сырой" svg текст — конвертим в base64
        if (s.startsWith('<svg') || s.includes('<svg')) {
          try {
            return Buffer.from(s, 'utf8').toString('base64');
          } catch {
            return null;
          }
        }

        // Иначе считаем, что уже base64
        return s;
      }

      function extractSupplyCode(o) {
        // Варианты ключей — на всякий случай (WB иногда меняет имена)
        const v =
          o.wb_supply_id ??
          o.supplyId ??
          o.supply_id ??
          o.supply ??
          o.supplyCode ??
          o.supply_code ??
          (o.supply && o.supply.id) ??
          null;

        if (!v) return null;
        const s = String(v).trim();
        if (!s) return null;
        // Обычно WB-GI-....
        return s;
      }

      function extractSticker(o) {
        // sticker_code
        const stickerCode =
          o.wb_sticker_code ??
          o.stickerCode ??
          (o.sticker && (o.sticker.code || o.sticker.stickerCode)) ??
          null;

        // sticker svg/base64
        const stickerSvg =
          o.wb_sticker ??
          o.stickerSvgBase64 ??
          o.sticker_svg_base64 ??
          o.stickerSvg ??
          o.stickerSVG ??
          o.stickerFile ??
          (o.sticker && (o.sticker.file || o.sticker.svg || o.sticker.base64)) ??
          null;

        return {
          sticker_code: stickerCode ? String(stickerCode).trim() : null,
          sticker_svg_base64: normalizeStickerBase64(stickerSvg),
        };
      }

      // SQL: UPSERT supplies
      const upsertSupplySql = `
        INSERT INTO public.mp_wb_supplies
          (supply_code, client_mp_account_id, sticker_code, sticker_svg_base64, updated_at)
        VALUES
          ($1, $2, $3, $4, now())
        ON CONFLICT (supply_code) DO UPDATE
        SET
          client_mp_account_id = EXCLUDED.client_mp_account_id,
          sticker_code         = COALESCE(EXCLUDED.sticker_code, public.mp_wb_supplies.sticker_code),
          sticker_svg_base64   = COALESCE(EXCLUDED.sticker_svg_base64, public.mp_wb_supplies.sticker_svg_base64),
          updated_at           = now()
      `;

      // SQL: UPSERT orders (добавили wb_supply_id + wb_sticker_code + wb_sticker + fetched_at)
      const insertOrderSql = `
        INSERT INTO public.mp_wb_orders (
          client_mp_account_id,
          wb_order_id,
          nm_id,
          chrt_id,
          article,
          barcode,
          warehouse_id,
          warehouse_name,
          region_name,
          price,
          converted_price,
          currency_code,
          status,
          created_at,
          wb_supply_id,
          wb_sticker_code,
          wb_sticker,
          fetched_at,
          raw
        )
        VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17, now(), $18
        )
        ON CONFLICT (client_mp_account_id, wb_order_id) DO UPDATE SET
          nm_id           = EXCLUDED.nm_id,
          chrt_id         = EXCLUDED.chrt_id,
          article         = EXCLUDED.article,
          barcode         = EXCLUDED.barcode,
          warehouse_id    = EXCLUDED.warehouse_id,
          warehouse_name  = EXCLUDED.warehouse_name,
          region_name     = EXCLUDED.region_name,
          price           = EXCLUDED.price,
          converted_price = EXCLUDED.converted_price,
          currency_code   = EXCLUDED.currency_code,
          status          = EXCLUDED.status,
          created_at      = EXCLUDED.created_at,
          wb_supply_id    = COALESCE(EXCLUDED.wb_supply_id, public.mp_wb_orders.wb_supply_id),
          wb_sticker_code = COALESCE(EXCLUDED.wb_sticker_code, public.mp_wb_orders.wb_sticker_code),
          wb_sticker      = COALESCE(EXCLUDED.wb_sticker, public.mp_wb_orders.wb_sticker),
          fetched_at      = now(),
          raw             = EXCLUDED.raw
      `;

      let stored = 0;
      let suppliesUpserted = 0;

      for (const o of orders) {
        // wb_order_id
        let wbOrderId = null;
        if (typeof o.id === 'number') wbOrderId = o.id;
        else if (typeof o.odid === 'number') wbOrderId = o.odid;
        else if (typeof o.orderId === 'number') wbOrderId = o.orderId;

        if (!wbOrderId) continue;

        const nmId = o.nmId ?? null;
        const chrtId = o.chrtId ?? null;
        const article = o.article ?? null;
        const barcode = Array.isArray(o.skus) ? o.skus[0] : (o.barcode ?? null);
        const warehouseId = o.warehouseId ?? null;
        const warehouseName = o.offices ? o.offices.join(', ') : (o.warehouseName ?? null);
        const regionName = o.regionName ?? null;
        const price = o.price ?? null;
        const convertedPrice = o.convertedPrice ?? null;
        const currencyCode = o.currencyCode ?? null;
        const status = o.deliveryType ?? o.status ?? null;
        const createdAt = o.createdAt ?? null;

        // supply + sticker
        const supplyCode = extractSupplyCode(o);
        const { sticker_code, sticker_svg_base64 } = extractSticker(o);

        // 1) mp_wb_supplies (если есть supplyCode)
        if (supplyCode) {
          await pool.query(upsertSupplySql, [
            supplyCode,
            clientMpAccountId,
            sticker_code,
            sticker_svg_base64,
          ]);
          suppliesUpserted += 1;
        }

        // 2) orders
        await pool.query(insertOrderSql, [
          clientMpAccountId,            // $1
          wbOrderId,                    // $2
          nmId,                         // $3
          chrtId,                       // $4
          article,                      // $5
          barcode,                      // $6
          warehouseId,                  // $7
          warehouseName,                // $8
          regionName,                   // $9
          price,                        // $10
          convertedPrice,               // $11
          currencyCode,                 // $12
          status,                       // $13
          createdAt,                    // $14
          supplyCode,                   // $15 wb_supply_id
          sticker_code,                 // $16 wb_sticker_code
          sticker_svg_base64,           // $17 wb_sticker (base64 svg)
          JSON.stringify(o),            // $18 raw
        ]);

        stored += 1;
      }

      return res.json({
        status: 'ok',
        fetched: orders.length,
        stored,
        supplies_upserted: suppliesUpserted,
        client_mp_account_id: clientMpAccountId,
      });
    } catch (err) {
      console.error('Ошибка тестового запроса WB (PROD):', err);
      return res.status(500).json({
        error: 'Ошибка тестового запроса WB',
        details: err.message,
      });
    }
  }
);

// -------------------------
// GET /mp/wb/supplies/:supplyCode/sticker
// Отдать / при необходимости подтянуть QR-код поставки (SVG base64)
// -------------------------
app.get(
  '/mp/wb/supplies/:supplyCode/sticker',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const supplyCode = String(req.params.supplyCode || '').trim();

      if (!supplyCode) {
        return res.status(400).json({ error: 'SUPPLY_CODE_REQUIRED' });
      }

      // 1) Ищем строку поставки в нашей таблице
      const supRes = await db.query(
        `
        SELECT
          s.supply_code,
          s.client_mp_account_id,
          s.sticker_code,
          s.sticker_svg_base64,
          a.api_token
        FROM public.mp_wb_supplies s
        LEFT JOIN public.mp_accounts a
          ON a.id = s.client_mp_account_id
         AND a.marketplace = 'wb'
         AND a.is_active = true
        WHERE s.supply_code = $1
        LIMIT 1
        `,
        [supplyCode]
      );

      if (!supRes.rowCount) {
        return res
          .status(404)
          .json({ error: 'SUPPLY_NOT_FOUND', supply_code: supplyCode });
      }

      const row = supRes.rows[0];

      // 2) Если стикер уже сохранён — просто отдаем
      if (row.sticker_svg_base64 && row.sticker_svg_base64.length > 0) {
        return res.json({
          status: 'ok',
          source: 'db',
          supply_code: row.supply_code,
          barcode: row.sticker_code || row.supply_code,
          file: row.sticker_svg_base64, // base64 SVG
        });
      }

      const apiToken = row.api_token;
      if (!apiToken) {
        return res.status(500).json({
          error: 'WB_TOKEN_MISSING',
          note: 'Для client_mp_account_id нет активного WB api_token в mp_accounts',
        });
      }

      // 3) Тянем QR у WB по правильному пути:
      //    GET https://marketplace-api.wildberries.ru/api/v3/supplies/{supplyId}/barcode?type=svg
      const url = `https://marketplace-api.wildberries.ru/api/v3/supplies/${encodeURIComponent(
        supplyCode
      )}/barcode?type=svg`;

      const resp = await fetch(url, {
        method: 'GET',
        headers: {
          Authorization: apiToken, // для WB достаточно "сырого" токена
          'Content-Type': 'application/json',
        },
      });

      if (!resp.ok) {
        const bodyText = await resp.text().catch(() => null);
        return res.status(502).json({
          error: 'WB_STICKER_FETCH_FAILED',
          supply_code: supplyCode,
          status: resp.status,
          body: bodyText,
        });
      }

      const data = await resp.json().catch(() => null);
      // ожидаемый формат по докам: { barcode: "WB-GI-...", file: "base64..." }
      if (!data || !data.file) {
        return res.status(502).json({
          error: 'WB_STICKER_BAD_RESPONSE',
          supply_code: supplyCode,
          raw: data,
        });
      }

      const barcode = data.barcode || supplyCode;
      const fileB64 = data.file;

      // 4) Сохраняем в БД
      await db.query(
        `
        UPDATE public.mp_wb_supplies
           SET sticker_code = $2,
               sticker_svg_base64 = $3,
               updated_at = NOW()
         WHERE supply_code = $1
        `,
        [supplyCode, barcode, fileB64]
      );

      // 5) Отдаём клиенту
      return res.json({
        status: 'ok',
        source: 'wb',
        supply_code: supplyCode,
        barcode,
        file: fileB64,
      });
    } catch (err) {
      console.error('/mp/wb/supplies/:supplyCode/sticker error:', err);
      return res.status(500).json({
        error: 'INTERNAL_ERROR',
        details: err.message,
      });
    } finally {
      db.release();
    }
  }
);



// ---------------- WB: GET ITEMS FROM DB ----------------
app.get(
  '/mp/wb/items',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const clientMpAccountId = Number(req.query.client_mp_account_id);
      const limit = Number(req.query.limit) || 50;
      const offset = Number(req.query.offset) || 0;

      if (!clientMpAccountId) {
        return res.status(400).json({ error: 'client_mp_account_id_required' });
      }

      console.log(
        '[WB_ITEMS LIST] client_mp_account_id =',
        clientMpAccountId,
        'limit =',
        limit,
        'offset =',
        offset
      );

      // 1. Узнаём общее количество
      const countSql = `
        SELECT COUNT(*) AS cnt
        FROM mp_wb_items
        WHERE client_mp_account_id = $1
      `;
      const countResult = await pool.query(countSql, [clientMpAccountId]);
      const total = Number(countResult.rows[0]?.cnt || 0);

      // 2. Вытаскиваем страницу данных
      const rowsSql = `
        SELECT
          id,
          client_mp_account_id,
          nm_id,
          imt_id,
          vendor_code,
          brand,
          title,
          chrt_id,
          barcode,
          pics_json,
          created_at,
          updated_at
        FROM mp_wb_items
        WHERE client_mp_account_id = $1
        ORDER BY id
        LIMIT $2 OFFSET $3
      `;
      const rowsResult = await pool.query(rowsSql, [
        clientMpAccountId,
        limit,
        offset,
      ]);

      // 3. Приводим pics_json к массиву
      const items = rowsResult.rows.map((row) => {
        let pics = [];
        if (row.pics_json) {
          try {
            pics = JSON.parse(row.pics_json);
          } catch (e) {
            console.error(
              '[WB_ITEMS LIST] JSON parse error for pics_json, id =',
              row.id,
              e.message
            );
          }
        }

        return {
          id: String(row.id),
          client_mp_account_id: row.client_mp_account_id,
          nm_id: String(row.nm_id),
          imt_id: row.imt_id ? String(row.imt_id) : null,
          vendor_code: row.vendor_code || null,
          brand: row.brand || null,
          title: row.title || null,
          chrt_id: row.chrt_id ? Number(row.chrt_id) : null,
          barcode: row.barcode || null,
          pics,
          created_at: row.created_at,
          updated_at: row.updated_at,
        };
      });

      return res.json({
        status: 'ok',
        total,
        limit,
        offset,
        items,
      });
    } catch (err) {
      console.error('/mp/wb/items error:', err);
      return res.status(500).json({
        error: 'internal_error',
        details: err.message || String(err),
      });
    }
  }
);



// ==============================
//  WB: справочник + остатки
// ==============================
app.get(
  '/mp/wb/items-with-stock',
  authRequired,
  requireRole('owner'),
  async (req, res) => {
    const client = await pool.connect();
    try {
      const clientMpAccountId = Number(req.query.client_mp_account_id || 0);
      const limit  = Number(req.query.limit  || 20);
      const offset = Number(req.query.offset || 0);

      if (!Number.isInteger(clientMpAccountId) || clientMpAccountId <= 0) {
        return res.status(400).json({
          error: 'INVALID_CLIENT_MP_ACCOUNT_ID',
          message: 'client_mp_account_id должен быть положительным целым числом',
        });
      }

      // 1. Находим связанный WMS-клиент (client_id)
      const accRes = await client.query(
        `
        SELECT id, client_id
        FROM mp_client_accounts
        WHERE id = $1
        `,
        [clientMpAccountId],
      );

      if (accRes.rowCount === 0) {
        return res.status(404).json({
          error: 'ACCOUNT_NOT_FOUND',
          message: `MP аккаунт с id=${clientMpAccountId} не найден`,
        });
      }

      const wmsClientId = accRes.rows[0].client_id;

      // 2. WB-товары (страница + лимит/офсет)
      //    Используем COALESCE, чтобы поддержать старые / новые названия колонок.
      const itemsRes = await client.query(
        `
        SELECT
          i.id,
          i.nm_id,
          i.chrt_id,
          COALESCE(i.article, i.vendor_code)          AS vendor_code,
          i.brand,
          COALESCE(i.item_name, i.title)             AS title,
          COALESCE(i.preview_image_url, i.preview_url) AS preview_url,
          b.barcode
        FROM mp_wb_items i
        LEFT JOIN mp_wb_items_barcodes b
          ON  b.client_mp_account_id = i.client_mp_account_id
          AND b.nm_id                = i.nm_id      -- упрощаем join: по nm_id
        WHERE i.client_mp_account_id = $1
        ORDER BY i.nm_id, i.chrt_id
        LIMIT $2 OFFSET $3
        `,
        [clientMpAccountId, limit, offset],
      );

      const wbItems = itemsRes.rows;

      // Если на странице нет товаров — сразу ответ
      if (wbItems.length === 0) {
        const totalRes = await client.query(
          `SELECT COUNT(*)::int AS cnt
             FROM mp_wb_items
            WHERE client_mp_account_id = $1`,
          [clientMpAccountId],
        );

        return res.json({
          items: [],
          total: totalRes.rows[0].cnt,
          limit,
          offset,
        });
      }

      // 3. Собираем штрихкоды
      const barcodes = wbItems
        .map(r => r.barcode)
        .filter(bc => bc && String(bc).trim() !== '');

      let stockByBarcode = {};
      if (barcodes.length > 0) {
        // 4. Агрегация остатков по barcode
        const stockRes = await client.query(
          `
          WITH acc AS (
            SELECT $1::int AS client_id
          )
          SELECT
            sk.barcode,
            SUM(s.qty) AS stock_qty,
            STRING_AGG(
              DISTINCT l.code,
              ', ' ORDER BY l.code
            ) AS stock_locations
          FROM acc a
          JOIN wms.sku sk
            ON  sk.client_id = a.client_id
           AND sk.barcode   = ANY($2::text[])
          JOIN wms.stock s
            ON  s.client_id = a.client_id
           AND s.sku_id    = sk.id
          LEFT JOIN masterdata.locations l
            ON l.id = s.location_id
          GROUP BY sk.barcode
          `,
          [wmsClientId, barcodes],
        );

        stockByBarcode = stockRes.rows.reduce((acc, row) => {
          acc[row.barcode] = {
            stock_qty: Number(row.stock_qty) || 0,
            stock_locations: row.stock_locations || '',
          };
          return acc;
        }, {});
      }

      // 5. Склеиваем результат для фронта
      const itemsWithStock = wbItems.map((row) => {
        const bc = row.barcode ? String(row.barcode) : null;
        const stockInfo = bc && stockByBarcode[bc]
          ? stockByBarcode[bc]
          : { stock_qty: 0, stock_locations: '' };

        return {
          nm_id:       row.nm_id,
          chrt_id:     row.chrt_id,
          vendor_code: row.vendor_code,
          brand:       row.brand,
          title:       row.title,
          preview_url: row.preview_url,
          barcode:     bc,
          stock_qty:   stockInfo.stock_qty,
          stock_locations: stockInfo.stock_locations,
        };
      });

      // 6. total для пагинации
      const totalRes = await client.query(
        `SELECT COUNT(*)::int AS cnt
           FROM mp_wb_items
          WHERE client_mp_account_id = $1`,
        [clientMpAccountId],
      );

      return res.json({
        items: itemsWithStock,
        total: totalRes.rows[0].cnt,
        limit,
        offset,
      });

    } catch (err) {
      console.error('/mp/wb/items-with-stock error:', err);
      return res.status(500).json({
        error: 'items-with-stock error',
        detail: err.message,
      });
    } finally {
      client.release();
    }
  }
);

// Универсальная загрузка строк отгрузки по поставке WB
async function loadShipmentLinesBySupply(db, wbSupplyId) {
  const { rows } = await db.query(
    `
      SELECT
        o.barcode,
        COUNT(*) AS qty,
        MAX(i.item_name)       AS item_name,
        MAX(o.wb_sticker_code) AS wb_sticker_code,
        MAX(o.wb_sticker)      AS wb_sticker,
        MAX(i.preview_url)     AS preview_url
      FROM public.mp_wb_orders o
      LEFT JOIN public.mp_wb_items_barcodes ib
             ON ib.barcode              = o.barcode
            AND ib.client_mp_account_id = o.client_mp_account_id
      LEFT JOIN public.mp_wb_items i
             ON i.nm_id                 = o.nm_id
            AND i.client_mp_account_id  = o.client_mp_account_id
      WHERE o.wb_supply_id = $1
      GROUP BY o.barcode
      ORDER BY o.barcode
    `,
    [wbSupplyId]
  );

  // qty в PG приходит строкой, приведём к числу
  return rows.map((r) => ({
    barcode: r.barcode,
    qty: Number(r.qty) || 0,
    item_name: r.item_name,
    wb_sticker_code: r.wb_sticker_code,
    wb_sticker: r.wb_sticker,
    preview_url: r.preview_url,
  }));
}


// ---------------- HELPERS: PREVIEW URL ИЗ КАРТОЧКИ WB ----------------

function getPreviewUrlFromCard(card) {
  if (!card) return null;

  // 1) Часто WB кладёт готовые ссылки сюда
  if (Array.isArray(card.mediaFiles) && card.mediaFiles.length > 0) {
    return card.mediaFiles[0];
  }

  // 2) Запасной вариант — photos
  if (Array.isArray(card.photos) && card.photos.length > 0) {
    const p = card.photos[0];

    if (typeof p === 'string') {
      return p;
    }

    // Популярные варианты полей
    if (p.big) return p.big;
    if (p.c1080x1440) return p.c1080x1440;
    if (p.c516x688) return p.c516x688;
    if (p.c246x328) return p.c246x328;
  }

  return null;
}


// Краткий список WB-аккаунтов (по client_mp_accounts + mp_accounts)
app.get(
  '/mp/wb-accounts-short',
  authRequired,
  requireRole('owner'),
  async (req, res) => {
    const client = await pool.connect();
    try {
      const q = await client.query(
        `
        SELECT
          mca.id              AS id,            -- ЭТО client_mp_account_id
          mca.account_name    AS label,         -- название для селекта
          mca.client_id,
          mca.wb_supplier_id  AS supplier_id,
          mca.is_active       AS client_acc_active,
          ma.account_code,
          ma.is_active        AS acc_active
        FROM mp_client_accounts mca
        JOIN mp_accounts ma
          ON ma.supplier_id = mca.wb_supplier_id
         AND LOWER(ma.marketplace) = 'wb'
        WHERE LOWER(mca.marketplace) = 'wb'
          AND mca.is_active = TRUE
          AND ma.is_active  = TRUE
        ORDER BY mca.client_id, mca.id
        `
      );

      return res.json({
        accounts: q.rows,
      });
    } catch (err) {
      console.error('/mp/wb-accounts-short error:', err);
      return res.status(500).json({
        error: 'wb-accounts-short error',
        detail: err.message,
      });
    } finally {
      client.release();
    }
  }
);



// ---------------- WB: IMPORT ITEMS (товары клиента) ----------------

app.post(
  '/mp/wb/import-items',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    // 0. Берём ID MP-аккаунта из query/body
    const accountId = Number(req.query.account_id || req.body.account_id);

    if (!accountId) {
      return res.status(400).json({ error: 'account_id_required' });
    }

    try {
      console.log('*** WB IMPORT ITEMS ***', new Date().toISOString());
      console.log('[WB IMPORT] account_id =', accountId);

      // 1. Достаём MP-аккаунт из mp_accounts (нам нужны и токен, и supplier_id)
      const { rows: accRows } = await pool.query(
        `
          SELECT id, marketplace, supplier_id, api_token
          FROM mp_accounts
          WHERE id = $1
        `,
        [accountId]
      );

      if (!accRows.length) {
        console.error('[WB_IMPORT] mp_account not found, id =', accountId);
        return res.status(404).json({ error: 'mp_account_not_found' });
      }

      const mpAcc = accRows[0];

      if (!mpAcc.api_token) {
        console.error('[WB_IMPORT] api_token is NULL for mp_account id =', accountId);
        return res.status(400).json({ error: 'api_token_missing' });
      }

      if (String(mpAcc.marketplace).toLowerCase() !== 'wb') {
        console.error(
          '[WB_IMPORT] wrong marketplace for mp_account id =',
          accountId,
          'marketplace =',
          mpAcc.marketplace
        );
        return res.status(400).json({ error: 'wrong_marketplace' });
      }

      // 2. Находим связанную строку в mp_client_accounts
      //    Именно её id = client_mp_account_id в mp_wb_items / mp_wb_items_barcodes
      const { rows: mcaRows } = await pool.query(
        `
          SELECT id, client_id, account_name, wb_supplier_id, is_active
          FROM mp_client_accounts
          WHERE wb_supplier_id = $1
            AND LOWER(marketplace) = 'wb'
            AND is_active = true
          ORDER BY id
          LIMIT 1
        `,
        [mpAcc.supplier_id]
      );

      if (!mcaRows.length) {
        console.error(
          '[WB_IMPORT] NO_CLIENT_ACCOUNT for supplier_id =',
          mpAcc.supplier_id,
          'mp_account_id =',
          mpAcc.id
        );
        return res.status(400).json({
          error: 'no_client_account',
          detail: 'Не найдена активная строка mp_client_accounts для этого WB supplier_id',
        });
      }

      const clientMpAccountId = mcaRows[0].id;     // <-- ЭТО пишем в client_mp_account_id
      const wmsClientId       = mcaRows[0].client_id;

      console.log('[WB_IMPORT] resolved mapping:', {
        mp_account_id: mpAcc.id,
        supplier_id: mpAcc.supplier_id,
        client_mp_account_id: clientMpAccountId,
        wms_client_id: wmsClientId,
        account_name: mcaRows[0].account_name,
      });

      const wbToken = mpAcc.api_token;

      // 3. Лимит карточек на одну "страницу" (дальше пагинация внутри fetchWbItems)
      const rawLimit = Number(req.query.limit || req.body.limit) || 100;
      console.log('[WB_IMPORT] incoming limit =', rawLimit);

      // 4. Тянем карточки с пагинацией (fetchWbItems уже сам крутит cursor/offset)
      const cards = await fetchWbItems(wbToken, {
        limit: rawLimit,
        maxPages: 50,
      });

      console.log('[WB IMPORT] fetched total cards:', cards.length);

      // client_mp_account_id в таблицах = ID из mp_client_accounts
      const clientIdForItems = clientMpAccountId;

      // ---------- 5. Подготовка данных для mp_wb_items ----------
      const itemValues = [];
      const itemParams = [];
      let idx = 1;

      // ---------- 6. Подготовка данных для mp_wb_items_barcodes ----------
      const bcValues = [];
      const bcParams = [];
      let bcIdx = 1;
      let skippedNoBarcode = 0;

      for (const c of cards) {
        const previewUrl = getPreviewUrlFromCard(c);

        // mp_wb_items (добавили preview_url как 7-е поле)
        itemValues.push(
          `($${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++}, $${idx++})`
        );
        itemParams.push(
          clientIdForItems,      // client_mp_account_id (ID из mp_client_accounts)
          c.nmID,                // nm_id
          c.imtID || null,       // imt_id
          c.vendorCode || null,  // vendor_code
          c.brand || null,       // brand
          c.title || null,       // title
          previewUrl             // preview_url
        );

        // barcodes
        const barcodes = extractCardBarcodes(c);
        if (!barcodes.length) {
          skippedNoBarcode += 1;
          continue;
        }

        for (const b of barcodes) {
          bcValues.push(
            `($${bcIdx++}, $${bcIdx++}, $${bcIdx++}, $${bcIdx++})`
          );
          bcParams.push(
            clientIdForItems, // client_mp_account_id (ID из mp_client_accounts)
            b.nm_id,          // nm_id
            b.chrt_id,        // chrt_id
            b.barcode         // barcode
          );
        }
      }

      let savedItems = 0;
      let savedBarcodes = 0;

      // 7. Сохраняем mp_wb_items
      if (itemValues.length > 0) {
        const sqlItems = `
          INSERT INTO mp_wb_items
            (client_mp_account_id, nm_id, imt_id, vendor_code, brand, title, preview_url)
          VALUES
            ${itemValues.join(',')}
          ON CONFLICT (client_mp_account_id, nm_id)
          DO UPDATE SET
            imt_id       = EXCLUDED.imt_id,
            vendor_code  = EXCLUDED.vendor_code,
            brand        = EXCLUDED.brand,
            title        = EXCLUDED.title,
            preview_url  = EXCLUDED.preview_url,
            updated_at   = NOW()
        `;
        const r1 = await pool.query(sqlItems, itemParams);
        savedItems = r1.rowCount || 0;
        console.log('[WB IMPORT] saved items rows:', savedItems);
      } else {
        console.log('[WB IMPORT] no items to save');
      }

      // 8. Сохраняем mp_wb_items_barcodes
      if (bcValues.length > 0) {
        const sqlBc = `
          INSERT INTO mp_wb_items_barcodes
            (client_mp_account_id, nm_id, chrt_id, barcode)
          VALUES
            ${bcValues.join(',')}
          ON CONFLICT (client_mp_account_id, nm_id, chrt_id, barcode)
          DO NOTHING
        `;
        const r2 = await pool.query(sqlBc, bcParams);
        savedBarcodes = r2.rowCount || 0;
        console.log('[WB IMPORT] saved barcodes rows:', savedBarcodes);
      } else {
        console.log('[WB IMPORT] no barcodes to save');
      }

      console.log(
        '[WB IMPORT] prepared items:', cards.length,
        'saved items:', savedItems,
        'saved barcodes:', savedBarcodes,
        'skipped (no barcode):', skippedNoBarcode
      );

      // 9. Автоматическая синхронизация masterdata.items из mp_wb_items
      //    Используем clientMpAccountId, т.к. он же лежит в client_mp_account_id в mp_wb_items
      let syncResult = null;
      try {
        if (typeof syncItemsForClient === 'function') {
          syncResult = await syncItemsForClient(clientMpAccountId);
          console.log('[WB IMPORT] syncItemsForClient result:', syncResult);
        } else {
          console.warn('[WB IMPORT] syncItemsForClient is not defined');
        }
      } catch (syncErr) {
        console.error('[WB IMPORT] syncItemsForClient error:', syncErr);
      }

      return res.json({
        status: 'ok',
        mp_account_id: mpAcc.id,
        client_mp_account_id: clientMpAccountId,
        wms_client_id: wmsClientId,
        fetched_cards: cards.length,
        saved_items: savedItems,
        saved_barcodes: savedBarcodes,
        skipped_no_barcode: skippedNoBarcode,
        sync_result: syncResult,
      });
    } catch (err) {
      console.error('WB IMPORT ITEMS ERROR', err);
      return res.status(500).json({
        error: 'internal_error',
        details: err.message || String(err),
      });
    }
  }
);

// -------------------------
// WB заказы: список из public.mp_wb_orders
// с привязкой к mp_accounts (аккаунт WB) и masterdata.clients (клиент по client_code=supplier_id)
// GET /mp/wb/orders?mp_account_id=2&limit=200&status=fbs&date_from=2026-02-16&date_to=2026-02-16
// -------------------------
app.get(
  '/mp/wb/orders',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const {
        mp_account_id,
        client_mp_account_id,
        limit,
        status,
        date_from,
        date_to,
      } = req.query;

      const accountId = Number(mp_account_id || client_mp_account_id || 0);
      const limitNum =
        Number.isInteger(Number(limit)) && Number(limit) > 0
          ? Math.min(Number(limit), 500)
          : 200;

      const params = [];
      let where = 'WHERE 1=1';

      if (accountId) {
        params.push(accountId);
        where += ` AND o.client_mp_account_id = $${params.length}`;
      }

      if (status && String(status).trim()) {
        params.push(String(status).trim());
        where += ` AND o.status = $${params.length}`;
      }

      if (date_from) {
        params.push(date_from);
        where += ` AND o.created_at >= $${params.length}::date`;
      }

      if (date_to) {
        params.push(date_to);
        where += ` AND o.created_at < ($${params.length}::date + interval '1 day')`;
      }

      params.push(limitNum);
      const limitIdx = params.length;

      const sql = `
        SELECT
          o.id,
          o.client_mp_account_id,
          o.wb_order_id,
          o.nm_id,
          o.chrt_id,
          o.article,
          o.barcode,
          o.warehouse_id,
          o.warehouse_name,
          o.region_name,
          o.price,
          o.converted_price,
          o.currency_code,
          o.status,            -- статус WB (у тебя сейчас часто 'fbs')
          o.wb_supply_id,      -- поставка WB-GI-...
          o.wb_sticker_code,
          o.wb_sticker,        -- стикер (пока null — подтянем позже)
          o.created_at,
          o.fetched_at,

          ma.label AS mp_account_label,

          -- клиент: ищем по supplier_id (165970) -> masterdata.clients.client_code ('165970')
          COALESCE(c.client_name, ma.label) AS client_name

        FROM public.mp_wb_orders o
        LEFT JOIN public.mp_accounts ma
          ON ma.id = o.client_mp_account_id
        LEFT JOIN masterdata.clients c
          ON c.client_code = (ma.supplier_id)::text

        ${where}
        ORDER BY o.created_at DESC
        LIMIT $${limitIdx}
      `;

      const dbRes = await pool.query(sql, params);

      return res.json({
        status: 'ok',
        rows: dbRes.rows,
        count: dbRes.rowCount,
      });
    } catch (err) {
      console.error('/mp/wb/orders error:', err);
      return res.status(500).json({
        error: 'Ошибка получения заказов WB',
        details: err.message,
      });
    }
  }
);


// ---------------- MP CLIENT ACCOUNTS (связка клиентов склада и MP-кабинетов) ----------------

app.get(
  '/mp/client-accounts',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const { rows } = await pool.query(`
        SELECT 
          mca.id,
          mca.client_id,
          c.name       AS client_name,
          mca.marketplace,
          mca.account_name,
          mca.wb_supplier_id,
          mca.is_active,
          mca.created_at,
          mca.updated_at
        FROM mp_client_accounts mca
        LEFT JOIN clients c ON c.id = mca.client_id
        ORDER BY mca.id
      `);

      res.json({ status: 'ok', items: rows });
    } catch (err) {
      console.error('GET /mp/client-accounts error', err);
      res.status(500).json({ status: 'error', message: 'internal_error', details: err.message });
    }
  }
);

app.post(
  '/mp/client-accounts',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const {
        id,              // необязателен: если есть -> обновляем, если нет -> создаём
        client_id,
        marketplace,
        account_name,
        wb_supplier_id,
        is_active
      } = req.body || {};

      if (!client_id || !marketplace) {
        return res.status(400).json({ status: 'error', message: 'client_id_and_marketplace_required' });
      }

      const active = (is_active === undefined || is_active === null) ? true : !!is_active;

      if (id) {
        // UPDATE
        const { rows } = await pool.query(`
          UPDATE mp_client_accounts
          SET
            client_id      = $1,
            marketplace    = $2,
            account_name   = $3,
            wb_supplier_id = $4,
            is_active      = $5,
            updated_at     = NOW()
          WHERE id = $6
          RETURNING *
        `, [
          client_id,
          marketplace,
          account_name || null,
          wb_supplier_id || null,
          active,
          id
        ]);

        return res.json({ status: 'ok', item: rows[0] });
      } else {
        // INSERT
        const { rows } = await pool.query(`
          INSERT INTO mp_client_accounts
            (client_id, marketplace, account_name, wb_supplier_id, is_active)
          VALUES ($1, $2, $3, $4, $5)
          RETURNING *
        `, [
          client_id,
          marketplace,
          account_name || null,
          wb_supplier_id || null,
          active
        ]);

        return res.json({ status: 'ok', item: rows[0] });
      }
    } catch (err) {
      console.error('POST /mp/client-accounts error', err);
      res.status(500).json({ status: 'error', message: 'internal_error', details: err.message });
    }
  }
);



// ---------------- STOCK MOVE (Перемещение между ячейками) ----------------

app.post('/stock/move', authRequired, requireRole('owner'), async (req, res) => {
  const client = await pool.connect();

  try {
    const {
      client_id,
      barcode,
      from_location_code,
      to_location_code,
      qty,
      movement_type,
      comment,
      ref_type,
      ref_id,
      sku_id,
    } = req.body || {};

    // 1) обязательные поля
    if (!client_id || !barcode || !from_location_code || !to_location_code || qty === undefined || !movement_type) {
      return res.status(400).json({
        error: 'Не хватает обязательных полей',
        missing_fields: [
          !client_id ? 'client_id' : null,
          !barcode ? 'barcode' : null,
          !from_location_code ? 'from_location_code' : null,
          !to_location_code ? 'to_location_code' : null,
          qty === undefined ? 'qty' : null,
          !movement_type ? 'movement_type' : null,
        ].filter(Boolean),
      });
    }

    const clientIdNum = Number(client_id);
    if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
      return res.status(400).json({ error: 'client_id должен быть положительным целым числом' });
    }

    const qtyNum = Number(qty);
    if (!Number.isInteger(qtyNum) || qtyNum <= 0) {
      return res.status(400).json({ error: 'qty должен быть положительным целым числом' });
    }

    const barcodeStr = String(barcode).trim();
    const fromCode = String(from_location_code).trim();
    const toCode = String(to_location_code).trim();

    // Нормализуем и валидируем тип движения
    let movementTypeNorm;
    try {
      movementTypeNorm = validateMovementType(movement_type);
    } catch (e) {
      return res.status(400).json({ error: e.message });
    }

    // Для этого эндпоинта по смыслу должен быть move
    if (movementTypeNorm !== 'move') {
      return res.status(400).json({
        error: 'Для /stock/move movement_type должен быть "move"',
        received: movement_type,
        normalized: movementTypeNorm,
      });
    }

    const refTypeNorm = normalizeRefType(ref_type);
    const refIdNorm = normalizeRefId(ref_id);

    await client.query('BEGIN');

    // 2) sku_id
    let skuIdFinal;
    if (sku_id !== undefined && sku_id !== null) {
      skuIdFinal = Number(sku_id);
      if (!Number.isInteger(skuIdFinal) || skuIdFinal <= 0) {
        throw new Error('sku_id должен быть положительным целым числом');
      }
    } else {
      // Привязка barcode -> sku_id через наш helper
      skuIdFinal = await resolveSkuIdOrCreate(client, { client_id: clientIdNum, barcode: barcodeStr });
    }

    // 3) Получаем/создаём location_id для FROM/TO
    const getOrCreateLocationId = async (code) => {
      const r1 = await client.query(
        `SELECT id FROM masterdata.locations WHERE code = $1 LIMIT 1`,
        [code]
      );
      if (r1.rowCount > 0) return r1.rows[0].id;

      const r2 = await client.query(
        `
        INSERT INTO masterdata.locations (code, is_active, created_at)
        VALUES ($1, true, NOW())
        RETURNING id
        `,
        [code]
      );
      return r2.rows[0].id;
    };

    const fromLocationId = await getOrCreateLocationId(fromCode);
    const toLocationId = await getOrCreateLocationId(toCode);

    // 4) Блокировка инвентаризацией по FROM/TO (по client_id + barcode + location_code)
    const invBlock = await client.query(
      `
      SELECT id, location_code, status
      FROM wms.inventory_tasks
      WHERE client_id     = $1
        AND barcode       = $2
        AND location_code = ANY($3::text[])
        AND status IN ('open','in_progress')
      LIMIT 1
      `,
      [clientIdNum, barcodeStr, [fromCode, toCode]]
    );

    if (invBlock.rowCount > 0) {
      await client.query('ROLLBACK');
      return res.status(409).json({
        error: 'Нельзя выполнить перемещение: ячейка заблокирована открытой инвентаризацией',
        inventory_task: invBlock.rows[0],
      });
    }

    // 5) Читаем FROM из wms.stock по (sku_id + location_id)
    const fromStock = await client.query(
      `
      SELECT qty
      FROM wms.stock
      WHERE sku_id      = $1
        AND location_id = $2
      FOR UPDATE
      `,
      [skuIdFinal, fromLocationId]
    );

    if (fromStock.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(400).json({
        error: 'Нет записи в stock для исходной ячейки (FROM) по этому sku',
        sku_id: skuIdFinal,
        from_location_code: fromCode,
      });
    }

    const fromCur = Number(fromStock.rows[0].qty) || 0;
    if (fromCur < qtyNum) {
      await client.query('ROLLBACK');
      return res.status(400).json({
        error: 'Недостаточно остатка в исходной ячейке',
        current_qty: fromCur,
        required_qty: qtyNum,
      });
    }

    const fromNew = fromCur - qtyNum;

    // Обновляем FROM
    await client.query(
      `
      UPDATE wms.stock
         SET qty = $1
       WHERE sku_id      = $2
         AND location_id = $3
      `,
      [fromNew, skuIdFinal, fromLocationId]
    );

    // 6) TO: читаем / создаём
    const toStock = await client.query(
      `
      SELECT qty
      FROM wms.stock
      WHERE sku_id      = $1
        AND location_id = $2
      FOR UPDATE
      `,
      [skuIdFinal, toLocationId]
    );

    let toNew;
    if (toStock.rowCount === 0) {
      toNew = qtyNum;
      await client.query(
        `
        INSERT INTO wms.stock (sku_id, location_id, qty, created_at)
        VALUES ($1, $2, $3, NOW())
        `,
        [skuIdFinal, toLocationId, toNew]
      );
    } else {
      const toCur = Number(toStock.rows[0].qty) || 0;
      toNew = toCur + qtyNum;
      await client.query(
        `
        UPDATE wms.stock
           SET qty = $1
         WHERE sku_id      = $2
           AND location_id = $3
        `,
        [toNew, skuIdFinal, toLocationId]
      );
    }

    // 7) Логируем движение в movements
    await client.query(
      `
      INSERT INTO wms.movements (
        created_at,
        user_id,
        client_id,
        sku_id,
        barcode,
        qty,
        from_location,
        to_location,
        movement_type,
        ref_type,
        ref_id,
        comment
      )
      VALUES (
        NOW(),
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10,
        $11
      )
      `,
      [
        req.user.id,       // $1
        clientIdNum,       // $2
        skuIdFinal,        // $3
        barcodeStr,        // $4
        qtyNum,            // $5
        fromCode,          // $6
        toCode,            // $7
        movementTypeNorm,  // $8 (move)
        refTypeNorm,       // $9
        refIdNorm,         // $10
        comment || null,   // $11
      ]
    );

    await client.query('COMMIT');

    return res.json({
      status: 'ok',
      sku_id: skuIdFinal,
      from_location_code: fromCode,
      to_location_code: toCode,
      qty_moved: qtyNum,
      from_new_qty: fromNew,
      to_new_qty: toNew,
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('Error in /stock/move:', err);
    return res.status(500).json({
      error: 'Internal server error in /stock/move',
      detail: err.message,
    });
  } finally {
    client.release();
  }
});


// ---------------- STOCK ADJUSTMENT + MOVEMENT LOG ----------------

app.post('/stock/adjust', authRequired, requireRole('owner'), async (req, res) => {
  const client = await pool.connect();
  try {
    const {
      sku_id,
      client_id,
      barcode,
      location_code,
      qty,        // может быть не передан
      qty_diff,   // delta
      qty_new,    // целевое (пока не поддерживаем)
      movement_type,
      comment,
      ref_type,
      ref_id,
    } = req.body || {};

    // 1) Для /stock/adjust принимаем только movement_type === 'adjust'
    if (String(movement_type || '').trim().toLowerCase() !== 'adjust') {
      return res.status(400).json({
        error: 'movement_type для /stock/adjust должен быть "adjust"',
        received: movement_type,
      });
    }

    // 2) Нормализуем количество (qty / qty_diff / qty_new)
    let effectiveQty = qty;
    if (effectiveQty === undefined || effectiveQty === null) {
      if (qty_diff !== undefined && qty_diff !== null) {
        effectiveQty = qty_diff;
      } else if (qty_new !== undefined && qty_new !== null) {
        return res.status(400).json({
          error: 'qty_new пока не поддерживается. Используй qty или qty_diff.',
        });
      }
    }

    // 3) обязательные поля
    if (!client_id || !barcode || !location_code || effectiveQty === undefined) {
      return res.status(400).json({
        error: 'Не хватает обязательных полей',
        missing_fields: [
          !client_id ? 'client_id' : null,
          !barcode ? 'barcode' : null,
          !location_code ? 'location_code' : null,
          effectiveQty === undefined ? 'qty/qty_diff' : null,
        ].filter(Boolean),
        received_body: req.body,
      });
    }

    const clientIdNum = Number(client_id);
    if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
      return res.status(400).json({ error: 'client_id должен быть положительным целым числом' });
    }

    const barcodeStr = String(barcode).trim();
    const locCode   = String(location_code).trim();

    const refTypeNorm = normalizeRefType(ref_type);
    const refIdNorm   = normalizeRefId(ref_id);

    const qtyDelta = Number(effectiveQty);
    if (!Number.isInteger(qtyDelta) || qtyDelta === 0) {
      return res.status(400).json({ error: 'qty (или qty_diff) должен быть ненулевым целым числом' });
    }

    const qtyForLog = Math.abs(qtyDelta);

    await client.query('BEGIN');

    // 4) Определяем sku_id (через client_id + barcode)
    let skuIdFinal;
    if (sku_id !== undefined && sku_id !== null) {
      skuIdFinal = Number(sku_id);
      if (!Number.isInteger(skuIdFinal) || skuIdFinal <= 0) {
        throw new Error('sku_id должен быть положительным целым числом');
      }
    } else {
      skuIdFinal = await resolveSkuIdOrCreate(client, {
        client_id: clientIdNum,
        barcode: barcodeStr,
      });
    }

    // 5) Блокировка по инвентаризации (по client_id + barcode + location_code)
    const invBlock = await client.query(
      `
      SELECT id, status
      FROM wms.inventory_tasks
      WHERE client_id     = $1
        AND barcode       = $2
        AND location_code = $3
        AND status IN ('open','in_progress')
      LIMIT 1
      `,
      [clientIdNum, barcodeStr, locCode],
    );

    if (invBlock.rowCount > 0) {
      await client.query('ROLLBACK');
      return res.status(409).json({
        error: 'Ячейка заблокирована заданием на инвентаризацию',
        task_id: invBlock.rows[0].id,
        task_status: invBlock.rows[0].status,
      });
    }

    // 6) Находим/создаём location_id (masterdata.locations)
    const locRes = await client.query(
      `SELECT id FROM masterdata.locations WHERE code = $1 LIMIT 1`,
      [locCode],
    );

    let locationId;
    if (locRes.rowCount > 0) {
      locationId = locRes.rows[0].id;
    } else {
      const insLoc = await client.query(
        `
        INSERT INTO masterdata.locations (code, is_active, created_at)
        VALUES ($1, true, NOW())
        RETURNING id
        `,
        [locCode],
      );
      locationId = insLoc.rows[0].id;
    }

    // 7) РАБОТАЕМ С wms.stock ПО client_id + sku_id + location_id
    const stockRes = await client.query(
      `
      SELECT qty
      FROM wms.stock
      WHERE client_id   = $1
        AND sku_id      = $2
        AND location_id = $3
      FOR UPDATE
      `,
      [clientIdNum, skuIdFinal, locationId],
    );

    let newQty;
    if (stockRes.rowCount === 0) {
      // записи нет — можем только увеличить остаток
      if (qtyDelta < 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: 'Нельзя списать: записи в stock по этой ячейке/sku нет',
        });
      }

      newQty = qtyDelta;
      await client.query(
        `
        INSERT INTO wms.stock (client_id, barcode, sku_id, location_id, qty, created_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        `,
        [clientIdNum, barcodeStr, skuIdFinal, locationId, newQty],
      );
    } else {
      const curQty = Number(stockRes.rows[0].qty) || 0;
      newQty = curQty + qtyDelta;

      if (newQty < 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: 'Нельзя сделать остаток меньше 0',
          current_qty: curQty,
          qty_delta: qtyDelta,
        });
      }

      await client.query(
        `
        UPDATE wms.stock
           SET qty = $1
         WHERE client_id   = $2
           AND sku_id      = $3
           AND location_id = $4
        `,
        [newQty, clientIdNum, skuIdFinal, locationId],
      );
    }

    // 8) Логируем движение (movements)
    await client.query(
      `
      INSERT INTO wms.movements (
        created_at,
        user_id,
        client_id,
        sku_id,
        barcode,
        qty,
        from_location,
        to_location,
        movement_type,
        ref_type,
        ref_id,
        comment
      )
      VALUES (
        NOW(),
        $1,
        $2,
        $3,
        $4,
        $5,
        NULL,
        $6,
        $7,
        $8,
        $9,
        $10
      )
      `,
      [
        req.user.id,          // $1
        clientIdNum,          // $2
        skuIdFinal,           // $3
        barcodeStr,           // $4
        qtyForLog,            // $5
        locCode,              // $6 (to_location)
        'adjust',             // $7 movement_type (жёстко)
        refTypeNorm,          // $8
        refIdNorm,            // $9
        comment || null,      // $10
      ],
    );

    await client.query('COMMIT');

    return res.json({
      status: 'ok',
      client_id: clientIdNum,
      barcode: barcodeStr,
      location_code: locCode,
      location_id: locationId,
      sku_id: skuIdFinal,
      qty_delta: qtyDelta,
      new_qty: newQty,
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('Error in /stock/adjust:', err);
    return res.status(500).json({
      error: 'Internal server error in /stock/adjust',
      detail: err.message,
    });
  } finally {
    client.release();
  }
});





// ==============================
//  RECEIVING ACCEPT (ПРИЁМКА)
//  Строгий режим + автоподтяжка из WB
// ==============================
//
// POST /receiving/accept
// body: { client_id, barcode, location_code, qty, ref_type?, ref_id? }
app.post(
  '/receiving/accept',
  authRequired,
  requireRole('owner'),
  async (req, res) => {
    const dbClient = await pool.connect();

    try {
      const {
        client_id,
        barcode,
        location_code,
        qty,
        ref_type,
        ref_id,
      } = req.body || {};

      // 1. Валидация client_id
      const clientIdNum = Number(client_id);
      if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
        return res.status(400).json({
          error: 'INVALID_CLIENT_ID',
          message: 'client_id должен быть положительным целым числом',
        });
      }

      // Валидация barcode
      const barcodeStr = String(barcode || '').trim();
      if (!barcodeStr) {
        return res.status(400).json({
          error: 'INVALID_BARCODE',
          message: 'barcode обязателен и не может быть пустым',
        });
      }

      // Валидация location_code
      const locCode = String(location_code || '').trim();
      if (!locCode) {
        return res.status(400).json({
          error: 'INVALID_LOCATION_CODE',
          message: 'location_code обязателен и не может быть пустым',
        });
      }

      // Валидация qty
      const qtyNum = Number(qty);
      if (!Number.isInteger(qtyNum) || qtyNum <= 0) {
        return res.status(400).json({
          error: 'INVALID_QTY',
          message: 'qty должен быть целым числом > 0',
        });
      }

      console.log(
        '[/receiving/accept] IN:',
        'client_id =', clientIdNum,
        'barcode =', barcodeStr,
        'loc =', locCode,
        'qty =', qtyNum
      );

      await dbClient.query('BEGIN');

      // 2. Строгая проверка товара + автоподтяжка из WB (masterdata.items + WB mapping)
      console.log('[/receiving/accept] resolveSkuIdStrict...');
      const { skuId, item } = await resolveSkuIdStrict(dbClient, {
        client_id: clientIdNum,
        barcode: barcodeStr,
      });
      console.log(
        '[/receiving/accept] resolveSkuIdStrict OK:',
        'item_id =', item.id,
        'sku_id =', skuId
      );

      // 3. Строгая проверка МХ: существует и активен
      console.log('[/receiving/accept] resolveLocationIdStrict...');
      const { locationId } = await resolveLocationIdStrict(dbClient, locCode);
      console.log(
        '[/receiving/accept] resolveLocationIdStrict OK:',
        'location_id =', locationId
      );

      // 4. Читаем текущий остаток по (client_id, sku_id, location_id) FOR UPDATE
      const stockRes = await dbClient.query(
        `
        SELECT qty
        FROM wms.stock
        WHERE client_id   = $1
          AND sku_id      = $2
          AND location_id = $3
        LIMIT 1
        FOR UPDATE
        `,
        [clientIdNum, skuId, locationId]
      );

      let currentQty = 0;
      if (stockRes.rowCount > 0) {
        currentQty = Number(stockRes.rows[0].qty) || 0;
      }

      const newQty   = currentQty + qtyNum;
      const qtyDelta = qtyNum; // приёмка — всегда плюс

      // 5. UPSERT в wms.stock (БЕЗ updated_at)
      if (stockRes.rowCount === 0) {
        console.log('[/receiving/accept] INSERT wms.stock...');
        await dbClient.query(
          `
          INSERT INTO wms.stock
            (client_id, sku_id, location_id, qty, created_at)
          VALUES
            ($1, $2, $3, $4, NOW())
          `,
          [clientIdNum, skuId, locationId, newQty]
        );
      } else {
        console.log('[/receiving/accept] UPDATE wms.stock...');
        await dbClient.query(
          `
          UPDATE wms.stock
             SET qty = $1
           WHERE client_id   = $2
             AND sku_id      = $3
             AND location_id = $4
          `,
          [newQty, clientIdNum, skuId, locationId]
        );
      }

      // 6. Логируем движение в wms.movements
      //    ⚠ здесь используем СТАРУЮ схему таблицы movements,
      //    такую же, как в /stock/adjust
      console.log('[/receiving/accept] INSERT wms.movements (incoming)...');
      await dbClient.query(
        `
        INSERT INTO wms.movements (
          created_at,
          user_id,
          client_id,
          sku_id,
          barcode,
          qty,
          from_location,
          to_location,
          movement_type,
          ref_type,
          ref_id,
          comment
        )
        VALUES (
          NOW(),
          $1,
          $2,
          $3,
          $4,
          $5,
          NULL,
          $6,
          'incoming',
          $7,
          $8,
          NULL
        )
        `,
        [
          req.user.id,          // $1
          clientIdNum,          // $2
          skuId,                // $3
          barcodeStr,           // $4
          qtyDelta,             // $5
          locCode,              // $6 (to_location)
          ref_type || 'receiving', // $7
          ref_id   || null,        // $8
        ]
      );

      await dbClient.query('COMMIT');
      console.log('[/receiving/accept] COMMIT OK');

      return res.json({
        status: 'ok',
        filters: {
          client_id: clientIdNum,
          barcode: barcodeStr,
          location_code: locCode,
        },
        item: {
          id: item.id,
        },
        stock_before: {
          client_id: clientIdNum,
          sku_id: skuId,
          location_id: locationId,
          qty: currentQty,
        },
        stock_after: {
          client_id: clientIdNum,
          sku_id: skuId,
          location_id: locationId,
          qty: newQty,
        },
      });
    } catch (err) {
      try { await dbClient.query('ROLLBACK'); } catch (_) {}
      console.error('[/receiving/accept] ERROR:', err, 'code =', err.code);

      if (err.code === 'ITEM_NOT_FOUND') {
        return res.status(400).json({
          error: 'ITEM_NOT_FOUND',
          message: `Товар со штрихкодом ${String(req.body?.barcode || '').trim()} для клиента ${req.body?.client_id} не найден ни в masterdata.items, ни в WB-справочнике.`,
        });
      }

      if (err.code === 'ITEM_INACTIVE') {
        return res.status(400).json({
          error: 'ITEM_INACTIVE',
          message: `Товар со штрихкодом ${String(req.body?.barcode || '').trim()} для клиента ${req.body?.client_id} отключён.`,
        });
      }

      if (err.code === 'LOCATION_NOT_FOUND') {
        return res.status(400).json({
          error: 'LOCATION_NOT_FOUND',
          message: `Место хранения "${String(req.body?.location_code || '').trim()}" не найдено в masterdata.locations.`,
        });
      }

      if (err.code === 'LOCATION_INACTIVE') {
        return res.status(400).json({
          error: 'LOCATION_INACTIVE',
          message: `Место хранения "${String(req.body?.location_code || '').trim()}" отключено.`,
        });
      }

      return res.status(500).json({
        error: 'Receiving accept error',
        detail: err.message,
        code: err.code,
      });
    } finally {
      dbClient.release();
    }
  }
);



// ---------------- INVENTORY TASKS (WMS.INVENTORY_TASKS) ----------------
// POST /inventory-tasks


// ---------------- INVENTORY TASKS (WMS.INVENTORY_TASKS) ----------------

// POST /inventory-tasks
// Создать задание на инвентаризацию МХ
// Body: client_id, barcode, location_code, (optional: sku_id, priority, reason, comment)
app.post('/inventory-tasks', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const {
      client_id,
      barcode,
      sku_id,
      location_code,
      priority,
      reason,
      comment,
    } = req.body || {};

    if (!client_id || !barcode || !location_code) {
      return res.status(400).json({ error: 'Обязательные поля: client_id, barcode, location_code' });
    }

    const clientIdNum = Number(client_id);
    if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
      return res.status(400).json({ error: 'client_id должен быть положительным числом' });
    }

    const barcodeStr = String(barcode).trim();
    if (!barcodeStr) {
      return res.status(400).json({ error: 'barcode не может быть пустым' });
    }

    const locCode = String(location_code).trim();
    if (!locCode) {
      return res.status(400).json({ error: 'location_code не может быть пустым' });
    }

    // priority: 1..5
    let pr = 3;
    if (priority !== undefined && priority !== null && priority !== '') {
      pr = Number(priority);
      if (!Number.isInteger(pr) || pr < 1 || pr > 5) {
        return res.status(400).json({ error: 'priority должен быть целым числом 1..5' });
      }
    }

    // sku_id optional
    let skuIdNum = null;
    if (sku_id !== undefined && sku_id !== null && sku_id !== '') {
      skuIdNum = Number(sku_id);
      if (!Number.isInteger(skuIdNum) || skuIdNum <= 0) {
        return res.status(400).json({ error: 'sku_id должен быть положительным целым числом' });
      }
    }

    // 1) Проверяем, нет ли уже открытой/в работе задачи по этой связке
    const existing = await pool.query(
      `
      SELECT
        id, client_id, barcode, sku_id, location_code,
        status, priority, reason, comment,
        created_at, created_by, updated_at, updated_by, closed_at, closed_by
      FROM wms.inventory_tasks
      WHERE client_id     = $1
        AND barcode       = $2
        AND location_code = $3
        AND status IN ('open', 'in_progress')
      LIMIT 1
      `,
      [clientIdNum, barcodeStr, locCode]
    );

    if (existing.rowCount > 0) {
      // Уже есть активная задача — новую не создаём
      return res.json({
        status: 'already_exists',
        task: existing.rows[0],
      });
    }

    // 2) Активной задачи нет — создаём новую
    const ins = await pool.query(
      `
      INSERT INTO wms.inventory_tasks
        (client_id, barcode, sku_id, location_code, status, priority, reason, comment,
         created_at, created_by, updated_at, updated_by)
      VALUES
        ($1, $2, $3, $4, 'open', $5, $6, $7,
         NOW(), $8, NOW(), $8)
      RETURNING
        id, client_id, barcode, sku_id, location_code, status, priority, reason, comment,
        created_at, created_by, updated_at, updated_by, closed_at, closed_by
      `,
      [
        clientIdNum,
        barcodeStr,
        skuIdNum,
        locCode,
        pr,
        reason || null,
        comment || null,
        req.user.id,
      ]
    );

    return res.status(201).json({ status: 'ok', task: ins.rows[0] });
  } catch (err) {
    console.error('Create inventory task error:', err);
    return res.status(500).json({ error: 'Create inventory task error', detail: err.message, code: err.code });
  }
});

// Допустимые статусы инвентаризационных задач
const validInventoryStatuses = ['open', 'in_progress', 'done', 'cancelled'];

function normalizeInventoryStatus(raw) {
  if (!raw) return null;
  const s = String(raw).trim().toLowerCase();
  return validInventoryStatuses.includes(s) ? s : null;
}

// GET /inventory-tasks
// Фильтры (всё необязательно):
//   - id
//   - client_id
//   - barcode
//   - location_code
//   - status
//   - limit, offset
app.get('/inventory-tasks', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const {
      id,
      client_id,
      barcode,
      location_code,
      status,
      limit,
      offset,
    } = req.query || {};

    const conditions = [];
    const values = [];
    let idx = 1;

    if (id !== undefined) {
      const idNum = Number(id);
      if (!Number.isInteger(idNum) || idNum <= 0) {
        return res.status(400).json({ error: 'id должен быть положительным целым числом' });
      }
      conditions.push(`t.id = $${idx++}`);
      values.push(idNum);
    }

    if (client_id !== undefined) {
      const clientIdNum = Number(client_id);
      if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
        return res.status(400).json({ error: 'client_id должен быть положительным целым числом' });
      }
      conditions.push(`t.client_id = $${idx++}`);
      values.push(clientIdNum);
    }

    if (barcode !== undefined) {
      const b = String(barcode).trim();
      if (!b) {
        return res.status(400).json({ error: 'barcode не может быть пустым' });
      }
      conditions.push(`t.barcode = $${idx++}`);
      values.push(b);
    }

    if (location_code !== undefined) {
      const loc = String(location_code).trim();
      if (!loc) {
        return res.status(400).json({ error: 'location_code не может быть пустым' });
      }
      conditions.push(`t.location_code = $${idx++}`);
      values.push(loc);
    }

    if (status !== undefined) {
      const s = normalizeInventoryStatus(status);
      if (!s) {
        return res.status(400).json({
          error: `Некорректный status. Допустимо: ${validInventoryStatuses.join(', ')}`,
        });
      }
      conditions.push(`t.status = $${idx++}`);
      values.push(s);
    }

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    let lim = 100;
    if (limit !== undefined) {
      lim = Number(limit);
      if (!Number.isInteger(lim) || lim <= 0 || lim > 500) {
        return res.status(400).json({ error: 'limit должен быть целым числом 1..500' });
      }
    }

    let off = 0;
    if (offset !== undefined) {
      off = Number(offset);
      if (!Number.isInteger(off) || off < 0) {
        return res.status(400).json({ error: 'offset должен быть целым числом >= 0' });
      }
    }

    const q = `
      SELECT
        t.id,
        t.client_id,
        t.barcode,
        t.sku_id,
        t.location_code,
        t.status,
        t.priority,
        t.reason,
        t.comment,
        t.created_at,
        t.created_by,
        t.updated_at,
        t.updated_by,
        t.closed_at,
        t.closed_by
      FROM wms.inventory_tasks t
      ${where}
      ORDER BY
        CASE t.status
          WHEN 'open'        THEN 1
          WHEN 'in_progress' THEN 2
          WHEN 'done'        THEN 3
          WHEN 'cancelled'   THEN 4
          ELSE 99
        END,
        t.priority ASC,
        t.created_at ASC,
        t.id ASC
      LIMIT ${lim} OFFSET ${off}
    `;

    const r = await pool.query(q, values);
    return res.json({ status: 'ok', tasks: r.rows, limit: lim, offset: off });
  } catch (err) {
    console.error('Get inventory tasks error:', err);
    return res.status(500).json({
      error: 'Get inventory tasks error',
      detail: err.message,
      code: err.code,
    });
  }
});


// PATCH /inventory-tasks/:id
// Можно менять: status, priority, reason, comment.
// При переходе в 'done' или 'cancelled' заполняем closed_at/closed_by.
app.patch('/inventory-tasks/:id', authRequired, requireRole('owner'), async (req, res) => {
  const client = await pool.connect();
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({ error: 'Некорректный id' });
    }

    const { status, priority, reason, comment } = req.body || {};

    if (
      status === undefined &&
      priority === undefined &&
      reason === undefined &&
      comment === undefined
    ) {
      return res.status(400).json({ error: 'Нет полей для обновления' });
    }

    let newStatus = null;
    if (status !== undefined) {
      newStatus = normalizeInventoryStatus(status);
      if (!newStatus) {
        return res.status(400).json({
          error: `Некорректный status. Допустимо: ${validInventoryStatuses.join(', ')}`,
        });
      }
    }

    let newPriority = null;
    if (priority !== undefined) {
      const pr = Number(priority);
      if (!Number.isInteger(pr) || pr < 1 || pr > 5) {
        return res.status(400).json({ error: 'priority должен быть целым числом 1..5' });
      }
      newPriority = pr;
    }

    await client.query('BEGIN');

    // Берём текущую задачу
    const cur = await client.query(
      `
      SELECT
        id,
        status
      FROM wms.inventory_tasks
      WHERE id = $1
      FOR UPDATE
      `,
      [id]
    );

    if (cur.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Задача инвентаризации не найдена' });
    }

    const currentStatus = cur.rows[0].status;

    // При желании можно запретить "реанимацию" уже закрытых задач
    if ((currentStatus === 'done' || currentStatus === 'cancelled') &&
        newStatus && (newStatus === 'open' || newStatus === 'in_progress')) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: 'Нельзя снова открыть уже закрытую задачу' });
    }

    const fields = [];
    const values = [];
    let idx = 1;

    if (newStatus !== null) {
      fields.push(`status = $${idx++}`);
      values.push(newStatus);
    }
    if (newPriority !== null) {
      fields.push(`priority = $${idx++}`);
      values.push(newPriority);
    }
    if (reason !== undefined) {
      fields.push(`reason = $${idx++}`);
      values.push(reason || null);
    }
    if (comment !== undefined) {
      fields.push(`comment = $${idx++}`);
      values.push(comment || null);
    }

    // updated_at / updated_by — всегда
    fields.push(`updated_at = NOW()`);
    fields.push(`updated_by = $${idx++}`);
    values.push(req.user.id);

    // Если статус становится done/cancelled — проставляем closed_at/closed_by
    if (newStatus === 'done' || newStatus === 'cancelled') {
      fields.push(`closed_at = NOW()`);
      fields.push(`closed_by = $${idx++}`);
      values.push(req.user.id);
    }

    values.push(id);

    const q = `
      UPDATE wms.inventory_tasks
      SET ${fields.join(', ')}
      WHERE id = $${idx}
      RETURNING
        id, client_id, barcode, sku_id, location_code,
        status, priority, reason, comment,
        created_at, created_by, updated_at, updated_by, closed_at, closed_by
    `;

    const upd = await client.query(q, values);

    await client.query('COMMIT');
    return res.json({ status: 'ok', task: upd.rows[0] });
  } catch (err) {
    await client.query('ROLLBACK');
    console.error('Update inventory task error:', err);
    return res.status(500).json({
      error: 'Update inventory task error',
      detail: err.message,
      code: err.code,
    });
  } finally {
    client.release();
  }
});
// ==============================
//  INVENTORY TASKS: COMPLETE
// ==============================
//
// POST /inventory-tasks/complete
// Body:
//   - inventory_task_id (обязательный)
//   - actual_qty        (обязательный, фактический остаток по результатам пересчёта, >=0)
//   - comment           (опционально)
//
// Логика:
//   1) Берём inventory_task FOR UPDATE
//   2) Только open/in_progress
//   3) sku_id: если нет — resolveSkuIdOrCreate
//   4) location_id: masterdata.locations (создаём если нет)
//   5) stock: wms.stock по (location_id, sku_id) FOR UPDATE
//   6) qty = actual_qty (INSERT/UPDATE). Допускаем 0.
//   7) Если delta != 0 → movement_type='inventory', ref_type='inventory_task'
//   8) inventory_task -> done (closed_at/closed_by)
//
// ВАЖНО: не используем колонку id в wms.stock и не трогаем updated_at,
// потому что этих колонок в таблице нет.
app.post('/inventory-tasks/complete', authRequired, requireRole('owner'), async (req, res) => {
  const client = await pool.connect();

  try {
    const { inventory_task_id, actual_qty, comment } = req.body || {};

    // 0. Валидация входных параметров
    const taskId = Number(inventory_task_id);
    if (!Number.isInteger(taskId) || taskId <= 0) {
      return res.status(400).json({ error: 'inventory_task_id должен быть положительным целым числом' });
    }

    const actualQtyNum = Number(actual_qty);
    if (!Number.isInteger(actualQtyNum) || actualQtyNum < 0) {
      return res.status(400).json({ error: 'actual_qty должен быть целым числом >= 0' });
    }

    await client.query('BEGIN');

    // 1) Берём задачу инвентаризации
    const taskRes = await client.query(
      `
      SELECT
        id, client_id, barcode, sku_id, location_code,
        status, priority, reason,
        comment AS task_comment,
        created_at, created_by, updated_at, updated_by,
        closed_at, closed_by
      FROM wms.inventory_tasks
      WHERE id = $1
      FOR UPDATE
      `,
      [taskId]
    );

    if (taskRes.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'Задача на инвентаризацию не найдена' });
    }

    const task = taskRes.rows[0];

    if (task.status !== 'open' && task.status !== 'in_progress') {
      await client.query('ROLLBACK');
      return res.status(409).json({
        error: 'Закрыть можно только задачу в статусе open / in_progress',
        current_status: task.status,
      });
    }

    const clientIdNum = Number(task.client_id);
    const barcodeStr  = String(task.barcode).trim();
    const locCode     = String(task.location_code).trim();

    // 2) sku_id
    let skuIdFinal;
    if (task.sku_id !== null && task.sku_id !== undefined) {
      skuIdFinal = Number(task.sku_id);
      if (!Number.isInteger(skuIdFinal) || skuIdFinal <= 0) {
        throw new Error('Некорректный sku_id в задаче на инвентаризацию');
      }
    } else {
      skuIdFinal = await resolveSkuIdOrCreate(client, { client_id: clientIdNum, barcode: barcodeStr });
    }

    // 3) location_id: masterdata.locations (создаём если нет)
    const locRes = await client.query(
      `SELECT id FROM masterdata.locations WHERE code = $1 LIMIT 1`,
      [locCode]
    );

    let locationId;
    if (locRes.rowCount > 0) {
      locationId = locRes.rows[0].id;
    } else {
      const insLoc = await client.query(
        `
        INSERT INTO masterdata.locations (code, is_active, created_at)
        VALUES ($1, true, NOW())
        RETURNING id
        `,
        [locCode]
      );
      locationId = insLoc.rows[0].id;
    }

    // 4) Читаем stock по (location_id, sku_id)
    //    ВНИМАНИЕ: здесь НЕТ колонки id в wms.stock, поэтому берём только qty
    const stockRes = await client.query(
      `
      SELECT qty
      FROM wms.stock
      WHERE location_id = $1
        AND sku_id      = $2
      LIMIT 1
      FOR UPDATE
      `,
      [locationId, skuIdFinal]
    );

    let currentQty = 0;

    if (stockRes.rowCount > 0) {
      currentQty = Number(stockRes.rows[0].qty) || 0;
    }

    const delta = actualQtyNum - currentQty;

    // 5) Применяем фактический остаток
    if (stockRes.rowCount === 0) {
      // записи нет — создаём только если actual_qty > 0
      if (actualQtyNum > 0) {
        await client.query(
          `
          INSERT INTO wms.stock (sku_id, location_id, qty, created_at)
          VALUES ($1, $2, $3, NOW())
          `,
          [skuIdFinal, locationId, actualQtyNum]
        );
      }
    } else {
      // запись есть — обновляем qty (0 тоже допустим)
      await client.query(
        `
        UPDATE wms.stock
           SET qty = $1
         WHERE location_id = $2
           AND sku_id      = $3
        `,
        [actualQtyNum, locationId, skuIdFinal]
      );
    }

    // 6) Логируем движение, только если delta != 0
    if (delta !== 0) {
      const qtyForLog    = Math.abs(delta);
      const fromLocation = delta < 0 ? locCode : null;
      const toLocation   = delta > 0 ? locCode : null;

      await client.query(
        `
        INSERT INTO wms.movements
          (created_at, user_id, client_id, sku_id, barcode, qty,
           from_location, to_location, movement_type, ref_type, ref_id, comment)
        VALUES
          (NOW(), $1, $2, $3, $4, $5,
           $6, $7, $8, $9, $10, $11)
        `,
        [
          req.user.id,
          clientIdNum,
          skuIdFinal,
          barcodeStr,
          qtyForLog,
          fromLocation,
          toLocation,
          'inventory',
          'inventory_task',
          taskId,
          comment || task.task_comment || null,
        ]
      );
    }

    // 7) Закрываем inventory_task
    //    Используем статус 'done' (он уже есть в списке допустимых)
    const updTaskRes = await client.query(
      `
      UPDATE wms.inventory_tasks
      SET
        status     = 'done',
        sku_id     = $1,
        comment    = COALESCE($2, comment),
        updated_at = NOW(),
        updated_by = $3,
        closed_at  = NOW(),
        closed_by  = $3
      WHERE id = $4
      RETURNING
        id, client_id, barcode, sku_id, location_code,
        status, priority, reason, comment,
        created_at, created_by, updated_at, updated_by, closed_at, closed_by
      `,
      [skuIdFinal, comment || null, req.user.id, taskId]
    );

    await client.query('COMMIT');

    return res.json({
      status: 'ok',
      inventory_task: updTaskRes.rows[0],
      stock_before: { client_id: clientIdNum, barcode: barcodeStr, location_code: locCode, qty: currentQty },
      stock_after:  { client_id: clientIdNum, barcode: barcodeStr, location_code: locCode, qty: actualQtyNum },
      delta,
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('Inventory complete error:', err);
    return res.status(500).json({
      error: 'Inventory complete error',
      detail: err.message,
      code: err.code,
    });
  } finally {
    client.release();
  }
});




app.get('/movements/summary', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const {
      client_id,
      barcode,
      location_code,
      date_from,
      date_to,
    } = req.query;

    if (!client_id) {
      return res.status(400).json({ error: 'client_id is required' });
    }

    // Разрешённые (боевые) типы движений
    const ALLOWED_MOVEMENT_TYPES = ['incoming', 'move', 'adjust', 'writeoff', 'inventory'];

    const where = [
      'm.client_id = $1',
      'm.movement_type = ANY($2)',
    ];
    const values = [
      Number(client_id),
      ALLOWED_MOVEMENT_TYPES,
    ];
    let idx = 3;

    if (barcode) {
      where.push(`m.barcode = $${idx}`);
      values.push(barcode);
      idx++;
    }

    if (location_code) {
      // Ячейка может быть либо from_location, либо to_location
      where.push(`(m.from_location = $${idx} OR m.to_location = $${idx})`);
      values.push(location_code);
      idx++;
    }

    if (date_from) {
      where.push(`m.created_at >= $${idx}`);
      values.push(date_from);
      idx++;
    }

    if (date_to) {
      // date_to НЕ включительно
      where.push(`m.created_at < $${idx}`);
      values.push(date_to);
      idx++;
    }

    const sql = `
      SELECT
        date_trunc('day', m.created_at) AS day,
        m.movement_type,
        SUM(m.qty) AS qty_sum,
        COUNT(*) AS rows_count
      FROM wms.movements m
      WHERE ${where.join(' AND ')}
      GROUP BY 1, 2
      ORDER BY 1 DESC, 2;
    `;

    const { rows } = await pool.query(sql, values);

    res.json({
      status: 'ok',
      client_id: Number(client_id),
      filters: {
        date_from: date_from || null,
        date_to: date_to || null,
        barcode: barcode || null,
        location_code: location_code || null,
      },
      rows,
    });
  } catch (err) {
    console.error('Error in /movements/summary:', err);
    res.status(500).json({
      error: 'Internal server error in /movements/summary',
      details: err.message,
    });
  }
});

async function touchShipmentStatusByPickingTask(client, pickingTaskId, newStatus) {
  // newStatus сейчас будем использовать как 'picking',
  // дальше можно тем же хелпером делать и другие статусы, если понадобится
  await client.query(
    `
    UPDATE wms.shipments s
    SET
      status     = $2,
      updated_at = NOW()
    FROM wms.picking_tasks t
    WHERE t.id = $1
      AND t.shipment_code = s.external_id
      AND s.status IS DISTINCT FROM $2
    `,
    [pickingTaskId, newStatus]
  );
}

// -------------------------
// Закрыть волну: припарковать короб на МХ упаковки
// -------------------------
app.post(
  '/picking/wave/complete',
  authRequired,
  requireRole(['owner', 'admin', 'picker']),
  async (req, res) => {
    const userId = req.user.id;
    const { location_code } = req.body || {};

    if (!location_code || !location_code.trim()) {
      return res.status(400).json({
        error: 'location_required',
        details: 'Нужно отсканировать МХ зоны упаковки',
      });
    }

    const scanned = location_code.trim();

    // 1) Жёстко контролируем префикс PAK-
    if (!/^PAK-/i.test(scanned)) {
      return res.status(400).json({
        error: 'invalid_buffer_location_prefix',
        details: 'Допустимы только МХ, начинающиеся с префикса PAK-',
      });
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      // 2) Находим активную/готовую волну для текущего сборщика
      const waveRes = await client.query(
        `
        SELECT
          shipment_code,
          client_id,
          status
        FROM wms.pick_waves
        WHERE picker_id = $1
          AND status IN ('open', 'in_progress', 'ready')
        ORDER BY shipment_code DESC
        LIMIT 1
        `,
        [userId]
      );

      if (!waveRes.rowCount) {
        await client.query('ROLLBACK');
        return res.status(409).json({
          error: 'NO_ACTIVE_WAVE',
          details: 'У сборщика нет активной волны',
        });
      }

      const wave = waveRes.rows[0];

      // 3) Ищем МХ упаковки в masterdata.locations по колонке code
      const locRes = await client.query(
        `
        SELECT id, code, is_active
        FROM masterdata.locations
        WHERE UPPER(TRIM(code)) = UPPER(TRIM($1))
          AND is_active = TRUE
        LIMIT 1
        `,
        [scanned]
      );

      if (!locRes.rowCount) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: 'location_not_found',
          details: `МХ ${scanned} не найден или неактивен`,
        });
      }

      const bufferLocationCode = locRes.rows[0].code;

      // 4) Обновляем волну: считаем её закрытой и фиксируем МХ упаковки
      await client.query(
        `
        UPDATE wms.pick_waves
        SET
          status = 'done',
          buffer_location_code = $1,
          ready_at = NOW()
        WHERE shipment_code = $2
          AND picker_id = $3
        `,
        [bufferLocationCode, wave.shipment_code, userId]
      );

      // 5) Фиксируем транзакцию по волне
      await client.query('COMMIT');

      // 6) Автоматически создаём задачи на упаковку по этой отгрузке
      try {
        console.log(
          '[picking/wave/complete] call autoCreatePackingTaskForShipment',
          {
            client_id: wave.client_id,
            shipment_code: wave.shipment_code,
            user_id: userId,
          }
        );

        await autoCreatePackingTaskForShipment(
          wave.client_id,
          wave.shipment_code,
          userId
        );
      } catch (err) {
        console.error(
          '[picking/wave/complete] autoCreatePackingTaskForShipment error:',
          err.message || err
        );
        // Ошибку логируем, но ответ пользователю всё равно 200,
        // чтобы не ломать поток сборщика.
      }

      return res.json({
        result: 'ok',
        shipment_code: wave.shipment_code,
        buffer_location_code: bufferLocationCode,
      });
    } catch (err) {
      try {
        await client.query('ROLLBACK');
      } catch (_) {}

      console.error('picking/wave/complete error:', err);
      return res.status(500).json({
        error: 'INTERNAL_ERROR',
        details: err.message,
      });
    } finally {
      client.release();
    }
  }
);



// ---------------- PICKING TASKS (WMS.PICKING_TASKS) ----------------

// Допустимые статусы задач на сборку
const validPickingStatuses = ['new', 'in_progress', 'done', 'cancelled'];

function normalizePickingStatus(raw) {
  if (!raw) return null;
  const s = String(raw).trim().toLowerCase();
  return validPickingStatuses.includes(s) ? s : null;
}

// POST /picking-tasks
// Создать задание на сборку
// Body: client_id, barcode, location_code, qty, (optional: sku_id, priority, order_ref, comment)
app.post('/picking-tasks', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const {
      client_id,
      barcode,
      sku_id,
      location_code,
      qty,
      priority,
      order_ref,
      comment,
    } = req.body || {};

    if (!client_id || !barcode || !location_code || qty === undefined) {
      return res.status(400).json({ error: 'Обязательные поля: client_id, barcode, location_code, qty' });
    }

    const clientIdNum = Number(client_id);
    if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
      return res.status(400).json({ error: 'client_id должен быть положительным числом' });
    }

    const barcodeStr = String(barcode).trim();
    if (!barcodeStr) {
      return res.status(400).json({ error: 'barcode не может быть пустым' });
    }

    const locCode = String(location_code).trim();
    if (!locCode) {
      return res.status(400).json({ error: 'location_code не может быть пустым' });
    }

    const qtyNum = Number(qty);
    if (!Number.isInteger(qtyNum) || qtyNum <= 0) {
      return res.status(400).json({ error: 'qty должен быть положительным целым числом' });
    }

    // priority: 1..5
    let pr = 3;
    if (priority !== undefined && priority !== null && priority !== '') {
      pr = Number(priority);
      if (!Number.isInteger(pr) || pr < 1 || pr > 5) {
        return res.status(400).json({ error: 'priority должен быть целым числом 1..5' });
      }
    }

    // sku_id optional
    let skuIdNum = null;
    if (sku_id !== undefined && sku_id !== null && sku_id !== '') {
      skuIdNum = Number(sku_id);
      if (!Number.isInteger(skuIdNum) || skuIdNum <= 0) {
        return res.status(400).json({ error: 'sku_id должен быть положительным целым числом' });
      }
    }

    const orderRefStr = order_ref ? String(order_ref).trim() : null;

    // Проверяем, нет ли уже "живой" задачи по этой связке
    const existing = await pool.query(
      `
      SELECT
        id, client_id, barcode, sku_id, location_code, qty,
        status, priority, order_ref, comment,
        created_at, created_by, updated_at, updated_by
      FROM wms.picking_tasks
      WHERE client_id     = $1
        AND barcode       = $2
        AND location_code = $3
        AND (order_ref IS NOT DISTINCT FROM $4)
        AND status IN ('new', 'in_progress')
      LIMIT 1
      `,
      [clientIdNum, barcodeStr, locCode, orderRefStr]
    );

    if (existing.rowCount > 0) {
      return res.json({
        status: 'already_exists',
        task: existing.rows[0],
      });
    }

    const ins = await pool.query(
      `
      INSERT INTO wms.picking_tasks
        (client_id, barcode, sku_id, location_code, qty,
         status, priority, order_ref, comment,
         created_at, created_by, updated_at, updated_by)
      VALUES
        ($1, $2, $3, $4, $5,
         'new', $6, $7, $8,
         NOW(), $9, NOW(), $9)
      RETURNING
        id, client_id, barcode, sku_id, location_code, qty,
        status, priority, order_ref, comment,
        created_at, created_by, updated_at, updated_by
      `,
      [
        clientIdNum,
        barcodeStr,
        skuIdNum,
        locCode,
        qtyNum,
        pr,
        orderRefStr,
        comment || null,
        req.user.id,
      ]
    );

    return res.status(201).json({ status: 'ok', task: ins.rows[0] });
  } catch (err) {
    console.error('Create picking task error:', err);
    return res.status(500).json({ error: 'Create picking task error', detail: err.message, code: err.code });
  }
});
// Простое экранирование одинарных кавычек для строковых литералов в SQL
function escLiteral(v) {
  return String(v).replace(/'/g, "''");
}

// ==============================
// GET /picking-tasks (admin list with filters)
// ==============================
app.get('/picking-tasks', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const clientId = Number(req.query.client_id);
    if (!Number.isInteger(clientId) || clientId <= 0) {
      return res
        .status(400)
        .json({ error: 'client_id is required and must be a positive integer' });
    }

    const status       = req.query.status        ? String(req.query.status).trim()        : null;
    const barcode      = req.query.barcode       ? String(req.query.barcode).trim()       : null;
    const locationCode = req.query.location_code ? String(req.query.location_code).trim() : null;
    const orderRef     = req.query.order_ref     ? String(req.query.order_ref).trim()     : null;

    const priority = (req.query.priority !== undefined && req.query.priority !== '')
      ? Number(req.query.priority)
      : null;

    const dateFrom = req.query.date_from ? String(req.query.date_from).trim() : null; // YYYY-MM-DD
    const dateTo   = req.query.date_to   ? String(req.query.date_to).trim()   : null; // YYYY-MM-DD

    // limit / offset — только числа, НИЧЕГО не передаём как параметры в PG
    let limit = Number(req.query.limit || 50);
    if (!Number.isInteger(limit) || limit <= 0) limit = 50;
    if (limit > 200) limit = 200;

    let offset = Number(req.query.offset || 0);
    if (!Number.isInteger(offset) || offset < 0) offset = 0;

    // -----------------------------
    // Сборка WHERE БЕЗ $1..$N — чистый текст
    // -----------------------------
    const whereParts = [`pt.client_id = ${clientId}`];

    if (status) {
      whereParts.push(`pt.status = '${escLiteral(status)}'`);
    }
    if (barcode) {
      whereParts.push(`pt.barcode = '${escLiteral(barcode)}'`);
    }
    if (locationCode) {
      whereParts.push(`pt.location_code = '${escLiteral(locationCode)}'`);
    }
    if (orderRef) {
      whereParts.push(`pt.order_ref ILIKE '%${escLiteral(orderRef)}%'`);
    }
    if (priority !== null && Number.isInteger(priority)) {
      whereParts.push(`pt.priority = ${priority}`);
    }
    if (dateFrom) {
      whereParts.push(`pt.created_at >= '${escLiteral(dateFrom)}'::date`);
    }
    if (dateTo) {
      whereParts.push(`pt.created_at < '${escLiteral(dateTo)}'::date`);
    }

    const whereSql = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';

    // -----------------------------
    // 1) count — БЕЗ параметров
    // -----------------------------
    const countSql = `
      SELECT COUNT(*)::int AS cnt
      FROM wms.picking_tasks pt
      ${whereSql}
    `;
    const countRes = await pool.query(countSql);
    const total = countRes.rows[0]?.cnt ?? 0;

    // -----------------------------
    // 2) rows — тоже БЕЗ параметров
    // -----------------------------
    const rowsSql = `
      SELECT
        pt.id::text,
        pt.client_id,
        pt.barcode,
        pt.sku_id,
        pt.location_code,
        pt.qty,
        pt.status,
        pt.priority,
        pt.order_ref,
        pt.picker_id,
        pt.reason,
        pt.comment,
        pt.created_at,
        pt.created_by,
        pt.updated_at,
        pt.updated_by,
        pt.started_at,
        pt.finished_at
      FROM wms.picking_tasks pt
      ${whereSql}
      ORDER BY
        pt.priority ASC,
        pt.created_at ASC,
        pt.id ASC
      LIMIT ${limit} OFFSET ${offset}
    `;
    const rowsRes = await pool.query(rowsSql);

    return res.json({
      status: 'ok',
      client_id: clientId,
      filters: {
        status,
        barcode,
        location_code: locationCode,
        order_ref: orderRef,
        priority,
        date_from: dateFrom,
        date_to: dateTo,
      },
      limit,
      offset,
      total,
      tasks: rowsRes.rows,
    });
  } catch (err) {
    console.error('Error in GET /picking-tasks:', err);
    return res.status(500).json({ error: 'Internal server error in /picking-tasks' });
  }
});


// -------------------------
// Ручной запуск синхронизации заказов WB -> mp_wb_orders
// -------------------------
app.post(
  '/wb/sync-orders',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const { mp_account_id } = req.body;

      if (!mp_account_id) {
        return res
          .status(400)
          .json({ status: 'error', error: 'mp_account_id is required' });
      }

      const result = await syncWbOrdersForAccount(mp_account_id);

      return res.json({
        status: 'ok',
        mp_account_id,
        imported: result.imported,
      });
    } catch (err) {
      console.error('wb/sync-orders error', err);
      return res.status(500).json({
        status: 'error',
        error: err.message,
      });
    }
  }
);

// ------------------------- 
// Авто-создание задачи на упаковку и shipment,
// когда все строки сборки по отгрузке в статусе 'done'
// -------------------------
async function autoCreatePackingTaskForShipment(clientId, shipmentCode, userId) {
  console.log('[autoCreatePackingTaskForShipment] start', {
    clientId,
    shipmentCode,
    userId,
  });

  if (!clientId || !shipmentCode) {
    console.log(
      '[autoCreatePackingTaskForShipment] no clientId or shipmentCode, skip'
    );
    return;
  }

  try {
    // 1) Создаём задачу на упаковку, если все picking_tasks done
    const packingRes = await pool.query(
      `
      INSERT INTO wms.packing_tasks (
        client_id,
        shipment_code,
        status,
        priority,
        packer_id,
        boxes_count,
        comment,
        wb_shipment_id,
        created_at,
        created_by
      )
      SELECT
        t.client_id,
        t.shipment_code,
        'new'     AS status,
        100       AS priority,
        NULL      AS packer_id,
        NULL      AS boxes_count,
        NULL      AS comment,
        NULL      AS wb_shipment_id,
        NOW()     AS created_at,
        $3        AS created_by
      FROM (
        SELECT
          client_id,
          shipment_code,
          bool_and(status = 'done') AS all_done
        FROM wms.picking_tasks
        WHERE client_id = $1
          AND shipment_code = $2
        GROUP BY client_id, shipment_code
      ) t
      LEFT JOIN wms.packing_tasks p
        ON p.client_id     = t.client_id
       AND p.shipment_code = t.shipment_code
      WHERE t.all_done = TRUE
        AND p.id IS NULL;
      `,
      [clientId, shipmentCode, userId]
    );

    console.log(
      '[autoCreatePackingTaskForShipment] inserted packing_tasks rows:',
      packingRes.rowCount
    );

    // 2) Гарантируем наличие shipment со статусом 'new'
    const shipmentsRes = await pool.query(
      `
      INSERT INTO wms.shipments (
        external_id,
        client_id,
        marketplace,
        status,
        packing_location_code,
        created_at,
        updated_at
      )
      SELECT
        t.shipment_code      AS external_id,
        t.client_id          AS client_id,
        'wb'                 AS marketplace,
        'new'                AS status,
        'PAK-01'             AS packing_location_code,
        NOW()                AS created_at,
        NOW()                AS updated_at
      FROM (
        SELECT DISTINCT client_id, shipment_code
        FROM wms.picking_tasks
        WHERE client_id = $1
          AND shipment_code = $2
      ) t
      LEFT JOIN wms.shipments s
        ON s.client_id   = t.client_id
       AND s.external_id = t.shipment_code
      WHERE s.id IS NULL;
      `,
      [clientId, shipmentCode]
    );

    console.log(
      '[autoCreatePackingTaskForShipment] inserted shipments rows:',
      shipmentsRes.rowCount
    );
  } catch (err) {
    console.error(
      '[autoCreatePackingTaskForShipment] error:',
      err.message || err
    );
  }
}

// -------------------------
// Формирование FBS-волны из заказов WB + подтянуть стикеры (SVG base64) + сохранить wb_sticker_code
// + создать задачи на сборку (wms.picking_tasks) по агрегированному количеству
// + создать/обновить волну wms.pick_waves (status = 'open')
// + создать shipment СРАЗУ (wms.shipments) — чтобы ID был стабильным с момента создания волны
// + создать задание на упаковку (wms.shipments_wb)
// -------------------------
app.post(
  '/picking/generate-from-wb',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const db = await pool.connect();

    async function safeText(resp) { try { return await resp.text(); } catch { return null; } }
    async function safeJson(resp) { try { return await resp.json(); } catch { return null; } }

    function extractStickerCodeFromBase64Svg(b64) {
      if (!b64 || typeof b64 !== 'string') return null;
      try {
        const svg = Buffer.from(b64, 'base64').toString('utf8');
        const nums = [];
        const re = /<text[^>]*>\s*([0-9]{4,})\s*<\/text>/gim;
        let m;
        while ((m = re.exec(svg)) !== null) nums.push(m[1]);
        if (nums.length >= 2) return `${nums[0]} ${nums[1]}`;
        if (nums.length === 1) return nums[0];
        return null;
      } catch {
        return null;
      }
    }

    async function wbAddOrdersToSupply({ apiToken, supplyId, orderIds }) {
      const urls = [
        `https://marketplace-api.wildberries.ru/api/marketplace/v3/supplies/${encodeURIComponent(supplyId)}/orders`,
        `https://marketplace-api.wildberries.ru/api/v3/supplies/${encodeURIComponent(supplyId)}/orders`,
      ];

      const authVariants = [apiToken, `Bearer ${apiToken}`];
      let lastErr = null;

      for (const url of urls) {
        for (const authHeader of authVariants) {
          const resp = await fetch(url, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json', 'Authorization': authHeader },
            body: JSON.stringify({ orders: orderIds }),
          });

          if (resp.ok) {
            return {
              ok: true,
              url,
              authUsed: authHeader.startsWith('Bearer ') ? 'bearer' : 'raw',
            };
          }

          const bodyText = await safeText(resp);
          lastErr = new Error(`WB add orders failed (${resp.status}) at ${url}: ${bodyText || 'no body'}`);
          if (![404, 401].includes(resp.status)) break;
        }
      }
      throw lastErr || new Error('WB add orders failed: unknown error');
    }

    async function wbTryFetchStickers({ apiToken, orderIds, type = 'svg', width = 58, height = 40 }) {
      const url =
        `https://marketplace-api.wildberries.ru/api/v3/orders/stickers` +
        `?type=${encodeURIComponent(type)}&width=${encodeURIComponent(width)}&height=${encodeURIComponent(height)}`;

      const authVariants = [apiToken, `Bearer ${apiToken}`];
      let last = null;

      for (const authHeader of authVariants) {
        const resp = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': authHeader },
          body: JSON.stringify({ orders: orderIds }),
        });

        if (!resp.ok) {
          const t = await safeText(resp);
          last = {
            ok: false,
            status: resp.status,
            body: t,
            authUsed: authHeader.startsWith('Bearer ') ? 'bearer' : 'raw',
          };
          if (resp.status === 401) continue;
          return last;
        }

        const j = await safeJson(resp);
        return {
          ok: true,
          data: j,
          authUsed: authHeader.startsWith('Bearer ') ? 'bearer' : 'raw',
        };
      }

      return last || { ok: false, status: 'n/a', body: 'unknown error' };
    }

    try {
      const { mp_account_id, limit } = req.body || {};
      const mpAccountId = Number(mp_account_id);

      if (!Number.isInteger(mpAccountId) || mpAccountId <= 0) {
        return res.status(400).json({ error: 'mp_account_id is required and must be > 0' });
      }

      const limitRaw = Number(limit);
      const limitOrders = Number.isInteger(limitRaw) && limitRaw > 0 ? Math.min(limitRaw, 500) : 50;

      // 1) WB токен + привязка к клиенту WMS
      const accRes = await db.query(
        `
          SELECT api_token, wms_client_id, label
          FROM public.mp_accounts
          WHERE id = $1
            AND marketplace = 'wb'
            AND is_active = true
          LIMIT 1
        `,
        [mpAccountId],
      );

      if (!accRes.rowCount) {
        return res.status(400).json({ error: `WB mp_account_id=${mpAccountId} not found or inactive` });
      }

      const { api_token: apiToken, wms_client_id: wmsClientIdRaw, label } = accRes.rows[0];
      if (!apiToken) {
        return res.status(400).json({ error: `WB api_token missing for mp_account_id=${mpAccountId}` });
      }

      const wmsClientId = Number(wmsClientIdRaw);
      if (!Number.isInteger(wmsClientId) || wmsClientId <= 0) {
        return res.status(400).json({
          error:
            `Для WB аккаунта mp_account_id=${mpAccountId} (${label || 'без имени'}) не задан wms_client_id. ` +
            `Выбери клиента WMS в настройках аккаунта и сохрани.`,
        });
      }

      // 2) Заказы без поставки
      const ordersRes = await db.query(
        `
          SELECT
            id,
            client_mp_account_id,
            wb_order_id,
            nm_id,
            chrt_id,
            barcode,
            warehouse_id,
            warehouse_name,
            status,
            wb_supply_id,
            created_at
          FROM public.mp_wb_orders
          WHERE client_mp_account_id = $1
            AND wb_supply_id IS NULL
            AND COALESCE(status,'') NOT IN ('confirm','complete','cancel')
          ORDER BY created_at ASC
          LIMIT $2
        `,
        [mpAccountId, limitOrders],
      );

      if (!ordersRes.rowCount) {
        return res.json({
          status: 'ok',
          mp_account_id: mpAccountId,
          createdSupplies: 0,
          attachedOrders: 0,
          message: 'Нет заказов без поставки для формирования волны',
        });
      }

      const orders = ordersRes.rows;

      // 3) Группируем по складу WB
      const groups = new Map();
      for (const row of orders) {
        const key = String(row.warehouse_id || '') + '|' + (row.warehouse_name || '');
        if (!groups.has(key)) {
          groups.set(key, {
            warehouse_id: row.warehouse_id,
            warehouse_name: row.warehouse_name,
            orders: [],
          });
        }
        groups.get(key).orders.push(row);
      }

      const suppliesResult = [];
      let attachedOrdersTotal = 0;

      for (const [, group] of groups.entries()) {
        const whId = group.warehouse_id;
        const whName = group.warehouse_name || 'UNKNOWN';

        const orderIds = group.orders
          .map((o) => Number(o.wb_order_id))
          .filter((x) => Number.isInteger(x) && x > 0);

        if (!orderIds.length) continue;

        // 4.1 создаём поставку в WB
        const supplyName = `WMS-${mpAccountId}-${whName}-${Date.now()}`;
        const createSupplyResp = await fetch(
          'https://marketplace-api.wildberries.ru/api/v3/supplies',
          {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': apiToken },
            body: JSON.stringify({ name: supplyName }),
          },
        );

        if (!createSupplyResp.ok) {
          const bodyText = await safeText(createSupplyResp);
          throw new Error(`WB create supply failed (${createSupplyResp.status}): ${bodyText || 'no body'}`);
        }

        const supplyBody = (await safeJson(createSupplyResp)) || {};
const supplyIdRaw = String(supplyBody.id || supplyBody.supplyId || supplyBody.supply_id || '').trim();
if (!supplyIdRaw) throw new Error('WB create supply: supplyId is missing in response');

// ✅ ЕДИНЫЙ КОД ОТГРУЗКИ ДЛЯ WMS
// Если WB уже вернул код с префиксом — не дублируем его
const shipmentCode = /^WB-GI-/i.test(supplyIdRaw)
  ? supplyIdRaw
  : `WB-GI-${supplyIdRaw}`;

        // 4.2 добавляем заказы в поставку в WB (WB ждёт raw supplyId)
        const addRes = await wbAddOrdersToSupply({ apiToken, supplyId: supplyIdRaw, orderIds });

        // 4.3 стикеры
        const stickersTry = await wbTryFetchStickers({
          apiToken,
          orderIds,
          type: 'svg',
          width: 58,
          height: 40,
        });

        await db.query('BEGIN');
        try {
          // 5.1 отмечаем поставку у нас
          await db.query(
            `
              UPDATE public.mp_wb_orders
                 SET wb_supply_id = $1,
                     status       = 'confirm'
               WHERE client_mp_account_id = $2
                 AND wb_order_id = ANY($3::bigint[])
            `,
            [shipmentCode, mpAccountId, orderIds],
          );

          // 5.2 сохраняем стикеры
          let stickersSaved = 0;
          let stickersCodesSaved = 0;

          if (stickersTry.ok && stickersTry.data && Array.isArray(stickersTry.data.stickers)) {
            for (const st of stickersTry.data.stickers) {
              if (!st || !st.orderId || !st.file) continue;

              const code = extractStickerCodeFromBase64Svg(st.file);

              await db.query(
                `
                  UPDATE public.mp_wb_orders
                     SET wb_sticker      = $1,
                         wb_sticker_code = $2
                   WHERE client_mp_account_id = $3
                     AND wb_order_id          = $4
                `,
                [st.file, code, mpAccountId, Number(st.orderId)],
              );

              stickersSaved++;
              if (code) stickersCodesSaved++;
            }
          }

          // ✅ 5.3 создаём shipment СРАЗУ
          await db.query(
            `
              INSERT INTO wms.shipments
                (external_id, client_id, marketplace, status, planned_ship_date, created_at, updated_at,
                 total_planned_qty, total_picked_qty, total_packed_qty, total_shipped_qty)
              VALUES
                ($1, $2, 'wb', 'new', NULL, NOW(), NOW(), 0, 0, 0, 0)
              ON CONFLICT (external_id) DO UPDATE
                SET client_id   = EXCLUDED.client_id,
                    marketplace = EXCLUDED.marketplace,
                    updated_at  = NOW()
            `,
            [shipmentCode, wmsClientId],
          );

          // 5.4 создаём/обновляем WMS задачи на сборку по поставке
          let tasksInserted = 0;
          let tasksUpdated = 0;

          const aggRes = await db.query(
            `
              SELECT wb_order_id, barcode, COUNT(*) AS qty
              FROM public.mp_wb_orders
              WHERE client_mp_account_id = $1
                AND wb_supply_id = $2
              GROUP BY wb_order_id, barcode
            `,
            [mpAccountId, shipmentCode],
          );

          for (const row of aggRes.rows) {
            const wbOrderId = Number(row.wb_order_id);
            const barcodeStr = String(row.barcode || '').trim();
            const qty = Number(row.qty) || 0;

            if (!Number.isInteger(wbOrderId) || wbOrderId <= 0) continue;
            if (!barcodeStr || qty <= 0) continue;

            const skuId = await resolveSkuIdOrCreate(db, {
              client_id: wmsClientId,
              barcode: barcodeStr,
            });

            const locPickRes = await db.query(
              `
                SELECT l.location_code
                FROM wms.stock s
                JOIN wms.locations l ON l.id = s.location_id
                WHERE s.sku_id    = $1
                  AND l.client_id = $2
                  AND l.is_active = true
                  AND s.qty > 0
                ORDER BY s.qty DESC, l.location_code ASC
                LIMIT 1
              `,
              [skuId, wmsClientId],
            );
            const locCode = locPickRes.rowCount ? locPickRes.rows[0].location_code : null;

            const existing = await db.query(
              `
                SELECT id, qty
                FROM wms.picking_tasks
                WHERE client_id     = $1
                  AND wb_order_id   = $2
                  AND barcode       = $3
                  AND shipment_code = $4
                LIMIT 1
              `,
              [wmsClientId, wbOrderId, barcodeStr, shipmentCode],
            );

            if (existing.rowCount) {
              const ex = existing.rows[0];
              if (Number(ex.qty) !== qty) {
                await db.query(
                  `
                    UPDATE wms.picking_tasks
                       SET qty        = $1,
                           updated_at = NOW(),
                           updated_by = $2
                     WHERE id = $3
                  `,
                  [qty, req.user.id, ex.id],
                );
                tasksUpdated++;
              }
            } else {
              await db.query(
                `
                  INSERT INTO wms.picking_tasks
                    (client_id, barcode, sku_id, location_code, qty, status, priority,
                     wb_order_id, shipment_code, created_at, updated_at, created_by, updated_by)
                  VALUES
                    ($1, $2, $3, $4, $5, 'new', 3,
                     $6, $7, NOW(), NOW(), $8, $8)
                `,
                [
                  wmsClientId,
                  barcodeStr,
                  skuId,
                  locCode,
                  qty,
                  wbOrderId,
                  shipmentCode,
                  req.user.id,
                ],
              );
              tasksInserted++;
            }
          }

          // 5.5 создаем/обновляем волну на сборку
          await db.query(
            `
              INSERT INTO wms.pick_waves
                (client_id, shipment_code, status, picker_id, buffer_location_code, created_at, ready_at)
              VALUES ($1, $2, 'open', NULL, NULL, NOW(), NULL)
              ON CONFLICT (shipment_code) DO UPDATE
                SET client_id = EXCLUDED.client_id
            `,
            [wmsClientId, shipmentCode],
          );

          // 5.6 создаём задание на упаковку
          await db.query(
            `
              INSERT INTO wms.shipments_wb
                (client_id, shipment_code, wb_sticker_code, marketplace, status, created_at, updated_at)
              VALUES ($1, $2, NULL, 'wb', 'ready_for_packing', NOW(), NOW())
              ON CONFLICT (shipment_code) DO NOTHING
            `,
            [wmsClientId, shipmentCode],
          );

          await db.query('COMMIT');

          attachedOrdersTotal += orderIds.length;

          suppliesResult.push({
            supplyId: supplyIdRaw,
            shipmentCode,
            warehouse_id: whId,
            warehouse_name: whName,
            ordersCount: orderIds.length,
            addOrdersUrlUsed: addRes.url,
            addOrdersAuthUsed: addRes.authUsed,
            tasksInserted,
            tasksUpdated,
            stickersSaved,
            stickersCodesSaved,
            stickersAuthUsed: stickersTry.authUsed || null,
            stickersNote: stickersTry.ok
              ? (stickersSaved ? 'ok' : 'WB вернул 200, но stickers пустые')
              : `stickers not fetched (status=${stickersTry.status || 'n/a'})`,
          });
        } catch (e) {
          try { await db.query('ROLLBACK'); } catch {}
          throw e;
        }
      }

      return res.json({
        status: 'ok',
        mp_account_id: mpAccountId,
        createdSupplies: suppliesResult.length,
        attachedOrders: attachedOrdersTotal,
        supplies: suppliesResult,
        message: `Сформировано поставок: ${suppliesResult.length}, заказов: ${attachedOrdersTotal}`,
      });
    } catch (err) {
      try { await db.query('ROLLBACK'); } catch {}
      console.error('picking/generate-from-wb wave error:', err);
      return res.status(500).json({
        error: 'Ошибка формирования волны FBS',
        details: err.message,
      });
    } finally {
      db.release();
    }
  },
);


// ==============================
// POST /picking/admin-cancel
// Админская отмена задачи сборки без движений по складу
// ==============================
app.post('/picking/admin-cancel', authRequired, requireRole('owner'), async (req, res) => {
  const client = await pool.connect();
  try {
    const pickingTaskId = Number(req.body.picking_task_id);
    if (!Number.isInteger(pickingTaskId) || pickingTaskId <= 0) {
      return res.status(400).json({ error: 'picking_task_id is required and must be a positive integer' });
    }

    const reason = req.body.reason ? String(req.body.reason) : 'ADMIN_CANCEL';
    const comment = req.body.comment ? String(req.body.comment) : null;

    await client.query('BEGIN');

    // 1. Блокируем строку задачи
    const tRes = await client.query(
      `
      SELECT *
      FROM wms.picking_tasks
      WHERE id = $1
      FOR UPDATE
      `,
      [pickingTaskId]
    );

    if (tRes.rowCount === 0) {
      await client.query('ROLLBACK');
      return res.status(404).json({ error: 'picking_task not found' });
    }

    const task = tRes.rows[0];

    // 2. Разрешаем отменять только new / in_progress
    if (!['new', 'in_progress'].includes(task.status)) {
      await client.query('ROLLBACK');
      return res.status(400).json({ error: `cannot cancel picking_task in status=${task.status}` });
    }

    // 3. Обновляем статус на cancelled, не трогаем склад и не создаём inventory-task
    const updRes = await client.query(
      `
      UPDATE wms.picking_tasks
      SET status      = 'cancelled',
          reason      = $2,
          comment     = COALESCE($3, comment),
          finished_at = NOW(),
          updated_at  = NOW(),
          updated_by  = $4
      WHERE id = $1
      RETURNING
        id::text, client_id, barcode, sku_id, location_code, qty,
        status, priority, order_ref, picker_id, reason, comment,
        created_at, created_by, updated_at, updated_by, started_at, finished_at
      `,
      [pickingTaskId, reason, comment, req.user.id]
    );

    await client.query('COMMIT');
// ❶ После COMMIT, но до return
// Автоматически создаём задачи на упаковку по отгрузкам этой волны
try {
  // Предполагаю, что в теле запроса у тебя есть shipment_code волны.
  // Если у тебя другое поле (например wave_id) — скажи, подправим.
  const { shipment_code } = req.body || {};

  if (shipment_code) {
    // Вытащим client_id + shipment_code из задач на сборку
    const groupsRes = await pool.query(
      `
      SELECT DISTINCT client_id, shipment_code
      FROM wms.picking_tasks
      WHERE shipment_code = $1
      `,
      [shipment_code]
    );

    for (const row of groupsRes.rows) {
      autoCreatePackingTaskForShipment(
        row.client_id,
        row.shipment_code,
        req.user.id
      ).catch(err => {
        console.error(
          '[picking/wave/complete] autoCreatePackingTaskForShipment error:',
          err.message || err
        );
      });
    }
  } else {
    console.warn(
      '[picking/wave/complete] shipment_code не передан, autoCreatePackingTaskForShipment не вызван'
    );
  }
} catch (err) {
  console.error(
    '[picking/wave/complete] ошибка при запуске autoCreatePackingTaskForShipment:',
    err.message || err
  );
}
    return res.json({ status: 'ok', picking_task: updRes.rows[0] });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('Error in POST /picking/admin-cancel:', err);
    return res.status(500).json({ error: 'Internal server error in /picking/admin-cancel' });
  } finally {
    client.release();
  }
});


// -------------------------
// Сбросить волну сборки у текущего сборщика
// Разрешено только до начала сборки (нет задач со status = 'done')
// -------------------------
app.post(
  '/picking/wave/reset',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const db = await pool.connect();

    try {
      const pickerId = Number(req.user.id);

      // 1) Ищем волну этого пользователя, которую можно сбросить
      const waveRes = await db.query(
        `
        SELECT client_id, shipment_code, status
        FROM wms.pick_waves
        WHERE picker_id = $1
          AND status IN ('active', 'ready', 'open')
        ORDER BY created_at DESC
        LIMIT 1
        `,
        [pickerId]
      );

      if (!waveRes.rowCount) {
        return res.status(404).json({
          error: 'wave_not_found',
          details: 'У пользователя нет волны для сброса',
        });
      }

      const wave = waveRes.rows[0];

      // 2) Проверяем, что нет собранных задач (status = done)
      const doneRes = await db.query(
        `
        SELECT COUNT(*)::int AS cnt
        FROM wms.picking_tasks
        WHERE client_id = $1
          AND shipment_code = $2
          AND status = 'done'
        `,
        [wave.client_id, wave.shipment_code]
      );

      if (doneRes.rows[0].cnt > 0) {
        return res.status(409).json({
          error: 'wave_has_done_tasks',
          details: 'Волну нельзя сбросить: по ней уже есть собранные позиции (status = done)',
        });
      }

      // 3) Сбрасываем волну и задачи в транзакции
      await db.query('BEGIN');

      // 3.1) Волна снова становится "open" и отвязывается от сборщика
      await db.query(
        `
        UPDATE wms.pick_waves
        SET
          picker_id = NULL,
          status = 'open',
          buffer_location_code = NULL,
          ready_at = NULL
        WHERE picker_id = $1
          AND client_id = $2
          AND shipment_code = $3
          AND status IN ('active', 'ready', 'open')
        `,
        [pickerId, wave.client_id, wave.shipment_code]
      );

      // 3.2) Все несобранные задачи возвращаем в new
      await db.query(
        `
        UPDATE wms.picking_tasks
        SET
          status = 'new',
          picker_id = NULL,
          started_at = NULL,
          finished_at = NULL,
          picked_qty = 0,
          updated_at = NOW(),
          updated_by = $1
        WHERE client_id = $2
          AND shipment_code = $3
          AND status IN ('new', 'in_progress')
        `,
        [pickerId, wave.client_id, wave.shipment_code]
      );

      await db.query('COMMIT');

      return res.json({
        result: 'ok',
        shipment_code: wave.shipment_code,
      });
    } catch (err) {
      try { await db.query('ROLLBACK'); } catch {}
      console.error('picking/wave/reset error:', err);
      return res.status(500).json({
        error: 'wave_reset_failed',
        details: err.message,
      });
    } finally {
      db.release();
    }
  }
);





// -------------------------
// GET /picking/wave/status
// Статус активной волны + прогресс
// -------------------------
app.get(
  '/picking/wave/status',
  authRequired,
  requireRole(['owner', 'admin', 'picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = Number(req.user.id);

      // 1) Находим волну этого сборщика
      const waveRes = await db.query(
        `
        SELECT client_id, shipment_code, status
        FROM wms.pick_waves
        WHERE picker_id = $1
          AND status IN ('active', 'ready')
        ORDER BY created_at DESC
        LIMIT 1
        `,
        [pickerId]
      );

      if (!waveRes.rowCount) {
        return res.json({ has_wave: false });
      }

      const row = waveRes.rows[0];
      const clientId = Number(row.client_id);
      const shipment = row.shipment_code;
      let waveStatus = String(row.status);

      // 2) Считаем прогресс по всем задачам этой волны
      const statsRes = await db.query(
        `
        SELECT
          COUNT(*)::int AS total,
          SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END)::int AS done,
          SUM(CASE WHEN status IN ('new', 'in_progress') THEN 1 ELSE 0 END)::int AS remaining
        FROM wms.picking_tasks
        WHERE client_id = $1
          AND shipment_code = $2
        `,
        [clientId, shipment]
      );

      const total = Number(statsRes.rows[0].total || 0);
      const done  = Number(statsRes.rows[0].done  || 0);
      const remaining = Number(statsRes.rows[0].remaining || 0);

      // 3) Авто-перевод active -> ready, когда всё собрано
      if (remaining === 0 && total > 0 && waveStatus === 'active') {
        await db.query(
          `
          UPDATE wms.pick_waves
          SET status = 'ready',
              ready_at = NOW()
          WHERE picker_id = $1
            AND client_id = $2
            AND shipment_code = $3
            AND status = 'active'
          `,
          [pickerId, clientId, shipment]
        );
        waveStatus = 'ready';
      }

      return res.json({
        has_wave: true,
        shipment_code: shipment,
        client_id: clientId,
        wave_status: waveStatus,
        total,
        done,
        remaining,
      });
    } catch (err) {
      console.error('wave/status error:', err);
      return res.status(500).json({
        error: 'wave/status error',
        details: err.message,
      });
    } finally {
      db.release();
    }
  }
);



// -------------------------
// POST /picking/wave/close
// Закрывает волну после печати/парковки
// -------------------------
app.post(
  '/picking/wave/close',
  authRequired,
  requireRole(['owner','admin','picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = Number(req.user.id);

      await db.query('BEGIN');

      const w = await db.query(
        `
        SELECT shipment_code, client_id, status
        FROM wms.pick_waves
        WHERE picker_id=$1 AND status IN ('ready','active')
        ORDER BY created_at DESC
        LIMIT 1
        FOR UPDATE
        `,
        [pickerId]
      );

      if (!w.rowCount) {
        await db.query('ROLLBACK');
        return res.status(404).json({ error:'NO_ACTIVE_WAVE' });
      }

      const shipment = w.rows[0].shipment_code;
      const clientId = Number(w.rows[0].client_id);
      const status = String(w.rows[0].status);

      const s = await db.query(
        `
        SELECT
          SUM(CASE WHEN status IN ('new','in_progress') THEN 1 ELSE 0 END)::int AS remaining
        FROM wms.picking_tasks
        WHERE shipment_code=$1 AND client_id=$2 AND picker_id=$3
        `,
        [shipment, clientId, pickerId]
      );

      const remaining = Number(s.rows[0].remaining || 0);

      if (remaining > 0) {
        await db.query('ROLLBACK');
        return res.status(409).json({ error:'WAVE_NOT_FINISHED', remaining });
      }

      // Если вдруг status ещё active — переведём в ready
      if (status === 'active') {
        await db.query(
          `
          UPDATE wms.pick_waves
          SET status='ready', ready_at=NOW()
          WHERE shipment_code=$1 AND client_id=$2 AND picker_id=$3
          `,
          [shipment, clientId, pickerId]
        );
      }

      // Финальное закрытие
      await db.query(
        `
        UPDATE wms.pick_waves
        SET status='closed', closed_at=NOW()
        WHERE shipment_code=$1 AND client_id=$2 AND picker_id=$3
        `,
        [shipment, clientId, pickerId]
      );

      await db.query('COMMIT');
      return res.json({ ok:true, shipment_code: shipment, client_id: clientId, status:'closed' });

    } catch (e) {
      try { await db.query('ROLLBACK'); } catch {}
      console.error('wave/close error', e);
      return res.status(500).json({ error:'wave/close error', details:e.message });
    } finally {
      db.release();
    }
  }
);

// -------------------------
// Взять волну сборки
// -------------------------
app.post(
  '/picking/wave/take',
  authRequired,
  requireRole(['owner', 'admin', 'picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = Number(req.user.id);

      // 1) Проверяем, нет ли уже активной / готовой волны у этого сборщика
      const currentWave = await db.query(
        `
        SELECT client_id, shipment_code, status
        FROM wms.pick_waves
        WHERE picker_id = $1
          AND status IN ('active', 'ready')
        ORDER BY created_at DESC
        LIMIT 1
        `,
        [pickerId]
      );

      if (currentWave.rowCount > 0) {
        const w = currentWave.rows[0];
        return res.json({
          has_wave: true,
          shipment_code: w.shipment_code,
          client_id: w.client_id,
          wave_status: w.status,
        });
      }

      // 2) Ищем свободную "open" волну с живыми заданиями
      await db.query('BEGIN');

      const openWave = await db.query(
        `
        SELECT
          pw.client_id,
          pw.shipment_code
        FROM wms.pick_waves pw
        WHERE pw.picker_id IS NULL
          AND COALESCE(pw.status, 'open') = 'open'
          AND EXISTS (
            SELECT 1
            FROM wms.picking_tasks t
            WHERE t.client_id = pw.client_id
              AND t.shipment_code = pw.shipment_code
              AND t.status IN ('new', 'in_progress')
          )
        ORDER BY pw.created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
        `
      );

      if (openWave.rowCount === 0) {
        await db.query('COMMIT');
        return res.json({ has_wave: false });
      }

      const ow = openWave.rows[0];

      // 3) Назначаем волну этому сборщику и переводим в active
      const assigned = await db.query(
        `
        UPDATE wms.pick_waves pw
        SET
          picker_id = $1,
          status    = 'active'
        WHERE pw.client_id     = $2
          AND pw.shipment_code = $3
          AND pw.picker_id IS NULL
          AND COALESCE(pw.status, 'open') = 'open'
        RETURNING client_id, shipment_code, status
        `,
        [pickerId, ow.client_id, ow.shipment_code]
      );

      await db.query('COMMIT');

      if (assigned.rowCount === 0) {
        // Кто-то успел забрать волну между SELECT и UPDATE
        return res.json({ has_wave: false });
      }

      const w = assigned.rows[0];

      return res.json({
        has_wave: true,
        shipment_code: w.shipment_code,
        client_id: w.client_id,
        wave_status: w.status,
      });
    } catch (err) {
      try { await db.query('ROLLBACK'); } catch {}
      console.error('picking/wave/take error:', err);
      return res.status(500).json({
        error: 'INTERNAL_ERROR',
        details: err.message,
      });
    } finally {
      db.release();
    }
  }
);


// -------------------------
// Формирование FBS-волны из заказов WB:
// - создаём поставку в WB
// - добавляем заказы в поставку
// - подтягиваем стикеры (SVG base64) и wb_sticker_code
// - помечаем заказы в mp_wb_orders
// - создаём задачи на сборку wms.picking_tasks (без дублей по (client_id, wb_order_id, shipment_code))
// - создаём/обновляем волну wms.pick_waves (status = 'open')
// - создаём/обновляем запись в wms.shipments (status = 'new', marketplace = 'wb') для табло/упаковки
//
// ВАЖНОЕ ПРАВИЛО:
// - В WMS (picking_tasks / pick_waves / shipments.external_id) храним shipment_code строго в виде: WB-GI-<digits>
// - В mp_wb_orders.wb_supply_id храним "сырой" supplyId от WB (обычно голые цифры)
// -------------------------
app.post(
  '/picking/generate-from-wb',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    const db = await pool.connect();

    async function safeText(resp) { try { return await resp.text(); } catch { return null; } }
    async function safeJson(resp) { try { return await resp.json(); } catch { return null; } }

    function extractStickerCodeFromBase64Svg(b64) {
      if (!b64 || typeof b64 !== 'string') return null;
      try {
        const svg = Buffer.from(b64, 'base64').toString('utf8');
        const nums = [];
        const re = /<text[^>]*>\s*([0-9]{4,})\s*<\/text>/gim;
        let m;
        while ((m = re.exec(svg)) !== null) nums.push(m[1]);
        if (nums.length >= 2) return `${nums[0]} ${nums[1]}`;
        if (nums.length === 1) return nums[0];
        return null;
      } catch {
        return null;
      }
    }

    // Нормализуем shipment_code для WMS: WB-GI-<digits>
    function toWmsShipmentCode(supplyIdRaw) {
      const s = String(supplyIdRaw || '').trim();
      if (!s) return null;
      if (/^WB-GI-/i.test(s)) return s;
      if (/^\d+$/.test(s)) return `WB-GI-${s}`;
      return s; // на случай если WB вдруг вернёт уже строку
    }

    async function wbAddOrdersToSupply({ apiToken, supplyIdRaw, orderIds }) {
      const supplyIdEnc = encodeURIComponent(String(supplyIdRaw));

      const urls = [
        `https://marketplace-api.wildberries.ru/api/marketplace/v3/supplies/${supplyIdEnc}/orders`,
        `https://marketplace-api.wildberries.ru/api/v3/supplies/${supplyIdEnc}/orders`,
      ];

      const authVariants = [apiToken, `Bearer ${apiToken}`];
      let lastErr = null;

      for (const url of urls) {
        for (const authHeader of authVariants) {
          const resp = await fetch(url, {
            method: 'PATCH',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': authHeader,
            },
            body: JSON.stringify({ orders: orderIds }),
          });

          if (resp.ok) {
            return {
              ok: true,
              url,
              authUsed: authHeader.startsWith('Bearer ') ? 'bearer' : 'raw',
            };
          }

          const bodyText = await safeText(resp);
          lastErr = new Error(
            `WB add orders failed (${resp.status}) at ${url}: ${bodyText || 'no body'}`
          );

          // 404/401 — пробуем другой эндпоинт/формат авторизации
          if (![404, 401].includes(resp.status)) break;
        }
      }

      throw lastErr || new Error('WB add orders failed: unknown error');
    }

    async function wbTryFetchStickers({ apiToken, orderIds, type = 'svg', width = 58, height = 40 }) {
      const url =
        'https://marketplace-api.wildberries.ru/api/v3/orders/stickers' +
        `?type=${encodeURIComponent(type)}&width=${encodeURIComponent(width)}&height=${encodeURIComponent(height)}`;

      const authVariants = [apiToken, `Bearer ${apiToken}`];
      let last = null;

      for (const authHeader of authVariants) {
        const resp = await fetch(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'Authorization': authHeader,
          },
          body: JSON.stringify({ orders: orderIds }),
        });

        if (!resp.ok) {
          const t = await safeText(resp);
          last = {
            ok: false,
            status: resp.status,
            body: t,
            authUsed: authHeader.startsWith('Bearer ') ? 'bearer' : 'raw',
          };
          if (resp.status === 401) continue;
          return last;
        }

        const j = await safeJson(resp);
        return {
          ok: true,
          data: j,
          authUsed: authHeader.startsWith('Bearer ') ? 'bearer' : 'raw',
        };
      }

      return last || { ok: false, status: 'n/a', body: 'unknown error' };
    }

    try {
      const { mp_account_id, limit } = req.body || {};
      const mpAccountId = Number(mp_account_id);

      if (!Number.isInteger(mpAccountId) || mpAccountId <= 0) {
        return res.status(400).json({ error: 'mp_account_id is required and must be > 0' });
      }

      const limitRaw = Number(limit);
      const limitOrders = Number.isInteger(limitRaw) && limitRaw > 0
        ? Math.min(limitRaw, 500)
        : 50;

      // 1) WB-аккаунт: токен + привязка к клиенту WMS
      const accRes = await db.query(
        `
          SELECT api_token, wms_client_id, label
          FROM public.mp_accounts
          WHERE id = $1
            AND marketplace = 'wb'
            AND is_active = true
          LIMIT 1
        `,
        [mpAccountId]
      );

      if (!accRes.rowCount) {
        return res.status(400).json({ error: `WB mp_account_id=${mpAccountId} not found or inactive` });
      }

      const { api_token: apiToken, wms_client_id: wmsClientIdRaw, label } = accRes.rows[0];
      if (!apiToken) {
        return res.status(400).json({ error: `WB api_token missing for mp_account_id=${mpAccountId}` });
      }

      const wmsClientId = Number(wmsClientIdRaw);
      if (!Number.isInteger(wmsClientId) || wmsClientId <= 0) {
        return res.status(400).json({
          error:
            `Для WB аккаунта mp_account_id=${mpAccountId} (${label || 'без имени'}) не задан wms_client_id. ` +
            `Выбери клиента WMS в настройках аккаунта и сохрани.`,
        });
      }

      // 2) Заказы без поставки
      const ordersRes = await db.query(
        `
          SELECT
            id,
            client_mp_account_id,
            wb_order_id,
            nm_id,
            chrt_id,
            barcode,
            warehouse_id,
            warehouse_name,
            status,
            wb_supply_id,
            created_at
          FROM public.mp_wb_orders
          WHERE client_mp_account_id = $1
            AND wb_supply_id IS NULL
            AND COALESCE(status, '') NOT IN ('confirm', 'complete', 'cancel')
          ORDER BY created_at ASC
          LIMIT $2
        `,
        [mpAccountId, limitOrders]
      );

      if (!ordersRes.rowCount) {
        return res.json({
          status: 'ok',
          mp_account_id: mpAccountId,
          createdSupplies: 0,
          attachedOrders: 0,
          message: 'Нет заказов без поставки для формирования волны',
        });
      }

      const orders = ordersRes.rows;

      // 3) Группировка по складу WB
      const groups = new Map();
      for (const row of orders) {
        const key = String(row.warehouse_id || '') + '|' + (row.warehouse_name || '');
        if (!groups.has(key)) {
          groups.set(key, {
            warehouse_id: row.warehouse_id,
            warehouse_name: row.warehouse_name,
            orders: [],
          });
        }
        groups.get(key).orders.push(row);
      }

      const suppliesResult = [];
      let attachedOrdersTotal = 0;

      for (const [, group] of groups.entries()) {
        const whId = group.warehouse_id;
        const whName = group.warehouse_name || 'UNKNOWN';

        const orderIds = group.orders
          .map(o => Number(o.wb_order_id))
          .filter(x => Number.isInteger(x) && x > 0);

        if (!orderIds.length) continue;

        // 4.1 создаём поставку в WB
        const supplyName = `WMS-${mpAccountId}-${whName}-${Date.now()}`;
        const createSupplyResp = await fetch(
          'https://marketplace-api.wildberries.ru/api/v3/supplies',
          {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
              'Authorization': apiToken,
            },
            body: JSON.stringify({ name: supplyName }),
          }
        );

        if (!createSupplyResp.ok) {
          const bodyText = await safeText(createSupplyResp);
          throw new Error(`WB create supply failed (${createSupplyResp.status}): ${bodyText || 'no body'}`);
        }

        const supplyBody = (await safeJson(createSupplyResp)) || {};
        const supplyIdRaw = (supplyBody.id || supplyBody.supplyId || supplyBody.supply_id);
        if (!supplyIdRaw) {
          throw new Error('WB create supply: supplyId is missing in response');
        }

        const wbSupplyId = String(supplyIdRaw).trim();          // то, что WB вернул (обычно цифры)
        const shipmentCode = toWmsShipmentCode(wbSupplyId);      // то, что кладём в WMS (WB-GI-xxxxx)
        if (!shipmentCode) throw new Error('WMS shipmentCode normalize failed');

        // 4.2 добавляем заказы в поставку (WB ждёт "сырой" supplyId)
        const addRes = await wbAddOrdersToSupply({ apiToken, supplyIdRaw: wbSupplyId, orderIds });

        // 4.3 стикеры
        const stickersTry = await wbTryFetchStickers({
          apiToken,
          orderIds,
          type: 'svg',
          width: 58,
          height: 40,
        });

        await db.query('BEGIN');
        try {
          // 5.1 отмечаем поставку в mp_wb_orders (ХРАНИМ wb_supply_id как WB вернул!)
          await db.query(
            `
              UPDATE public.mp_wb_orders
                 SET wb_supply_id = $1,
                     status       = 'confirm'
               WHERE client_mp_account_id = $2
                 AND wb_order_id = ANY($3::bigint[])
            `,
            [wbSupplyId, mpAccountId, orderIds]
          );

          // 5.2 сохраняем стикеры
          let stickersSaved = 0;
          let stickersCodesSaved = 0;

          if (stickersTry.ok && stickersTry.data && Array.isArray(stickersTry.data.stickers)) {
            for (const st of stickersTry.data.stickers) {
              if (!st || !st.orderId || !st.file) continue;

              const code = extractStickerCodeFromBase64Svg(st.file);

              await db.query(
                `
                  UPDATE public.mp_wb_orders
                     SET wb_sticker      = $1,
                         wb_sticker_code = $2
                   WHERE client_mp_account_id = $3
                     AND wb_order_id          = $4
                `,
                [st.file, code, mpAccountId, Number(st.orderId)]
              );

              stickersSaved++;
              if (code) stickersCodesSaved++;
            }
          }

          // 5.3 создаём задачи на сборку (shipment_code = WB-GI-...)
          let tasksInserted = 0;

          for (const o of group.orders) {
            const wbOrderId = Number(o.wb_order_id);
            if (!Number.isInteger(wbOrderId) || wbOrderId <= 0) continue;

            const barcodeStr = String(o.barcode || '').trim();
            if (!barcodeStr) continue;

            // sku_id по баркоду
            const skuId = await resolveSkuIdOrCreate(db, {
              client_id: wmsClientId,
              barcode: barcodeStr,
            });

            // подбираем МХ с остатком
            const locPickRes = await db.query(
              `
                SELECT l.location_code
                FROM wms.stock s
                JOIN wms.locations l ON l.id = s.location_id
                WHERE s.sku_id    = $1
                  AND l.client_id = $2
                  AND l.is_active = true
                  AND s.qty > 0
                ORDER BY s.qty DESC, l.location_code ASC
                LIMIT 1
              `,
              [skuId, wmsClientId]
            );
            const locCode = locPickRes.rowCount ? locPickRes.rows[0].location_code : null;

            // защита от дублей по (client_id, wb_order_id, shipment_code)
            const ins = await db.query(
              `
                INSERT INTO wms.picking_tasks
                  (client_id, barcode, sku_id, location_code, qty, status, priority,
                   wb_order_id, shipment_code, created_at, updated_at, created_by, updated_by)
                SELECT
                  $1, $2, $3, $4, 1, 'new', 3,
                  $5, $6, NOW(), NOW(), $7, $7
                WHERE NOT EXISTS (
                  SELECT 1
                  FROM wms.picking_tasks t
                  WHERE t.client_id     = $1
                    AND t.wb_order_id   = $5
                    AND TRIM(t.shipment_code) = TRIM($6)
                )
                RETURNING id
              `,
              [
                wmsClientId,      // $1
                barcodeStr,       // $2
                skuId,            // $3
                locCode,          // $4
                wbOrderId,        // $5
                shipmentCode,     // $6 (WB-GI-...)
                req.user.id,      // $7
              ]
            );

            if (ins.rowCount) tasksInserted++;
          }

          // 5.4 создаём / обновляем волну на сборку
          const waveUpd = await db.query(
            `
              UPDATE wms.pick_waves
                 SET client_id = $1
               WHERE TRIM(shipment_code) = TRIM($2)
            `,
            [wmsClientId, shipmentCode]
          );
          if (waveUpd.rowCount === 0) {
            await db.query(
              `
                INSERT INTO wms.pick_waves
                  (client_id, shipment_code, status, picker_id, buffer_location_code, created_at, ready_at)
                VALUES ($1, $2, 'open', NULL, NULL, NOW(), NULL)
              `,
              [wmsClientId, shipmentCode]
            );
          }

          // 5.5 создаём / обновляем запись в wms.shipments — ОСНОВА для табло
          // external_id = WB-GI-...
          const shipUpd = await db.query(
            `
              UPDATE wms.shipments
                 SET client_id   = $1,
                     marketplace = 'wb',
                     updated_at  = NOW()
               WHERE TRIM(external_id) = TRIM($2)
            `,
            [wmsClientId, shipmentCode]
          );
          if (shipUpd.rowCount === 0) {
            await db.query(
              `
                INSERT INTO wms.shipments
                  (external_id, client_id, marketplace, status, planned_ship_date, created_at, updated_at)
                VALUES ($1, $2, 'wb', 'new', NULL, NOW(), NOW())
              `,
              [shipmentCode, wmsClientId]
            );
          }

          await db.query('COMMIT');

          attachedOrdersTotal += orderIds.length;

          suppliesResult.push({
            wb_supply_id: wbSupplyId,   // как WB вернул
            shipment_code: shipmentCode, // как в WMS
            warehouse_id: whId,
            warehouse_name: whName,
            ordersCount: orderIds.length,
            addOrdersUrlUsed: addRes.url,
            addOrdersAuthUsed: addRes.authUsed,
            tasksInserted,
            stickersSaved,
            stickersCodesSaved,
            stickersAuthUsed: stickersTry.authUsed || null,
            stickersNote: stickersTry.ok
              ? (stickersSaved ? 'ok' : 'WB вернул 200, но stickers пустые')
              : `stickers not fetched (status=${stickersTry.status || 'n/a'})`,
          });
        } catch (e) {
          try { await db.query('ROLLBACK'); } catch {}
          throw e;
        }
      }

      // 6) Страховка: добиваем ВСЕ wb_supply_id этого аккаунта в wms.shipments,
      //    если по ним ещё нет строки (чтобы табло точно всё видело)
      // FIX: planned_ship_date у тебя DATE -> NULL::date
      // FIX2: external_id в shipments должен быть WB-GI-<wb_supply_id>
      await db.query(
        `
          INSERT INTO wms.shipments (
            external_id,
            client_id,
            marketplace,
            status,
            planned_ship_date,
            created_at,
            updated_at
          )
          SELECT DISTINCT
            ('WB-GI-' || TRIM(o.wb_supply_id)::text) AS external_id,
            $1                                       AS client_id,
            'wb'                                     AS marketplace,
            'new'                                    AS status,
            NULL::date                               AS planned_ship_date,
            NOW()                                    AS created_at,
            NOW()                                    AS updated_at
          FROM public.mp_wb_orders o
          WHERE
            o.client_mp_account_id = $2
            AND o.wb_supply_id IS NOT NULL
            AND NOT EXISTS (
              SELECT 1
              FROM wms.shipments s
              WHERE TRIM(s.external_id) = ('WB-GI-' || TRIM(o.wb_supply_id)::text)
            )
        `,
        [wmsClientId, mpAccountId]
      );

      return res.json({
        status: 'ok',
        mp_account_id: mpAccountId,
        createdSupplies: suppliesResult.length,
        attachedOrders: attachedOrdersTotal,
        supplies: suppliesResult,
        message: `Сформировано поставок: ${suppliesResult.length}, заказов: ${attachedOrdersTotal}`,
      });
    } catch (err) {
      try { await db.query('ROLLBACK'); } catch {}
      console.error('picking/generate-from-wb wave error:', err);
      return res.status(500).json({
        error: 'Ошибка формирования волны FBS',
        details: err.message,
      });
    } finally {
      db.release();
    }
  }
);



// -------------------------
// POST /picking/wave/close
// -------------------------
app.post(
  '/picking/wave/close',
  authRequired,
  requireRole(['owner', 'admin', 'picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = Number(req.user.id);

      const w = await db.query(
        `SELECT shipment_code FROM wms.pick_waves WHERE picker_id=$1 AND status='open' LIMIT 1`,
        [pickerId]
      );
      if (!w.rowCount) return res.status(409).json({ error: 'NO_OPEN_WAVE' });

      const shipment = w.rows[0].shipment_code;

      const remaining = await db.query(
        `
        SELECT COUNT(*)::int AS remaining
        FROM wms.picking_tasks
        WHERE shipment_code=$1 AND picker_id=$2
          AND status IN ('new','in_progress')
        `,
        [shipment, pickerId]
      );

      if ((remaining.rows[0].remaining || 0) > 0) {
        return res.status(409).json({ error: 'WAVE_NOT_FINISHED', shipment_code: shipment, remaining: remaining.rows[0].remaining });
      }

      await db.query(
        `
        UPDATE wms.pick_waves
        SET status='closed', closed_at=NOW()
        WHERE shipment_code=$1 AND picker_id=$2 AND status='open'
        `,
        [shipment, pickerId]
      );

      return res.json({ ok: true, shipment_code: shipment, status: 'closed' });

    } catch (e) {
      console.error('wave/close error', e);
      return res.status(500).json({ error: 'wave/close error', details: e.message });
    } finally {
      db.release();
    }
  }
);


// -------------------------
// GET /shipments/board
// Табло отгрузок (WB волны/поставки)
// FIX:
// - shipment создаётся сразу при волне
// - ID на табло = реальный wms.shipments.id
// - сборка = по picking_tasks
// - упаковка = по wms.shipments.total_packed_qty
// - ready_to_ship = когда total_packed_qty >= plan_qty
// -------------------------
app.get(
  '/shipments/board',
  authRequired,
  requireRole(['owner', 'admin', 'picker', 'packer']),
  async (req, res) => {
    console.log('*** HIT /shipments/board ***', req.originalUrl);

    const client = await pool.connect();
    try {
      const { client_id, status, marketplace, date_from, date_to } = req.query || {};

      const STATUS_MAP = {
        'Создано': ['new'],
        'Сборка': ['picking'],
        'Упаковка': ['packing'],
        'Готово': ['ready_to_ship'],
        'Готово к отгрузке': ['ready_to_ship'],
        'Отгрузка': ['shipping'],
        'В пути': ['in_transit'],
        'Завершено': ['done'],
        'Отменено': ['cancelled', 'cancelled_test'],

        'new': ['new'],
        'picking': ['picking'],
        'packing': ['packing'],
        'ready_to_ship': ['ready_to_ship'],
        'shipping': ['shipping'],
        'in_transit': ['in_transit'],
        'done': ['done'],
        'cancelled': ['cancelled'],
        'cancelled_test': ['cancelled_test'],
      };

      const statusList = (status && STATUS_MAP[status]) ? STATUS_MAP[status] : null;

      const params = [
        client_id ? Number(client_id) : null, // $1
        statusList ? statusList : null,       // $2
        marketplace || null,                  // $3
        date_from || null,                    // $4
        date_to || null,                      // $5
      ];

      const sql = `
        WITH pick_agg AS (
          SELECT
            shipment_code,
            COUNT(*)::int AS pick_lines,
            COALESCE(SUM(qty), 0)::int AS plan_qty,
            COALESCE(SUM(COALESCE(picked_qty, 0)), 0)::int AS picked_qty,
            COUNT(*) FILTER (WHERE status IS NOT NULL AND status <> 'new')::int AS started_lines
          FROM wms.picking_tasks
          GROUP BY shipment_code
        ),
        latest_shipments AS (
          SELECT DISTINCT ON (TRIM(s.external_id), s.client_id)
            s.id::bigint AS id,
            TRIM(s.external_id) AS external_id,
            s.client_id,
            s.status AS shipment_status,
            s.created_at,
            s.total_planned_qty,
            s.total_picked_qty,
            s.total_packed_qty,
            s.total_shipped_qty
          FROM wms.shipments s
          ORDER BY TRIM(s.external_id), s.client_id, s.id DESC
        ),
        base AS (
          SELECT
            ls.id AS id,
            w.shipment_code,
            w.shipment_code AS external_id,

            w.client_id,
            c.client_name,

            COALESCE(sw.marketplace, 'wb') AS marketplace,

            w.status AS wave_status,
            sw.status AS wb_status,
            ls.shipment_status,

            COALESCE(ls.created_at, w.created_at) AS created_at,

            COALESCE(p.pick_lines, 0) AS pick_lines,
            COALESCE(p.plan_qty, 0) AS plan_qty,
            COALESCE(p.picked_qty, 0) AS picked_qty,
            COALESCE(p.started_lines, 0) AS started_lines,

            COALESCE(ls.total_packed_qty, 0)::int AS total_packed_qty

          FROM wms.pick_waves w
          LEFT JOIN masterdata.clients c
            ON c.id = w.client_id
          LEFT JOIN wms.shipments_wb sw
            ON TRIM(sw.shipment_code) = TRIM(w.shipment_code)
          LEFT JOIN latest_shipments ls
            ON TRIM(ls.external_id) = TRIM(w.shipment_code)
           AND ls.client_id = w.client_id
          LEFT JOIN pick_agg p
            ON TRIM(p.shipment_code) = TRIM(w.shipment_code)

          WHERE 1=1
            AND ($1::int IS NULL OR w.client_id = $1)
            AND (
                 $3::text IS NULL
              OR COALESCE(sw.marketplace, 'wb') ILIKE '%' || $3 || '%'
            )
            AND ($4::date IS NULL OR w.created_at::date >= $4::date)
            AND ($5::date IS NULL OR w.created_at::date <= $5::date)
        )
        SELECT
          b.id,
          b.shipment_code,
          b.external_id,
          b.client_id,
          b.client_name,
          b.marketplace,

          CASE
            WHEN b.wb_status IN ('in_transit','done') THEN b.wb_status
            WHEN b.shipment_status IN ('shipping','in_transit','done') THEN b.shipment_status
            WHEN b.plan_qty > 0 AND b.total_packed_qty >= b.plan_qty THEN 'ready_to_ship'
            WHEN b.total_packed_qty > 0 AND b.total_packed_qty < b.plan_qty THEN 'packing'
            WHEN b.pick_lines > 0 AND b.plan_qty > 0 AND b.picked_qty = b.plan_qty THEN 'packing'
            WHEN b.pick_lines > 0 AND b.plan_qty > 0 AND (b.started_lines > 0 OR b.picked_qty > 0) THEN 'picking'
            ELSE 'new'
          END AS status,

          NULL::date AS planned_ship_date,
          b.created_at,

          b.plan_qty AS lines_count,

          CASE
            WHEN b.plan_qty = 0 THEN 0
            ELSE ROUND((b.picked_qty::numeric / b.plan_qty::numeric) * 100)::int
          END AS picking_progress,

          CASE
            WHEN b.plan_qty = 0 THEN 0
            ELSE LEAST(100, ROUND((b.total_packed_qty::numeric / b.plan_qty::numeric) * 100)::int)
          END AS packing_progress,

          CASE
            WHEN b.wb_status IN ('shipping','in_transit','done') THEN 100
            WHEN b.shipment_status IN ('shipping','in_transit','done') THEN 100
            ELSE 0
          END AS shipping_progress,

          -- Диагностика
          b.wave_status,
          b.wb_status,
          b.shipment_status,
          b.pick_lines,
          b.plan_qty,
          b.picked_qty,
          b.total_packed_qty

        FROM base b
        WHERE 1=1
          AND (
               $2::text[] IS NULL
            OR (
              CASE
                WHEN b.wb_status IN ('in_transit','done') THEN b.wb_status
                WHEN b.shipment_status IN ('shipping','in_transit','done') THEN b.shipment_status
                WHEN b.plan_qty > 0 AND b.total_packed_qty >= b.plan_qty THEN 'ready_to_ship'
                WHEN b.total_packed_qty > 0 AND b.total_packed_qty < b.plan_qty THEN 'packing'
                WHEN b.pick_lines > 0 AND b.plan_qty > 0 AND b.picked_qty = b.plan_qty THEN 'packing'
                WHEN b.pick_lines > 0 AND b.plan_qty > 0 AND (b.started_lines > 0 OR b.picked_qty > 0) THEN 'picking'
                ELSE 'new'
              END
            ) = ANY($2)
          )
        ORDER BY b.created_at DESC
        LIMIT 200;
      `;

      const result = await client.query(sql, params);

      res.set('Cache-Control', 'no-store');
      return res.json({ shipments: result.rows });

    } catch (e) {
      console.error('shipments/board error:', e);
      return res.status(500).json({ error: 'Ошибка получения табло отгрузок' });
    } finally {
      client.release();
    }
  }
);


// -------------------------
// GET /shipments/details
// Детализация отгрузки (состав, прогресс по строкам)
// query: shipment_code=WB-GI-...  (рекомендовано)
//    or shipment_id=123 (если есть реальная запись wms.shipments)
// -------------------------
app.get(
  '/shipments/details',
  authRequired,
  requireRole(['owner', 'admin', 'picker', 'packer']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const { shipment_code, shipment_id } = req.query || {};

      let shipCode = shipment_code ? String(shipment_code).trim() : null;
      let shipIdNum = shipment_id != null ? Number(shipment_id) : null;

      if (!shipCode && (!Number.isInteger(shipIdNum) || shipIdNum <= 0)) {
        return res.status(400).json({ error: 'Передай shipment_code или shipment_id' });
      }

      // 1) Если дали shipment_id — достаём shipment_code из wms.shipments
      let shipmentRow = null;
      if (!shipCode && Number.isInteger(shipIdNum) && shipIdNum > 0) {
        const sr = await db.query(
          `
          SELECT id, client_id, external_id, status, packing_location_code, created_at
          FROM wms.shipments
          WHERE id = $1
          LIMIT 1
          `,
          [shipIdNum]
        );
        if (sr.rowCount === 0) {
          return res.status(404).json({ error: 'Отгрузка (shipment_id) не найдена' });
        }
        shipmentRow = sr.rows[0];
        shipCode = String(shipmentRow.external_id).trim();
      }

      // 2) Находим волну (источник истины для client_id)
      const waveRes = await db.query(
        `
        SELECT shipment_code, client_id, status, created_at
        FROM wms.pick_waves
        WHERE shipment_code = $1
        LIMIT 1
        `,
        [shipCode]
      );
      if (waveRes.rowCount === 0) {
        return res.status(404).json({ error: 'Волна (pick_wave) не найдена по shipment_code' });
      }
      const wave = waveRes.rows[0];
      const clientId = Number(wave.client_id);

      // 3) Подтягиваем реальную wms.shipments (если уже создана)
      if (!shipmentRow) {
        const sr2 = await db.query(
          `
          SELECT id, client_id, external_id, status, packing_location_code, created_at
          FROM wms.shipments
          WHERE external_id = $1 AND client_id = $2
          LIMIT 1
          `,
          [shipCode, clientId]
        );
        shipmentRow = sr2.rowCount ? sr2.rows[0] : null;
      }

      const shipmentId = shipmentRow ? Number(shipmentRow.id) : null;

      // 4) План/факт сборки по строкам (picking_tasks)
      // plan = SUM(qty)
      // picked = SUM(COALESCE(picked_qty, CASE WHEN status='done' THEN qty ELSE 0 END))
      const pickLinesRes = await db.query(
        `
        SELECT
          pt.barcode,
          MAX(pt.location_code) AS location_code,
          COALESCE(SUM(pt.qty),0)::int AS qty_plan,
          COALESCE(SUM(COALESCE(pt.picked_qty, CASE WHEN pt.status='done' THEN pt.qty ELSE 0 END)),0)::int AS qty_picked
        FROM wms.picking_tasks pt
        WHERE pt.client_id = $1
          AND pt.shipment_code = $2
          AND pt.status IN ('new','in_progress','done')
        GROUP BY pt.barcode
        ORDER BY pt.barcode
        `,
        [clientId, shipCode]
      );

      // 5) Факт упаковки по строкам (movements packing_item) — только если есть wms.shipments.id
      let packedMap = new Map();
      if (shipmentId) {
        const packedRes = await db.query(
          `
          SELECT
            m.barcode,
            COALESCE(SUM(m.qty),0)::int AS qty_packed
          FROM wms.movements m
          WHERE m.client_id = $1
            AND m.movement_type = 'packing_item'
            AND m.ref_type = 'shipment'
            AND m.ref_id = $2
          GROUP BY m.barcode
          `,
          [clientId, shipmentId]
        );

        packedMap = new Map(
          packedRes.rows.map(r => [String(r.barcode), Number(r.qty_packed || 0)])
        );
      }

      // 6) Метаданные (название, картинка, стикер) — используем твою функцию
      // ВАЖНО: эта функция у тебя уже есть и используется в packing/scan-item
      const metaLines = await loadShipmentLinesForPackingOrShipping(pool, shipCode);

      const metaMap = new Map(
        (metaLines || []).map(l => [String(l.barcode), l])
      );

      // 7) Собираем итоговые строки
      const lines = (pickLinesRes.rows || []).map(r => {
        const barcode = String(r.barcode);
        const meta = metaMap.get(barcode) || {};

        const qty_plan = Number(r.qty_plan || 0);
        const qty_picked = Number(r.qty_picked || 0);
        const qty_packed = Number(packedMap.get(barcode) || 0);

        return {
          barcode,
          item_name: meta.item_name || null,
          location_code: meta.location_code || r.location_code || null,
          qty_plan,
          qty_picked,
          qty_packed,
          wb_sticker_code: meta.wb_sticker_code || null,
          wb_sticker: meta.wb_sticker || null,
          preview_url: meta.preview_url || null,
        };
      });

      // 8) Заголовок отгрузки для UI
      const header = {
        shipment_code: shipCode,
        client_id: clientId,
        wave_status: wave.status || null,
        wave_created_at: wave.created_at || null,
        shipment_id: shipmentId,
        shipment_status: shipmentRow ? (shipmentRow.status || null) : null,
        packing_location_code: shipmentRow ? (shipmentRow.packing_location_code || null) : null,
        created_at: shipmentRow ? (shipmentRow.created_at || null) : (wave.created_at || null),
      };

      res.set('Cache-Control', 'no-store');
      return res.json({ shipment: header, lines });

    } catch (e) {
      console.error('shipments/details error:', e);
      return res.status(500).json({ error: 'Ошибка получения детализации отгрузки', detail: e.message });
    } finally {
      db.release();
    }
  }
);

// -------------------------
// GET /shipments/:id — шапка + строки отгрузки
// FIXED:
// - per-line packed allocation
// - stickers строго по wb_order_id
// - preview_url подтягивается и для деталки
// - QR поставки остаётся в shipment.wb_supply_qr_base64
//
// Поддерживает:
// 1) /shipments/<shipment.id>
// 2) /shipments/<shipment.external_id>
// 3) /shipments/<shipment_code из wms.picking_tasks> даже если записи в wms.shipments нет
// -------------------------
app.get(
  '/shipments/:id',
  authRequired,
  requireRole(['owner', 'admin', 'picker', 'packer']),
  async (req, res) => {
    const client = await pool.connect();
    try {
      const keyRaw = String(req.params.id || '').trim();
      if (!keyRaw) {
        return res.status(400).json({ error: 'Не передан идентификатор отгрузки' });
      }

      const asInt = Number(keyRaw);
      const isInt = Number.isInteger(asInt) && asInt > 0;

      const headerSqlById = `
        SELECT
          s.id,
          s.external_id,
          s.client_id,
          COALESCE(s.marketplace, 'wb') AS marketplace,
          COALESCE(s.status, 'new') AS status,
          s.planned_ship_date,
          s.created_at,
          s.packing_started_at,
          s.packing_finished_at,
          s.packing_location_code,
          s.shipped_at,
          s.total_planned_qty,
          s.total_picked_qty,
          s.total_packed_qty,
          s.total_shipped_qty,
          s.wb_supply_qr_base64,
          s.packer_id,
          c.client_name
        FROM wms.shipments s
        LEFT JOIN masterdata.clients c
          ON c.id = s.client_id
        WHERE s.id = $1
        LIMIT 1
      `;

      const headerSqlByExternal = `
        SELECT
          s.id,
          s.external_id,
          s.client_id,
          COALESCE(s.marketplace, 'wb') AS marketplace,
          COALESCE(s.status, 'new') AS status,
          s.planned_ship_date,
          s.created_at,
          s.packing_started_at,
          s.packing_finished_at,
          s.packing_location_code,
          s.shipped_at,
          s.total_planned_qty,
          s.total_picked_qty,
          s.total_packed_qty,
          s.total_shipped_qty,
          s.wb_supply_qr_base64,
          s.packer_id,
          c.client_name
        FROM wms.shipments s
        LEFT JOIN masterdata.clients c
          ON c.id = s.client_id
        WHERE TRIM(s.external_id) = TRIM($1)
        ORDER BY s.id DESC
        LIMIT 1
      `;

      let shipment = null;

      if (isInt) {
        const r = await client.query(headerSqlById, [asInt]);
        if (r.rowCount > 0) shipment = r.rows[0];
      }

      if (!shipment) {
        const r = await client.query(headerSqlByExternal, [keyRaw]);
        if (r.rowCount > 0) shipment = r.rows[0];
      }

      let shipmentCode = null;
      let clientIdFromTasks = null;

      if (shipment) {
        shipmentCode = String(shipment.external_id || '').trim();
      } else {
        const taskHeadSql = `
          SELECT
            pt.shipment_code,
            pt.client_id,
            MIN(pt.created_at) AS created_at
          FROM wms.picking_tasks pt
          WHERE TRIM(pt.shipment_code) = TRIM($1)
          GROUP BY pt.shipment_code, pt.client_id
          LIMIT 1
        `;
        const t = await client.query(taskHeadSql, [keyRaw]);

        if (t.rowCount === 0) {
          return res.status(404).json({ error: 'Отгрузка не найдена' });
        }

        shipmentCode = String(t.rows[0].shipment_code || '').trim();
        clientIdFromTasks = Number(t.rows[0].client_id);

        const r2 = await client.query(headerSqlByExternal, [shipmentCode]);
        if (r2.rowCount > 0) {
          shipment = r2.rows[0];
        } else {
          const clientNameRes = await client.query(
            `SELECT client_name FROM masterdata.clients WHERE id = $1 LIMIT 1`,
            [clientIdFromTasks]
          );

          shipment = {
            id: -1,
            external_id: shipmentCode,
            client_id: clientIdFromTasks,
            marketplace: 'wb',
            status: 'new',
            planned_ship_date: null,
            created_at: t.rows[0].created_at,
            packing_started_at: null,
            packing_finished_at: null,
            packing_location_code: null,
            shipped_at: null,
            total_planned_qty: null,
            total_picked_qty: null,
            total_packed_qty: null,
            total_shipped_qty: null,
            wb_supply_qr_base64: null,
            packer_id: null,
            client_name: clientNameRes.rowCount ? clientNameRes.rows[0].client_name : null,
          };
        }
      }

      if (!shipmentCode) {
        return res.status(500).json({ error: 'Не удалось определить shipment_code' });
      }

      const effectiveClientId = Number(shipment.client_id || clientIdFromTasks || 0);
      if (!effectiveClientId) {
        return res.status(500).json({ error: 'Не удалось определить client_id' });
      }

      // 1) planned/picked из picking_tasks
      const pickAggSql = `
        SELECT
          COALESCE(SUM(pt.qty), 0)::int AS total_planned_qty,
          COALESCE(SUM(CASE WHEN pt.status = 'done' THEN pt.qty ELSE 0 END), 0)::int AS total_picked_qty
        FROM wms.picking_tasks pt
        WHERE pt.client_id = $1
          AND TRIM(pt.shipment_code) = TRIM($2)
          AND pt.status IN ('new','in_progress','done')
      `;
      const pickAggRes = await client.query(pickAggSql, [effectiveClientId, shipmentCode]);
      const calcPlanned = Number(pickAggRes.rows[0]?.total_planned_qty || 0);
      const calcPicked = Number(pickAggRes.rows[0]?.total_picked_qty || 0);

      // 2) packed из movements
      let calcPacked = 0;
      let packedByBarcode = new Map();

      if (Number(shipment.id) > 0) {
        const packedAggSql = `
          SELECT COALESCE(SUM(m.qty), 0)::int AS total_packed_qty
          FROM wms.movements m
          WHERE m.client_id = $1
            AND m.movement_type = 'packing_item'
            AND m.ref_type = 'shipment'
            AND m.ref_id = $2
        `;
        const packedAggRes = await client.query(packedAggSql, [effectiveClientId, shipment.id]);
        calcPacked = Number(packedAggRes.rows[0]?.total_packed_qty || 0);

        const packedByBarcodeSql = `
          SELECT m.barcode, COALESCE(SUM(m.qty), 0)::int AS packed_qty
          FROM wms.movements m
          WHERE m.client_id = $1
            AND m.movement_type = 'packing_item'
            AND m.ref_type = 'shipment'
            AND m.ref_id = $2
          GROUP BY m.barcode
        `;
        const packedByBarcodeRes = await client.query(packedByBarcodeSql, [effectiveClientId, shipment.id]);
        packedByBarcode = new Map(
          (packedByBarcodeRes.rows || []).map(r => [String(r.barcode), Number(r.packed_qty || 0)])
        );
      }

      // 3) строки picking_tasks
      const linesSql = `
        SELECT
          pt.id,
          pt.wb_order_id,
          pt.order_ref,
          pt.barcode,
          pt.sku_id,
          i.item_name,
          pt.location_code,
          pt.qty,
          pt.status,
          pt.picked_qty,
          pt.packed_qty,
          pt.packing_status,
          pt.created_at,
          pt.started_at,
          pt.finished_at
        FROM wms.picking_tasks pt
        LEFT JOIN masterdata.items i
          ON i.barcode = pt.barcode
         AND i.client_id = pt.client_id
        WHERE TRIM(pt.shipment_code) = TRIM($1)
        ORDER BY pt.id
      `;
      const linesRes = await client.query(linesSql, [shipmentCode]);
      const baseLines = linesRes.rows || [];

      // 4) мета по заказам: sticker строго по wb_order_id
      const orderIds = baseLines
        .map(r => Number(r.wb_order_id))
        .filter(x => Number.isInteger(x) && x > 0);

      let metaByOrderId = new Map();
      if (orderIds.length) {
        const metaSql = `
          SELECT
            o.wb_order_id,
            o.wb_sticker,
            o.wb_sticker_code
          FROM public.mp_wb_orders o
          WHERE o.wb_order_id = ANY($1::bigint[])
            AND TRIM(o.wb_supply_id) = TRIM($2)
        `;
        const metaRes = await client.query(metaSql, [orderIds, shipmentCode]);
        metaByOrderId = new Map(
          (metaRes.rows || []).map(r => [Number(r.wb_order_id), r])
        );
      }

      // 5) preview_url и другие медиа через helper
      let helperLines = [];
      try {
        if (typeof loadShipmentLinesForPackingOrShipping === 'function') {
          helperLines = await loadShipmentLinesForPackingOrShipping(pool, shipmentCode);
        }
      } catch (e) {
        console.warn('[GET /shipments/:id] helper media load failed:', e?.message || e);
      }

      const helperByOrderId = new Map();
      const helperByBarcode = new Map();

      for (const row of helperLines || []) {
        const wbOrderIdNum = Number(row.wb_order_id || row.order_id || 0);
        const barcodeKey = String(row.barcode || '').trim();

        if (Number.isInteger(wbOrderIdNum) && wbOrderIdNum > 0 && !helperByOrderId.has(wbOrderIdNum)) {
          helperByOrderId.set(wbOrderIdNum, row);
        }
        if (barcodeKey && !helperByBarcode.has(barcodeKey)) {
          helperByBarcode.set(barcodeKey, row);
        }
      }

      // 6) распределяем packed по строкам последовательно
      const remainPackedByBarcode = new Map(packedByBarcode);

      const lines = baseLines.map((row) => {
        const barcode = String(row.barcode || '').trim();
        const qtyPlan = Number(row.qty || 0);

        const pickedQty =
          row.picked_qty != null
            ? Number(row.picked_qty || 0)
            : (row.status === 'done' ? qtyPlan : 0);

        let packedFromMovLine = 0;
        if (qtyPlan > 0) {
          const remain = Number(remainPackedByBarcode.get(barcode) || 0);
          packedFromMovLine = Math.max(0, Math.min(qtyPlan, remain));
          remainPackedByBarcode.set(barcode, Math.max(0, remain - packedFromMovLine));
        }

        const packedFromTask = row.packed_qty != null ? Number(row.packed_qty || 0) : 0;
        const packedQty = Math.max(packedFromTask, packedFromMovLine);

        const metaOrder = metaByOrderId.get(Number(row.wb_order_id)) || {};
        const metaHelper =
          helperByOrderId.get(Number(row.wb_order_id)) ||
          helperByBarcode.get(barcode) ||
          {};

        const rawSticker = metaOrder.wb_sticker || metaHelper.wb_sticker || null;
        const safeSticker = rawSticker && String(rawSticker).trim() ? String(rawSticker).trim() : null;

        const previewUrl = metaHelper.preview_url || null;

        const packingStatus =
          row.packing_status ||
          (qtyPlan > 0 && packedQty >= qtyPlan ? 'done' : (packedQty > 0 ? 'in_progress' : 'new'));

        return {
          ...row,
          item_name: row.item_name || metaHelper.item_name || null,
          picked_qty: pickedQty,
          packed_qty: packedQty,
          packing_status: packingStatus,
          wb_sticker_code: metaOrder.wb_sticker_code || metaHelper.wb_sticker_code || null,
          wb_sticker: safeSticker,
          preview_url: previewUrl,
        };
      });

      // totals
      const total_planned_qty =
        shipment.total_planned_qty != null && Number(shipment.total_planned_qty) > 0
          ? Number(shipment.total_planned_qty)
          : calcPlanned;

      const total_picked_qty =
        shipment.total_picked_qty != null && Number(shipment.total_picked_qty) > 0
          ? Number(shipment.total_picked_qty)
          : calcPicked;

      const total_packed_qty =
        shipment.total_packed_qty != null && Number(shipment.total_packed_qty) > 0
          ? Number(shipment.total_packed_qty)
          : calcPacked;

      const shipmentOut = {
        ...shipment,
        external_id: shipmentCode,
        total_planned_qty,
        total_picked_qty,
        total_packed_qty,
      };

      return res.json({ shipment: shipmentOut, lines });
    } catch (err) {
      console.error('GET /shipments/:id error:', err?.message || err);
      console.error(err?.stack || err);
      return res.status(500).json({
        error: 'Ошибка при получении деталей отгрузки',
        details: err?.message || String(err),
      });
    } finally {
      client.release();
    }
  }
);


// -------------------------
// POST /packing/scan-item
// Скан ШК на упаковке: проверка плана, дублей и запись движения
// + обновление totals в wms.shipments (planned/picked/packed) в реальном времени
// -------------------------
app.post(
  '/packing/scan-item',
  authRequired,
  requireRole(['owner', 'admin', 'packer']),
  async (req, res) => {
    const db = await pool.connect();

    try {
      const userId = req.user.id;
      const { shipment_code, scan_code } = req.body || {};

      const shipCode = String(shipment_code || '').trim();
      const barcode = String(scan_code || '').trim();

      if (!shipCode) {
        return res.status(400).json({ error: 'Код отгрузки (shipment_code) не передан' });
      }
      if (!barcode) {
        return res.status(400).json({ error: 'Штрихкод (scan_code) не передан' });
      }

      await db.query('BEGIN');

      // 1) Находим и блокируем отгрузку по external_id
      const shipRes = await db.query(
        `
        SELECT id, client_id, external_id, status, packing_location_code, wb_supply_qr_base64
        FROM wms.shipments
        WHERE external_id = $1
        ORDER BY id DESC
        LIMIT 1
        FOR UPDATE
        `,
        [shipCode]
      );

      if (shipRes.rowCount === 0) {
        await db.query('ROLLBACK');
        return res.status(400).json({ error: 'Отгрузка с таким shipment_code не найдена' });
      }

      const shipment = shipRes.rows[0];

      // 2) План по этому ШК (из wms.picking_tasks)
      const planRes = await db.query(
        `
        SELECT COALESCE(SUM(pt.qty), 0)::int AS planned_qty
        FROM wms.picking_tasks pt
        WHERE pt.client_id     = $1
          AND pt.shipment_code = $2
          AND pt.barcode       = $3
          AND pt.status IN ('new','in_progress','done')
        `,
        [shipment.client_id, shipment.external_id, barcode]
      );

      const plannedQty = Number(planRes.rows[0]?.planned_qty || 0);
      if (plannedQty <= 0) {
        await db.query('ROLLBACK');
        return res.status(400).json({ error: 'Для этого ШК нет плана на упаковку' });
      }

      // 3) Уже упаковано (из wms.movements) — под блокировку той же транзакции
      const packedRes = await db.query(
        `
        SELECT COALESCE(SUM(m.qty), 0)::int AS packed_qty
        FROM wms.movements m
        WHERE m.client_id     = $1
          AND m.barcode       = $2
          AND m.movement_type = 'packing_item'
          AND m.ref_type      = 'shipment'
          AND m.ref_id        = $3
        `,
        [shipment.client_id, barcode, shipment.id]
      );

      const alreadyPacked = Number(packedRes.rows[0]?.packed_qty || 0);
      if (alreadyPacked >= plannedQty) {
        await db.query('ROLLBACK');
        return res.status(400).json({ error: 'Этот товар уже полностью упакован (повторный скан)' });
      }

      // 4) Пишем движение упаковки (1 шт)
      await db.query(
        `
        INSERT INTO wms.movements
          (created_at, user_id, client_id, barcode, qty,
           from_location, to_location,
           movement_type, ref_type, ref_id, comment, sku_id)
        VALUES
          (NOW(), $1, $2, $3, 1,
           NULL, NULL,
           'packing_item', 'shipment', $4, 'Упаковка заказа на стол', NULL)
        `,
        [userId, shipment.client_id, barcode, shipment.id]
      );

      const newPackedQty = alreadyPacked + 1;

      // 5) Пересчитываем totals по отгрузке (planned/picked из picking_tasks, packed из movements)
      const totalsRes = await db.query(
        `
        WITH p AS (
          SELECT
            COALESCE(SUM(qty),0)::int AS total_planned_qty,
            COALESCE(SUM(CASE WHEN status='done' THEN qty ELSE 0 END),0)::int AS total_picked_qty
          FROM wms.picking_tasks
          WHERE client_id = $1
            AND shipment_code = $2
        ),
        m AS (
          SELECT
            COALESCE(SUM(qty),0)::int AS total_packed_qty
          FROM wms.movements
          WHERE client_id = $1
            AND movement_type = 'packing_item'
            AND ref_type = 'shipment'
            AND ref_id = $3
        )
        SELECT
          p.total_planned_qty,
          p.total_picked_qty,
          m.total_packed_qty
        FROM p, m
        `,
        [shipment.client_id, shipment.external_id, shipment.id]
      );

      const totalPlan = Number(totalsRes.rows[0]?.total_planned_qty || 0);
      const totalPicked = Number(totalsRes.rows[0]?.total_picked_qty || 0);
      const totalPacked = Number(totalsRes.rows[0]?.total_packed_qty || 0);

      // 6) Пишем totals в shipments (это нужно для табло/прогресса)
      await db.query(
        `
        UPDATE wms.shipments
        SET
          total_planned_qty = $1,
          total_picked_qty  = $2,
          total_packed_qty  = $3,
          updated_at        = NOW()
        WHERE id = $4
        `,
        [totalPlan, totalPicked, totalPacked, shipment.id]
      );

      // 7) Метаданные по конкретному ШК (стикер/картинка)
      const metaLines = await loadShipmentLinesForPackingOrShipping(pool, shipment.external_id);
      const meta = metaLines.find((l) => String(l.barcode) === String(barcode)) || {};

      await db.query('COMMIT');

      return res.json({
        shipment: {
          id: shipment.id,
          client_id: shipment.client_id,
          external_id: shipment.external_id,
          status: shipment.status,
          packing_location_code: shipment.packing_location_code,
          wb_supply_qr_base64: shipment.wb_supply_qr_base64 || null,

          // <<< ВАЖНО: отдаём totals прямо отсюда (табло/фронт может их использовать)
          total_planned_qty: totalPlan,
          total_picked_qty: totalPicked,
          total_packed_qty: totalPacked,
          packing_pct: totalPlan > 0 ? Math.round((totalPacked * 100) / totalPlan) : 0,
        },
        line: {
          barcode,
          planned_qty: plannedQty,
          packed_qty: newPackedQty,
          item_name: meta.item_name || null,
          wb_sticker_code: meta.wb_sticker_code || null,
          wb_sticker: meta.wb_sticker || null,
          preview_url: meta.preview_url || null,
        },
      });
    } catch (err) {
      await db.query('ROLLBACK').catch(() => {});
      console.error('packing/scan-item error:', err);
      return res.status(500).json({ error: 'Ошибка при обработке скана на упаковке', detail: err.message });
    } finally {
      db.release();
    }
  }
);

// -------------------------
// Детали задачи на упаковку: состав отгрузки
// -------------------------
app.post(
  '/packing/task-details',
  authRequired,
  requireRole(['owner', 'admin', 'packer']),
  async (req, res) => {
    const { packing_task_id } = req.body || {};

    if (!packing_task_id) {
      return res.status(400).json({ error: 'Не передан packing_task_id' });
    }

    try {
      // 1. Читаем задачу на упаковку
      const taskRes = await pool.query(
        `
        SELECT *
        FROM wms.packing_tasks
        WHERE id = $1;
        `,
        [packing_task_id]
      );

      if (taskRes.rows.length === 0) {
        return res.status(404).json({ error: 'Задача на упаковку не найдена' });
      }

      const packingTask = taskRes.rows[0];

      // 2. Строки сборки по этой отгрузке:
      //    wms.picking_tasks + masterdata.items + mp_wb_orders + wb-картинка
      const linesRes = await pool.query(
        `
        SELECT
          pt.id,
          pt.client_id,
          pt.shipment_code,
          pt.wb_order_id,
          pt.order_ref,
          pt.barcode,
          pt.qty,
          pt.packed_qty,              -- 🔥 факт упаковки
          pt.location_code,
          pt.status AS picking_status,

          -- справочник товара WMS
          i.item_name,
          i.vendor_code,
          i.wb_vendor_code,

          -- WB карточка / картинка
          COALESCE(wi.preview_url, wi.preview_image_url) AS photo_url,

          -- WB стикер
          o.wb_sticker      AS sticker_svg_base64,
          o.wb_sticker_code

        FROM wms.picking_tasks pt
        LEFT JOIN masterdata.items i
          ON i.client_id = pt.client_id
         AND i.barcode   = pt.barcode

        -- заказы WB (стикеры)
        LEFT JOIN public.mp_wb_orders o
          ON o.wb_order_id = pt.wb_order_id

        -- привязка баркода к карточке WB
        LEFT JOIN public.mp_wb_items_barcodes bib
          ON bib.barcode = pt.barcode
        LEFT JOIN public.mp_wb_items wi
          ON wi.nm_id = bib.nm_id

        WHERE pt.client_id     = $1
          AND pt.shipment_code = $2
        ORDER BY pt.id;
        `,
        [packingTask.client_id, packingTask.shipment_code]
      );

      return res.json({
        packing_task: packingTask,
        lines: linesRes.rows,
      });
    } catch (err) {
      console.error('packing/task-details error:', err);
      return res
        .status(500)
        .json({ error: 'Ошибка при получении деталей задачи на упаковку' });
    }
  }
);

// -------------------------
// GET /packing/current
// Текущее задание на упаковку + состав отгрузки
// -------------------------
app.get(
  '/packing/current',
  authRequired,
  requireRole(['owner', 'admin', 'packer']),
  async (req, res) => {
    const db = await pool.connect();

    try {
      const userId = req.user.id;

      // 1) Берём активное задание на упаковку для текущего упаковщика
      const taskRes = await db.query(
        `
        SELECT *
        FROM wms.packing_tasks
        WHERE status IN ('in_progress', 'new')
          AND packer_id = $1
        ORDER BY
          CASE WHEN status = 'in_progress' THEN 0 ELSE 1 END,
          id
        LIMIT 1
        `,
        [userId]
      );

      if (taskRes.rowCount === 0) {
        // Нет активного задания
        return res.json({ shipment: null, lines: [] });
      }

      const task = taskRes.rows[0];

      // 2) Находим отгрузку по shipment_code (он же external_id) — БЕРЁМ ПОСЛЕДНЮЮ
const shipRes = await db.query(
  `
  SELECT *
  FROM wms.shipments
  WHERE TRIM(external_id) = TRIM($1)
  ORDER BY id DESC
  LIMIT 1
  `,
  [task.shipment_code]
);

if (shipRes.rowCount === 0) {
  return res.json({ shipment: null, lines: [] });
}

const shipment = shipRes.rows[0];

      // 3) План по ШК + фактически упаковано из wms.movements
      const linesRes = await db.query(
        `
        WITH plan AS (
          SELECT
            pt.barcode,
            SUM(pt.qty) AS qty
          FROM wms.picking_tasks pt
          WHERE pt.client_id     = $1
            AND pt.shipment_code = $2
            AND pt.status IN ('new','in_progress','done')
          GROUP BY pt.barcode
        ),
        packed AS (
          SELECT
            m.barcode,
            SUM(m.qty) AS packed_qty
          FROM wms.movements m
          WHERE m.client_id     = $1
            AND m.movement_type = 'packing_item'
            AND m.ref_type      = 'shipment'
            AND m.ref_id        = $3
          GROUP BY m.barcode
        )
        SELECT
          p.barcode,
          p.qty,
          COALESCE(pk.packed_qty, 0) AS packed_qty
        FROM plan p
        LEFT JOIN packed pk
          ON pk.barcode = p.barcode
        ORDER BY p.barcode
        `,
        [shipment.client_id, shipment.external_id, shipment.id]
      );

      // 4) Метаданные по строкам (наименование, фото, стикер WB)
      //    Берём через общий хелпер, который уже использует mp_wb_orders и wb_sticker
      const metaLines = await loadShipmentLinesForPackingOrShipping(
        pool,
        shipment.external_id
      );

      const metaMap = new Map(
        metaLines.map((m) => [String(m.barcode), m])
      );

      const lines = linesRes.rows.map((r) => {
        const meta = metaMap.get(String(r.barcode)) || {};
        return {
          barcode: r.barcode,
          qty: Number(r.qty || 0),
          packed_qty: Number(r.packed_qty || 0),
          item_name: meta.item_name || null,
          wb_sticker_code: meta.wb_sticker_code || null,
          wb_sticker: meta.wb_sticker || null,
          preview_url: meta.preview_url || null,
        };
      });

      return res.json({
        task_id: task.id,
        shipment: {
          id: shipment.id,
          client_id: shipment.client_id,
          external_id: shipment.external_id,
          status: shipment.status,
          packing_location_code: shipment.packing_location_code,
          wb_supply_qr_base64: shipment.wb_supply_qr_base64 || null,
        },
        lines,
      });
    } catch (err) {
      console.error('packing/current error:', err);
      return res
        .status(500)
        .json({ error: 'Ошибка при загрузке текущего задания на упаковку' });
    } finally {
      db.release();
    }
  }
);

// -------------------------
// POST /packing/next
// Взять следующее задание на упаковку (назначаем packer_id, делаем in_progress)
// + возвращаем состав отгрузки с packed_qty из movements (чтобы было 3/3 и подсветка)
// -------------------------
app.post(
  '/packing/next',
  authRequired,
  requireRole(['owner', 'admin', 'packer']),
  async (req, res) => {
    const db = await pool.connect();

    try {
      const userId = req.user.id;

      await db.query('BEGIN');

      // 0) Если у упаковщика уже есть активное задание — возвращаем его (чтобы не плодить)
      const existingTaskRes = await db.query(
        `
        SELECT *
        FROM wms.packing_tasks
        WHERE status IN ('in_progress','new')
          AND packer_id = $1
        ORDER BY
          CASE WHEN status = 'in_progress' THEN 0 ELSE 1 END,
          id
        LIMIT 1
        `,
        [userId]
      );

      let task = null;

      if (existingTaskRes.rowCount > 0) {
        task = existingTaskRes.rows[0];

        // если оно было new — переводим в in_progress
        if (task.status === 'new') {
          const upd = await db.query(
            `
            UPDATE wms.packing_tasks
            SET status = 'in_progress',
                updated_at = NOW(),
                updated_by = $1
            WHERE id = $2
            RETURNING *
            `,
            [userId, task.id]
          );
          task = upd.rows[0];
        }
      } else {
        // 1) Берём первое свободное задание (packer_id IS NULL) и назначаем
        const takeRes = await db.query(
          `
          UPDATE wms.packing_tasks
          SET packer_id = $1,
              status = 'in_progress',
              updated_at = NOW(),
              updated_by = $1
          WHERE id = (
            SELECT id
            FROM wms.packing_tasks
            WHERE status = 'new'
              AND packer_id IS NULL
            ORDER BY id
            LIMIT 1
            FOR UPDATE SKIP LOCKED
          )
          RETURNING *
          `,
          [userId]
        );

        if (takeRes.rowCount === 0) {
          await db.query('ROLLBACK');
          return res.json({ shipment: null, lines: [] });
        }

        task = takeRes.rows[0];
      }

      // 2) Находим отгрузку по shipment_code (он же external_id) — БЕРЁМ ПОСЛЕДНЮЮ
      const shipRes = await db.query(
        `
        SELECT *
        FROM wms.shipments
        WHERE TRIM(external_id) = TRIM($1)
        ORDER BY id DESC
        LIMIT 1
        `,
        [task.shipment_code]
      );

      if (shipRes.rowCount === 0) {
        await db.query('ROLLBACK');
        return res.status(400).json({ error: 'Отгрузка для packing_task не найдена в wms.shipments' });
      }

      const shipment = shipRes.rows[0];

      // 3) План по ШК + фактически упаковано из movements
      const linesRes = await db.query(
        `
        WITH plan AS (
          SELECT
            pt.barcode,
            SUM(pt.qty)::int AS qty
          FROM wms.picking_tasks pt
          WHERE pt.client_id     = $1
            AND TRIM(pt.shipment_code) = TRIM($2)
            AND pt.status IN ('new','in_progress','done')
          GROUP BY pt.barcode
        ),
        packed AS (
          SELECT
            m.barcode,
            SUM(m.qty)::int AS packed_qty
          FROM wms.movements m
          WHERE m.client_id     = $1
            AND m.movement_type = 'packing_item'
            AND m.ref_type      = 'shipment'
            AND m.ref_id        = $3
          GROUP BY m.barcode
        )
        SELECT
          p.barcode,
          p.qty,
          COALESCE(pk.packed_qty, 0)::int AS packed_qty
        FROM plan p
        LEFT JOIN packed pk ON pk.barcode = p.barcode
        ORDER BY p.barcode
        `,
        [shipment.client_id, shipment.external_id, shipment.id]
      );

      // 4) Метаданные по строкам (наименование, фото, стикер WB)
      const metaLines = await loadShipmentLinesForPackingOrShipping(pool, shipment.external_id);
      const metaMap = new Map(metaLines.map(m => [String(m.barcode), m]));

      const lines = (linesRes.rows || []).map((r) => {
        const meta = metaMap.get(String(r.barcode)) || {};
        return {
          barcode: r.barcode,
          qty: Number(r.qty || 0),
          packed_qty: Number(r.packed_qty || 0),
          item_name: meta.item_name || null,
          wb_sticker_code: meta.wb_sticker_code || null,
          wb_sticker: meta.wb_sticker || null,
          preview_url: meta.preview_url || null,
        };
      });

      await db.query('COMMIT');

      return res.json({
        task_id: task.id,
        shipment: {
          id: shipment.id,
          client_id: shipment.client_id,
          external_id: shipment.external_id,
          status: shipment.status,
          packing_location_code: shipment.packing_location_code,
          wb_supply_qr_base64: shipment.wb_supply_qr_base64 || null,
        },
        lines,
      });

    } catch (err) {
      await db.query('ROLLBACK').catch(() => {});
      console.error('packing/next error:', err);
      return res.status(500).json({ error: 'Ошибка при получении задания на упаковку', detail: err.message });
    } finally {
      db.release();
    }
  }
);


// -------------------------
// POST /packing/confirm
// Проверка, что всё упаковано, + закрытие задания
// + обновление totals в wms.shipments (planned/picked/packed)
// -------------------------
app.post(
  '/packing/confirm',
  authRequired,
  requireRole(['owner', 'admin', 'packer']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const userId = req.user.id;
      const { shipment_id, location_code, boxes_count, comment } = req.body || {};

      const shipIdNum = Number(shipment_id);
      if (!Number.isInteger(shipIdNum) || shipIdNum <= 0) {
        return res.status(400).json({ error: 'Некорректный shipment_id при подтверждении упаковки' });
      }

      const boxesCountNumRaw = Number(boxes_count);
      const boxesCountNum =
        Number.isInteger(boxesCountNumRaw) && boxesCountNumRaw > 0 ? boxesCountNumRaw : 1;

      await db.query('BEGIN');

      // 1) Блокируем отгрузку
      const shipRes = await db.query(
        `
        SELECT id, client_id, external_id, status, packing_location_code, packing_started_at
        FROM wms.shipments
        WHERE id = $1
        FOR UPDATE
        `,
        [shipIdNum]
      );

      if (shipRes.rowCount === 0) {
        await db.query('ROLLBACK');
        return res.status(404).json({ error: 'Отгрузка не найдена' });
      }

      const shipment = shipRes.rows[0];

      // 2) planned/picked из picking_tasks
      const pickProgRes = await db.query(
        `
        SELECT
          COALESCE(SUM(qty),0)::int AS total_planned_qty,
          COALESCE(SUM(CASE WHEN status='done' THEN qty ELSE 0 END),0)::int AS total_picked_qty
        FROM wms.picking_tasks
        WHERE client_id = $1
          AND shipment_code = $2
        `,
        [shipment.client_id, shipment.external_id]
      );

      const totalPlan = Number(pickProgRes.rows[0].total_planned_qty || 0);
      const totalPicked = Number(pickProgRes.rows[0].total_picked_qty || 0);

      if (totalPlan <= 0) {
        await db.query('ROLLBACK');
        return res.status(400).json({
          error: 'Нет плана по отгрузке (picking_tasks пустые) — упаковку подтвердить нельзя',
        });
      }

      // 3) packed из movements packing_item
      const packedRes = await db.query(
        `
        SELECT COALESCE(SUM(qty),0)::int AS total_packed_qty
        FROM wms.movements
        WHERE client_id     = $1
          AND movement_type = 'packing_item'
          AND ref_type      = 'shipment'
          AND ref_id        = $2
        `,
        [shipment.client_id, shipment.id]
      );

      const totalPacked = Number(packedRes.rows[0].total_packed_qty || 0);

      // 4) Проверка "всё упаковано"
      if (totalPacked < totalPlan) {
        await db.query('ROLLBACK');
        return res.status(400).json({
          error: `Не все позиции упакованы. План=${totalPlan}, Упаковано=${totalPacked}`,
          total_plan: totalPlan,
          total_packed: totalPacked,
        });
      }

      // 5) Закрываем packing_tasks
      await db.query(
        `
        UPDATE wms.packing_tasks
        SET status       = 'done',
            boxes_count  = COALESCE(boxes_count, $1),
            comment      = COALESCE($2, comment),
            packer_id    = $3,
            updated_at   = NOW(),
            updated_by   = $3
        WHERE shipment_code = $4
          AND status IN ('new','in_progress')
        `,
        [boxesCountNum, comment || null, userId, shipment.external_id]
      );

      // 6) Обновляем shipment: totals + статус ready_to_ship + тайминги
      await db.query(
        `
        UPDATE wms.shipments
        SET
          total_planned_qty      = $1,
          total_picked_qty       = $2,
          total_packed_qty       = $3,
          status                 = 'ready_to_ship',
          packing_location_code  = COALESCE($4, packing_location_code),
          packing_started_at     = COALESCE(packing_started_at, NOW()),
          packing_finished_at    = NOW(),
          packer_id              = $5,
          updated_at             = NOW()
        WHERE id = $6
        `,
        [
          totalPlan,
          totalPicked,
          totalPacked,
          location_code || shipment.packing_location_code,
          userId,
          shipment.id,
        ]
      );

      await db.query('COMMIT');

      return res.json({
        ok: true,
        shipment_id: shipment.id,
        status: 'ready_to_ship',
        total_plan: totalPlan,
        total_picked: totalPicked,
        total_packed: totalPacked,
        packing_pct: totalPlan > 0 ? Math.round((totalPacked * 100) / totalPlan) : 0,
      });
    } catch (err) {
      await db.query('ROLLBACK').catch(() => {});
      console.error('packing/confirm error:', err);
      return res.status(500).json({ error: 'Ошибка при подтверждении упаковки', detail: err.message });
    } finally {
      db.release();
    }
  }
);


// -------------------------
// Получить текущее задание на отгрузку
// SAFE VERSION: без join к masterdata.locations
// -------------------------
app.get(
  '/shipping/current',
  authRequired,
  requireRole(['owner', 'admin', 'packer']),
  async (req, res) => {
    const client = await pool.connect();
    try {
      const { rows } = await client.query(
        `
        SELECT
          s.*
        FROM wms.shipments s
        WHERE s.status IN ('shipping', 'ready_to_ship')
        ORDER BY
          CASE WHEN s.status = 'shipping' THEN 0 ELSE 1 END,
          s.updated_at DESC,
          s.id DESC
        LIMIT 1
        `
      );

      if (!rows.length) {
        return res.json({ shipment: null, lines: [] });
      }

      const shipment = rows[0];

      const lines = await loadShipmentLinesForPackingOrShipping(
        pool,
        shipment.external_id
      );

      return res.json({ shipment, lines });
    } catch (err) {
      console.error('shipping/current error:', err);
      return res.status(500).json({ message: 'Ошибка сервера' });
    } finally {
      client.release();
    }
  }
);



// -------------------------
// Взять следующее задание на отгрузку
// SAFE VERSION: без join к masterdata.locations
// -------------------------
app.post(
  '/shipping/next',
  authRequired,
  requireRole(['owner', 'admin', 'packer']),
  async (req, res) => {
    const client = await pool.connect();
    try {
      // 1) Сначала смотрим, нет ли уже "shipping" в работе
      const current = await client.query(
        `
        SELECT
          s.*
        FROM wms.shipments s
        WHERE s.status = 'shipping'
        ORDER BY s.updated_at DESC, s.id DESC
        LIMIT 1
        `
      );

      if (current.rows.length) {
        const shipment = current.rows[0];
        const lines = await loadShipmentLinesForPackingOrShipping(
          client,
          shipment.external_id
        );
        return res.json({ shipment, lines });
      }

      // 2) Берём следующую отгрузку, готовую к отгрузке
      await client.query('BEGIN');

      const nextRes = await client.query(
        `
        UPDATE wms.shipments s
        SET status = 'shipping',
            updated_at = NOW()
        WHERE s.id = (
          SELECT id
          FROM wms.shipments
          WHERE status = 'ready_to_ship'
          ORDER BY updated_at, created_at, id
          FOR UPDATE SKIP LOCKED
          LIMIT 1
        )
        RETURNING *
        `
      );

      if (!nextRes.rows.length) {
        await client.query('ROLLBACK');
        return res.status(404).json({ message: 'Нет доступных заданий на отгрузку' });
      }

      const shipment = nextRes.rows[0];

      const lines = await loadShipmentLinesForPackingOrShipping(
        client,
        shipment.external_id
      );

      await client.query('COMMIT');

      return res.json({ shipment, lines });
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      console.error('shipping/next error:', err);
      return res.status(500).json({ message: 'Ошибка сервера' });
    } finally {
      client.release();
    }
  }
);

// -------------------------
// История движений /movements/list
// -------------------------
app.post(
  '/movements/list',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const {
        date_from,
        date_to,
        barcode,
        client_id,
        movement_type,
        ref_type,
        ref_id,
        user_id
      } = req.body || {};

      const params = [];
      const where = [];

      if (date_from) {
        params.push(date_from);
        where.push(`m.created_at >= $${params.length}`);
      }

      if (date_to) {
        params.push(date_to);
        where.push(`m.created_at <= $${params.length}`);
      }

      if (barcode) {
        params.push(barcode);
        where.push(`m.barcode = $${params.length}`);
      }

      if (client_id) {
        params.push(client_id);
        where.push(`m.client_id = $${params.length}`);
      }

      if (movement_type) {
        params.push(movement_type);
        where.push(`m.movement_type = $${params.length}`);
      }

      if (ref_type) {
        params.push(ref_type);
        where.push(`m.ref_type = $${params.length}`);
      }

      if (ref_id) {
        params.push(ref_id);
        where.push(`m.ref_id = $${params.length}`);
      }

      if (user_id) {
        params.push(user_id);
        where.push(`m.user_id = $${params.length}`);
      }

      const whereSql = where.length ? ('WHERE ' + where.join(' AND ')) : '';

      const sql = `
        SELECT
          m.id,
          m.created_at,
          m.user_id,
          u.username AS user_name,
          m.client_id,
          m.barcode,
          m.sku_id,
          m.qty,
          m.from_location,
          m.to_location,
          m.movement_type,
          m.ref_type,
          m.ref_id,
          m.comment
        FROM wms.movements m
        LEFT JOIN users u ON u.id = m.user_id
        ${whereSql}
        ORDER BY m.created_at DESC
        LIMIT 500
      `;

      const result = await pool.query(sql, params);
      return res.json({ rows: result.rows });

    } catch (err) {
      console.error('movements/list error:', err);
      return res.status(500).json({ message: 'Ошибка получения истории движений' });
    }
  }
);

// -------------------------
// POST /shipping/confirm
// Подтверждение отгрузки (скан внутреннего ШК поставки)
// + movement shipping
// + shipped_at / shipped_by
// + total_shipped_qty = total_planned_qty
// + подтверждение в WB и сохранение QR
// -------------------------
app.post(
  '/shipping/confirm',
  authRequired,
  requireRole(['owner', 'admin', 'packer']),
  async (req, res) => {
    const userId = req.user.id;
    const { shipment_id, wg_code } = req.body || {};

    const wg = String(wg_code || '').trim();
    if (!wg) {
      return res.status(400).json({ message: 'Не передан код поставки' });
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      let shipment = null;

      // 1) Находим shipment
      if (shipment_id) {
        const q = await client.query(
          `
          SELECT
            s.id,
            s.client_id,
            s.external_id,
            s.status,
            s.total_planned_qty,
            s.total_picked_qty,
            s.total_packed_qty,
            s.total_shipped_qty,
            s.packing_location_code
          FROM wms.shipments s
          WHERE s.id = $1
          FOR UPDATE
          `,
          [shipment_id]
        );

        if (q.rowCount === 0) {
          await client.query('ROLLBACK');
          return res.status(404).json({ message: 'Отгрузка не найдена' });
        }

        shipment = q.rows[0];

        if (String(shipment.external_id || '').trim() !== wg) {
          await client.query('ROLLBACK');
          return res.status(400).json({ message: 'Код поставки не совпадает с кодом отгрузки' });
        }
      } else {
        const q = await client.query(
          `
          SELECT
            s.id,
            s.client_id,
            s.external_id,
            s.status,
            s.total_planned_qty,
            s.total_picked_qty,
            s.total_packed_qty,
            s.total_shipped_qty,
            s.packing_location_code
          FROM wms.shipments s
          WHERE TRIM(s.external_id) = TRIM($1)
          ORDER BY s.id DESC
          LIMIT 1
          FOR UPDATE
          `,
          [wg]
        );

        if (q.rowCount === 0) {
          await client.query('ROLLBACK');
          return res.status(404).json({ message: `Отгрузка не найдена по коду: ${wg}` });
        }

        shipment = q.rows[0];
      }

      // 2) Статус-гейт
      const currentStatus = String(shipment.status || '').trim();
      const ALLOWED = new Set([
        'ready_to_ship',
        'shipping',
        'in_transit',
        'ready_for_packing',
        'packing',
        'packed',
      ]);

      if (!ALLOWED.has(currentStatus)) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          message: `Отгрузка в статусе '${currentStatus}', не могу подтвердить отгрузку`,
        });
      }

      // 3) movement shipping
      await client.query(
        `
        INSERT INTO wms.movements
          (created_at, user_id, client_id, barcode, qty,
           from_location, to_location,
           movement_type, ref_type, ref_id, comment, sku_id)
        VALUES
          (NOW(), $1, $2, NULL, 1,
           $3, NULL,
           'shipping', 'shipment', $4, 'Отгрузка перевозчику', NULL)
        `,
        [
          userId,
          shipment.client_id,
          shipment.packing_location_code || null,
          shipment.id,
        ]
      );

      // 4) totals: shipped = planned
      const planned = Number(shipment.total_planned_qty || 0);
      const newStatus = 'in_transit';

      await client.query(
        `
        UPDATE wms.shipments
           SET status            = $1,
               total_shipped_qty = $2,
               updated_at        = NOW(),
               shipped_at        = NOW(),
               shipped_by        = $3
         WHERE id = $4
        `,
        [newStatus, planned, userId, shipment.id]
      );

      // 5) WB API confirm shipment + QR
      let qrBase64 = null;
      try {
        const wbAcc = await getWbAccountForClient(client, shipment.client_id);
        console.log('[WB] use account', wbAcc.id, wbAcc.label, 'for wms_client_id=', shipment.client_id);

        const { qrBase64: qrFromWb } = await wbService.confirmShipmentAndGetQr({
          token: wbAcc.api_token,
          shipmentCode: wg,
        });

        qrBase64 = qrFromWb || null;

        if (qrBase64) {
          await client.query(
            `
            UPDATE wms.shipments
               SET wb_supply_qr_base64 = $1
             WHERE id = $2
            `,
            [qrBase64, shipment.id]
          );
        }
      } catch (e) {
        console.error('[shipping/confirm] WB confirm shipment error:', e);
      }

      await client.query('COMMIT');

      return res.json({
        ok: true,
        qr_base64: qrBase64,
        status: newStatus,
        shipment_id: shipment.id,
        shipped_at: new Date().toISOString(),
      });
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      console.error('shipping/confirm error:', err);
      return res.status(500).json({ message: 'Ошибка подтверждения отгрузки' });
    } finally {
      client.release();
    }
  }
);


// ------------------------- 
// GET /picking/next
// Строго работаем ТОЛЬКО в рамках активной волны сборщика
// -------------------------
app.get(
  '/picking/next',
  authRequired,
  requireRole(['owner', 'admin', 'picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = Number(req.user.id);

      // 0) Находим активную волну сборщика
      const waveRes = await db.query(
        `
        SELECT shipment_code, client_id, status
        FROM wms.pick_waves
        WHERE picker_id = $1
          AND status IN ('active', 'open', 'offered')
        ORDER BY created_at DESC
        LIMIT 1
        `,
        [pickerId]
      );

      if (!waveRes.rowCount) {
        return res.json({});
      }

      const shipment = waveRes.rows[0].shipment_code;
      const clientId = Number(waveRes.rows[0].client_id);

      // Общий "развёрнутый" запрос карточки (task)
      const qCard = `
        SELECT
          t.id::text,
          t.client_id,
          ('client_id=' || t.client_id::text) AS client_name,
          t.wb_order_id::text,
          t.barcode,
          t.qty,
          COALESCE(t.location_code, best_loc.location_code, '—') AS location_code,
          t.shipment_code,
          COALESCE(mi.title, t.comment, 'Без названия') AS item_name,
          o.warehouse_name,
          mi.preview_url,
          t.status,
          t.scan_step,
          t.picked_qty
        FROM wms.picking_tasks t
        LEFT JOIN public.mp_wb_orders o
          ON o.wb_order_id = t.wb_order_id
        LEFT JOIN public.mp_wb_items mi
          ON mi.nm_id = o.nm_id
        LEFT JOIN LATERAL (
          SELECT l.code AS location_code
          FROM wms.stock s
          JOIN masterdata.locations l ON l.id = s.location_id
          WHERE s.sku_id = t.sku_id
            AND s.client_id = t.client_id
            AND s.qty > 0
          ORDER BY s.qty DESC, l.code ASC
          LIMIT 1
        ) best_loc ON TRUE
        WHERE t.id = $1
        LIMIT 1
      `;

      // 1) Если есть активная задача in_progress в этой волне — возвращаем её
      const inProg = await db.query(
        `
        SELECT t.id
        FROM wms.picking_tasks t
        WHERE t.picker_id = $1
          AND t.client_id = $2
          AND t.shipment_code = $3
          AND t.status = 'in_progress'
        ORDER BY t.started_at NULLS LAST, t.id ASC
        LIMIT 1
        `,
        [pickerId, clientId, shipment]
      );

      if (inProg.rowCount) {
        const taskId = inProg.rows[0].id;
        const card = await db.query(qCard, [taskId]);
        return res.json(card.rowCount ? card.rows[0] : {});
      }

      // 2) Берём следующую new задачу ТОЛЬКО из активной волны
      //    (без фильтра по picker_id, чтобы после генерации волны задачи выпадали)
      await db.query('BEGIN');

      const pick = await db.query(
        `
        SELECT t.id
        FROM wms.picking_tasks t
        WHERE t.status = 'new'
          AND t.client_id = $1
          AND t.shipment_code = $2
        ORDER BY t.priority ASC, t.created_at ASC, t.id ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
        `,
        [clientId, shipment]
      );

      if (!pick.rowCount) {
        await db.query('COMMIT');
        return res.json({});
      }

      const taskId = pick.rows[0].id;

      // Переводим в in_progress + подставляем location_code, если пусто
      await db.query(
        `
        UPDATE wms.picking_tasks t
        SET
          status       = 'in_progress',
          picker_id    = $1,
          started_at   = NOW(),
          updated_at   = NOW(),
          updated_by   = $1,
          scan_step    = COALESCE(NULLIF(t.scan_step, ''), 'await_location'),
          location_code = COALESCE(
            NULLIF(t.location_code, ''),
            (
              SELECT l.code
              FROM wms.stock s
              JOIN masterdata.locations l ON l.id = s.location_id
              WHERE s.sku_id   = t.sku_id
                AND s.client_id = t.client_id
                AND s.qty > 0
              ORDER BY s.qty DESC, l.code ASC
              LIMIT 1
            )
          )
        WHERE t.id = $2
        `,
        [pickerId, taskId]
      );

      await db.query('COMMIT');

      // 3) Возвращаем карточку
      const card = await db.query(qCard, [taskId]);
      return res.json(card.rowCount ? card.rows[0] : {});

    } catch (e) {
      try { await db.query('ROLLBACK'); } catch {}
      console.error('picking_next_error', e);
      return res.status(500).json({ error: 'picking_next_error', details: e.message });
    } finally {
      db.release();
    }
  }
);





app.get(
  '/picking/wave/offer',
  authRequired,
  requireRole(['owner','admin','picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = Number(req.user.id);

      // Если уже есть активная волна — просто сообщаем какую
      const active = await db.query(
        `
        SELECT shipment_code, client_id, status
        FROM wms.pick_waves
        WHERE picker_id=$1 AND status='active'
        LIMIT 1
        `,
        [pickerId]
      );
      if (active.rowCount) {
        const sc = active.rows[0].shipment_code;
        const stats = await db.query(
          `
          SELECT
            COUNT(*)::int AS total,
            SUM(CASE WHEN status='done' THEN 1 ELSE 0 END)::int AS done
          FROM wms.picking_tasks
          WHERE shipment_code=$1
          `,
          [sc]
        );
        return res.json({
          has_active_wave: true,
          shipment_code: sc,
          client_id: active.rows[0].client_id,
          tasks_total: stats.rows[0].total,
          tasks_done: stats.rows[0].done
        });
      }

      // Иначе — предлагаем следующую offered (самую раннюю)
      const offer = await db.query(
        `
        SELECT w.shipment_code, w.client_id
        FROM wms.pick_waves w
        WHERE w.status='offered'
        ORDER BY w.created_at ASC, w.shipment_code ASC
        LIMIT 1
        `
      );

      if (!offer.rowCount) return res.json({ has_offer:false });

      const sc = offer.rows[0].shipment_code;
      const stats = await db.query(
        `
        SELECT COUNT(*)::int AS total
        FROM wms.picking_tasks
        WHERE shipment_code=$1
        `,
        [sc]
      );

      return res.json({
        has_offer: true,
        shipment_code: sc,
        client_id: offer.rows[0].client_id,
        tasks_total: stats.rows[0].total
      });
    } catch (e) {
      console.error('wave/offer error', e);
      return res.status(500).json({ error:'wave/offer error', detail:e.message });
    } finally {
      db.release();
    }
  }
);


app.post(
  '/picking/wave/accept',
  authRequired,
  requireRole(['owner','admin','picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = Number(req.user.id);
      const { shipment_code } = req.body || {};
      const sc = String(shipment_code || '').trim();
      if (!sc) return res.status(400).json({ error:'shipment_code обязателен' });

      await db.query('BEGIN');

      // запрет второй активной волны
      const active = await db.query(
        `SELECT shipment_code FROM wms.pick_waves WHERE picker_id=$1 AND status='active' LIMIT 1 FOR UPDATE`,
        [pickerId]
      );
      if (active.rowCount) {
        await db.query('ROLLBACK');
        return res.status(409).json({ error:'У сборщика уже есть активная волна', shipment_code: active.rows[0].shipment_code });
      }

      // лочим волну
      const w = await db.query(
        `
        SELECT shipment_code, client_id, status
        FROM wms.pick_waves
        WHERE shipment_code=$1
        FOR UPDATE
        `,
        [sc]
      );
      if (!w.rowCount) {
        await db.query('ROLLBACK');
        return res.status(404).json({ error:'Волна не найдена' });
      }
      if (w.rows[0].status !== 'offered') {
        await db.query('ROLLBACK');
        return res.status(409).json({ error:'Волна уже не offered', status:w.rows[0].status });
      }

      await db.query(
        `
        UPDATE wms.pick_waves
        SET status='active', picker_id=$1, accepted_at=NOW()
        WHERE shipment_code=$2
        `,
        [pickerId, sc]
      );

      // закрепляем задачи волны за сборщиком (статус new оставляем)
      await db.query(
        `
        UPDATE wms.picking_tasks
        SET picker_id=$1, updated_at=NOW(), updated_by=$1
        WHERE shipment_code=$2
          AND status='new'
          AND (picker_id IS NULL OR picker_id=$1)
        `,
        [pickerId, sc]
      );

      await db.query('COMMIT');
      return res.json({ result:'ok', shipment_code: sc, status:'active' });
    } catch (e) {
      try { await db.query('ROLLBACK'); } catch {}
      console.error('wave/accept error', e);
      return res.status(500).json({ error:'wave/accept error', detail:e.message });
    } finally {
      db.release();
    }
  }
);

app.post(
  '/picking/wave/close',
  authRequired,
  requireRole(['owner','admin','picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = Number(req.user.id);
      const { shipment_code } = req.body || {};
      const sc = String(shipment_code || '').trim();
      if (!sc) return res.status(400).json({ error:'shipment_code обязателен' });

      await db.query('BEGIN');

      const w = await db.query(
        `
        SELECT shipment_code, picker_id, status
        FROM wms.pick_waves
        WHERE shipment_code=$1
        FOR UPDATE
        `,
        [sc]
      );
      if (!w.rowCount) {
        await db.query('ROLLBACK');
        return res.status(404).json({ error:'Волна не найдена' });
      }
      if (Number(w.rows[0].picker_id) !== Number(pickerId)) {
        await db.query('ROLLBACK');
        return res.status(403).json({ error:'Это не твоя волна' });
      }
      if (w.rows[0].status !== 'active') {
        await db.query('ROLLBACK');
        return res.status(409).json({ error:'Волна не active', status:w.rows[0].status });
      }

      const stats = await db.query(
        `
        SELECT
          COUNT(*)::int AS total,
          SUM(CASE WHEN status='done' THEN 1 ELSE 0 END)::int AS done
        FROM wms.picking_tasks
        WHERE shipment_code=$1
        `,
        [sc]
      );

      if (stats.rows[0].done !== stats.rows[0].total) {
        await db.query('ROLLBACK');
        return res.status(409).json({
          error:'Нельзя закрыть волну: не все задачи done',
          tasks_total: stats.rows[0].total,
          tasks_done: stats.rows[0].done
        });
      }

      await db.query(
        `
        UPDATE wms.pick_waves
        SET status='closed', closed_at=NOW()
        WHERE shipment_code=$1
        `,
        [sc]
      );

      await db.query('COMMIT');
      return res.json({ result:'ok', shipment_code: sc, status:'closed' });
    } catch (e) {
      try { await db.query('ROLLBACK'); } catch {}
      console.error('wave/close error', e);
      return res.status(500).json({ error:'wave/close error', detail:e.message });
    } finally {
      db.release();
    }
  }
);


// ==============================
//  PICKING: GET ACTIVE TASK FOR CURRENT PICKER
// ==============================
app.get('/picking/active', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const userId = req.user.id;

    // 1. Ищем активную задачу данного сборщика
    const activeResult = await pool.query(
      `
      SELECT *
      FROM wms.picking_tasks
      WHERE picker_id = $1
        AND status    = 'in_progress'
      ORDER BY priority ASC, created_at ASC
      LIMIT 1
      `,
      [userId],
    );

    if (activeResult.rows.length === 0) {
      return res.json({
        status: 'ok',
        task: null,
      });
    }

    const task = activeResult.rows[0];

    // 2. Подтягиваем товар
    const itemResult = await pool.query(
      `
      SELECT *
      FROM masterdata.items
      WHERE barcode   = $1
        AND client_id = $2
      LIMIT 1
      `,
      [task.barcode, task.client_id],
    );
    const item = itemResult.rows.length > 0 ? itemResult.rows[0] : null;

    // 3. Остатки по SKU
    const skuIdForStock = task.sku_id
      ? Number(task.sku_id)
      : await resolveSkuIdOrCreate(pool, {
          client_id: task.client_id,
          barcode: task.barcode,
        });

    const stockResult = await pool.query(
      `
      SELECT
        s.location_id,
        l.code AS location_code,
        s.qty
      FROM wms.stock s
      JOIN masterdata.locations l ON l.id = s.location_id
      WHERE s.sku_id = $1
      ORDER BY l.code ASC
      `,
      [skuIdForStock]
    );

    let totalQty = 0;
    const locations = stockResult.rows.map((row) => {
      const qtyNum = Number(row.qty) || 0;
      totalQty += qtyNum;
      return {
        location_id: row.location_id,
        location_code: row.location_code,
        qty: qtyNum,
      };
    });

    return res.json({
      status: 'ok',
      task,
      item,
      stock: {
        total_qty: totalQty,
        locations,
      },
    });
  } catch (err) {
    console.error('Error in GET /picking/active:', err);
    return res.status(500).json({
      error: 'Internal server error in /picking/active',
      detail: err.message,
    });
  }
});


app.post('/picking/next', authRequired, async (req, res) => {
  const pickerId = req.user.id;  // admin = 1
  const { client_id } = req.body || {};

  if (!client_id) {
    return res.status(400).json({ status: 'error', error: 'client_id is required' });
  }

  try {
    const { rows } = await pool.query(
      `
      SELECT
        pt.id,
        pt.client_id,
        pt.barcode,
        pt.sku_id,
        -- COALESCE: если в задаче уже есть location_code, берём его,
        -- иначе подставляем из склада
        COALESCE(loc.code, pt.location_code) AS location_code,
        pt.qty,
        pt.status,
        pt.priority,
        pt.order_ref,
        pt.picker_id,
        pt.wb_order_id,
        pt.shipment_code,
        pt.created_at,
        pt.started_at,
        pt.finished_at,

        c.client_name,

        o.warehouse_name,
        o.region_name,
        o.nm_id,
        o.chrt_id,

        i.item_name,
        i.preview_url,
        i.title,

        w.name AS wb_warehouse_name,

        st.qty AS stock_qty

      FROM wms.picking_tasks pt
      LEFT JOIN masterdata.clients c
             ON c.id = pt.client_id

      LEFT JOIN public.mp_wb_orders o
             ON o.wb_order_id = pt.wb_order_id

      -- Баркод → SKU
      LEFT JOIN wms.sku sku
             ON sku.client_id = pt.client_id
            AND sku.barcode   = pt.barcode

      -- SKU → остатки по ячейкам (берём только ячейки с остатком)
      LEFT JOIN LATERAL (
          SELECT s.sku_id, s.location_id, s.qty
          FROM wms.stock s
          WHERE s.client_id = pt.client_id
            AND s.sku_id    = sku.id
            AND s.qty       > 0
          ORDER BY s.qty DESC
          LIMIT 1
      ) st ON TRUE

      -- Ячейка
      LEFT JOIN wms.locations loc
             ON loc.id = st.location_id

      -- Карточка товара (через связку с баркодами)
      LEFT JOIN public.mp_wb_items_barcodes ib
             ON ib.barcode = pt.barcode

      LEFT JOIN public.mp_wb_items i
             ON i.nm_id                = ib.nm_id
            AND i.client_mp_account_id = ib.client_mp_account_id

      LEFT JOIN public.mp_wb_warehouses w
             ON w.wb_warehouse_id = o.warehouse_id

      WHERE pt.client_id = $1
        AND (
             (pt.status = 'in_progress' AND pt.picker_id = $2)
             OR
             (pt.status = 'new'         AND pt.picker_id IS NULL)
        )
      ORDER BY
        CASE
          WHEN pt.status = 'in_progress' AND pt.picker_id = $2 THEN 0
          ELSE 1
        END,
        pt.priority DESC,
        pt.id
      LIMIT 1;
      `,
      [client_id, pickerId]
    );

    if (!rows.length) {
      return res.json({ status: 'ok', task: null });
    }

    const task = rows[0];

    // Если только что взяли "new" задачу — помечаем её как in_progress
    if (task.status === 'new' && task.picker_id == null) {
      await pool.query(
        `
        UPDATE wms.picking_tasks
           SET status    = 'in_progress',
               picker_id = $2,
               started_at = NOW()
         WHERE id = $1;
        `,
        [task.id, pickerId]
      );
      task.status = 'in_progress';
      task.picker_id = pickerId;
    }

    return res.json({ status: 'ok', task });
  } catch (err) {
    console.error('picking/next error', err);
    return res.status(500).json({ status: 'error', error: 'internal_error' });
  }
});


// -------------------------
// POST /picking/scan/location
// -------------------------
app.post(
  '/picking/scan/location',
  authRequired,
  requireRole(['owner','admin','picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = req.user?.id;
      const { picking_task_id, scanned_location_code } = req.body || {};
      const taskId = Number(picking_task_id);

      const scannedLocRaw = String(scanned_location_code || '').trim();
      const scannedLoc = scannedLocRaw.toUpperCase();

      if (!pickerId) return res.status(401).json({ error: 'Не определён пользователь' });
      if (!taskId || !scannedLocRaw) {
        return res.status(400).json({ error: 'picking_task_id и scanned_location_code обязательны' });
      }

      await db.query('BEGIN');

      const tRes = await db.query(
        `
        SELECT id, picker_id, status, location_code, scan_step
        FROM wms.picking_tasks
        WHERE id=$1
        FOR UPDATE
        `,
        [taskId]
      );

      if (!tRes.rowCount) {
        await db.query('ROLLBACK');
        return res.status(404).json({ error: 'Задача не найдена' });
      }

      const t = tRes.rows[0];

      if (t.status !== 'in_progress') {
        await db.query('ROLLBACK');
        return res.status(409).json({ error: 'Задача не in_progress', current_status: t.status });
      }
      if (Number(t.picker_id) !== Number(pickerId)) {
        await db.query('ROLLBACK');
        return res.status(403).json({ error: 'Это не твоя задача' });
      }

      const step = String(t.scan_step || 'await_location');
      if (step !== 'await_location') {
        await db.query('ROLLBACK');
        return res.status(409).json({ error: 'Ожидается скан МХ', scan_step: step });
      }

      const expectedLocRaw = String(t.location_code || '').trim();
      const expectedLoc = expectedLocRaw.toUpperCase();

      // Если в задаче МХ задан — строго сверяем
      if (expectedLoc && scannedLoc !== expectedLoc) {
        // лог скана (если таблица есть)
        try {
          await db.query(
            `
            INSERT INTO wms.picking_scans(picking_task_id,picker_id,scan_type,expected,scanned,result,message)
            VALUES($1,$2,'location',$3,$4,'mismatch','Неверный МХ')
            `,
            [taskId, pickerId, expectedLocRaw, scannedLocRaw]
          );
        } catch (_) {}

        await db.query('ROLLBACK');
        return res.status(400).json({
          result: 'error',
          code: 'LOCATION_MISMATCH',
          expected: expectedLocRaw,
          scanned: scannedLocRaw
        });
      }

      // Если в задаче МХ пустой — первый скан задаёт МХ
      if (!expectedLoc) {
        await db.query(
          `
          UPDATE wms.picking_tasks
          SET location_code=$1,
              scan_step='await_item',
              updated_at=NOW(),
              updated_by=$2
          WHERE id=$3
          `,
          [scannedLocRaw, pickerId, taskId]
        );
      } else {
        await db.query(
          `
          UPDATE wms.picking_tasks
          SET scan_step='await_item',
              updated_at=NOW(),
              updated_by=$1
          WHERE id=$2
          `,
          [pickerId, taskId]
        );
      }

      try {
        await db.query(
          `
          INSERT INTO wms.picking_scans(picking_task_id,picker_id,scan_type,expected,scanned,result,message)
          VALUES($1,$2,'location',$3,$4,'ok','')
          `,
          [taskId, pickerId, expectedLocRaw || scannedLocRaw, scannedLocRaw]
        );
      } catch (_) {}

      await db.query('COMMIT');
      return res.json({ result: 'ok', next_step: 'await_item' });

    } catch (e) {
      try { await db.query('ROLLBACK'); } catch {}
      console.error('scan/location error', e);
      return res.status(500).json({ error: 'scan/location error', detail: e.message });
    } finally {
      db.release();
    }
  }
);

// -------------------------
// POST /picking/scan/item
// -------------------------
app.post(
  '/picking/scan/item',
  authRequired,
  requireRole(['owner', 'admin', 'picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = req.user?.id;
      const { picking_task_id, scanned_barcode, comment } = req.body || {};
      const taskId = Number(picking_task_id);
      const scanned = String(scanned_barcode || '').trim();

      if (!pickerId) {
        return res.status(401).json({ error: 'Не определён пользователь' });
      }
      if (!taskId || !scanned) {
        return res
          .status(400)
          .json({ error: 'picking_task_id и scanned_barcode обязательны' });
      }

      await db.query('BEGIN');

      // Берём задачу c shipment_code под блокировку
      const taskRes = await db.query(
        `
        SELECT
          id,
          client_id,
          barcode,
          sku_id,
          location_code,
          qty,
          status,
          picker_id,
          scan_step,
          picked_qty,
          shipment_code,
          comment AS task_comment
        FROM wms.picking_tasks
        WHERE id = $1
        FOR UPDATE
        `,
        [taskId]
      );

      if (!taskRes.rowCount) {
        await db.query('ROLLBACK');
        return res.status(404).json({ error: 'Задача не найдена' });
      }

      const t = taskRes.rows[0];

      if (t.status !== 'in_progress') {
        await db.query('ROLLBACK');
        return res
          .status(409)
          .json({ error: 'Задача не in_progress', current_status: t.status });
      }
      if (Number(t.picker_id) !== Number(pickerId)) {
        await db.query('ROLLBACK');
        return res.status(403).json({ error: 'Это не твоя задача' });
      }

      const step = String(t.scan_step || 'await_location');
      if (step !== 'await_item') {
        await db.query('ROLLBACK');
        return res
          .status(409)
          .json({ error: 'Ожидается скан товара', scan_step: step });
      }

      const expectedBarcode = String(t.barcode || '').trim();
      if (!expectedBarcode) {
        await db.query('ROLLBACK');
        return res
          .status(400)
          .json({ error: 'В задаче пустой barcode' });
      }

      // Проверка штрихкода
      if (scanned !== expectedBarcode) {
        try {
          await db.query(
            `
            INSERT INTO wms.picking_scans
              (picking_task_id, picker_id, scan_type, expected, scanned, result, message)
            VALUES ($1, $2, 'item', $3, $4, 'mismatch', 'Неверный штрихкод')
            `,
            [taskId, pickerId, expectedBarcode, scanned]
          );
        } catch (_) {}

        await db.query('ROLLBACK');
        return res.status(400).json({
          result: 'error',
          code: 'BARCODE_MISMATCH',
          expected: expectedBarcode,
          scanned,
        });
      }

      // picked_qty++
      const qtyToPick = Math.max(1, Number(t.qty || 1));
      let pickedQty = Number(t.picked_qty || 0) + 1;
      if (pickedQty > qtyToPick) pickedQty = qtyToPick;

      try {
        await db.query(
          `
          INSERT INTO wms.picking_scans
            (picking_task_id, picker_id, scan_type, expected, scanned, result, message)
          VALUES ($1, $2, 'item', $3, $4, 'ok', '')
          `,
          [taskId, pickerId, expectedBarcode, scanned]
        );
      } catch (_) {}

      // Если ещё не добрали — только обновляем прогресс
      if (pickedQty < qtyToPick) {
        await db.query(
          `
          UPDATE wms.picking_tasks
          SET picked_qty = $1,
              updated_at = NOW(),
              updated_by = $2
          WHERE id = $3
          `,
          [pickedQty, pickerId, taskId]
        );

        await db.query('COMMIT');
        return res.json({
          result: 'ok',
          picked_qty: pickedQty,
          qty: qtyToPick,
          next_step: 'await_item',
        });
      }

      // ---------- финальный добор: списание ----------
      const clientIdNum = Number(t.client_id);
      const barcodeStr = expectedBarcode;
      let locCode = t.location_code ? String(t.location_code).trim() : null;
      const shipmentCode = t.shipment_code
        ? String(t.shipment_code).trim()
        : null;

      // sku_id
      let skuIdFinal;
      if (t.sku_id !== null && t.sku_id !== undefined) {
        skuIdFinal = Number(t.sku_id);
        if (!Number.isInteger(skuIdFinal) || skuIdFinal <= 0) {
          throw new Error('Некорректный sku_id в задаче');
        }
      } else {
        skuIdFinal = await resolveSkuIdOrCreate(db, {
          client_id: clientIdNum,
          barcode: barcodeStr,
        });
      }

      // location_id: работаем ТОЛЬКО через masterdata.locations.code
      let locationId;

      if (locCode) {
        const locIdRes = await db.query(
          `
          SELECT id
          FROM masterdata.locations
          WHERE code = $1
            AND is_active = true
          LIMIT 1
          `,
          [locCode]
        );
        if (!locIdRes.rowCount) {
          throw new Error(`Не найдена ячейка: ${locCode}`);
        }
        locationId = locIdRes.rows[0].id;
      } else {
        // если location_code не заполнен — подберём лучшую ячейку из стока
        const locRes = await db.query(
          `
          SELECT s.location_id, l.code AS code
          FROM wms.stock s
          JOIN masterdata.locations l ON l.id = s.location_id
          WHERE s.sku_id = $1
            AND s.client_id = $2
            AND s.qty > 0
            AND l.is_active = true
          ORDER BY s.qty DESC, l.code ASC
          LIMIT 1
          `,
          [skuIdFinal, clientIdNum]
        );

        if (!locRes.rowCount) {
          throw new Error(
            'Не найдена ячейка с остатком для SKU (по клиенту)'
          );
        }
        locationId = locRes.rows[0].location_id;
        locCode = locRes.rows[0].code;

        await db.query(
          `UPDATE wms.picking_tasks SET location_code = $1 WHERE id = $2`,
          [locCode, taskId]
        );
      }

      // stock lock + update
      const stockRes = await db.query(
        `
        SELECT qty
        FROM wms.stock
        WHERE location_id = $1
          AND sku_id = $2
        FOR UPDATE
        `,
        [locationId, skuIdFinal]
      );
      if (!stockRes.rowCount) {
        throw new Error('В ячейке нет остатка по этому SKU');
      }

      const currentQty = Number(stockRes.rows[0].qty || 0);
      if (currentQty < qtyToPick) {
        throw new Error(
          `Недостаточно остатка: есть ${currentQty}, нужно ${qtyToPick}`
        );
      }

      const newQty = currentQty - qtyToPick;
      await db.query(
        `
        UPDATE wms.stock
        SET qty = $1
        WHERE location_id = $2
          AND sku_id = $3
        `,
        [newQty, locationId, skuIdFinal]
      );

      // movement
      await db.query(
        `
        INSERT INTO wms.movements
          (created_at, user_id, client_id, sku_id, barcode,
           qty, from_location, to_location, movement_type,
           ref_type, ref_id, comment)
        VALUES
          (NOW(), $1, $2, $3, $4,
           $5, $6, $7, $8,
           $9, $10, $11)
        `,
        [
          pickerId,
          clientIdNum,
          skuIdFinal,
          barcodeStr,
          qtyToPick,
          locCode,
          null,
          'writeoff',
          'picking_task',
          taskId,
          comment || t.task_comment || null,
        ]
      );

      // done (сама задача)
      const upd = await db.query(
        `
        UPDATE wms.picking_tasks
        SET
          status      = 'done',
          scan_step   = 'done',
          picked_qty  = $1,
          finished_at = NOW(),
          sku_id      = $2,
          comment     = COALESCE($3, comment),
          updated_at  = NOW(),
          updated_by  = $4
        WHERE id = $5
        RETURNING id, status, picked_qty, qty, location_code, sku_id, shipment_code
        `,
        [qtyToPick, skuIdFinal, comment || null, pickerId, taskId]
      );

      // ---------- обновление статуса отгрузки в wms.shipments ----------
      if (shipmentCode) {
        const progRes = await db.query(
          `
          SELECT
            COUNT(*)                                    AS total,
            COUNT(*) FILTER (WHERE status = 'done')     AS done_cnt
          FROM wms.picking_tasks
          WHERE client_id = $1
            AND shipment_code = $2
          `,
          [clientIdNum, shipmentCode]
        );

        if (progRes.rowCount) {
          const total = Number(progRes.rows[0].total || 0);
          const doneCnt = Number(progRes.rows[0].done_cnt || 0);

          if (total > 0 && doneCnt >= 0) {
            // По умолчанию статус не меняем
            let newStatus = null;

            if (doneCnt > 0 && doneCnt < total) {
              // Сборка в процессе
              newStatus = 'picking';
            } else if (doneCnt === total) {
              // Все задачи по отгрузке собраны — можно передавать в упаковку
              newStatus = 'packing';
            }

            if (newStatus === 'picking') {
              await db.query(
                `
                UPDATE wms.shipments
                SET status = 'picking',
                    updated_at = NOW()
                WHERE client_id = $1
                  AND external_id = $2
                  AND status = 'CREATED'
                `,
                [clientIdNum, shipmentCode]
              );
            } else if (newStatus === 'packing') {
              await db.query(
                `
                UPDATE wms.shipments
                SET status = 'packing',
                    updated_at = NOW()
                WHERE client_id = $1
                  AND external_id = $2
                  AND status IN ('CREATED', 'picking')
                `,
                [clientIdNum, shipmentCode]
              );
            }
          }
        }
      }

      await db.query('COMMIT');

      return res.json({
        result: 'ok',
        done: true,
        task: upd.rows[0],
        stock_after: {
          location_code: locCode,
          sku_id: skuIdFinal,
          qty: newQty,
        },
      });
    } catch (e) {
      try {
        await db.query('ROLLBACK'); } catch {}
      console.error('scan/item error', e);
      return res
        .status(500)
        .json({ error: 'scan/item error', detail: e.message });
    } finally {
      db.release();
    }
  }
);



app.post(
  '/picking/skip',
  authRequired,
  requireRole(['owner','admin','picker']),
  async (req, res) => {
    const db = await pool.connect();
    try {
      const pickerId = req.user.id;
      const { picking_task_id, reason, comment } = req.body || {};
      const taskId = Number(picking_task_id);

      if (!taskId) return res.status(400).json({ error:'picking_task_id обязателен' });

      await db.query('BEGIN');

      const tRes = await db.query(
        `SELECT id, client_id, barcode, location_code, status, picker_id
         FROM wms.picking_tasks
         WHERE id=$1
         FOR UPDATE`,
        [taskId]
      );
      if (!tRes.rowCount) { await db.query('ROLLBACK'); return res.status(404).json({ error:'Задача не найдена' }); }

      const t = tRes.rows[0];
      if (t.status !== 'in_progress') { await db.query('ROLLBACK'); return res.status(409).json({ error:'Skip можно только из in_progress', current_status:t.status }); }
      if (Number(t.picker_id) !== Number(pickerId)) { await db.query('ROLLBACK'); return res.status(403).json({ error:'Это не твоя задача' }); }

      const clientId = Number(t.client_id);
      const barcode = String(t.barcode || '').trim();
      const locCode = String(t.location_code || '').trim() || null;

      // 1) найдём активную inventory_task или создадим
      let invId = null;
      if (barcode && locCode) {
        const invFind = await db.query(
          `
          SELECT id
          FROM wms.inventory_tasks
          WHERE client_id=$1 AND barcode=$2 AND location_code=$3
            AND status IN ('open','
')
          ORDER BY created_at DESC
          LIMIT 1
          `,
          [clientId, barcode, locCode]
        );

        if (invFind.rowCount) {
          invId = invFind.rows[0].id;
        } else {
          const invIns = await db.query(
            `
            INSERT INTO wms.inventory_tasks
              (client_id, barcode, location_code, status, priority, reason, comment, created_at, created_by, updated_at, updated_by)
            VALUES
              ($1,$2,$3,'open',100,$4,$5,NOW(),$6,NOW(),$6)
            RETURNING id
            `,
            [clientId, barcode, locCode, reason || 'picker_not_found', comment || null, pickerId]
          );
          invId = invIns.rows[0].id;
        }
      }

      // 2) отменим текущую задачу
      await db.query(
        `
        UPDATE wms.picking_tasks
        SET status='cancelled',
            reason=COALESCE($1, reason),
            comment=COALESCE($2, comment),
            updated_at=NOW(),
            updated_by=$3
        WHERE id=$4
        `,
        [reason || 'picker_not_found', comment || null, pickerId, taskId]
      );

      // 3) лог скана/действия
      await db.query(
        `INSERT INTO wms.picking_scans(picking_task_id,picker_id,scan_type,expected,scanned,result,message)
         VALUES($1,$2,'skip',NULL,$3,'ok',$4)`,
        [taskId, pickerId, barcode, `inventory_task_id=${invId || 'n/a'}`]
      );

      await db.query('COMMIT');
      return res.json({ result:'ok', inventory_task_id: invId });

    } catch (e) {
      try { await db.query('ROLLBACK'); } catch {}
      console.error('picking/skip error', e);
      return res.status(500).json({ error:'picking/skip error', detail:e.message });
    } finally {
      db.release();
    }
  }
);


// ==============================
//  PICKING: MY TASKS LIST (для текущего сборщика)
// ==============================
app.get('/picking/my-tasks', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const pickerId = req.user.id;

    const {
      client_id,
      status,
      date_from,
      date_to,
      limit,
      offset,
    } = req.query || {};

    const conditions = ['pt.picker_id = $1'];
    const values = [pickerId];
    let idx = 2;

    if (client_id !== undefined) {
      const clientIdNum = Number(client_id);
      if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
        return res.status(400).json({ error: 'client_id должен быть положительным целым числом' });
      }
      conditions.push(`pt.client_id = $${idx}`);
      values.push(clientIdNum);
      idx++;
    }

    if (status !== undefined) {
      const st = normalizePickingStatus(status);
      if (!st) {
        return res.status(400).json({
          error: `Некорректный status. Допустимо: ${validPickingStatuses.join(', ')}`,
        });
      }
      conditions.push(`pt.status = $${idx}`);
      values.push(st);
      idx++;
    }

    if (date_from !== undefined) {
      conditions.push(`pt.created_at >= $${idx}`);
      values.push(date_from);
      idx++;
    }

    if (date_to !== undefined) {
      conditions.push(`pt.created_at < $${idx}`);
      values.push(date_to);
      idx++;
    }

    const whereSql = `WHERE ${conditions.join(' AND ')}`;

    let lim = Number(limit || 50);
    if (!Number.isInteger(lim) || lim <= 0) lim = 50;
    if (lim > 200) lim = 200;

    let off = Number(offset || 0);
    if (!Number.isInteger(off) || off < 0) off = 0;

    // COUNT
    const countSql = `
      SELECT COUNT(*)::int AS total
      FROM wms.picking_tasks pt
      ${whereSql}
    `;
    const countRes = await pool.query(countSql, values);
    const total = countRes.rows[0]?.total ?? 0;

    // ROWS
    const rowsSql = `
      SELECT
        pt.id::text,
        pt.client_id,
        pt.barcode,
        pt.sku_id,
        pt.location_code,
        pt.qty,
        pt.status,
        pt.priority,
        pt.order_ref,
        pt.picker_id,
        pt.reason,
        pt.comment,
        pt.created_at,
        pt.created_by,
        pt.updated_at,
        pt.updated_by,
        pt.started_at,
        pt.finished_at
      FROM wms.picking_tasks pt
      ${whereSql}
      ORDER BY
        pt.status,
        pt.priority ASC,
        pt.created_at ASC,
        pt.id ASC
      LIMIT ${lim} OFFSET ${off}
    `;
    const rowsRes = await pool.query(rowsSql, values);

    return res.json({
      status: 'ok',
      picker_id: pickerId,
      filters: {
        client_id: client_id ? Number(client_id) : null,
        status: status || null,
        date_from: date_from || null,
        date_to: date_to || null,
      },
      limit: lim,
      offset: off,
      total,
      tasks: rowsRes.rows,
    });
  } catch (err) {
    console.error('Error in GET /picking/my-tasks:', err);
    return res.status(500).json({
      error: 'Internal server error in /picking/my-tasks',
      detail: err.message,
    });
  }
});

// ==============================
//  RECEIVING ACCEPT (ПРИЁМКА)
//  СТРОГИЙ РЕЖИМ + автозаведение из WB
// ==============================
//
// POST /receiving/accept
// body: { client_id, barcode, location_code, qty }
//
app.post('/receiving/accept', authRequired, requireRole('owner'), async (req, res) => {
  const client = await pool.connect();

  try {
    const { client_id, barcode, location_code, qty } = req.body || {};

    const clientIdNum = Number(client_id);
    const barcodeStr  = String(barcode || '').trim();
    const locCode     = String(location_code || '').trim();
    const qtyNum      = Number(qty);

    console.log('[/receiving/accept] IN:',
      'client_id =', clientIdNum,
      'barcode =', barcodeStr,
      'loc =', locCode,
      'qty =', qtyNum
    );

    // --- 1. Валидация входных данных
    if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
      return res.status(400).json({
        error: 'INVALID_CLIENT_ID',
        message: 'client_id должен быть положительным целым числом',
      });
    }

    if (!barcodeStr) {
      return res.status(400).json({
        error: 'INVALID_BARCODE',
        message: 'barcode обязателен и не может быть пустым',
      });
    }

    if (!locCode) {
      return res.status(400).json({
        error: 'INVALID_LOCATION_CODE',
        message: 'location_code обязателен и не может быть пустым',
      });
    }

    if (!Number.isInteger(qtyNum) || qtyNum <= 0) {
      return res.status(400).json({
        error: 'INVALID_QTY',
        message: 'qty должен быть целым числом > 0',
      });
    }

    await client.query('BEGIN');

    // --- 2. Строгая проверка товара + автозаведение из WB (через mpClients)
    console.log('[/receiving/accept] resolveSkuIdStrict...');
    const { skuId, item } = await resolveSkuIdStrict(client, {
      client_id: clientIdNum,
      barcode: barcodeStr,
    });
    console.log(
      '[/receiving/accept] resolveSkuIdStrict OK:',
      'item_id =', item.id,
      'sku_id =', skuId
    );

    // --- 3. Строгая проверка МХ
    console.log('[/receiving/accept] resolveLocationIdStrict...');
    const { locationId } = await resolveLocationIdStrict(client, locCode);
    console.log(
      '[/receiving/accept] resolveLocationIdStrict OK:',
      'location_id =', locationId
    );

    // --- 4. Работа с wms.stock (client_id + sku_id + location_id, без updated_at)
    console.log('[/receiving/accept] INSERT/UPDATE wms.stock...');

    const stockRes = await client.query(
      `
      SELECT qty
      FROM wms.stock
      WHERE client_id   = $1
        AND sku_id      = $2
        AND location_id = $3
      LIMIT 1
      FOR UPDATE
      `,
      [clientIdNum, skuId, locationId]
    );

    let currentQty = 0;
    if (stockRes.rowCount > 0) {
      currentQty = Number(stockRes.rows[0].qty) || 0;
    }

    const newQty   = currentQty + qtyNum;
    const qtyDelta = qtyNum; // приёмка → всегда плюс

    if (stockRes.rowCount === 0) {
      // Вставка НОВОЙ строки
      await client.query(
        `
        INSERT INTO wms.stock
          (client_id, barcode, sku_id, location_id, qty, created_at)
        VALUES
          ($1,        $2,      $3,   $4,          $5,  NOW())
        `,
        [clientIdNum, barcodeStr, skuId, locationId, newQty]
      );
    } else {
      // Обновление существующей строки
      await client.query(
        `
        UPDATE wms.stock
           SET qty = $1
         WHERE client_id   = $2
           AND sku_id      = $3
           AND location_id = $4
        `,
        [newQty, clientIdNum, skuId, locationId]
      );
    }

    // --- 5. Запись движения
    await client.query(
      `
      INSERT INTO wms.movements
        (created_at, user_id, client_id, sku_id, barcode, qty,
         from_location, to_location, movement_type, ref_type, ref_id, comment)
      VALUES
        (NOW(), $1, $2, $3, $4, $5,
         $6,     $7, $8, $9, $10, $11)
      `,
      [
        req.user.id,
        clientIdNum,
        skuId,
        barcodeStr,
        qtyDelta,
        null,        // from_location — приход "с улицы"
        locCode,     // to_location
        'incoming',  // movement_type
        'receiving', // ref_type
        null,        // ref_id (номер поставки потом добавим)
        null,        // comment
      ]
    );

    await client.query('COMMIT');

    return res.json({
      status: 'ok',
      filters: {
        client_id: clientIdNum,
        barcode: barcodeStr,
        location_code: locCode,
      },
      stock_before: {
        client_id: clientIdNum,
        barcode: barcodeStr,
        location_code: locCode,
        qty: currentQty,
      },
      stock_after: {
        client_id: clientIdNum,
        barcode: barcodeStr,
        location_code: locCode,
        qty: newQty,
      },
    });
  } catch (err) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    console.error('[/receiving/accept] ERROR:', err, 'code =', err.code);
    const status = (err.code === 'ITEM_NOT_FOUND' || err.code === 'ITEM_INACTIVE')
      ? 400
      : 500;

    return res.status(status).json({
      error: status === 400 ? err.code : 'Receiving accept error',
      message: err.message,
      code: err.code,
    });
  } finally {
    client.release();
  }
});


async function getLocationIdByCode(db, locationCode) {
  const code = String(locationCode || '').trim();
  if (!code) throw new Error('Пустой код ячейки');

  // 1) пробуем masterdata.locations
  let r = await db.query(
    `SELECT id FROM masterdata.locations WHERE code=$1 AND is_active=true LIMIT 1`,
    [code]
  );
  if (r.rowCount) return r.rows[0].id;

  // 2) fallback на wms.locations (если вдруг где-то оно ещё живое)
  r = await db.query(
    `SELECT id FROM wms.locations WHERE (location_code=$1 OR code=$1) AND is_active=true LIMIT 1`,
    [code]
  );
  if (r.rowCount) return r.rows[0].id;

  throw new Error(`Не найдена ячейка: ${code}`);
}


// -------------------------
// POST /picking/confirm
// -------------------------
app.post(
  '/picking/confirm',
  authRequired,
  requireRole('owner'),
  async (req, res) => {
    const client = await pool.connect();

    try {
      const { picking_task_id, comment } = req.body || {};

      const taskId = Number(picking_task_id);
      if (!Number.isInteger(taskId) || taskId <= 0) {
        return res.status(400).json({ error: 'Некорректный picking_task_id' });
      }

      await client.query('BEGIN');

      // 1) Берём задачу на сборку с блокировкой
      const taskRes = await client.query(
        `
        SELECT
          id, client_id, barcode, sku_id, location_code,
          qty, status, priority, order_ref, comment AS task_comment,
          shipment_code
        FROM wms.picking_tasks
        WHERE id = $1
        FOR UPDATE
        `,
        [taskId]
      );

      if (taskRes.rowCount === 0) {
        await client.query('ROLLBACK');
        return res.status(404).json({ error: 'Задача на сборку не найдена' });
      }

      const task = taskRes.rows[0];

      if (task.status !== 'in_progress') {
        await client.query('ROLLBACK');
        return res.status(409).json({
          error: 'Подтвердить можно только задачу в статусе in_progress',
          current_status: task.status,
        });
      }

      const clientIdNum = Number(task.client_id);
      const barcodeStr = String(task.barcode).trim();
      let locCode = task.location_code ? String(task.location_code).trim() : null;
      const qtyToPick = Number(task.qty);
      const shipmentCode = task.shipment_code ? String(task.shipment_code).trim() : null;

      if (!Number.isInteger(qtyToPick) || qtyToPick <= 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({ error: 'Некорректное qty в задаче на сборку' });
      }

      if (!shipmentCode) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: 'В задаче на сборку пустой shipment_code — не могу привязать сборку к отгрузке',
        });
      }

      // 2) sku_id
      let skuIdFinal;
      if (task.sku_id !== null && task.sku_id !== undefined) {
        skuIdFinal = Number(task.sku_id);
        if (!Number.isInteger(skuIdFinal) || skuIdFinal <= 0) {
          throw new Error('Некорректный sku_id в задаче на сборку');
        }
      } else {
        skuIdFinal = await resolveSkuIdOrCreate(client, {
          client_id: clientIdNum,
          barcode: barcodeStr,
        });
      }

      // 3) Определяем location_id
      let locationId;

      if (locCode) {
        locationId = await getLocationIdByCode(client, clientIdNum, locCode);
      } else {
        const locRes = await client.query(
          `
          SELECT s.location_id, l.code
          FROM wms.stock s
          JOIN wms.locations l ON l.id = s.location_id
          WHERE s.client_id = $1
            AND s.sku_id    = $2
            AND s.qty       > 0
          ORDER BY s.qty DESC, l.code
          LIMIT 1
          `,
          [clientIdNum, skuIdFinal]
        );

        if (locRes.rowCount === 0) {
          await client.query('ROLLBACK');
          return res.status(400).json({
            error:
              'Не найдена ячейка с остатком для этого SKU. ' +
              'location_code в задаче пустой, и в stock нет записей.',
          });
        }

        locationId = locRes.rows[0].location_id;
        locCode = locRes.rows[0].code;

        await client.query(
          `
          UPDATE wms.picking_tasks
          SET location_code = $1
          WHERE id = $2
          `,
          [locCode, taskId]
        );
      }

      // 4) Берём остаток и списываем по (location_id, sku_id)
      const stockRes = await client.query(
        `
        SELECT qty
        FROM wms.stock
        WHERE location_id = $1 AND sku_id = $2
        FOR UPDATE
        `,
        [locationId, skuIdFinal]
      );

      if (stockRes.rowCount === 0) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error:
            'Невозможно списать при сборке: в выбранной ячейке нет остатка по этому SKU',
        });
      }

      const currentQty = Number(stockRes.rows[0].qty || 0);
      if (currentQty < qtyToPick) {
        await client.query('ROLLBACK');
        return res.status(400).json({
          error: 'Недостаточно остатка для списания при сборке',
          current_qty: currentQty,
          required_qty: qtyToPick,
        });
      }

      const newQty = currentQty - qtyToPick;

      await client.query(
        `
        UPDATE wms.stock
        SET qty = $1
        WHERE location_id = $2 AND sku_id = $3
        `,
        [newQty, locationId, skuIdFinal]
      );

      // 5) Движение writeoff
      await client.query(
        `
        INSERT INTO wms.movements
          (created_at, user_id, client_id, sku_id, barcode,
           qty, from_location, to_location, movement_type,
           ref_type, ref_id, comment)
        VALUES
          (NOW(), $1, $2, $3, $4,
           $5, $6, $7, $8,
           $9, $10, $11)
        `,
        [
          req.user.id,
          clientIdNum,
          skuIdFinal,
          barcodeStr,
          qtyToPick,
          locCode,
          null,
          'writeoff',
          'picking_task',
          taskId,
          comment || task.task_comment || null,
        ]
      );

      // 6) Задача -> done
      const updRes = await client.query(
        `
        UPDATE wms.picking_tasks
        SET
          status       = 'done',
          comment      = COALESCE($1, comment),
          sku_id       = $2,
          updated_at   = NOW(),
          updated_by   = $3
        WHERE id = $4
        RETURNING
          id, client_id, barcode, sku_id, location_code,
          qty, status, priority, order_ref, comment,
          shipment_code,
          created_at, created_by, updated_at, updated_by
        `,
        [comment || null, skuIdFinal, req.user.id, taskId]
      );

      const updatedTask = updRes.rows[0];

      // -------------------------
      // 6.1) КЛЮЧЕВОЙ ФИКС:
      // если по отгрузке больше нет незавершённых picking_tasks,
      // переводим shipment в status='packing'
      // + обновляем total_picked_qty/total_planned_qty
      // -------------------------
      let shipmentMovedToPacking = false;

      // берём отгрузку под блокировку (по external_id = WG-код)
      const shRes = await client.query(
        `
        SELECT id, status
        FROM wms.shipments
        WHERE client_id = $1
          AND external_id = $2
        FOR UPDATE
        `,
        [clientIdNum, shipmentCode]
      );

      if (shRes.rowCount > 0) {
        // считаем прогресс по сборке в рамках отгрузки
        const progRes = await client.query(
          `
          SELECT
            COALESCE(SUM(qty),0)::int AS total_planned_qty,
            COALESCE(SUM(CASE WHEN status = 'done' THEN qty ELSE 0 END),0)::int AS total_picked_qty,
            COALESCE(SUM(CASE WHEN status <> 'done' THEN 1 ELSE 0 END),0)::int AS not_done_cnt
          FROM wms.picking_tasks
          WHERE client_id = $1
            AND shipment_code = $2
          `,
          [clientIdNum, shipmentCode]
        );

        const totalPlannedQty = Number(progRes.rows[0].total_planned_qty || 0);
        const totalPickedQty = Number(progRes.rows[0].total_picked_qty || 0);
        const notDoneCnt = Number(progRes.rows[0].not_done_cnt || 0);

        // обновляем цифры в shipments всегда
        await client.query(
          `
          UPDATE wms.shipments
          SET
            total_planned_qty = $1,
            total_picked_qty  = $2,
            updated_at        = NOW()
          WHERE client_id = $3
            AND external_id = $4
          `,
          [totalPlannedQty, totalPickedQty, clientIdNum, shipmentCode]
        );

        // если все сборки done — переводим в packing (НО не откатываем назад)
        const currentShipmentStatus = String(shRes.rows[0].status || '').trim();

        if (notDoneCnt === 0 && ['new', 'picking'].includes(currentShipmentStatus)) {
          await client.query(
            `
            UPDATE wms.shipments
            SET
              status     = 'packing',
              updated_at = NOW()
            WHERE client_id = $1
              AND external_id = $2
            `,
            [clientIdNum, shipmentCode]
          );
          shipmentMovedToPacking = true;
        }
      }

      // 7) фиксируем транзакцию по сборке
      await client.query('COMMIT');

      // 8) Автоматически создаём задачу на упаковку
      // Важно: чтобы не плодить дубликаты — создаём только когда реально перевели shipment в packing
      if (shipmentMovedToPacking) {
        console.log('[picking/confirm] call autoCreatePackingTaskForShipment', {
          client_id: updatedTask.client_id,
          shipment_code: updatedTask.shipment_code,
          user_id: req.user.id,
        });

        autoCreatePackingTaskForShipment(
          updatedTask.client_id,
          updatedTask.shipment_code,
          req.user.id
        ).catch(err => {
          console.error('autoCreatePackingTaskForShipment error:', err);
        });
      } else {
        console.log('[picking/confirm] packing task not created (shipment not moved to packing yet)', {
          client_id: updatedTask.client_id,
          shipment_code: updatedTask.shipment_code,
        });
      }

      return res.json({
        status: 'ok',
        task: updatedTask,
        shipment_moved_to_packing: shipmentMovedToPacking,
        stock_after: {
          location_code: locCode,
          sku_id: skuIdFinal,
          qty: newQty,
        },
      });
    } catch (err) {
      try {
        await client.query('ROLLBACK');
      } catch (_) {}

      console.error('Picking confirm error:', err);
      return res.status(500).json({
        error: 'Picking confirm error',
        detail: err.message,
        code: err.code,
      });
    } finally {
      client.release();
    }
  }
);

// -------------------------
// ADMIN: очистка WB / заданий
// -------------------------

// Очистить заказы WB (mp_wb_orders)
app.post(
  '/admin/cleanup/wb-orders',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      await pool.query(
        'TRUNCATE TABLE public.mp_wb_orders RESTART IDENTITY CASCADE;'
      );

      res.json({
        ok: true,
        message: 'Заказы WB очищены (mp_wb_orders).',
      });
    } catch (err) {
      console.error('admin/cleanup/wb-orders error:', err);
      res.status(500).json({
        message: 'Ошибка очистки заказов WB.',
      });
    }
  }
);

// Очистить задания на сборку (picking_tasks + движения по ним)
app.post(
  '/admin/cleanup/picking-tasks',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      await pool.query('TRUNCATE TABLE wms.picking_tasks RESTART IDENTITY;');
      await pool.query(
        "DELETE FROM wms.movements WHERE ref_type = 'picking_task';"
      );

      res.json({
        ok: true,
        message:
          'Задания на сборку (wms.picking_tasks) и движения ref_type = picking_task очищены.',
      });
    } catch (err) {
      console.error('admin/cleanup/picking-tasks error:', err);
      res.status(500).json({
        message: 'Ошибка очистки заданий на сборку.',
      });
    }
  }
);

// вспомогательная функция: очистка отгрузок/упаковки
async function cleanupShipmentsAndMovements() {
  // порядок важен: сначала линии, потом шапка
  await pool.query('TRUNCATE TABLE wms.shipment_lines RESTART IDENTITY;');
  await pool.query('TRUNCATE TABLE wms.shipments RESTART IDENTITY;');

  // движения по упаковке/отгрузке
  await pool.query(
    `
    DELETE FROM wms.movements
    WHERE ref_type = 'shipment'
       OR movement_type IN ('packing', 'shipping');
  `
  );
}

// Очистить задания на упаковку
app.post(
  '/admin/cleanup/packing-tasks',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      await cleanupShipmentsAndMovements();

      res.json({
        ok: true,
        message:
          'Задания на упаковку (wms.shipments / wms.shipment_lines) и движения packing/shipping очищены.',
      });
    } catch (err) {
      console.error('admin/cleanup/packing-tasks error:', err);
      res.status(500).json({
        message: 'Ошибка очистки заданий на упаковку.',
      });
    }
  }
);

// Очистить задания на отгрузку
// (по сути то же самое, просто отдельная кнопка для удобства)
app.post(
  '/admin/cleanup/shipping-tasks',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      await cleanupShipmentsAndMovements();

      res.json({
        ok: true,
        message:
          'Задания на отгрузку и связанные движения очищены (shipments / shipment_lines / movements).',
      });
    } catch (err) {
      console.error('admin/cleanup/shipping-tasks error:', err);
      res.status(500).json({
        message: 'Ошибка очистки заданий на отгрузку.',
      });
    }
  }
);
	
// ---------------- MOVEMENTS (WMS.MOVEMENTS) ----------------
// GET /movements
// Фильтры (все опциональны):
//   - client_id        (int > 0)
//   - barcode          (string)
//   - sku_id           (int > 0)
//   - movement_type    (incoming | move | writeoff | inventory | picking | adjust)
//   - ref_type         (любой строковый идентификатор, в т.ч. 'picking_task')
//   - ref_id           (int > 0)
//   - location_code    (from_location ИЛИ to_location = код)
//   - date_from        (YYYY-MM-DD или ISO, по DATE(m.created_at) >= date_from)
//   - date_to          (YYYY-MM-DD или ISO, по DATE(m.created_at) <= date_to)
//   - limit            (по умолчанию 500, максимум 5000)
//   - offset           (по умолчанию 0)
app.get('/movements', authRequired, requireRole('owner'), async (req, res) => {
  console.log('*** /movements NEW HANDLER HIT ***', req.originalUrl);
  try {
    // остальной код...

    const {
      client_id,
      barcode,
      sku_id,
      movement_type,
      ref_type,
      ref_id,
      location_code,
      date_from,
      date_to,
      limit,
      offset,
    } = req.query || {};

    const conditions = [];
    const values = [];
    let idx = 1;

    // client_id
    let clientIdNum = null;
    if (client_id !== undefined) {
      clientIdNum = Number(client_id);
      if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
        return res.status(400).json({ error: 'client_id должен быть положительным целым числом' });
      }
      conditions.push(`m.client_id = $${idx}`);
      values.push(clientIdNum);
      idx++;
    }

    // barcode
    let barcodeFilter = null;
    if (barcode !== undefined) {
      const b = String(barcode).trim();
      if (!b) {
        return res.status(400).json({ error: 'barcode не может быть пустым, если передан' });
      }
      barcodeFilter = b;
      conditions.push(`m.barcode = $${idx}`);
      values.push(b);
      idx++;
    }

    // sku_id
    let skuIdNum = null;
    if (sku_id !== undefined) {
      skuIdNum = Number(sku_id);
      if (!Number.isInteger(skuIdNum) || skuIdNum <= 0) {
        return res.status(400).json({ error: 'sku_id должен быть положительным целым числом' });
      }
      conditions.push(`m.sku_id = $${idx}`);
      values.push(skuIdNum);
      idx++;
    }

    // movement_type — нормализуем и проверяем по validMovementTypes
    let movementTypeUsed = null;
    if (movement_type !== undefined) {
      const mtNorm = validateMovementType(movement_type); // уже приводит к нижнему регистру
      if (!mtNorm || !validMovementTypes.includes(mtNorm)) {
        return res.status(400).json({
          error: `Некорректный movement_type. Допустимо: ${validMovementTypes.join(', ')}`,
        });
      }
      movementTypeUsed = mtNorm;
      conditions.push(`m.movement_type = $${idx}`);
      values.push(mtNorm);
      idx++;
    }

    // ref_type — фильтруем по фактическому значению в movements, НЕ нормализуем через normalizeRefType
    let refTypeUsed = null;
    if (ref_type !== undefined) {
      const rt = String(ref_type).trim().toLowerCase();
      if (!rt) {
        return res.status(400).json({ error: 'ref_type не может быть пустым, если передан' });
      }
      refTypeUsed = rt;
      conditions.push(`m.ref_type = $${idx}`);
      values.push(rt);
      idx++;
    }

    // ref_id
    let refIdUsed = null;
    if (ref_id !== undefined) {
      const rid = Number(ref_id);
      if (!Number.isInteger(rid) || rid <= 0) {
        return res.status(400).json({ error: 'ref_id должен быть положительным целым числом' });
      }
      refIdUsed = rid;
      conditions.push(`m.ref_id = $${idx}`);
      values.push(rid);
      idx++;
    }

    // location_code — либо from_location, либо to_location
    let locationCodeFilter = null;
    if (location_code !== undefined) {
      const loc = String(location_code).trim();
      if (!loc) {
        return res.status(400).json({ error: 'location_code не может быть пустым, если передан' });
      }
      locationCodeFilter = loc;
      conditions.push(`(m.from_location = $${idx} OR m.to_location = $${idx})`);
      values.push(loc);
      idx++;
    }

    // date_from / date_to — через DATE(m.created_at), чтобы совпадать с отчётами
    const fromYmd = normalizeDateParamToYmd(date_from);
    const toYmd   = normalizeDateParamToYmd(date_to);
    let usedFrom = null;
    let usedTo   = null;

    if (date_from !== undefined) {
      if (!fromYmd) {
        return res.status(400).json({
          error: 'date_from должен быть в формате YYYY-MM-DD или ISO с этой датой',
        });
      }
      usedFrom = fromYmd;
      conditions.push(`DATE(m.created_at) >= $${idx}::date`);
      values.push(fromYmd);
      idx++;
    }

    if (date_to !== undefined) {
      if (!toYmd) {
        return res.status(400).json({
          error: 'date_to должен быть в формате YYYY-MM-DD или ISO с этой датой',
        });
      }
      usedTo = toYmd;
      conditions.push(`DATE(m.created_at) <= $${idx}::date`);
      values.push(toYmd);
      idx++;
    }

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';

    // limit / offset
    let limitNum = Number(limit || 500);
    if (!Number.isInteger(limitNum) || limitNum <= 0) limitNum = 500;
    if (limitNum > 5000) limitNum = 5000;

    let offsetNum = Number(offset || 0);
    if (!Number.isInteger(offsetNum) || offsetNum < 0) offsetNum = 0;

    const sql = `
      SELECT
        m.id,
        m.created_at,
        m.user_id,
        m.client_id,
        m.sku_id,
        m.barcode,
        m.qty,
        m.from_location,
        m.to_location,
        m.movement_type,
        m.ref_type,
        m.ref_id,
        m.comment
      FROM wms.movements m
      ${where}
      ORDER BY m.created_at DESC, m.id DESC
      LIMIT ${limitNum}
      OFFSET ${offsetNum}
    `;

    console.log('[MOVEMENTS] WHERE:', where || '(no where)', 'VALUES:', values);

    const r = await pool.query(sql, values);

    return res.json({
      status: 'ok',
      client_id: clientIdNum || null,
      filters: {
        barcode: barcodeFilter || null,
        sku_id: skuIdNum || null,
        movement_type: movementTypeUsed || null,
        ref_type: refTypeUsed || null,
        ref_id: refIdUsed || null,
        location_code: locationCodeFilter || null,
        date_from: usedFrom || (date_from || null),
        date_to: usedTo || (date_to || null),
      },
      limit: limitNum,
      offset: offsetNum,
      rows: r.rows,
    });
  } catch (err) {
    console.error('Get movements error:', err);
    return res.status(500).json({
      error: 'Get movements error',
      detail: err.message,
      code: err.code,
    });
  }
});


// Утилита: нормализуем параметр даты в формат 'YYYY-MM-DD' без сюрпризов с часовыми поясами
function normalizeDateParamToYmd(param) {
  if (param === undefined || param === null) return null;

  const raw = String(param).trim();
  if (!raw) return null;

  // Если простой формат 'YYYY-MM-DD' — считаем, что это локальная "календарная" дата и отдаем как есть
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return raw;
  }

  // Иначе пробуем распарсить как дату/ISO
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) {
    return null;
  }

  // Берём UTC дату, это уже "лучше чем ничего" для экзотических форматов
  return d.toISOString().slice(0, 10);
}






// ==============================
//  REPORTS: ТЕСТОВЫЕ МАРШРУТЫ
// ==============================

// Простейший пинг без auth
app.get('/reports/ping', (req, res) => {
  console.log('HIT /reports/ping, path =', req.path, 'originalUrl =', req.originalUrl);
  return res.json({ status: 'ok', path: req.path, originalUrl: req.originalUrl });
});


// ==============================
//  REPORTS: PICKING SUMMARY
//  GET /reports/picking-summary
// ==============================
//
// Параметры:
//   client_id  (обязательный, положительный int)
//   picker_id  (опциональный, положительный int)
//   date_from  (опциональный, YYYY-MM-DD или ISO — фильтр по DATE(m.created_at) >= date_from)
//   date_to    (опциональный, YYYY-MM-DD или ISO — фильтр по DATE(m.created_at) <= date_to)
//

app.get('/reports/picking-summary', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const {
      client_id,
      picker_id,
      date_from,
      date_to,
    } = req.query || {};

    console.log('INCOMING REPORT picking-summary:', req.originalUrl);

    // ---- client_id (обязательный) ----
    const clientIdNum = Number(client_id);
    if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
      return res.status(400).json({ error: 'client_id обязателен и должен быть положительным целым числом' });
    }

    const conditions = [
      'm.client_id = $1',
      "m.movement_type = 'writeoff'",
      "m.ref_type = 'picking_task'",
    ];
    const values = [clientIdNum];
    let idx = 2;

    // ---- picker_id (опциональный) ----
    let pickerIdNum = null;
    if (picker_id !== undefined) {
      pickerIdNum = Number(picker_id);
      if (!Number.isInteger(pickerIdNum) || pickerIdNum <= 0) {
        return res.status(400).json({ error: 'picker_id должен быть положительным целым числом' });
      }
      conditions.push(`m.user_id = $${idx}`);
      values.push(pickerIdNum);
      idx++;
    }

    // ---- date_from / date_to (по DATE(m.created_at)) ----
    let dateFromStr = null;
    let dateToStr   = null;

    if (date_from !== undefined) {
      const rawFrom = String(date_from).trim();
      const df = rawFrom.slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(df)) {
        return res.status(400).json({ error: 'date_from должен быть в формате YYYY-MM-DD или ISO с этой датой' });
      }
      dateFromStr = df;
      conditions.push(`DATE(m.created_at) >= $${idx}::date`);
      values.push(df);
      idx++;
    }

    if (date_to !== undefined) {
      const rawTo = String(date_to).trim();
      const dt = rawTo.slice(0, 10);
      if (!/^\d{4}-\d{2}-\d{2}$/.test(dt)) {
        return res.status(400).json({ error: 'date_to должен быть в формате YYYY-MM-DD или ISO с этой датой' });
      }
      dateToStr = dt;
      conditions.push(`DATE(m.created_at) <= $${idx}::date`);
      values.push(dt);
      idx++;
    }

    const whereSql = `WHERE ${conditions.join(' AND ')}`;

    const sql = `
      SELECT
        TO_CHAR(DATE(m.created_at), 'YYYY-MM-DD') AS pick_date,
        m.user_id                                 AS picker_id,
        COUNT(DISTINCT m.ref_id)::int             AS tasks_count,
        SUM(m.qty)::int                           AS items_picked,
        AVG(m.qty::numeric)                       AS avg_items_per_task,
        COALESCE(
          SUM(m.qty * COALESCE(i.processing_fee, 0)),
          0
        ) AS total_processing_fee
      FROM wms.movements m
      LEFT JOIN masterdata.items i
        ON i.id = m.sku_id
      ${whereSql}
      GROUP BY TO_CHAR(DATE(m.created_at), 'YYYY-MM-DD'), m.user_id
      ORDER BY pick_date ASC, picker_id ASC
    `;

    console.log('[REPORT picking-summary] WHERE:', whereSql, 'VALUES:', values);

    const r = await pool.query(sql, values);

    return res.json({
      status: 'ok',
      client_id: clientIdNum,
      filters: {
        picker_id: pickerIdNum,
        date_from: dateFromStr,
        date_to: dateToStr,
      },
      rows: r.rows.map((row) => ({
        date: row.pick_date,  // 'YYYY-MM-DD'
        picker_id: row.picker_id,
        tasks_count: Number(row.tasks_count) || 0,
        items_picked: Number(row.items_picked) || 0,
        avg_items_per_task: Number(row.avg_items_per_task) || 0,
        total_processing_fee: row.total_processing_fee || '0',
      })),
    });
  } catch (err) {
    console.error('Error in GET /reports/picking-summary:', err);
    return res.status(500).json({
      error: 'Internal server error in /reports/picking-summary',
      detail: err.message,
      code: err.code,
    });
  }
});

// ==============================
//  REPORTS: PICKING ROWS (детализация движений)
//  GET /reports/picking-rows
// ==============================
//
// Параметры:
//   client_id  (обязательный, положительный int)
//   picker_id  (опциональный, положительный int)
//   date_from  (опциональный, YYYY-MM-DD, DATE(m.created_at) >= date_from)
//   date_to    (опциональный, YYYY-MM-DD, DATE(m.created_at) <= date_to)
//
app.get('/reports/picking-rows', authRequired, requireRole('owner'), async (req, res) => {
  try {
    const {
      client_id,
      picker_id,
      date_from,
      date_to,
    } = req.query || {};

    // ---- client_id (обязательный) ----
    const clientIdNum = Number(client_id);
    if (!Number.isInteger(clientIdNum) || clientIdNum <= 0) {
      return res.status(400).json({ error: 'client_id обязателен и должен быть положительным целым числом' });
    }

    const conditions = [
      'm.client_id = $1',
      "m.movement_type = 'writeoff'",
      "m.ref_type = 'picking_task'",
    ];
    const values = [clientIdNum];
    let idx = 2;

    // ---- picker_id (опциональный) ----
    let pickerIdNum = null;
    if (picker_id !== undefined) {
      pickerIdNum = Number(picker_id);
      if (!Number.isInteger(pickerIdNum) || pickerIdNum <= 0) {
        return res.status(400).json({ error: 'picker_id должен быть положительным целым числом' });
      }
      conditions.push(`m.user_id = $${idx}`);
      values.push(pickerIdNum);
      idx++;
    }

    // ---- date_from / date_to (YYYY-MM-DD, по DATE(m.created_at)) ----
    let dateFromStr = null;
    let dateToStr   = null;

    if (date_from !== undefined) {
      const dfStr = String(date_from).trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(dfStr)) {
        return res.status(400).json({ error: 'date_from должен быть в формате YYYY-MM-DD' });
      }
      dateFromStr = dfStr;
      conditions.push(`DATE(m.created_at) >= $${idx}::date`);
      values.push(dfStr);
      idx++;
    }

    if (date_to !== undefined) {
      const dtStr = String(date_to).trim();
      if (!/^\d{4}-\d{2}-\d{2}$/.test(dtStr)) {
        return res.status(400).json({ error: 'date_to должен быть в формате YYYY-MM-DD' });
      }
      dateToStr = dtStr;
      conditions.push(`DATE(m.created_at) <= $${idx}::date`);
      values.push(dtStr);
      idx++;
    }

    const whereSql = `WHERE ${conditions.join(' AND ')}`;

    console.log('[REPORT picking-rows] WHERE:', whereSql, 'VALUES:', values);

    const sql = `
      SELECT
        m.id,
        m.created_at,
        DATE(m.created_at)          AS pick_date,
        m.user_id                   AS picker_id,
        m.client_id,
        m.sku_id,
        m.barcode,
        m.qty,
        m.from_location,
        m.to_location,
        m.movement_type,
        m.ref_type,
        m.ref_id,
        m.comment
      FROM wms.movements m
      ${whereSql}
      ORDER BY pick_date ASC, m.user_id ASC, m.id ASC
    `;

    const r = await pool.query(sql, values);

    return res.json({
      status: 'ok',
      client_id: clientIdNum,
      filters: {
        picker_id: pickerIdNum,
        date_from: dateFromStr,
        date_to: dateToStr,
      },
      rows: r.rows.map((row) => ({
        id: row.id,
        created_at: row.created_at,
        date: row.pick_date,          // 'YYYY-MM-DD'
        picker_id: row.picker_id,
        client_id: row.client_id,
        sku_id: row.sku_id,
        barcode: row.barcode,
        qty: row.qty,
        from_location: row.from_location,
        to_location: row.to_location,
        movement_type: row.movement_type,
        ref_type: row.ref_type,
        ref_id: row.ref_id,
        comment: row.comment,
      })),
    });
  } catch (err) {
    console.error('Error in GET /reports/picking-rows:', err);
    return res.status(500).json({
      error: 'Internal server error in /reports/picking-rows',
      detail: err.message,
      code: err.code,
    });
  }
});


// DEBUG: печать всех зарегистрированных маршрутов (Express 4/5 safe)
function printRoutesSafe(appInstance) {
  console.log('Registered routes:');

  // Express 4 обычно: app._router.stack
  // Express 5 может быть: app.router.stack
  const stack =
    (appInstance && appInstance._router && appInstance._router.stack) ||
    (appInstance && appInstance.router && appInstance.router.stack) ||
    null;

  if (!stack) {
    console.log('  (router stack is not available in this Express version / state)');
    return;
  }

  stack
    .filter((layer) => layer && layer.route && layer.route.path)
    .forEach((layer) => {
      const methods = layer.route.methods
        ? Object.keys(layer.route.methods).map((m) => m.toUpperCase()).join(',')
        : '';
      console.log(`  ${methods} ${layer.route.path}`);
    });
}


// ==============================
// DEBUG: список зарегистрированных роутов
// GET /debug/routes
// ==============================
app.get('/debug/routes', (req, res) => {
  const list = [];

  if (app._router && app._router.stack) {
    app._router.stack.forEach((layer) => {
      if (layer.route && layer.route.path) {
        const methods = Object.keys(layer.route.methods)
          .filter((m) => layer.route.methods[m])
          .map((m) => m.toUpperCase());
        list.push({
          path: layer.route.path,
          methods,
        });
      }
    });
  }

  res.json({ routes: list });
});


/// ---------------- ЗАПУСК СЕРВЕРА ----------------
const PORT = Number(process.env.PORT || 3000);

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server запущен на http://0.0.0.0:${PORT}`);

  // Печать маршрутов после старта (safe)
  try {
    printRoutesSafe(app);
  } catch (e) {
    console.log('printRoutesSafe failed:', e?.message || e);
  }
});
