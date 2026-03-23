const express = require('express');
const router = express.Router();

const db = require('../db');
const pool = db.pool || db;

const { authRequired, requireRole } = require('../authMiddleware');

function toBool(value, defaultValue = false) {
  if (value === undefined || value === null || value === '') return defaultValue;
  if (typeof value === 'boolean') return value;
  if (typeof value === 'string') {
    const v = value.trim().toLowerCase();
    if (['true', '1', 'yes', 'y'].includes(v)) return true;
    if (['false', '0', 'no', 'n'].includes(v)) return false;
  }
  return Boolean(value);
}

function toIntOrNull(value) {
  if (value === undefined || value === null || value === '') return null;
  const n = Number(value);
  return Number.isInteger(n) ? n : null;
}

// -------------------------------------------------------
// DEBUG LOG ДЛЯ PRINTERS
// -------------------------------------------------------
router.use((req, res, next) => {
  console.log('[ROUTES/PRINTERS]');
  console.log('method:', req.method);
  console.log('originalUrl:', req.originalUrl);
  console.log('content-type:', req.headers['content-type']);
  console.log('req.body:', req.body);
  next();
});

// Получить все принтеры
router.get('/', authRequired, async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT *
      FROM wms.printers
      ORDER BY id
    `);

    return res.json({
      status: 200,
      ok: true,
      data: result.rows
    });
  } catch (err) {
    console.error('GET /printers error:', err);
    return res.status(500).json({
      status: 500,
      ok: false,
      error: err.message
    });
  }
});

// Получить один принтер
router.get('/:id', authRequired, async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({
        status: 400,
        ok: false,
        error: 'Некорректный id принтера'
      });
    }

    const result = await pool.query(
      `
      SELECT *
      FROM wms.printers
      WHERE id = $1
      `,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        status: 404,
        ok: false,
        error: 'Принтер не найден'
      });
    }

    return res.json({
      status: 200,
      ok: true,
      data: result.rows[0]
    });
  } catch (err) {
    console.error('GET /printers/:id error:', err);
    return res.status(500).json({
      status: 500,
      ok: false,
      error: err.message
    });
  }
});

// Добавить принтер
router.post('/', authRequired, requireRole(['owner', 'admin']), async (req, res) => {
  try {
    console.log('POST /printers BODY RAW =', req.body);

    const {
      printer_code,
      printer_name,
      printer_type,
      connection_type,
      agent_code,
      device_name,
      ip_address,
      port,
      warehouse_code,
      zone_code,
      is_default,
      is_active,
      notes
    } = req.body || {};

    if (!req.body || typeof req.body !== 'object') {
      return res.status(400).json({
        status: 400,
        ok: false,
        error: 'Тело запроса не распознано как JSON',
        debug_body: req.body
      });
    }

    if (!printer_code || !String(printer_code).trim()) {
      return res.status(400).json({
        status: 400,
        ok: false,
        error: 'printer_code обязателен',
        debug_body: req.body
      });
    }

    if (!printer_name || !String(printer_name).trim()) {
      return res.status(400).json({
        status: 400,
        ok: false,
        error: 'printer_name обязателен',
        debug_body: req.body
      });
    }

    const duplicate = await pool.query(
      `
      SELECT id, printer_code
      FROM wms.printers
      WHERE printer_code = $1
      LIMIT 1
      `,
      [String(printer_code).trim()]
    );

    if (duplicate.rows.length > 0) {
      return res.status(409).json({
        status: 409,
        ok: false,
        error: 'Принтер с таким printer_code уже существует',
        data: duplicate.rows[0]
      });
    }

    const result = await pool.query(
      `
      INSERT INTO wms.printers (
        printer_code,
        printer_name,
        printer_type,
        connection_type,
        agent_code,
        device_name,
        ip_address,
        port,
        warehouse_code,
        zone_code,
        is_default,
        is_active,
        notes
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13
      )
      RETURNING *
      `,
      [
        String(printer_code).trim(),
        String(printer_name).trim(),
        printer_type ? String(printer_type).trim() : 'label',
        connection_type ? String(connection_type).trim() : 'agent',
        agent_code ? String(agent_code).trim() : null,
        device_name ? String(device_name).trim() : null,
        ip_address ? String(ip_address).trim() : null,
        toIntOrNull(port),
        warehouse_code ? String(warehouse_code).trim() : null,
        zone_code ? String(zone_code).trim() : null,
        toBool(is_default, false),
        toBool(is_active, true),
        notes ? String(notes).trim() : null
      ]
    );

    return res.status(201).json({
      status: 201,
      ok: true,
      data: result.rows[0]
    });
  } catch (err) {
    console.error('POST /printers error:', err);
    return res.status(500).json({
      status: 500,
      ok: false,
      error: err.message
    });
  }
});

// Обновить принтер
router.patch('/:id', authRequired, requireRole(['owner', 'admin']), async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({
        status: 400,
        ok: false,
        error: 'Некорректный id принтера'
      });
    }

    const payload = req.body || {};
    const allowedFields = [
      'printer_code',
      'printer_name',
      'printer_type',
      'connection_type',
      'agent_code',
      'device_name',
      'ip_address',
      'port',
      'warehouse_code',
      'zone_code',
      'is_default',
      'is_active',
      'notes'
    ];

    const fields = [];
    const values = [];

    for (const field of allowedFields) {
      if (Object.prototype.hasOwnProperty.call(payload, field)) {
        let value = payload[field];

        if ([
          'printer_code',
          'printer_name',
          'printer_type',
          'connection_type',
          'agent_code',
          'device_name',
          'ip_address',
          'warehouse_code',
          'zone_code',
          'notes'
        ].includes(field)) {
          value = value === null || value === '' ? null : String(value).trim();
        }

        if (field === 'port') {
          value = toIntOrNull(value);
        }

        if (field === 'is_default') {
          value = toBool(value, false);
        }

        if (field === 'is_active') {
          value = toBool(value, true);
        }

        values.push(value);
        fields.push(`${field} = $${values.length}`);
      }
    }

    if (fields.length === 0) {
      return res.status(400).json({
        status: 400,
        ok: false,
        error: 'Нет полей для обновления'
      });
    }

    if (Object.prototype.hasOwnProperty.call(payload, 'printer_code')) {
      const newPrinterCode = payload.printer_code === null ? null : String(payload.printer_code).trim();

      if (!newPrinterCode) {
        return res.status(400).json({
          status: 400,
          ok: false,
          error: 'printer_code не может быть пустым'
        });
      }

      const duplicate = await pool.query(
        `
        SELECT id
        FROM wms.printers
        WHERE printer_code = $1
          AND id <> $2
        LIMIT 1
        `,
        [newPrinterCode, id]
      );

      if (duplicate.rows.length > 0) {
        return res.status(409).json({
          status: 409,
          ok: false,
          error: 'Принтер с таким printer_code уже существует'
        });
      }
    }

    if (Object.prototype.hasOwnProperty.call(payload, 'printer_name')) {
      const newPrinterName = payload.printer_name === null ? null : String(payload.printer_name).trim();

      if (!newPrinterName) {
        return res.status(400).json({
          status: 400,
          ok: false,
          error: 'printer_name не может быть пустым'
        });
      }
    }

    values.push(id);

    const result = await pool.query(
      `
      UPDATE wms.printers
      SET ${fields.join(', ')},
          updated_at = now()
      WHERE id = $${values.length}
      RETURNING *
      `,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        status: 404,
        ok: false,
        error: 'Принтер не найден'
      });
    }

    return res.json({
      status: 200,
      ok: true,
      data: result.rows[0]
    });
  } catch (err) {
    console.error('PATCH /printers/:id error:', err);
    return res.status(500).json({
      status: 500,
      ok: false,
      error: err.message
    });
  }
});

// Удалить принтер
router.delete('/:id', authRequired, requireRole(['owner', 'admin']), async (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({
        status: 400,
        ok: false,
        error: 'Некорректный id принтера'
      });
    }

    const result = await pool.query(
      `
      DELETE FROM wms.printers
      WHERE id = $1
      RETURNING *
      `,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        status: 404,
        ok: false,
        error: 'Принтер не найден'
      });
    }

    return res.json({
      status: 200,
      ok: true,
      message: 'Принтер удалён',
      data: result.rows[0]
    });
  } catch (err) {
    console.error('DELETE /printers/:id error:', err);
    return res.status(500).json({
      status: 500,
      ok: false,
      error: err.message
    });
  }
});

module.exports = router;