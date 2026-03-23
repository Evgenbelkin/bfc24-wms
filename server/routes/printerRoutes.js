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


// -----------------------------------------------------
// GET /printer-routes
// -----------------------------------------------------

router.get('/', authRequired, async (req, res) => {
  try {

    const result = await pool.query(`
      SELECT
        pr.*,
        p.printer_code,
        p.printer_name
      FROM wms.printer_routes pr
      LEFT JOIN wms.printers p
        ON p.id = pr.printer_id
      ORDER BY pr.id
    `);

    return res.json({
      ok: true,
      data: result.rows
    });

  } catch (err) {

    console.error('GET /printer-routes error:', err);

    return res.status(500).json({
      ok: false,
      error: err.message
    });

  }
});


// -----------------------------------------------------
// POST /printer-routes
// -----------------------------------------------------

router.post('/', authRequired, requireRole(['owner','admin']), async (req,res) => {

  try {

    const {
      route_code,
      doc_type,
      warehouse_code,
      zone_code,
      client_id,
      printer_id,
      is_default,
      is_active,
      notes
    } = req.body || {};

    if (!route_code || !String(route_code).trim()) {
      return res.status(400).json({
        ok:false,
        error:'route_code обязателен'
      });
    }

    if (!doc_type || !String(doc_type).trim()) {
      return res.status(400).json({
        ok:false,
        error:'doc_type обязателен'
      });
    }

    if (!printer_id) {
      return res.status(400).json({
        ok:false,
        error:'printer_id обязателен'
      });
    }

    const result = await pool.query(

      `
      INSERT INTO wms.printer_routes
      (
        route_code,
        doc_type,
        warehouse_code,
        zone_code,
        client_id,
        printer_id,
        is_default,
        is_active,
        notes
      )
      VALUES
      (
        $1,$2,$3,$4,$5,$6,$7,$8,$9
      )
      RETURNING *
      `,

      [
        String(route_code).trim(),
        String(doc_type).trim(),
        warehouse_code ? String(warehouse_code).trim() : null,
        zone_code ? String(zone_code).trim() : null,
        toIntOrNull(client_id),
        Number(printer_id),
        toBool(is_default,false),
        toBool(is_active,true),
        notes ? String(notes).trim() : null
      ]

    );

    return res.json({
      ok:true,
      data:result.rows[0]
    });

  } catch(err) {

    console.error('POST /printer-routes error:',err);

    return res.status(500).json({
      ok:false,
      error:err.message
    });

  }

});


// -----------------------------------------------------
// PATCH /printer-routes/:id
// -----------------------------------------------------

router.patch('/:id', authRequired, requireRole(['owner','admin']), async (req,res) => {

  try {

    const id = Number(req.params.id);

    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({
        ok:false,
        error:'Некорректный id'
      });
    }

    const payload = req.body || {};

    const allowed = [
      'route_code',
      'doc_type',
      'warehouse_code',
      'zone_code',
      'client_id',
      'printer_id',
      'is_default',
      'is_active',
      'notes'
    ];

    const fields = [];
    const values = [];

    for (const field of allowed) {

      if (Object.prototype.hasOwnProperty.call(payload, field)) {

        let value = payload[field];

        if ([
          'route_code',
          'doc_type',
          'warehouse_code',
          'zone_code',
          'notes'
        ].includes(field)) {

          value = value === null || value === ''
            ? null
            : String(value).trim();

        }

        if (field === 'client_id') {
          value = toIntOrNull(value);
        }

        if (field === 'printer_id') {
          value = Number(value);
        }

        if (field === 'is_default') {
          value = toBool(value,false);
        }

        if (field === 'is_active') {
          value = toBool(value,true);
        }

        values.push(value);

        fields.push(`${field} = $${values.length}`);

      }

    }

    if (fields.length === 0) {
      return res.status(400).json({
        ok:false,
        error:'Нет полей для обновления'
      });
    }

    values.push(id);

    const result = await pool.query(

      `
      UPDATE wms.printer_routes
      SET ${fields.join(', ')},
          updated_at = now()
      WHERE id = $${values.length}
      RETURNING *
      `,

      values

    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        ok:false,
        error:'Маршрут не найден'
      });
    }

    return res.json({
      ok:true,
      data:result.rows[0]
    });

  } catch(err) {

    console.error('PATCH /printer-routes/:id error:',err);

    return res.status(500).json({
      ok:false,
      error:err.message
    });

  }

});


// -----------------------------------------------------
// DELETE /printer-routes/:id
// -----------------------------------------------------

router.delete('/:id', authRequired, requireRole(['owner','admin']), async (req,res)=>{

  try {

    const id = Number(req.params.id);

    if (!Number.isInteger(id) || id <= 0) {
      return res.status(400).json({
        ok:false,
        error:'Некорректный id'
      });
    }

    const result = await pool.query(

      `
      DELETE
      FROM wms.printer_routes
      WHERE id = $1
      RETURNING *
      `,

      [id]

    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        ok:false,
        error:'Маршрут не найден'
      });
    }

    return res.json({
      ok:true,
      message:'Deleted'
    });

  } catch(err) {

    console.error('DELETE /printer-routes/:id error:',err);

    return res.status(500).json({
      ok:false,
      error:err.message
    });

  }

});

module.exports = router;