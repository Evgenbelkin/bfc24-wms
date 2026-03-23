const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const { authRequired, requireRole } = require('../authMiddleware');

function normalizeLeadStatus(value) {
  const allowed = ['new', 'in_progress', 'waiting', 'qualified', 'won', 'lost', 'archive'];
  const v = String(value || '').trim().toLowerCase();
  return allowed.includes(v) ? v : null;
}

router.get(
  '/leads',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const status = normalizeLeadStatus(req.query.status);
      const q = String(req.query.q || '').trim();
      const dateFrom = String(req.query.date_from || '').trim();
      const dateTo = String(req.query.date_to || '').trim();

      const where = [];
      const params = [];
      let p = 1;

      if (status) {
        where.push(`l.status = $${p++}`);
        params.push(status);
      }

      if (q) {
        where.push(`(
          l.name ILIKE $${p}
          OR l.contact ILIKE $${p}
          OR COALESCE(l.comment, '') ILIKE $${p}
          OR COALESCE(l.next_action, '') ILIKE $${p}
          OR CAST(l.id AS TEXT) ILIKE $${p}
        )`);
        params.push(`%${q}%`);
        p++;
      }

      if (dateFrom) {
        where.push(`l.created_at >= $${p++}::timestamptz`);
        params.push(`${dateFrom} 00:00:00+03`);
      }

      if (dateTo) {
        where.push(`l.created_at <= $${p++}::timestamptz`);
        params.push(`${dateTo} 23:59:59+03`);
      }

      const sql = `
        SELECT
          l.id,
          l.name,
          l.contact,
          l.orders_volume,
          l.sku_count,
          l.comment,
          l.source,
          l.status,
          l.assigned_user_id,
          l.next_action,
          l.next_contact_at,
          l.processed_at,
          l.created_at,
          l.updated_at,
          NULL::text AS assigned_username,
          (
            SELECT slc.comment_text
            FROM public.site_lead_comments slc
            WHERE slc.lead_id = l.id
            ORDER BY slc.created_at DESC
            LIMIT 1
          ) AS last_comment
        FROM public.site_leads l
        ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
        ORDER BY l.created_at DESC
      `;

      const result = await pool.query(sql, params);
      return res.json({ ok: true, rows: result.rows });
    } catch (error) {
      console.error('[GET /leads] error:', error);
      return res.status(500).json({ error: 'Ошибка получения заявок' });
    }
  }
);

router.get(
  '/leads/:id',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const leadId = Number(req.params.id);
      if (!leadId) {
        return res.status(400).json({ error: 'Некорректный ID заявки' });
      }

      const sql = `
        SELECT
          l.id,
          l.name,
          l.contact,
          l.orders_volume,
          l.sku_count,
          l.comment,
          l.source,
          l.status,
          l.assigned_user_id,
          l.next_action,
          l.next_contact_at,
          l.processed_at,
          l.created_at,
          l.updated_at,
          NULL::text AS assigned_username
        FROM public.site_leads l
        WHERE l.id = $1
        LIMIT 1
      `;

      const result = await pool.query(sql, [leadId]);

      if (!result.rows.length) {
        return res.status(404).json({ error: 'Заявка не найдена' });
      }

      return res.json({ ok: true, row: result.rows[0] });
    } catch (error) {
      console.error('[GET /leads/:id] error:', error);
      return res.status(500).json({ error: 'Ошибка получения заявки' });
    }
  }
);

router.patch(
  '/leads/:id',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const leadId = Number(req.params.id);
      if (!leadId) {
        return res.status(400).json({ error: 'Некорректный ID заявки' });
      }

      const status = req.body.status !== undefined ? normalizeLeadStatus(req.body.status) : undefined;
      const assignedUserId = req.body.assigned_user_id !== undefined && req.body.assigned_user_id !== null
        ? Number(req.body.assigned_user_id)
        : req.body.assigned_user_id;
      const nextAction = req.body.next_action !== undefined ? String(req.body.next_action || '').trim() : undefined;
      const nextContactAt = req.body.next_contact_at !== undefined ? String(req.body.next_contact_at || '').trim() : undefined;

      if (req.body.status !== undefined && !status) {
        return res.status(400).json({ error: 'Некорректный статус' });
      }

      const updates = [];
      const params = [];
      let p = 1;

      if (status !== undefined) {
        updates.push(`status = $${p++}`);
        params.push(status);

        if (['won', 'lost', 'archive'].includes(status)) {
          updates.push(`processed_at = NOW()`);
        }
      }

      if (assignedUserId !== undefined) {
        if (assignedUserId === null || assignedUserId === '') {
          updates.push(`assigned_user_id = NULL`);
        } else if (Number.isFinite(assignedUserId) && assignedUserId > 0) {
          updates.push(`assigned_user_id = $${p++}`);
          params.push(assignedUserId);
        } else {
          return res.status(400).json({ error: 'Некорректный assigned_user_id' });
        }
      }

      if (nextAction !== undefined) {
        updates.push(`next_action = $${p++}`);
        params.push(nextAction || null);
      }

      if (nextContactAt !== undefined) {
        if (!nextContactAt) {
          updates.push(`next_contact_at = NULL`);
        } else {
          updates.push(`next_contact_at = $${p++}::timestamptz`);
          params.push(nextContactAt);
        }
      }

      updates.push(`updated_at = NOW()`);

      if (!updates.length) {
        return res.status(400).json({ error: 'Нет данных для обновления' });
      }

      params.push(leadId);

      const sql = `
        UPDATE public.site_leads
        SET ${updates.join(', ')}
        WHERE id = $${p}
        RETURNING *
      `;

      const result = await pool.query(sql, params);

      if (!result.rows.length) {
        return res.status(404).json({ error: 'Заявка не найдена' });
      }

      return res.json({ ok: true, row: result.rows[0] });
    } catch (error) {
      console.error('[PATCH /leads/:id] error:', error);
      return res.status(500).json({ error: 'Ошибка обновления заявки' });
    }
  }
);

router.get(
  '/leads/:id/comments',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const leadId = Number(req.params.id);
      if (!leadId) {
        return res.status(400).json({ error: 'Некорректный ID заявки' });
      }

      const sql = `
        SELECT
          c.id,
          c.lead_id,
          c.comment_text,
          c.created_by,
          c.created_at,
          NULL::text AS created_by_username
        FROM public.site_lead_comments c
        WHERE c.lead_id = $1
        ORDER BY c.created_at DESC, c.id DESC
      `;

      const result = await pool.query(sql, [leadId]);
      return res.json({ ok: true, rows: result.rows });
    } catch (error) {
      console.error('[GET /leads/:id/comments] error:', error);
      return res.status(500).json({ error: 'Ошибка получения комментариев' });
    }
  }
);

router.post(
  '/leads/:id/comments',
  authRequired,
  requireRole(['owner', 'admin']),
  async (req, res) => {
    try {
      const leadId = Number(req.params.id);
      const commentText = String(req.body.comment_text || '').trim();

      if (!leadId) {
        return res.status(400).json({ error: 'Некорректный ID заявки' });
      }

      if (!commentText) {
        return res.status(400).json({ error: 'Комментарий обязателен' });
      }

      const sql = `
        INSERT INTO public.site_lead_comments (
          lead_id,
          comment_text,
          created_by
        )
        VALUES ($1, $2, $3)
        RETURNING *
      `;

      const result = await pool.query(sql, [
        leadId,
        commentText,
        req.user?.id || null
      ]);

      await pool.query(
        `UPDATE public.site_leads SET updated_at = NOW() WHERE id = $1`,
        [leadId]
      );

      return res.status(201).json({ ok: true, row: result.rows[0] });
    } catch (error) {
      console.error('[POST /leads/:id/comments] error:', error);
      return res.status(500).json({ error: 'Ошибка добавления комментария' });
    }
  }
);

module.exports = router;