const express = require('express');
const router = express.Router();
const { pool } = require('../db');
const { authRequired, requireRole } = require('../authMiddleware');
const bcrypt = require('bcrypt');


// =====================================================
// GET /sellers/clients
// список клиентов для привязки селлера
// =====================================================

router.get('/clients', authRequired, requireRole(['owner','admin']), async (req,res)=>{

  try{

    const sql = `
      SELECT
        id,
        client_name
      FROM masterdata.clients
      ORDER BY client_name
    `;

    const { rows } = await pool.query(sql);

    res.json({ items: rows });

  }catch(err){

    console.error('sellers/clients error',err);
    res.status(500).json({error:'Ошибка загрузки клиентов'});

  }

});


// =====================================================
// GET /sellers
// список seller пользователей
// =====================================================

router.get('/', authRequired, requireRole(['owner','admin']), async (req,res)=>{

  try{

    const sql = `
      SELECT
        u.id,
        u.username,
        u.role,
        u.is_active,
        u.created_at,
        c.id as client_id,
        c.client_name
      FROM auth.users u
      LEFT JOIN masterdata.clients c
        ON c.id = u.client_id
      WHERE u.role = 'seller'
      ORDER BY u.id DESC
    `;

    const { rows } = await pool.query(sql);

    res.json({ items: rows });

  }catch(err){

    console.error('sellers list error',err);
    res.status(500).json({error:'Ошибка загрузки sellers'});

  }

});



// =====================================================
// POST /sellers
// создать seller
// =====================================================

router.post('/', authRequired, requireRole(['owner','admin']), async (req,res)=>{

  const client = await pool.connect();

  try{

    const { username,password,client_id,is_active } = req.body;

    if(!username || !password || !client_id){

      return res.status(400).json({
        error:'username, password, client_id обязательны'
      });

    }

    const exists = await client.query(
      `SELECT id FROM auth.users WHERE username=$1`,
      [username]
    );

    if(exists.rowCount){

      return res.status(400).json({
        error:'Логин уже существует'
      });

    }

    const hash = await bcrypt.hash(password,10);

    const sql = `
      INSERT INTO auth.users
      (username,password_hash,role,client_id,is_active,created_at)
      VALUES ($1,$2,'seller',$3,$4,now())
      RETURNING id
    `;

    const result = await client.query(sql,[
      username,
      hash,
      client_id,
      is_active
    ]);

    res.json({
      ok:true,
      id: result.rows[0].id
    });

  }catch(err){

    console.error('create seller error',err);
    res.status(500).json({error:'Ошибка создания seller'});

  }finally{

    client.release();

  }

});



// =====================================================
// PATCH /sellers/:id
// смена пароля / активации
// =====================================================

router.patch('/:id', authRequired, requireRole(['owner','admin']), async (req,res)=>{

  const id = Number(req.params.id);

  const { password,is_active } = req.body;

  try{

    if(password){

      const hash = await bcrypt.hash(password,10);

      await pool.query(
        `UPDATE auth.users
         SET password_hash=$1
         WHERE id=$2`,
        [hash,id]
      );

    }

    if(is_active !== undefined){

      await pool.query(
        `UPDATE auth.users
         SET is_active=$1
         WHERE id=$2`,
        [is_active,id]
      );

    }

    res.json({ok:true});

  }catch(err){

    console.error('update seller error',err);
    res.status(500).json({error:'Ошибка обновления seller'});

  }

});


module.exports = router;