// reset_admin.js
require('dotenv').config();
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');

(async () => {
  try {
    const pool = new Pool({
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT || 5432),
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
    });

    const username = 'admin';
    const newPassword = 'admin'; // Можешь здесь поставить любой другой пароль

    const hash = await bcrypt.hash(newPassword, 10);

    const result = await pool.query(
      `UPDATE auth.users
       SET password_hash = $1
       WHERE username = $2
       RETURNING id, username`,
      [hash, username]
    );

    if (result.rowCount === 0) {
      console.log('Пользователь admin не найден');
    } else {
      console.log('Пароль для admin сброшен. ID =', result.rows[0].id);
    }

    await pool.end();
  } catch (err) {
    console.error('Ошибка при сбросе пароля admin:', err);
    process.exit(1);
  }
})();
