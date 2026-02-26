require('dotenv').config();
const { Pool } = require('pg');
const bcrypt = require('bcryptjs');

// TODO: ПЕРЕД ЗАПУСКОМ ИЗМЕНИ эти значения под себя
const USERNAME = 'admin';
const PASSWORD = '29101988go';
const ROLE = 'owner';

const pool = new Pool({
    host: process.env.DB_HOST,
    port: process.env.DB_PORT,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
});

(async () => {
    try {
        const hash = await bcrypt.hash(PASSWORD, 10);

        const result = await pool.query(
            `INSERT INTO auth.users (username, password_hash, role)
             VALUES ($1, $2, $3)
             RETURNING id`,
            [USERNAME, hash, ROLE]
        );

        console.log('Создан пользователь с id =', result.rows[0].id);
    } catch (err) {
        console.error('Ошибка при создании пользователя:', err);
    } finally {
        await pool.end();
    }
})();
