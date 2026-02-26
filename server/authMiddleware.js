// authMiddleware.js
const jwt = require('jsonwebtoken');

/**
 * Обязательная авторизация.
 * Проверяем Bearer-токен, кладём юзера в req.user.
 */
function authRequired(req, res, next) {
  const authHeader = req.headers['authorization'] || '';
  const token = authHeader.startsWith('Bearer ')
    ? authHeader.slice('Bearer '.length)
    : null;

  if (!token) {
    return res.status(401).json({ error: 'auth_required' });
  }

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET || 'secret');

    req.user = {
      id: decoded.id,
      username: decoded.username,
      role: decoded.role,
    };

    next();
  } catch (err) {
    console.error('[authRequired] token error', err);
    // тут может быть TokenExpiredError или JsonWebTokenError,
    // но для фронта достаточно "invalid_token"
    return res.status(401).json({ error: 'invalid_token' });
  }
}

/**
 * Проверка роли.
 * requiredRoles — строка ('owner') или массив строк (['owner','admin']).
 */
function requireRole(requiredRoles) {
  let roles = [];

  if (typeof requiredRoles === 'string') {
    roles = [requiredRoles];
  } else if (Array.isArray(requiredRoles)) {
    roles = requiredRoles.flat();
  } else {
    throw new Error('requireRole: invalid argument, use string or array');
  }

  return (req, res, next) => {
    const actualRole = req.user?.role || null;

    console.log(
      '[requireRole] check',
      'requiredRoles =', roles,
      'actualRole =', actualRole
    );

    if (!actualRole || !roles.includes(actualRole)) {
      return res.status(403).json({
        error: 'Forbidden: not enough rights',
        requiredRoles: roles,
        actualRole,
      });
    }

    next();
  };
}

module.exports = {
  authRequired,
  requireRole,
};
