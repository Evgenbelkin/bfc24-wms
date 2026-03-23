// authMiddleware.js
const jwt = require('jsonwebtoken');

/**
 * Достаём Bearer token из Authorization header
 */
function extractBearerToken(req) {
  const authHeader = req.headers['authorization'] || '';

  if (!authHeader.startsWith('Bearer ')) {
    return null;
  }

  return authHeader.slice(7).trim();
}

/**
 * Обязательная авторизация.
 * Проверяем Bearer-токен, кладём юзера в req.user.
 */
function authRequired(req, res, next) {
  const token = extractBearerToken(req);

  if (!token) {
    return res.status(401).json({ error: 'auth_required' });
  }

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);

    const userId = Number(decoded.id);

    if (!Number.isInteger(userId) || userId <= 0) {
      console.error('[authRequired] invalid token payload id:', decoded.id);
      return res.status(401).json({ error: 'invalid_token_payload' });
    }

    req.user = {
      id: userId,
      username: decoded.username || null,
      role: decoded.role || null,
      client_id: decoded.client_id || null
    };

    console.log('[authRequired] req.user =', req.user);

    next();
  } catch (err) {
    console.error('[authRequired] token error', err);
    return res.status(401).json({ error: 'invalid_token' });
  }
}

/**
 * Проверка роли.
 * Поддерживает:
 *   requireRole('owner')
 *   requireRole('owner', 'admin')
 *   requireRole(['owner', 'admin'])
 */
function requireRole(...requiredRolesInput) {
  let roles = [];

  if (requiredRolesInput.length === 1 && Array.isArray(requiredRolesInput[0])) {
    roles = requiredRolesInput[0].flat().filter(Boolean);
  } else {
    roles = requiredRolesInput.flat().filter(Boolean);
  }

  if (!roles.length) {
    throw new Error('requireRole: at least one role is required');
  }

  return (req, res, next) => {
    if (!req.user) {
      return res.status(401).json({ error: 'unauthorized' });
    }

    const actualRole = req.user.role || null;

    console.log(
      '[requireRole] check requiredRoles =',
      roles,
      'actualRole =',
      actualRole,
      'client_id =',
      req.user.client_id || null
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