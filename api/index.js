require('dotenv').config();

const express = require('express');
const cors = require('cors');
const nodemailer = require('nodemailer');
const path = require('path');
const cookieParser = require('cookie-parser');
const fs = require('fs');
const db = require('./database');

const app = express();
const PORT = Number(process.env.PORT) || 3000;

// Session secret for authentication (saved in .env for persistence)
const SESSION_SECRET = process.env.SESSION_SECRET || 'default_secret_key_change_me';

// Middleware
app.use(
  cors({
    origin: true,
    credentials: false,
  }),
);
app.use(express.json({ limit: '250kb' }));
app.use(express.urlencoded({ extended: true, limit: '250kb' }));
app.use(cookieParser());

const PUBLIC_DIR = path.join(__dirname, '..', 'public');
const VIEWS_DIR = path.join(__dirname, '..', 'views');

// Simple Request Logger
app.use((req, res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.path}`);
  next();
});

// Authentication Middleware
const PROTECTED_PAGES = ['/', '/index.html', '/dashboard.html'];
const authMiddleware = (req, res, next) => {
  const cleanPath = req.path === '/' ? '/index.html' : req.path;

  // If it's a protected page, check for auth cookie
  if (PROTECTED_PAGES.includes(cleanPath)) {
    if (req.cookies.auth === SESSION_SECRET) {
      return next();
    } else {
      return res.redirect('/login.html');
    }
  }

  // If it's an API call that should be protected, check for auth cookie
  const isProtectedApi =
    req.path.startsWith('/api/surveys') ||
    req.path.startsWith('/api/backup') ||
    (req.path.startsWith('/api/survey-locks') && req.method === 'POST') ||
    (req.path.startsWith('/api/survey/') && req.method === 'DELETE');

  if (isProtectedApi) {
    if (req.cookies.auth === SESSION_SECRET) {
      return next();
    } else {
      console.log(`[AUTH] Unauthorized API access blocked: ${req.method} ${req.path}`);
      return res.status(401).json({ status: 'error', message: 'Unauthorized' });
    }
  }
  next();
};

app.use(authMiddleware);
app.use(express.static(PUBLIC_DIR));

// Root-level ping to verify server update on Hostinger
app.get('/ping', (req, res) => {
  res.json({
    status: 'ok',
    source: 'api/index.js',
    last_updated: '2026-02-20 18:30'
  });
});

// Explicitly serve Arabic files to avoid encoding issues on Vercel
app.get(['/استبيان عمال.html', '/استبيان%20عمال.html'], (req, res) => res.sendFile(path.join(PUBLIC_DIR, 'استبيان عمال.html')));
app.get(['/استبيان مدراء.html', '/استبيان%20مدراء.html'], (req, res) => res.sendFile(path.join(PUBLIC_DIR, 'استبيان مدراء.html')));

// Specific routes for protected views
app.get('/index.html', (req, res) => res.sendFile(path.join(VIEWS_DIR, 'index.html')));
app.get('/dashboard.html', (req, res) => res.sendFile(path.join(VIEWS_DIR, 'dashboard.html')));

app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  if (username === 'admin' && password === 'Admin@2000') {
    // secure: false allows cookie over HTTP (common on VPS without SSL)
    res.cookie('auth', SESSION_SECRET, {
      httpOnly: true,
      secure: false,
      sameSite: 'lax',
      maxAge: 24 * 60 * 60 * 1000 // 24 hours
    });
    return res.json({ status: 'success' });
  }
  res.status(401).json({ status: 'error', message: 'Invalid credentials' });
});

// Logout Endpoint
app.get('/api/logout', (req, res) => {
  res.clearCookie('auth');
  res.redirect('/login.html');
});

function redactEmailAddress(email) {
  if (!email || typeof email !== 'string') return '';
  const at = email.indexOf('@');
  if (at <= 1) return '***';
  return `${email.slice(0, 2)}***${email.slice(at)}`;
}

function normalizeFieldValue(value) {
  if (value === null || value === undefined) return '';
  if (Array.isArray(value)) return value.map((v) => String(v)).join(', ');
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function buildEmailContent(data, meta) {
  const entries = Object.entries(data || {}).filter(([key]) => !String(key).startsWith('_'));

  const lines = [];
  lines.push(`Time: ${meta.receivedAtIso}`);
  if (meta.ip) lines.push(`IP: ${meta.ip}`);
  if (meta.userAgent) lines.push(`User-Agent: ${meta.userAgent}`);
  lines.push('');

  for (const [key, value] of entries) {
    lines.push(`${key}: ${normalizeFieldValue(value) || '—'}`);
  }

  const escapeHtml = (s) =>
    String(s)
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');

  const rowsHtml = entries
    .map(([key, value]) => {
      const v = normalizeFieldValue(value) || '—';
      return `<tr>
        <td style="padding:8px;border:1px solid #e5e7eb;font-weight:600;vertical-align:top;white-space:pre-wrap;">${escapeHtml(
        key,
      )}</td>
        <td style="padding:8px;border:1px solid #e5e7eb;vertical-align:top;white-space:pre-wrap;">${escapeHtml(
        v,
      )}</td>
      </tr>`;
    })
    .join('');

  const html = `<!doctype html>
<html>
  <body style="font-family:Arial, sans-serif;background:#f7fafc;padding:16px;">
    <div style="max-width:760px;margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:10px;overflow:hidden;">
      <div style="background:#219150;color:#fff;padding:14px 16px;font-size:16px;font-weight:700;">
        New website form submission
      </div>
      <div style="padding:14px 16px;color:#111827;font-size:13px;">
        <div style="margin-bottom:10px;color:#374151;">
          <div><b>Time:</b> ${escapeHtml(meta.receivedAtIso)}</div>
          ${meta.ip ? `<div><b>IP:</b> ${escapeHtml(meta.ip)}</div>` : ''}
          ${meta.userAgent ? `<div><b>User-Agent:</b> ${escapeHtml(meta.userAgent)}</div>` : ''}
        </div>
        <table style="width:100%;border-collapse:collapse;font-size:13px;">
          <thead>
            <tr>
              <th style="text-align:left;padding:8px;border:1px solid #e5e7eb;background:#f3f4f6;">Field</th>
              <th style="text-align:left;padding:8px;border:1px solid #e5e7eb;background:#f3f4f6;">Value</th>
            </tr>
          </thead>
          <tbody>
            ${rowsHtml || `<tr><td colspan="2" style="padding:10px;border:1px solid #e5e7eb;">No fields received.</td></tr>`}
          </tbody>
        </table>
      </div>
    </div>
  </body>
</html>`;

  return { text: lines.join('\n'), html };
}

function getTransporterOrNull() {
  const host = process.env.SMTP_HOST;
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  if (!host || !user || !pass) return null;

  const port = Number(process.env.SMTP_PORT) || 587;
  const secure = String(process.env.SMTP_SECURE || '').toLowerCase() === 'true';

  return nodemailer.createTransport({
    host,
    port,
    secure,
    auth: { user, pass },
  });
}

app.get('/health', (_req, res) => {
  res.json({ ok: true });
});

// --- NEW DASHBOARD API ---

// Diagnostic route to verify server is running latest code
app.get('/api/ping', (req, res) => {
  res.json({
    status: 'ok',
    message: 'Server is running latest code',
    timestamp: new Date().toISOString()
  });
});

app.get('/api/surveys', async (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const limitQuery = req.query.limit;
  const limit = limitQuery === 'all' ? 'all' : (parseInt(limitQuery) || 50);
  const offset = limit === 'all' ? 0 : (page - 1) * limit;
  const search = req.query.search || '';
  let filters = {};

  try {
    if (req.query.filters) {
      filters = JSON.parse(req.query.filters);
    }
  } catch (e) {
    console.error("Error parsing filters:", e);
  }

  console.log(`[API] Fetch surveys: page=${page}, limit=${limit}, search=${search}, filters=${JSON.stringify(filters)}`);
  const startTime = Date.now();

  try {
    // Run all database queries in parallel for maximum speed
    const [managers, workers, totalManagers, totalWorkers] = await Promise.all([
      db.getAllManagers(limit, offset, search, filters),
      db.getAllWorkers(limit, offset, search, filters),
      db.getManagersCount(search, filters),
      db.getWorkersCount(search, filters)
    ]);

    const duration = Date.now() - startTime;
    console.log(`[API] Surveys fetched successfully in ${duration}ms`);

    res.json({
      managers,
      workers,
      pagination: {
        page,
        limit: limit === 'all' ? Math.max(totalManagers, totalWorkers) : limit,
        totalManagers,
        totalWorkers,
        totalPagesManagers: limit === 'all' ? 1 : Math.ceil(totalManagers / (limit || 50)),
        totalPagesWorkers: limit === 'all' ? 1 : Math.ceil(totalWorkers / (limit || 50))
      }
    });
  } catch (err) {
    console.error(`[API ERROR] ${err.message}`);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error while fetching data',
      detail: err.message
    });
  }
});

app.get('/api/surveys/unique-values', async (req, res) => {
  const { type, field } = req.query;
  if (!type || !field) {
    return res.status(400).json({ status: 'error', message: 'Missing type or field' });
  }

  try {
    const values = await db.getUniqueValues(type, field);
    res.json({ status: 'success', values });
  } catch (err) {
    console.error(`[API ERROR] ${err.message}`);
    res.status(500).json({ status: 'error', message: 'Failed to fetch unique values' });
  }
});

app.post('/api/save-survey', async (req, res) => {
  const { type, data } = req.body;

  if (!type || !data) {
    return res.status(400).json({ status: 'error', message: 'Missing type or data' });
  }

  const entry = {
    ...data,
    id: Date.now().toString(),
    receivedAt: new Date().toISOString()
  };

  try {
    if (type === 'manager') {
      await db.addManager(entry);
    } else if (type === 'worker') {
      await db.addWorker(entry);
    } else {
      return res.status(400).json({ status: 'error', message: 'Invalid survey type' });
    }
    console.log(`Saved ${type} survey to database.`);
    res.json({ status: 'success' });
  } catch (err) {
    console.error('Error saving survey:', err);
    res.status(500).json({ status: 'error', message: 'Failed to save survey' });
  }
});

app.delete('/api/survey/:type/:id', async (req, res) => {
  const { type, id } = req.params;

  try {
    let deleted = false;
    if (type === 'manager') {
      deleted = await db.deleteManager(id);
    } else if (type === 'worker') {
      deleted = await db.deleteWorker(id);
    } else {
      return res.status(400).json({ status: 'error', message: 'Invalid type' });
    }

    if (!deleted) {
      return res.status(404).json({ status: 'error', message: 'Not found' });
    }
    console.log(`Deleted ${type} survey ID ${id}`);
    res.json({ status: 'success' });
  } catch (err) {
    console.error('Error deleting survey:', err);
    res.status(500).json({ status: 'error', message: 'Failed to delete survey' });
  }
});

// Get current lock status for surveys (public read so forms can know they are closed)
app.get('/api/survey-locks', async (_req, res) => {
  try {
    const locks = await db.getSurveyLockStatus();
    res.json(locks);
  } catch (err) {
    console.error('Error fetching survey lock status:', err);
    res.status(500).json({ status: 'error', message: 'Failed to fetch lock status' });
  }
});

// Update lock status for a specific survey (protected by auth middleware)
app.post('/api/survey-locks', async (req, res) => {
  const { type, locked } = req.body || {};

  if (type !== 'worker' && type !== 'manager') {
    return res.status(400).json({ status: 'error', message: 'Invalid survey type' });
  }

  const lockedBool = Boolean(locked);

  try {
    await db.setSurveyLock(type, lockedBool);
    const locks = await db.getSurveyLockStatus();
    res.json({ status: 'success', locks });
  } catch (err) {
    console.error('Error updating survey lock status:', err);
    res.status(500).json({ status: 'error', message: 'Failed to update lock status' });
  }
});

// Download backup of the SQLite database file
app.get(['/api/backup', '/api/backup/'], (req, res) => {
  if (db.IS_MYSQL) {
    return res
      .status(400)
      .json({ status: 'error', message: 'Backup download is only supported for SQLite mode.' });
  }

  const dbPath = db.DB_PATH;
  if (!dbPath) {
    return res.status(500).json({ status: 'error', message: 'Database path is not configured.' });
  }

  if (!fs.existsSync(dbPath)) {
    return res.status(404).json({ status: 'error', message: 'Database file not found.' });
  }

  const fileName = `survey-backup-${new Date().toISOString().slice(0, 10)}.db`;
  res.download(dbPath, fileName, (err) => {
    if (err) {
      console.error('Error sending backup file:', err);
      if (!res.headersSent) {
        res.status(500).json({ status: 'error', message: 'Failed to download backup file.' });
      }
    }
  });
});

// --- Catch-all for unknown /api routes ---
app.use('/api', (req, res) => {
  console.log(`[404 NOT FOUND] ${req.method} ${req.originalUrl}`);
  res.status(404).json({
    status: 'error',
    message: `API endpoint ${req.method} ${req.path} not found.`,
    hint: 'Ensure your server is running the latest code and has been restarted.'
  });
});

// --- END DASHBOARD API ---

app.post('/send-email', async (req, res) => {
  console.log('--- New Submission Received ---');

  const data = req.body || {};
  const meta = {
    receivedAtIso: new Date().toISOString(),
    ip: req.headers['x-forwarded-for']?.toString().split(',')[0]?.trim() || req.socket.remoteAddress,
    userAgent: req.headers['user-agent'] || '',
  };

  const mailTo = process.env.MAIL_TO;
  const mailFrom = process.env.MAIL_FROM || process.env.SMTP_USER;

  if (!mailTo) {
    return res.status(500).json({
      status: 'error',
      message: 'Server is not configured (MAIL_TO is missing).',
    });
  }

  const transporter = getTransporterOrNull();
  if (!transporter) {
    return res.status(500).json({
      status: 'error',
      message: 'Server is not configured (SMTP_HOST/SMTP_USER/SMTP_PASS are missing).',
    });
  }

  const subjectRaw = typeof data._subject === 'string' ? data._subject : 'New form submission';
  const subject = subjectRaw.slice(0, 180);
  const { text, html } = buildEmailContent(data, meta);

  try {
    console.log(`Sending email to ${redactEmailAddress(mailTo)}...`);

    await transporter.sendMail({
      from: mailFrom,
      to: mailTo,
      subject,
      text,
      html,
    });

    console.log('✅ Email sent');
    return res.json({ status: 'success', message: 'Sent successfully.' });
  } catch (error) {
    console.error('❌ Email send failed:', error && error.message ? error.message : error);
    return res.status(500).json({ status: 'error', message: 'Email send failed on server.' });
  }
});

// For Vercel/Single Page Routing: Redirect any unknown GET requests to index.html
// (Only if they aren't API calls)
app.get(/^(?!\/api|\/send-email).*/, (req, res) => {
  if (req.path === '/' || req.path === '/index.html') {
    return res.sendFile(path.join(VIEWS_DIR, 'index.html'));
  }
  res.sendFile(path.join(VIEWS_DIR, 'index.html'));
});

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`=========================================`);
    console.log(`🚀 SERVER RUNNING`);
    console.log(`🌍 URL: http://localhost:${PORT}`);
    console.log(`📪 Receiver (MAIL_TO): ${process.env.MAIL_TO ? redactEmailAddress(process.env.MAIL_TO) : '(not set)'}`);
    console.log(
      `✉️  SMTP (SMTP_USER): ${process.env.SMTP_USER ? redactEmailAddress(process.env.SMTP_USER) : '(not set)'}`,
    );
    console.log(`=========================================`);
  });
}

module.exports = app;
