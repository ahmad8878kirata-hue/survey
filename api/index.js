require('dotenv').config();

const express = require('express');
const cors = require('cors');
const nodemailer = require('nodemailer');
const path = require('path');
const cookieParser = require('cookie-parser');
const compression = require('compression');
const db = require('./database');

const app = express();
const PORT = Number(process.env.PORT) || 3000;

// Session secret for authentication (saved in .env for persistence)
const SESSION_SECRET = process.env.SESSION_SECRET || 'default_secret_key_change_me';

// Middleware
app.use(compression());
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

  // If it's an API call to surveys, check for auth cookie
  if (req.path.startsWith('/api/surveys') || (req.path.startsWith('/api/survey') && req.method === 'DELETE')) {
    if (req.cookies.auth === SESSION_SECRET) {
      return next();
    } else {
      return res.status(401).json({ status: 'error', message: 'Unauthorized' });
    }
  }
  next();
};

app.use(authMiddleware);
app.use(express.static(PUBLIC_DIR));

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

app.get('/api/surveys', (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 50;
    const offset = (page - 1) * limit;

    const managers = db.getAllManagers(limit, offset);
    const workers = db.getAllWorkers(limit, offset);

    const totalManagers = db.getManagersCount();
    const totalWorkers = db.getWorkersCount();

    res.json({
      managers,
      workers,
      pagination: {
        page,
        limit,
        totalManagers,
        totalWorkers,
        totalPagesManagers: Math.ceil(totalManagers / limit),
        totalPagesWorkers: Math.ceil(totalWorkers / limit)
      }
    });
  } catch (err) {
    console.error('Error fetching surveys:', err);
    res.status(500).json({ status: 'error', message: 'Failed to fetch surveys' });
  }
});

app.post('/api/save-survey', (req, res) => {
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
      db.addManager(entry);
    } else if (type === 'worker') {
      db.addWorker(entry);
    } else {
      return res.status(400).json({ status: 'error', message: 'Invalid survey type' });
    }
    console.log(`Saved ${type} survey to SQLite.`);
    res.json({ status: 'success' });
  } catch (err) {
    console.error('Error saving survey:', err);
    res.status(500).json({ status: 'error', message: 'Failed to save survey' });
  }
});

app.delete('/api/survey/:type/:id', (req, res) => {
  const { type, id } = req.params;

  try {
    let deleted = false;
    if (type === 'manager') {
      deleted = db.deleteManager(id);
    } else if (type === 'worker') {
      deleted = db.deleteWorker(id);
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
