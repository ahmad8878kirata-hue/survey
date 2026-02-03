require('dotenv').config();

const express = require('express');
const cors = require('cors');
const nodemailer = require('nodemailer');
const fs = require('fs');
const path = require('path');
const cookieParser = require('cookie-parser');

const app = express();
const PORT = Number(process.env.PORT) || 3000;

// Handle DB path for Vercel (must use /tmp for write access)
const IS_VERCEL = process.env.VERCEL === '1';
const DB_PATH = IS_VERCEL
  ? path.join('/tmp', 'database.json')
  : path.join(__dirname, 'database.json');

// Ensure DB exists safely
function initDB() {
  try {
    if (!fs.existsSync(DB_PATH)) {
      fs.writeFileSync(DB_PATH, JSON.stringify({ managers: [], workers: [] }, null, 2));
      console.log('Database initialized at:', DB_PATH);
    }
  } catch (err) {
    console.warn('Warning: Could not initialize database file. This is expected if the filesystem is read-only and not on /tmp.', err.message);
  }
}
initDB();

function readDB() {
  try {
    const data = fs.readFileSync(DB_PATH, 'utf-8');
    return JSON.parse(data);
  } catch (err) {
    console.error('Error reading DB:', err);
    return { managers: [], workers: [] };
  }
}

function writeDB(data) {
  try {
    fs.writeFileSync(DB_PATH, JSON.stringify(data, null, 2));
  } catch (err) {
    console.error('Error writing DB (Check permissions):', err.message);
  }
}

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

// Authentication Middleware
const PROTECTED_PAGES = ['/', '/index.html', '/dashboard.html'];
const authMiddleware = (req, res, next) => {
  // If it's a protected page, check for auth cookie
  if (PROTECTED_PAGES.includes(req.path)) {
    if (req.cookies.auth === 'true') {
      return next();
    } else {
      return res.redirect('/login.html');
    }
  }
  // If it's an API call to surveys, check for auth cookie
  if (req.path.startsWith('/api/surveys') || (req.path.startsWith('/api/survey') && req.method === 'DELETE')) {
    if (req.cookies.auth === 'true') {
      return next();
    } else {
      return res.status(401).json({ status: 'error', message: 'Unauthorized' });
    }
  }
  next();
};

app.use(authMiddleware);
app.use(express.static(__dirname));

// Login Endpoint
app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  if (username === 'admin' && password === 'Admin@2000') {
    res.cookie('auth', 'true', { httpOnly: true, maxAge: 24 * 60 * 60 * 1000 }); // 24 hours
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

app.get('/api/surveys', (_req, res) => {
  const data = readDB();
  res.json(data);
});

app.post('/api/save-survey', (req, res) => {
  const { type, data } = req.body;

  if (!type || !data) {
    return res.status(400).json({ status: 'error', message: 'Missing type or data' });
  }

  const db = readDB();
  const entry = {
    ...data,
    id: Date.now().toString(),
    receivedAt: new Date().toISOString()
  };

  if (type === 'manager') {
    if (!db.managers) db.managers = [];
    db.managers.push(entry);
  } else if (type === 'worker') {
    if (!db.workers) db.workers = [];
    db.workers.push(entry);
  } else {
    return res.status(400).json({ status: 'error', message: 'Invalid survey type' });
  }

  writeDB(db);
  console.log(`Saved ${type} survey locally.`);
  res.json({ status: 'success' });
});

app.delete('/api/survey/:type/:id', (req, res) => {
  const { type, id } = req.params;
  const db = readDB();

  let targetArray = null;
  if (type === 'manager') targetArray = db.managers;
  else if (type === 'worker') targetArray = db.workers;
  else return res.status(400).json({ status: 'error', message: 'Invalid type' });

  if (!targetArray) return res.status(404).json({ status: 'error', message: 'Not found' });

  const initialLength = targetArray.length;
  // Filter out the item
  if (type === 'manager') {
    db.managers = db.managers.filter(item => item.id !== id);
  } else {
    db.workers = db.workers.filter(item => item.id !== id);
  }

  writeDB(db);
  console.log(`Deleted ${type} survey ID ${id}`);
  res.json({ status: 'success' });
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

module.exports = app;
