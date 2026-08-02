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
const PROTECTED_PAGES = ['/', '/index.html', '/dashboard.html', '/reports_dashboard.html', '/shift_dashboard.html', '/analysis'];
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
    req.path.startsWith('/api/clean-branches') ||
    req.path.startsWith('/api/generate_monthly_report') ||
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
app.get(['/استبيان مشرفين.html', '/استبيان%20مشرفين.html'], (req, res) => res.sendFile(path.join(PUBLIC_DIR, 'استبيان مشرفين.html')));
app.get(['/استبيان الدوام.html', '/استبيان%20الدوام.html'], (req, res) => res.sendFile(path.join(PUBLIC_DIR, 'استبيان الدوام.html')));
app.get(['/تقرير يومي.html', '/تقرير%20يومي.html'], (req, res) => res.sendFile(path.join(PUBLIC_DIR, 'تقرير يومي.html')));

// Specific routes for protected views
app.get('/index.html', (req, res) => res.sendFile(path.join(VIEWS_DIR, 'index.html')));
app.get('/dashboard.html', (req, res) => res.sendFile(path.join(VIEWS_DIR, 'dashboard.html')));
app.get('/reports_dashboard.html', (req, res) => res.sendFile(path.join(VIEWS_DIR, 'reports_dashboard.html')));
app.get('/shift_dashboard.html', (req, res) => res.sendFile(path.join(VIEWS_DIR, 'shift_dashboard.html')));
app.get('/analysis', (req, res) => res.sendFile(path.join(VIEWS_DIR, 'analysis.html')));

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

// Cleaning Utility Endpoint
app.post('/api/clean-branches', async (req, res) => {
  try {
    const updatedCount = await db.cleanBranchNames();
    res.json({ status: 'success', updatedCount });
  } catch (err) {
    console.error('[CLEAN] Error cleaning branch names:', err);
    res.status(500).json({ status: 'error', message: err.message });
  }
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
    const [managers, workers, supervisors, shiftWorkers, totalManagers, totalWorkers, totalSupervisors, totalShiftWorkers] = await Promise.all([
      db.getAllManagers(limit, offset, search, filters),
      db.getAllWorkers(limit, offset, search, filters),
      db.getAllSupervisors(limit, offset, search, filters),
      db.getAllShiftWorkers(limit, offset, search, filters),
      db.getManagersCount(search, filters),
      db.getWorkersCount(search, filters),
      db.getSupervisorsCount(search, filters),
      db.getShiftWorkersCount(search, filters)
    ]);

    const duration = Date.now() - startTime;
    console.log(`[API] Surveys fetched successfully in ${duration}ms`);

    res.json({
      managers,
      workers,
      supervisors,
      shiftWorkers,
      pagination: {
        page,
        limit: limit === 'all' ? Math.max(totalManagers, totalWorkers, totalSupervisors, totalShiftWorkers) : limit,
        totalManagers,
        totalWorkers,
        totalSupervisors,
        totalShiftWorkers,
        totalPagesManagers: limit === 'all' ? 1 : Math.ceil(totalManagers / (limit || 50)),
        totalPagesWorkers: limit === 'all' ? 1 : Math.ceil(totalWorkers / (limit || 50)),
        totalPagesSupervisors: limit === 'all' ? 1 : Math.ceil(totalSupervisors / (limit || 50)),
        totalPagesShiftWorkers: limit === 'all' ? 1 : Math.ceil(totalShiftWorkers / (limit || 50))
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

app.get('/api/daily_reports', async (req, res) => {
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

  console.log(`[API] Fetch daily reports: page=${page}, limit=${limit}, search=${search}, filters=${JSON.stringify(filters)}`);

  try {
    const [reports, totalReports] = await Promise.all([
      db.getAllDailyReports(limit, offset, search, filters),
      db.getDailyReportsCount(search, filters)
    ]);

    res.json({
      reports,
      pagination: {
        page,
        limit: limit === 'all' ? totalReports : limit,
        totalReports,
        totalPagesReports: limit === 'all' ? 1 : Math.ceil(totalReports / (limit || 50))
      }
    });
  } catch (err) {
    console.error(`[API ERROR] ${err.message}`);
    res.status(500).json({ status: 'error', message: 'Internal server error while fetching daily reports' });
  }
});

app.get('/api/surveys/unique-values', async (req, res) => {
  const { type, field } = req.query;
  let filters = {};

  try {
    if (req.query.filters) {
      filters = JSON.parse(req.query.filters);
    }
  } catch (e) {
    console.error("Error parsing filters:", e);
  }

  if (!type || !field) {
    return res.status(400).json({ status: 'error', message: 'Missing type or field' });
  }

  try {
    const values = await db.getUniqueValues(type, field, filters);
    res.json({ status: 'success', values });
  } catch (err) {
    console.error(`[API ERROR] ${err.message}`);
    res.status(500).json({ status: 'error', message: 'Failed to fetch unique values' });
  }
});

app.get('/api/analysis', async (req, res) => {
  const branch = req.query.branch || null;
  try {
    const analysis = await db.getAnalysisAggregation(branch);
    res.json({ status: 'success', data: analysis });
  } catch (err) {
    console.error(`[API ERROR] ${err.message}`);
    res.status(500).json({ status: 'error', message: 'Failed to fetch analysis data' });
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
    } else if (type === 'supervisor') {
      await db.addSupervisor(entry);
    } else if (type === 'shift') {
      await db.addShiftWorker(entry);
    } else if (type === 'daily_report') {
      await db.addDailyReport(entry);
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
    } else if (type === 'supervisor') {
      deleted = await db.deleteSupervisor(id);
    } else if (type === 'shift') {
      deleted = await db.deleteShiftWorker(id);
    } else if (type === 'daily_report') {
      deleted = await db.deleteDailyReport(id);
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

  if (type !== 'worker' && type !== 'manager' && type !== 'supervisor' && type !== 'shift') {
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

// Restore backup of the SQLite database file
app.post('/api/restore', express.raw({ type: '*/*', limit: '50mb' }), async (req, res) => {
  try {
    await db.restoreDatabase(req.body);
    res.json({ status: 'success', message: 'Database restored successfully.' });
  } catch (err) {
    console.error('Error restoring database:', err);
    res.status(500).json({ status: 'error', message: err.message || 'Failed to restore database.' });
  }
});

// Generate monthly report as DOCX
app.get('/api/generate_monthly_report', async (req, res) => {
  const { month } = req.query;

  if (!month || !/^\d{4}-\d{2}$/.test(month)) {
    return res.status(400).json({ status: 'error', message: 'Invalid month format. Use YYYY-MM.' });
  }

  try {
    const dailyReports = await db.getRecordsByMonth('daily_reports', month);

    // Group reports by branch
    const branchReports = {};
    for (const report of dailyReports) {
      const branch = report['اسم الفرع'] || 'غير محدد';
      if (!branchReports[branch]) {
        branchReports[branch] = { reports: [], stations: [], issues: [], gasPlants: [] };
      }
      branchReports[branch].reports.push(report);

      if (Array.isArray(report['المحطات'])) {
        const reportDate = report['تاريخ الجولة'] || report['receivedAt']?.slice(0, 10) || 'غير محدد';
        const stationsWithDate = report['المحطات'].map(st => ({
          ...st,
          reportDate: reportDate
        }));
        branchReports[branch].stations.push(...stationsWithDate);
      }
      if (Array.isArray(report['القضايا'])) {
        // Inject report date to each issue so we can display it in the table
        const reportDate = report['تاريخ الجولة'] || report['receivedAt']?.slice(0, 10) || '';
        const issuesWithDate = report['القضايا'].map(issue => ({ ...issue, reportDate: reportDate }));
        branchReports[branch].issues.push(...issuesWithDate);
      }
      if (Array.isArray(report['معامل الغاز'])) {
        branchReports[branch].gasPlants.push(...report['معامل الغاز']);
      }
    }

    const monthDisplay = month;
    const branchNames = Object.keys(branchReports).sort();

    const sanitizeText = (text) => {
      if (!text) return '';
      return String(text)
        .replace(/\(/g, ' - ')
        .replace(/\)/g, ' - ')
        .replace(/\s*-\s*-\s*/g, ' - ')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&apos;')
        .trim();
    };

    let html = `<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head><meta charset="UTF-8"><style>
  body { font-family: 'Traditional Arabic', Arial, sans-serif; font-size: 14pt; line-height: 1.8; color: #333; }
  table { border-collapse: collapse; margin: 15px 0; }
  th, td { border: 1px solid #a5d6a7; padding: 10px 12px; text-align: center; font-size: 12pt; }
  th { background-color: #e8f5e9; font-weight: bold; color: #2e7d32; font-size: 13pt; }
  ul { margin: 8px 0; padding-right: 30px; }
  li { margin: 5px 0; line-height: 1.6; }
</style></head>
<body>
<p style="text-align: center; font-weight: bold; font-size: 24pt; color: #1b5e20;">السيد المدير العام</p>
<p style="text-align: center; font-size: 16pt; color: #555; margin-bottom: 30px;">التقرير الشهري المجمع لجولات الرقابة والمتابعة - شهر ${monthDisplay}</p>
`;

    for (const branch of branchNames) {
      const data = branchReports[branch];
      const { reports, stations, issues } = data;

      html += `<p style="page-break-before: always; color: #c62828; font-size: 20pt; font-weight: bold; text-align: center; margin-top: 35px; margin-bottom: 25px;">تقرير فرع: ${sanitizeText(branch)}</p>`;

      // First: Tour count & Activity Summary
      html += `<p style="color: #2e7d32; font-weight: bold; font-size: 16pt; margin-top: 25px;">أولاً: عدد الجولات التفتيشية</p>
<ul>
  <li>عدد الجولات المنفذة في فرع ${sanitizeText(branch)}: ${reports.length}</li>
</ul>`;

      // Second: Performance evaluation
      const evalScores = { 'ممتاز': 5, 'جيد جداً': 4, 'جيد جدا': 4, 'جيد': 3, 'مقبول': 2, 'ضعيف': 1 };
      const evalTexts = { 5: 'ممتاز', 4: 'جيد جداً', 3: 'جيد', 2: 'مقبول', 1: 'ضعيف' };

      const branchEvaluations = [];
      const freqMap = {};
      
      for (const report of reports) {
        const val = report['التقييم اليومي'];
        if (val) {
          branchEvaluations.push(val);
          freqMap[val] = (freqMap[val] || 0) + 1;
        }
      }

      let averageScoreText = 'لا يوجد بيانات';
      let mostFrequentEval = 'لا يوجد بيانات';
      let avgScoreValue = 0;
      
      if (branchEvaluations.length > 0) {
        let totalScore = 0;
        let validCount = 0;
        for (const evalVal of branchEvaluations) {
          const score = evalScores[evalVal];
          if (score !== undefined) {
            totalScore += score;
            validCount++;
          }
        }
        if (validCount > 0) {
          avgScoreValue = totalScore / validCount;
          const roundedScore = Math.round(avgScoreValue);
          averageScoreText = evalTexts[roundedScore] || 'غير محدد';
        }
        let maxCount = 0;
        for (const [evalVal, count] of Object.entries(freqMap)) {
          if (count > maxCount) { maxCount = count; mostFrequentEval = evalVal; }
        }
      }

      html += `<p style="color: #2e7d32; font-weight: bold; font-size: 16pt; margin-top: 25px;">ثانياً: تقييم الأداء العام للفرع خلال الشهر</p>
<table style="border-collapse: collapse; margin-top: 10px; margin-bottom: 20px;">
  <thead>
    <tr style="background-color: #e8f5e9;">
      <th style="border: 1px solid #a5d6a7; padding: 12px; text-align: right; font-weight: bold; color: #2e7d32;">مؤشر تقييم الفرع</th>
      <th style="border: 1px solid #a5d6a7; padding: 12px; text-align: center; font-weight: bold; color: #2e7d32;">التقييم والنتيجة</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="border: 1px solid #a5d6a7; padding: 12px; text-align: right; font-weight: bold;">التقييم الحسابي العام للفرع</td>
      <td style="border: 1px solid #a5d6a7; padding: 12px; text-align: center; color: #2e7d32; font-weight: bold; font-size: 13pt;">${sanitizeText(averageScoreText)} - النتيجة: ${avgScoreValue.toFixed(1)} من 5</td>
    </tr>
    <tr>
      <td style="border: 1px solid #a5d6a7; padding: 12px; text-align: right; font-weight: bold;">التقييم الأكثر تكراراً</td>
      <td style="border: 1px solid #a5d6a7; padding: 12px; text-align: center; color: #2e7d32; font-weight: bold; font-size: 13pt;">${sanitizeText(mostFrequentEval)} - الأكثر شيوعاً</td>
    </tr>
  </tbody>
</table>`;

      // Third: Maintenance and operations
      const badMaintenanceItems = [];
      if (Array.isArray(stations)) {
        for (const st of stations) {
          const stName = st['اسم المحطة'] || 'محطة غير مسماة';
          const reportDate = st.reportDate || 'غير محدد';
          if (st['الصندوق'] === 'عجز') badMaintenanceItems.push({ location: stName, date: reportDate, type: 'عجز في الصندوق', note: st['ملاحظات الصندوق'] || '' });
          if (st['متابعة القضايا'] === 'يوجد قضايا سابقة') badMaintenanceItems.push({ location: stName, date: reportDate, type: 'قضايا سابقة', note: st['ملاحظات متابعة القضايا'] || '' });
          if (st['مخالفات في المحطة'] === 'نعم') badMaintenanceItems.push({ location: stName, date: reportDate, type: 'وجود مخالفات', note: st['ملاحظات المخالفات'] || '' });
          if (st['جودة المضخات'] === 'سيئة') badMaintenanceItems.push({ location: stName, date: reportDate, type: 'سوء جودة المضخات', note: st['ملاحظات جودة المضخات'] || '' });
          if (st['منظومة المراقبة'] === 'لا تعمل' || st['منظومة المراقبة'] === 'لا يوجد') badMaintenanceItems.push({ location: stName, date: reportDate, type: st['منظومة المراقبة'] === 'لا تعمل' ? 'منظومة المراقبة لا تعمل' : 'منظومة المراقبة غير متوفرة', note: st['ملاحظات منظومة المراقبة'] || '' });
          if (st['الطفايات'] === 'بحاجة صيانة' || st['الطفايات'] === 'غير متوفرة') badMaintenanceItems.push({ location: stName, date: reportDate, type: st['الطفايات'] === 'بحاجة صيانة' ? 'الطفايات بحاجة صيانة' : 'الطفايات غير متوفرة', note: st['ملاحظات الطفايات'] || '' });
        }
      }

      html += `<p style="color: #2e7d32; font-weight: bold; font-size: 16pt; margin-top: 25px;">ثالثاً: الصيانة والتحسينات التشغيلية - السلبيات والملاحظات الحرجة</p>
<table style="border-collapse: collapse; margin-top: 10px;">
  <thead>
    <tr style="background-color: #2e7d32; color: white;">
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: center; font-size: 12pt;">التاريخ</th>
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: right; font-size: 12pt;">الموقع / المحطة</th>
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: right; font-size: 12pt;">نوع السلبية / المخالفة</th>
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: right; font-size: 12pt;">الملاحظات والتفاصيل</th>
    </tr>
  </thead>
  <tbody>`;

      if (badMaintenanceItems.length === 0) {
        html += `  <tr>
    <td colspan="4" style="border: 1px solid #a5d6a7; padding: 14px; text-align: center; color: #2e7d32; font-weight: bold;">
      ممتاز! لم يتم تسجيل أي بنود سلبية أو أعطال في فرع ${sanitizeText(branch)} خلال هذا الشهر.
    </td>
  </tr>`;
      } else {
        for (const item of badMaintenanceItems) {
          html += `  <tr>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: center;">${sanitizeText(item.date)}</td>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: right; font-weight: bold;">${sanitizeText(item.location)}</td>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: right; color: #b71c1c; font-weight: bold;">${sanitizeText(item.type)}</td>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: right;">${sanitizeText(item.note) || '—'}</td>
  </tr>`;
        }
      }

      html += `  </tbody>
</table>`;

      // Fourth: Warnings and issues
      html += `<p style="color: #2e7d32; font-weight: bold; font-size: 16pt; margin-top: 25px;">رابعاً: الإنذارات والقضايا</p>
<table style="border-collapse: collapse; margin-top: 10px; margin-bottom: 20px; width: 100%;">
  <thead>
    <tr style="background: linear-gradient(145deg, #1b5e20, #2e7d32); color: white;">
      <th style="border: 1px solid #1b5e20; padding: 14px 10px; font-weight: bold; text-align: center; font-size: 12pt; letter-spacing: 0.5px;">التاريخ</th>
      <th style="border: 1px solid #1b5e20; padding: 14px 10px; font-weight: bold; text-align: right; font-size: 12pt; letter-spacing: 0.5px;">الموقع / الجهة المعنية</th>
      <th style="border: 1px solid #1b5e20; padding: 14px 10px; font-weight: bold; text-align: right; font-size: 12pt; letter-spacing: 0.5px;">الموضوع / المخالفة</th>
      <th style="border: 1px solid #1b5e20; padding: 14px 10px; font-weight: bold; text-align: center; font-size: 12pt; letter-spacing: 0.5px;">الإجراء المتخذ</th>
    </tr>
  </thead>
  <tbody>`;

      if (issues.length === 0) {
        html += `  <tr>
    <td colspan="4" style="border: 1px solid #c8e6c9; padding: 20px; text-align: center; color: #2e7d32; font-weight: bold; font-size: 13pt; background-color: #f1f8e9;">
      ✅ لا توجد قضايا أو إنذارات مسجلة لهذا الفرع خلال الشهر.
    </td>
  </tr>`;
      } else {
        for (let idx = 0; idx < issues.length; idx++) {
          const issue = issues[idx];
          const rawDate = issue['تاريخ القضية'] || issue.reportDate || 'غير محدد';
          const location = issue['الجهة المعنية'] || 'غير محدد';
          const subject = issue['موضوع القضية'] || 'غير محدد';
          const detailsText = issue['تفاصيل القضية'] ? sanitizeText(issue['تفاصيل القضية']) : '';
          const details = detailsText ? `<div style="font-size: 10pt; color: #666; margin-top: 6px; padding-top: 6px; border-top: 1px dashed #e0e0e0;">📝 ${detailsText}</div>` : '';
          const action = issue['الإجراء المتخذ'] || '—';
          const rowBg = idx % 2 === 0 ? '#fafff5' : '#ffffff';
          const actionColor = action !== '—' ? '#2e7d32' : '#b71c1c';
          const actionBadge = action !== '—' ? '✅' : '⚠️';
          
          html += `  <tr style="background-color: ${rowBg};">
    <td style="border: 1px solid #c8e6c9; padding: 12px 10px; text-align: center; font-weight: bold; color: #1b5e20; font-size: 11pt;">${sanitizeText(rawDate)}</td>
    <td style="border: 1px solid #c8e6c9; padding: 12px 10px; text-align: right; font-weight: bold; font-size: 11pt;">${sanitizeText(location)}</td>
    <td style="border: 1px solid #c8e6c9; padding: 12px 10px; text-align: right; font-size: 11pt;">${sanitizeText(subject)}${details}</td>
    <td style="border: 1px solid #c8e6c9; padding: 12px 10px; text-align: center; font-weight: bold; color: ${actionColor}; font-size: 11pt;">${actionBadge} ${sanitizeText(action)}</td>
  </tr>`;
        }
      }

      html += `  </tbody>
</table>`;

      // Fifth: Suggestions and general notes
      html += `<p style="color: #2e7d32; font-weight: bold; font-size: 16pt; margin-top: 25px;">خامساً: الاقتراحات والملاحظات العامة</p>
<ul>`;

      const allNotes = [];
      const stationNameKey = 'اسم المحطة';
      
      for (const report of reports) {
        if (report['ملاحظات'] && report['ملاحظات'].trim()) {
          allNotes.push(`ملاحظة الجولة: ${sanitizeText(report['ملاحظات'])}`);
        }
        if (Array.isArray(report['المحطات'])) {
          for (const st of report['المحطات']) {
            const note = st['ملاحظات عامة للمحطة'];
            if (note && note.trim()) {
              allNotes.push(`${sanitizeText(st[stationNameKey]) || 'محطة'}: ${sanitizeText(note)}`);
            }
          }
        }
      }
      for (const gp of data.gasPlants) {
        if (gp['ملاحظات عامة وتوصيات'] && gp['ملاحظات عامة وتوصيات'].trim()) {
          allNotes.push(`معمل غاز: ${sanitizeText(gp['ملاحظات عامة وتوصيات'])}`);
        }
      }

      if (allNotes.length === 0) {
        html += `<li>لا يوجد اقتراحات مسجلة</li>`;
      } else {
        for (const note of allNotes) {
          if (note) html += `<li>${note}</li>`;
        }
      }

      html += `</ul><br><br>`;
    }

    // Footer
    html += `
<br><br><br>
<table style="border-collapse: collapse; border: none; margin-top: 40px;">
  <tr style="border: none;">
    <td style="font-weight: bold; text-align: center; border: none; font-size: 14pt; padding: 10px; border-top: 1px solid #333;">مدير قسم الرقابة والمتابعة</td>
    <td style="font-weight: bold; text-align: center; border: none; font-size: 14pt; padding: 10px; border-top: 1px solid #333;">المدير العام</td>
  </tr>
</table>
<br><br>
  <p style="margin: 0; font-weight: bold;">نسخة الـ:</p>
  <p style="margin: 5px 0 0 20px;">_ مكتب الديوان.</p>
</body>
</html>`;

    const htmlToDocx = require('html-to-docx');
    const docxBuffer = await htmlToDocx(html, null, {
      table: { row: { cantSplit: true } }
    });

    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.setHeader('Content-Disposition', `attachment; filename=Monthly_Report_${month}.docx`);
    res.send(Buffer.from(docxBuffer));

  } catch (err) {
    console.error('[MONTHLY REPORT ERROR]', err);
    res.status(500).json({ status: 'error', message: err.message || 'فشل في إنشاء التقرير الشهري' });
  }
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
