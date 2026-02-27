const Database = require('better-sqlite3');
const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');

// --- Environment Variables for MySQL ---
const {
  DB_HOST,
  DB_USER,
  DB_PASS,
  DB_NAME,
  DB_PORT = 3306,
  USE_MYSQL = 'false'
} = process.env;

const IS_MYSQL = USE_MYSQL === 'true';

// --- SQLite file location ---
const IS_VERCEL = process.env.VERCEL === '1';
const ENV_DB_PATH = process.env.SQLITE_DB_PATH && String(process.env.SQLITE_DB_PATH).trim();

function resolveDbPath() {
  if (ENV_DB_PATH) {
    return path.isAbsolute(ENV_DB_PATH) ? ENV_DB_PATH : path.resolve(process.cwd(), ENV_DB_PATH);
  }
  if (IS_VERCEL) return path.join('/tmp', 'survey.db');
  return path.join(process.cwd(), 'data', 'survey.db');
}

const DB_PATH = resolveDbPath();

// --- Database Instances ---
let sqliteDb = null;
let mysqlPool = null;

// Initialize SQLite
function initSQLite() {
  const dir = path.dirname(DB_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const db = new Database(DB_PATH);
  db.pragma('foreign_keys = ON');
  db.exec(`
    CREATE TABLE IF NOT EXISTS managers (
      id VARCHAR(255) PRIMARY KEY,
      receivedAt DATETIME NOT NULL,
      data JSON NOT NULL
    )
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS workers (
      id VARCHAR(255) PRIMARY KEY,
      receivedAt DATETIME NOT NULL,
      data JSON NOT NULL
    )
  `);
  db.exec(`
    CREATE TABLE IF NOT EXISTS settings (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL
    )
  `);
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_managers_receivedAt ON managers(receivedAt);
    CREATE INDEX IF NOT EXISTS idx_workers_receivedAt ON workers(receivedAt);
  `);
  console.log('SQLite database initialized at:', DB_PATH);
  return db;
}

// Initialize MySQL
async function initMySQL() {
  if (mysqlPool) return mysqlPool;

  try {
    mysqlPool = mysql.createPool({
      host: DB_HOST,
      user: DB_USER,
      password: DB_PASS,
      database: DB_NAME,
      port: DB_PORT,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0
    });

    // Create tables if they don't exist
    const connection = await mysqlPool.getConnection();
    try {
      await connection.query(`
        CREATE TABLE IF NOT EXISTS managers (
          id VARCHAR(255) PRIMARY KEY,
          receivedAt DATETIME NOT NULL,
          data JSON NOT NULL,
          INDEX idx_managers_receivedAt (receivedAt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `);
      await connection.query(`
        CREATE TABLE IF NOT EXISTS workers (
          id VARCHAR(255) PRIMARY KEY,
          receivedAt DATETIME NOT NULL,
          data JSON NOT NULL,
          INDEX idx_workers_receivedAt (receivedAt)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `);
      await connection.query(`
        CREATE TABLE IF NOT EXISTS settings (
          \`key\` VARCHAR(255) PRIMARY KEY,
          \`value\` VARCHAR(255) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
      `);
      console.log('MySQL connection pool initialized and tables verified.');
    } finally {
      connection.release();
    }
    return mysqlPool;
  } catch (err) {
    console.error('Failed to initialize MySQL pool:', err.message);
    throw err;
  }
}

// Get Database Instance
function getSQLite() {
  if (!sqliteDb) sqliteDb = initSQLite();
  return sqliteDb;
}

// Helper to build WHERE clause for filtering and searching
function buildWhereClause(search, filters, isMysql) {
  const conditions = [];
  const params = [];

  if (search) {
    if (isMysql) {
      conditions.push("(CAST(data AS CHAR) LIKE ? OR receivedAt LIKE ?)");
    } else {
      conditions.push("(data LIKE ? OR receivedAt LIKE ?)");
    }
    params.push(`%${search}%`, `%${search}%`);
  }

  if (filters && typeof filters === 'object') {
    for (const [key, values] of Object.entries(filters)) {
      if (Array.isArray(values) && values.length > 0) {
        if (key === 'receivedAt') {
          conditions.push(`receivedAt IN (${values.map(() => '?').join(',')})`);
          params.push(...values);
        } else {
          let condition;
          if (isMysql) {
            condition = `data->>'$.${key}' IN (${values.map(() => '?').join(',')})`;
          } else {
            condition = `json_extract(data, '$.${key}') IN (${values.map(() => '?').join(',')})`;
          }
          conditions.push(condition);
          params.push(...values);
        }
      }
    }
  }

  const whereFragment = conditions.length > 0 ? "WHERE " + conditions.join(" AND ") : "";
  return { whereFragment, params };
}

// Database operations
const dbOperations = {
  // Get all managers with pagination
  async getAllManagers(limit = 50, offset = 0, search = '', filters = {}) {
    console.log(`[DB DEBUG] getAllManagers: limit=${limit}, offset=${offset}, search=${search}, filters=${JSON.stringify(filters)}, IS_MYSQL=${IS_MYSQL}`);
    const { whereFragment, params } = buildWhereClause(search, filters, IS_MYSQL);

    if (IS_MYSQL) {
      const pool = await initMySQL();
      let query = `SELECT id, receivedAt, data FROM managers ${whereFragment} ORDER BY receivedAt DESC`;
      let queryParams = [...params];

      if (limit !== 'all') {
        query += ' LIMIT ? OFFSET ?';
        queryParams.push(Number(limit), Number(offset));
      }

      const [rows] = await pool.query(query, queryParams);
      return rows.map(row => ({
        id: row.id,
        receivedAt: row.receivedAt instanceof Date ? row.receivedAt.toISOString() : row.receivedAt,
        ...(typeof row.data === 'string' ? JSON.parse(row.data) : row.data)
      }));
    } else {
      const db = getSQLite();
      let query = `SELECT * FROM managers ${whereFragment} ORDER BY receivedAt DESC`;
      let queryParams = [...params];

      if (limit !== 'all') {
        query += ' LIMIT ? OFFSET ?';
        queryParams.push(Number(limit), Number(offset));
      }

      const rows = db.prepare(query).all(...queryParams);
      return rows.map(row => ({
        id: row.id,
        receivedAt: row.receivedAt,
        ...JSON.parse(row.data)
      }));
    }
  },

  // Get all workers with pagination
  async getAllWorkers(limit = 50, offset = 0, search = '', filters = {}) {
    console.log(`[DB DEBUG] getAllWorkers: limit=${limit}, offset=${offset}, search=${search}, filters=${JSON.stringify(filters)}, IS_MYSQL=${IS_MYSQL}`);
    const { whereFragment, params } = buildWhereClause(search, filters, IS_MYSQL);

    if (IS_MYSQL) {
      const pool = await initMySQL();
      let query = `SELECT id, receivedAt, data FROM workers ${whereFragment} ORDER BY receivedAt DESC`;
      let queryParams = [...params];

      if (limit !== 'all') {
        query += ' LIMIT ? OFFSET ?';
        queryParams.push(Number(limit), Number(offset));
      }

      const [rows] = await pool.query(query, queryParams);
      return rows.map(row => ({
        id: row.id,
        receivedAt: row.receivedAt instanceof Date ? row.receivedAt.toISOString() : row.receivedAt,
        ...(typeof row.data === 'string' ? JSON.parse(row.data) : row.data)
      }));
    } else {
      const db = getSQLite();
      let query = `SELECT * FROM workers ${whereFragment} ORDER BY receivedAt DESC`;
      let queryParams = [...params];

      if (limit !== 'all') {
        query += ' LIMIT ? OFFSET ?';
        queryParams.push(Number(limit), Number(offset));
      }

      const rows = db.prepare(query).all(...queryParams);
      return rows.map(row => ({
        id: row.id,
        receivedAt: row.receivedAt,
        ...JSON.parse(row.data)
      }));
    }
  },

  // Get total manager count
  async getManagersCount(search = '', filters = {}) {
    const { whereFragment, params } = buildWhereClause(search, filters, IS_MYSQL);
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [rows] = await pool.query(`SELECT COUNT(*) as count FROM managers ${whereFragment}`, params);
      return rows[0].count;
    } else {
      const db = getSQLite();
      const result = db.prepare(`SELECT COUNT(*) as count FROM managers ${whereFragment}`).get(...params);
      return result ? result.count : 0;
    }
  },

  // Get total worker count
  async getWorkersCount(search = '', filters = {}) {
    const { whereFragment, params } = buildWhereClause(search, filters, IS_MYSQL);
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [rows] = await pool.query(`SELECT COUNT(*) as count FROM workers ${whereFragment}`, params);
      return rows[0].count;
    } else {
      const db = getSQLite();
      const result = db.prepare(`SELECT COUNT(*) as count FROM workers ${whereFragment}`).get(...params);
      return result ? result.count : 0;
    }
  },

  // Add a manager survey
  async addManager(entry) {
    const { id, receivedAt, ...data } = entry;
    const newId = id || (Date.now().toString() + Math.random().toString(36).slice(2));
    const finalReceivedAt = receivedAt || new Date().toISOString();
    const finalData = JSON.stringify(data);

    if (IS_MYSQL) {
      const pool = await initMySQL();
      await pool.query(
        'INSERT INTO managers (id, receivedAt, data) VALUES (?, ?, ?)',
        [newId, finalReceivedAt.replace('T', ' ').replace('Z', ''), finalData]
      );
    } else {
      const db = getSQLite();
      const insert = db.prepare('INSERT INTO managers (id, receivedAt, data) VALUES (?, ?, ?)');
      insert.run(newId, finalReceivedAt, finalData);
    }
    return newId;
  },

  // Add a worker survey
  async addWorker(entry) {
    const { id, receivedAt, ...data } = entry;
    const newId = id || (Date.now().toString() + Math.random().toString(36).slice(2));
    const finalReceivedAt = receivedAt || new Date().toISOString();
    const finalData = JSON.stringify(data);

    if (IS_MYSQL) {
      const pool = await initMySQL();
      await pool.query(
        'INSERT INTO workers (id, receivedAt, data) VALUES (?, ?, ?)',
        [newId, finalReceivedAt.replace('T', ' ').replace('Z', ''), finalData]
      );
    } else {
      const db = getSQLite();
      const insert = db.prepare('INSERT INTO workers (id, receivedAt, data) VALUES (?, ?, ?)');
      insert.run(newId, finalReceivedAt, finalData);
    }
    return newId;
  },

  // Delete a manager survey
  async deleteManager(id) {
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [result] = await pool.query('DELETE FROM managers WHERE id = ?', [id]);
      return result.affectedRows > 0;
    } else {
      const db = getSQLite();
      const deleteStmt = db.prepare('DELETE FROM managers WHERE id = ?');
      const result = deleteStmt.run(id);
      return result.changes > 0;
    }
  },

  // Delete a worker survey
  async deleteWorker(id) {
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [result] = await pool.query('DELETE FROM workers WHERE id = ?', [id]);
      return result.affectedRows > 0;
    } else {
      const db = getSQLite();
      const deleteStmt = db.prepare('DELETE FROM workers WHERE id = ?');
      const result = deleteStmt.run(id);
      return result.changes > 0;
    }
  },

  // Get a manager by ID
  async getManagerById(id) {
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [rows] = await pool.query('SELECT * FROM managers WHERE id = ?', [id]);
      if (rows.length === 0) return null;
      const row = rows[0];
      return {
        id: row.id,
        receivedAt: row.receivedAt instanceof Date ? row.receivedAt.toISOString() : row.receivedAt,
        ...(typeof row.data === 'string' ? JSON.parse(row.data) : row.data)
      };
    } else {
      const db = getSQLite();
      const row = db.prepare('SELECT * FROM managers WHERE id = ?').get(id);
      if (!row) return null;
      return {
        id: row.id,
        receivedAt: row.receivedAt,
        ...JSON.parse(row.data)
      };
    }
  },

  // Get a worker by ID
  async getWorkerById(id) {
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [rows] = await pool.query('SELECT * FROM workers WHERE id = ?', [id]);
      if (rows.length === 0) return null;
      const row = rows[0];
      return {
        id: row.id,
        receivedAt: row.receivedAt instanceof Date ? row.receivedAt.toISOString() : row.receivedAt,
        ...(typeof row.data === 'string' ? JSON.parse(row.data) : row.data)
      };
    } else {
      const db = getSQLite();
      const row = db.prepare('SELECT * FROM workers WHERE id = ?').get(id);
      if (!row) return null;
      return {
        id: row.id,
        receivedAt: row.receivedAt,
        ...JSON.parse(row.data)
      };
    }
  },

  // Get lock status for surveys
  async getSurveyLockStatus() {
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [rows] = await pool.query('SELECT `key`, `value` FROM settings WHERE `key` IN (?, ?)', [
        'lock_worker',
        'lock_manager'
      ]);
      let worker = false;
      let manager = false;
      for (const row of rows) {
        if (row.key === 'lock_worker') worker = row.value === '1';
        if (row.key === 'lock_manager') manager = row.value === '1';
      }
      return { worker, manager };
    } else {
      const db = getSQLite();
      const rows = db
        .prepare('SELECT key, value FROM settings WHERE key IN (?, ?)')
        .all('lock_worker', 'lock_manager');
      let worker = false;
      let manager = false;
      for (const row of rows) {
        if (row.key === 'lock_worker') worker = row.value === '1';
        if (row.key === 'lock_manager') manager = row.value === '1';
      }
      return { worker, manager };
    }
  },

  // Set lock status for a specific survey type
  async setSurveyLock(type, locked) {
    const key = type === 'worker' ? 'lock_worker' : 'lock_manager';
    const value = locked ? '1' : '0';

    if (IS_MYSQL) {
      const pool = await initMySQL();
      await pool.query(
        'INSERT INTO settings (`key`, `value`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `value` = VALUES(`value`)',
        [key, value]
      );
    } else {
      const db = getSQLite();
      const stmt = db.prepare(
        'INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value'
      );
      stmt.run(key, value);
    }
  },

  // Get unique values for a specific column/field
  async getUniqueValues(type, field, filters = {}) {
    const table = type === 'worker' ? 'workers' : 'managers';
    const { whereFragment, params } = buildWhereClause('', filters, IS_MYSQL);

    if (IS_MYSQL) {
      const pool = await initMySQL();
      let query;
      if (field === 'receivedAt') {
        query = `SELECT DISTINCT receivedAt as value FROM ${table} ${whereFragment} ORDER BY receivedAt DESC`;
      } else {
        query = `SELECT DISTINCT TRIM(data->>'$.${field}') as value FROM ${table} ${whereFragment} ORDER BY value ASC`;
      }
      const [rows] = await pool.query(query, params);
      const uniqueValues = Array.from(new Set(rows.map(r => r.value).filter(v => v !== null && v !== '')));
      return uniqueValues.sort();
    } else {
      const db = getSQLite();
      let query;
      if (field === 'receivedAt') {
        query = `SELECT DISTINCT receivedAt as value FROM ${table} ${whereFragment} ORDER BY receivedAt DESC`;
      } else {
        query = `SELECT DISTINCT TRIM(json_extract(data, '$.${field}')) as value FROM ${table} ${whereFragment} ORDER BY value ASC`;
      }
      const rows = db.prepare(query).all(...params);
      const uniqueValues = Array.from(new Set(rows.map(r => r.value).filter(v => v !== null && v !== '')));
      return uniqueValues.sort();
    }
  },

  async getAnalysisAggregation(branchName = null) {
    let filters = {};
    if (branchName) {
      filters['اسم الفرع'] = [branchName];
    }
    const workers = await this.getAllWorkers('all', 0, '', filters);
    const managers = await this.getAllManagers('all', 0, '', filters);

    const aggregate = (data, field) => {
      const counts = {};
      data.forEach(row => {
        let val = row[field];
        if (typeof val === 'string') val = val.trim();
        if (val) counts[val] = (counts[val] || 0) + 1;
      });
      return counts;
    };

    const aggregateMulti = (data, field) => {
      const counts = {};
      data.forEach(row => {
        const val = row[field];
        if (typeof val === 'string') {
          val.split(',').forEach(v => {
            const trimmed = v.trim();
            if (trimmed) counts[trimmed] = (counts[trimmed] || 0) + 1;
          });
        } else if (Array.isArray(val)) {
          val.forEach(v => {
            const trimmed = typeof v === 'string' ? v.trim() : v;
            if (trimmed) counts[trimmed] = (counts[trimmed] || 0) + 1;
          });
        }
      });
      return counts;
    };

    const getTimeline = (data) => {
      return data.reduce((acc, row) => {
        if (!row.receivedAt) return acc;
        const date = row.receivedAt.includes('T') ? row.receivedAt.split('T')[0] : row.receivedAt.split(' ')[0];
        acc[date] = (acc[date] || 0) + 1;
        return acc;
      }, {});
    };

    return {
      worker: {
        total: workers.length || 0,
        satisfaction: aggregate(workers, 'مدى الرضا عن العمل'),
        supervision: aggregate(workers, 'الرقابة عادلة؟'),
        salary: aggregate(workers, 'الراتب مناسب؟'),
        hours: aggregate(workers, 'ساعات العمل مناسبة؟'),
        appreciation: aggregate(workers, 'الشعور بالتقدير؟'),
        stability: aggregate(workers, 'الاستقرار الوظيفي؟'),
        recommendation: aggregate(workers, 'هل تنصح بالعمل في المحطة؟'),
        violations: aggregateMulti(workers, 'أسباب المخالفات'),
        branches: aggregate(workers, 'اسم الفرع'),
        dissatisfactionByBranch: (function () {
          const dissatisfiedWorkers = workers.filter(w => w['مدى الرضا عن العمل'] === 'غير راضي أبداً');
          return aggregate(dissatisfiedWorkers, 'اسم الفرع');
        })(),
        timeline: getTimeline(workers)
      },
      manager: {
        total: managers.length || 0,
        branches: aggregate(managers, 'اسم الفرع'),
        durations: aggregate(managers, 'مدة العمل في منصب مدير محطة'),
        timeline: getTimeline(managers)
      }
    };
  },

  async cleanBranchNames() {
    const tables = ['workers', 'managers'];
    let updatedCount = 0;

    // Helper to normalize Arabic characters for better matching
    const normalizeAr = (text) => {
      if (!text || typeof text !== 'string') return '';
      return text
        .replace(/[أإآ]/g, 'ا')
        .replace(/[ة]/g, 'ه')
        .replace(/[ى]/g, 'ي')
        .replace(/\./g, '') // Remove dots (e.g., القر.احه)
        .trim();
    };

    const idlibKeywords = [
      'ادلب', 'سرمدا', 'اريحا', 'القصور', 'المدنيه', 'الغابات', 'الفاروق', 'الفرع', 'الدانه',
      'جسر الشغور', 'حارم', 'دلب', 'سراقب', 'باب الهوي', 'صرمدا', 'معره النعمان', 'كنصفره',
      'جبل الزاويه', 'اداب', 'عزمارين', 'معره مصرين', 'معرتمصرين', 'الدانا',
      'الاول', 'الثاني', 'الثالث', 'الرابع', 'الخامس', 'السادس', 'السابع', 'الثامن', 'التاسع', 'العاشر',
      'المدينه', 'المدينة', 'معرة نعمان', 'الحساوي', 'الامانه', 'الجسر', 'القنيه', 'بسيدا', 'وتار', 'حساوي'
    ].map(normalizeAr);

    const lattakiaKeywords = [
      'اللاذقيه', 'الاذقيه', 'حسدؤه', 'حيدره', 'الرمل الشمالي', 'القraحه', 'القرداحه',
      'طيب الرحمن', 'الرحمن', 'دوار الجامعه', 'الزقيه', 'دم صرخه', 'للاللاذقيه',
      'دمسرخو', 'للاذقيه', 'الزقيه', 'القرداحة', 'القرداحه', 'حيدره', 'الذقية', 'دمسرخو', 'للاذقية',
      'اللازقيه', 'الساحل'
    ].map(normalizeAr);

    const aleppoKeywords = ['حلب', 'اعزاز', 'الشعار', 'خان السبل', 'عفرين', 'منبج', 'الجنوبي', 'الباب', 'جرابلس'].map(normalizeAr);
    const tartusKeywords = ['طرطوس', 'الرمال', 'بانياس', 'صرصوس'].map(normalizeAr);
    const raqqaKeywords = ['الرقة', 'الرقه', 'الطبفه', 'الطبقه'].map(normalizeAr);
    const homsKeywords = ['حمص', 'تدمر', 'الرصيف', 'اللؤلؤه', 'السلام', 'رامي عبد الكريم'].map(normalizeAr);
    const hamaKeywords = ['حماه', 'حماة', 'مصياف', 'الوزير', 'الانشاءات', 'النشائات', 'رنجوس'].map(normalizeAr);
    const daraaKeywords = ['درعا'].map(normalizeAr);
    const deirEzorKeywords = ['دير الزور', 'ديرالزور'].map(normalizeAr);
    const damascusKeywords = ['دمشق', 'دمش', 'دمشف', 'المجتهد', 'زين الشام', 'الزبداني', 'الحسين', 'حسين', 'التركاوي', 'التكروري', 'ببيلا', 'بوابه الشام', 'القدم'].map(normalizeAr);

    for (const table of tables) {
      let rows = [];
      if (IS_MYSQL) {
        const pool = await initMySQL();
        const [mysqlRows] = await pool.query(`SELECT id, data FROM ${table}`);
        rows = mysqlRows;
      } else {
        const db = getSQLite();
        rows = db.prepare(`SELECT id, data FROM ${table}`).all();
      }

      console.log(`[CLEAN] Scanning table: ${table}, rows: ${rows.length}`);

      for (const row of rows) {
        const data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;

        // Robust key finding
        let branchKey = 'اسم الفرع';
        let branchValue = data[branchKey];
        if (branchValue === undefined) {
          const foundKey = Object.keys(data).find(k => {
            const nk = normalizeAr(k);
            return nk.includes('فرع') || nk.includes('محطه');
          });
          if (foundKey) {
            branchKey = foundKey;
            branchValue = data[foundKey];
          }
        }

        branchValue = branchValue || '';
        if (typeof branchValue !== 'string') branchValue = String(branchValue);

        const branchNormalized = normalizeAr(branchValue);
        let newBranch = null;

        // Matching Logic
        if (aleppoKeywords.some(k => branchNormalized.includes(k))) {
          newBranch = 'حلب';
        }
        else if (tartusKeywords.some(k => branchNormalized.includes(k))) {
          newBranch = 'طرطوس';
        }
        else if (raqqaKeywords.some(k => branchNormalized.includes(k))) {
          newBranch = 'الرقة';
        }
        else if (hamaKeywords.some(k => branchNormalized.includes(k))) {
          newBranch = 'حماه';
        }
        else if (daraaKeywords.some(k => branchNormalized.includes(k))) {
          newBranch = 'درعا';
        }
        else if (deirEzorKeywords.some(k => branchNormalized.includes(k))) {
          newBranch = 'دير الزور';
        }
        else if (homsKeywords.some(k => branchNormalized.includes(k))) {
          newBranch = 'حمص';
        }
        else if (lattakiaKeywords.some(k => branchNormalized.includes(k))) {
          newBranch = 'اللاذقية';
        }
        else if (damascusKeywords.some(k => branchNormalized.includes(k))) {
          newBranch = 'دمشق';
        }
        // Idlib: matches keywords, English numbers, or Arabic numbers ١-١٠
        else if (idlibKeywords.some(k => branchNormalized.includes(k)) || /\d+/.test(branchValue) || /[١٢٣٤٥٦٧٨٩٠]/.test(branchValue)) {
          newBranch = 'إدلب';
        }

        if (newBranch && newBranch !== branchValue) {
          console.log(`[CLEAN] Updated ID ${row.id}: "${branchValue}" -> "${newBranch}"`);
          data[branchKey] = newBranch;
          const updatedJson = JSON.stringify(data);

          if (IS_MYSQL) {
            const pool = await initMySQL();
            await pool.query(`UPDATE ${table} SET data = ? WHERE id = ?`, [updatedJson, row.id]);
          } else {
            const db = getSQLite();
            db.prepare(`UPDATE ${table} SET data = ? WHERE id = ?`).run(updatedJson, row.id);
          }
          updatedCount++;
        }
      }
    }
    console.log(`[CLEAN] Finished. Total updated: ${updatedCount}`);
    return updatedCount;
  }
};

// Initialize SQLite if not using MySQL
if (!IS_MYSQL) {
  getSQLite();
}

// Expose some internals for other modules (read-only usage)
dbOperations.DB_PATH = DB_PATH;
dbOperations.IS_MYSQL = IS_MYSQL;

module.exports = dbOperations;
