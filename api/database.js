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
  async getUniqueValues(type, field) {
    const table = type === 'worker' ? 'workers' : 'managers';

    if (IS_MYSQL) {
      const pool = await initMySQL();
      let query;
      if (field === 'receivedAt') {
        query = `SELECT DISTINCT receivedAt as value FROM ${table} ORDER BY receivedAt DESC`;
      } else {
        query = `SELECT DISTINCT data->>'$.${field}' as value FROM ${table} ORDER BY value ASC`;
      }
      const [rows] = await pool.query(query);
      return rows.map(r => r.value).filter(v => v !== null);
    } else {
      const db = getSQLite();
      let query;
      if (field === 'receivedAt') {
        query = `SELECT DISTINCT receivedAt as value FROM ${table} ORDER BY receivedAt DESC`;
      } else {
        query = `SELECT DISTINCT json_extract(data, '$.${field}') as value FROM ${table} ORDER BY value ASC`;
      }
      const rows = db.prepare(query).all();
      return rows.map(r => r.value).filter(v => v !== null);
    }
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
