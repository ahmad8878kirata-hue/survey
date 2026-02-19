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

// Database operations
const dbOperations = {
  // Get all managers with pagination
  async getAllManagers(limit = 50, offset = 0) {
    console.log(`[DB DEBUG] getAllManagers: limit=${limit}, offset=${offset}, IS_MYSQL=${IS_MYSQL}`);
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [rows] = await pool.query(
        'SELECT id, receivedAt, data FROM managers ORDER BY receivedAt DESC LIMIT ? OFFSET ?',
        [limit, offset]
      );
      return rows.map(row => ({
        id: row.id,
        receivedAt: row.receivedAt instanceof Date ? row.receivedAt.toISOString() : row.receivedAt,
        ...(typeof row.data === 'string' ? JSON.parse(row.data) : row.data)
      }));
    } else {
      const db = getSQLite();
      const rows = db.prepare('SELECT * FROM managers ORDER BY receivedAt DESC LIMIT ? OFFSET ?').all(limit, offset);
      return rows.map(row => ({
        id: row.id,
        receivedAt: row.receivedAt,
        ...JSON.parse(row.data)
      }));
    }
  },

  // Get all workers with pagination
  async getAllWorkers(limit = 50, offset = 0) {
    console.log(`[DB DEBUG] getAllWorkers: limit=${limit}, offset=${offset}, IS_MYSQL=${IS_MYSQL}`);
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [rows] = await pool.query(
        'SELECT id, receivedAt, data FROM workers ORDER BY receivedAt DESC LIMIT ? OFFSET ?',
        [limit, offset]
      );
      return rows.map(row => ({
        id: row.id,
        receivedAt: row.receivedAt instanceof Date ? row.receivedAt.toISOString() : row.receivedAt,
        ...(typeof row.data === 'string' ? JSON.parse(row.data) : row.data)
      }));
    } else {
      const db = getSQLite();
      const rows = db.prepare('SELECT * FROM workers ORDER BY receivedAt DESC LIMIT ? OFFSET ?').all(limit, offset);
      return rows.map(row => ({
        id: row.id,
        receivedAt: row.receivedAt,
        ...JSON.parse(row.data)
      }));
    }
  },

  // Get total manager count
  async getManagersCount() {
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [rows] = await pool.query('SELECT COUNT(*) as count FROM managers');
      return rows[0].count;
    } else {
      const db = getSQLite();
      const result = db.prepare('SELECT COUNT(*) as count FROM managers').get();
      return result ? result.count : 0;
    }
  },

  // Get total worker count
  async getWorkersCount() {
    if (IS_MYSQL) {
      const pool = await initMySQL();
      const [rows] = await pool.query('SELECT COUNT(*) as count FROM workers');
      return rows[0].count;
    } else {
      const db = getSQLite();
      const result = db.prepare('SELECT COUNT(*) as count FROM workers').get();
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
  }
};

// Initialize SQLite if not using MySQL
if (!IS_MYSQL) {
  getSQLite();
}

module.exports = dbOperations;
