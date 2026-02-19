const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

// --- SQLite file location ---
// On some hosts (e.g. serverless), the filesystem can be ephemeral.
// For persistent hosting (VPS/shared hosting running Node), store the DB in a persistent folder.
//
// You can override the database file location with:
//   SQLITE_DB_PATH=/absolute/or/relative/path/to/survey.db
//
// If deployed on Vercel, we must use /tmp for write access (but it is NOT permanent there).
const IS_VERCEL = process.env.VERCEL === '1';
const ENV_DB_PATH = process.env.SQLITE_DB_PATH && String(process.env.SQLITE_DB_PATH).trim();

function resolveDbPath() {
  if (ENV_DB_PATH) {
    // If a relative path is provided, resolve it from the project root (process cwd).
    return path.isAbsolute(ENV_DB_PATH) ? ENV_DB_PATH : path.resolve(process.cwd(), ENV_DB_PATH);
  }

  if (IS_VERCEL) return path.join('/tmp', 'survey.db');

  // Default: persist under <projectRoot>/data/survey.db
  return path.join(process.cwd(), 'data', 'survey.db');
}

const DB_PATH = resolveDbPath();

// Initialize database
function initDatabase() {
  // Ensure parent directory exists (important for ./data/survey.db)
  const dir = path.dirname(DB_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

  const db = new Database(DB_PATH);
  
  // Enable foreign keys
  db.pragma('foreign_keys = ON');
  
  // Create managers table
  db.exec(`
    CREATE TABLE IF NOT EXISTS managers (
      id TEXT PRIMARY KEY,
      receivedAt TEXT NOT NULL,
      data TEXT NOT NULL
    )
  `);
  
  // Create workers table
  db.exec(`
    CREATE TABLE IF NOT EXISTS workers (
      id TEXT PRIMARY KEY,
      receivedAt TEXT NOT NULL,
      data TEXT NOT NULL
    )
  `);
  
  // Create indexes for faster queries
  db.exec(`
    CREATE INDEX IF NOT EXISTS idx_managers_receivedAt ON managers(receivedAt);
    CREATE INDEX IF NOT EXISTS idx_workers_receivedAt ON workers(receivedAt);
  `);
  
  console.log('SQLite database initialized at:', DB_PATH);
  return db;
}

// Get database instance
let dbInstance = null;
function getDB() {
  if (!dbInstance) {
    dbInstance = initDatabase();
  }
  return dbInstance;
}

// Migrate data from JSON to SQLite
function migrateFromJSON() {
  // JSON file is always in api/ directory (or deployment root)
  const jsonPath = path.join(__dirname, 'database.json');
  
  // Check if JSON file exists
  if (!fs.existsSync(jsonPath)) {
    console.log('No JSON database file found, skipping migration');
    return;
  }
  
  const db = getDB();
  
  try {
    const jsonData = JSON.parse(fs.readFileSync(jsonPath, 'utf-8'));
    
    // Check if tables already have data
    const managerCount = db.prepare('SELECT COUNT(*) as count FROM managers').get();
    const workerCount = db.prepare('SELECT COUNT(*) as count FROM workers').get();
    
    if (managerCount.count > 0 || workerCount.count > 0) {
      console.log('Database already contains data, skipping migration');
      return;
    }
    
    // Migrate managers
    if (jsonData.managers && Array.isArray(jsonData.managers)) {
      const insertManager = db.prepare('INSERT INTO managers (id, receivedAt, data) VALUES (?, ?, ?)');
      const insertManagers = db.transaction((managers) => {
        for (const manager of managers) {
          const { id, receivedAt, ...data } = manager;
          insertManager.run(
            id || Date.now().toString() + Math.random().toString(36).slice(2),
            receivedAt || new Date().toISOString(),
            JSON.stringify(data)
          );
        }
      });
      insertManagers(jsonData.managers);
      console.log(`Migrated ${jsonData.managers.length} manager records`);
    }
    
    // Migrate workers
    if (jsonData.workers && Array.isArray(jsonData.workers)) {
      const insertWorker = db.prepare('INSERT INTO workers (id, receivedAt, data) VALUES (?, ?, ?)');
      const insertWorkers = db.transaction((workers) => {
        for (const worker of workers) {
          const { id, receivedAt, ...data } = worker;
          insertWorker.run(
            id || Date.now().toString() + Math.random().toString(36).slice(2),
            receivedAt || new Date().toISOString(),
            JSON.stringify(data)
          );
        }
      });
      insertWorkers(jsonData.workers);
      console.log(`Migrated ${jsonData.workers.length} worker records`);
    }
    
    console.log('Migration completed successfully');
  } catch (err) {
    console.error('Error during migration:', err);
  }
}

// Database operations
const dbOperations = {
  // Get all managers with pagination
  getAllManagers(limit = 50, offset = 0) {
    const db = getDB();
    const rows = db.prepare('SELECT * FROM managers ORDER BY receivedAt DESC LIMIT ? OFFSET ?').all(limit, offset);
    return rows.map(row => ({
      id: row.id,
      receivedAt: row.receivedAt,
      ...JSON.parse(row.data)
    }));
  },
  
  // Get all workers with pagination
  getAllWorkers(limit = 50, offset = 0) {
    const db = getDB();
    const rows = db.prepare('SELECT * FROM workers ORDER BY receivedAt DESC LIMIT ? OFFSET ?').all(limit, offset);
    return rows.map(row => ({
      id: row.id,
      receivedAt: row.receivedAt,
      ...JSON.parse(row.data)
    }));
  },

  // Get total manager count
  getManagersCount() {
    const db = getDB();
    const result = db.prepare('SELECT COUNT(*) as count FROM managers').get();
    return result ? result.count : 0;
  },

  // Get total worker count
  getWorkersCount() {
    const db = getDB();
    const result = db.prepare('SELECT COUNT(*) as count FROM workers').get();
    return result ? result.count : 0;
  },
  
  // Add a manager survey
  addManager(entry) {
    const db = getDB();
    const { id, receivedAt, ...data } = entry;
    const insert = db.prepare('INSERT INTO managers (id, receivedAt, data) VALUES (?, ?, ?)');
    const newId = id || (Date.now().toString() + Math.random().toString(36).slice(2));
    insert.run(
      newId,
      receivedAt || new Date().toISOString(),
      JSON.stringify(data)
    );
    return newId;
  },
  
  // Add a worker survey
  addWorker(entry) {
    const db = getDB();
    const { id, receivedAt, ...data } = entry;
    const insert = db.prepare('INSERT INTO workers (id, receivedAt, data) VALUES (?, ?, ?)');
    const newId = id || (Date.now().toString() + Math.random().toString(36).slice(2));
    insert.run(
      newId,
      receivedAt || new Date().toISOString(),
      JSON.stringify(data)
    );
    return newId;
  },
  
  // Delete a manager survey
  deleteManager(id) {
    const db = getDB();
    const deleteStmt = db.prepare('DELETE FROM managers WHERE id = ?');
    const result = deleteStmt.run(id);
    return result.changes > 0;
  },
  
  // Delete a worker survey
  deleteWorker(id) {
    const db = getDB();
    const deleteStmt = db.prepare('DELETE FROM workers WHERE id = ?');
    const result = deleteStmt.run(id);
    return result.changes > 0;
  },
  
  // Get a manager by ID
  getManagerById(id) {
    const db = getDB();
    const row = db.prepare('SELECT * FROM managers WHERE id = ?').get(id);
    if (!row) return null;
    return {
      id: row.id,
      receivedAt: row.receivedAt,
      ...JSON.parse(row.data)
    };
  },
  
  // Get a worker by ID
  getWorkerById(id) {
    const db = getDB();
    const row = db.prepare('SELECT * FROM workers WHERE id = ?').get(id);
    if (!row) return null;
    return {
      id: row.id,
      receivedAt: row.receivedAt,
      ...JSON.parse(row.data)
    };
  }
};

// Initialize database and migrate on first load
getDB();
migrateFromJSON();

module.exports = dbOperations;
