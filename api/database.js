const Database = require('better-sqlite3');
const fs = require('fs');
const path = require('path');

// Handle DB path for Vercel (must use /tmp for write access)
const IS_VERCEL = process.env.VERCEL === '1';
const DB_PATH = IS_VERCEL
  ? path.join('/tmp', 'survey.db')
  : path.join(__dirname, 'survey.db');

// Initialize database
function initDatabase() {
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
  // Get all managers
  getAllManagers() {
    const db = getDB();
    const rows = db.prepare('SELECT * FROM managers ORDER BY receivedAt DESC').all();
    return rows.map(row => ({
      id: row.id,
      receivedAt: row.receivedAt,
      ...JSON.parse(row.data)
    }));
  },
  
  // Get all workers
  getAllWorkers() {
    const db = getDB();
    const rows = db.prepare('SELECT * FROM workers ORDER BY receivedAt DESC').all();
    return rows.map(row => ({
      id: row.id,
      receivedAt: row.receivedAt,
      ...JSON.parse(row.data)
    }));
  },
  
  // Add a manager survey
  addManager(entry) {
    const db = getDB();
    const { id, receivedAt, ...data } = entry;
    const insert = db.prepare('INSERT INTO managers (id, receivedAt, data) VALUES (?, ?, ?)');
    insert.run(
      id || Date.now().toString() + Math.random().toString(36).slice(2),
      receivedAt || new Date().toISOString(),
      JSON.stringify(data)
    );
    return id || Date.now().toString() + Math.random().toString(36).slice(2);
  },
  
  // Add a worker survey
  addWorker(entry) {
    const db = getDB();
    const { id, receivedAt, ...data } = entry;
    const insert = db.prepare('INSERT INTO workers (id, receivedAt, data) VALUES (?, ?, ?)');
    insert.run(
      id || Date.now().toString() + Math.random().toString(36).slice(2),
      receivedAt || new Date().toISOString(),
      JSON.stringify(data)
    );
    return id || Date.now().toString() + Math.random().toString(36).slice(2);
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
