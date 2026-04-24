const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.resolve(process.cwd(), 'data', 'survey.db');
console.log('Opening database at:', dbPath);
const db = new Database(dbPath);

const rows = db.prepare('SELECT data FROM supervisors').all();
const keys = new Set();
rows.forEach(row => {
    const data = JSON.parse(row.data);
    Object.keys(data).forEach(k => keys.add(k));
});

console.log('Current Supervisor Keys:');
console.log(JSON.stringify(Array.from(keys), null, 2));
