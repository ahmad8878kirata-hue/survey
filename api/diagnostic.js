const Database = require('better-sqlite3');
const path = require('path');
const fs = require('fs');

async function diagnose() {
    console.log('--- Starting Diagnostic ---');
    const dbPath = path.join(process.cwd(), 'data', 'survey.db');
    console.log('Database Path:', dbPath);
    console.log('File Exists:', fs.existsSync(dbPath));

    if (fs.existsSync(dbPath)) {
        const stats = fs.statSync(dbPath);
        console.log('Size:', stats.size, 'bytes');
    }

    try {
        console.log('Attempting to open database...');
        const start = Date.now();
        const db = new Database(dbPath);
        console.log('Opened in:', Date.now() - start, 'ms');

        console.log('Running simple query...');
        const qStart = Date.now();
        const count = db.prepare('SELECT count(*) as count FROM managers').get();
        console.log('Query completed in:', Date.now() - qStart, 'ms');
        console.log('Managers count:', count.count);

        console.log('Fetching a sample (limit 5)...');
        const sStart = Date.now();
        const sample = db.prepare('SELECT * FROM managers LIMIT 5').all();
        console.log('Sample fetched in:', Date.now() - sStart, 'ms');

        db.close();
        console.log('Database closed.');
    } catch (err) {
        console.error('DIAGNOSTIC FAILED:', err.message);
    }
    console.log('--- Diagnostic Finished ---');
}

diagnose();
