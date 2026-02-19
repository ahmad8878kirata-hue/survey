require('dotenv').config();
const Database = require('better-sqlite3');
const mysql = require('mysql2/promise');
const path = require('path');

async function migrate() {
    const {
        DB_HOST,
        DB_USER,
        DB_PASS,
        DB_NAME,
        DB_PORT = 3306
    } = process.env;

    console.log('--- Starting Migration from SQLite to MySQL ---');

    // 1. Connect to SQLite
    const sqlitePath = path.join(process.cwd(), 'data', 'survey.db');
    let sqliteDb;
    try {
        sqliteDb = new Database(sqlitePath, { fileMustExist: true });
        console.log('Connected to SQLite at:', sqlitePath);
    } catch (err) {
        console.error('SQLite database not found or inaccessible:', err.message);
        process.exit(1);
    }

    // 2. Connect to MySQL
    let mysqlConn;
    try {
        mysqlConn = await mysql.createConnection({
            host: DB_HOST,
            user: DB_USER,
            password: DB_PASS,
            database: DB_NAME,
            port: DB_PORT
        });
        console.log('Connected to MySQL database:', DB_NAME);
    } catch (err) {
        console.error('Failed to connect to MySQL. Ensure the database exists and credentials are correct.');
        console.error('Error:', err.message);
        process.exit(1);
    }

    try {
        // 3. Migrate Managers
        console.log('Migrating managers...');
        const managers = sqliteDb.prepare('SELECT * FROM managers').all();
        for (const m of managers) {
            await mysqlConn.execute(
                'INSERT IGNORE INTO managers (id, receivedAt, data) VALUES (?, ?, ?)',
                [m.id, m.receivedAt.replace('T', ' ').replace('Z', ''), m.data]
            );
        }
        console.log(`Migrated ${managers.length} manager records.`);

        // 4. Migrate Workers
        console.log('Migrating workers...');
        const workers = sqliteDb.prepare('SELECT * FROM workers').all();
        for (const w of workers) {
            await mysqlConn.execute(
                'INSERT IGNORE INTO workers (id, receivedAt, data) VALUES (?, ?, ?)',
                [w.id, w.receivedAt.replace('T', ' ').replace('Z', ''), w.data]
            );
        }
        console.log(`Migrated ${workers.length} worker records.`);

        console.log('--- Migration Completed Successfully ---');

    } catch (err) {
        console.error('Migration failed during data transfer:', err.message);
    } finally {
        sqliteDb.close();
        await mysqlConn.end();
    }
}

migrate();
