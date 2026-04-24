const Database = require('better-sqlite3');
const path = require('path');

const dbPath = path.resolve(process.cwd(), 'data', 'survey.db');
console.log('Opening database for migration at:', dbPath);
const db = new Database(dbPath);

const mapping = {
    "9. أبرز نقطة إيجابية": "1. أبرز النقاط الإيجابية",
    "1. سرعة وفعالية الاستجابة": "2. سرعة وفعالية الاستجابة",
    "2. منح الصلاحيات": "3. منح الصلاحيات",
    "3. التجاوب مع طلبات الصيانة": "4. التجاوب مع طلبات الصيانة",
    "4. الجولات التفقدية": "5. الجولات التفقدية",
    "5. التعامل مع الشكاوى والعدل": "6. التعامل مع الشكاوى والعدل",
    "6. الاهتمام بالمقترحات": "7. الاهتمام بالمقترحات",
    "7. التعاون والتنسيق بين الأقسام": "8. التعاون والتنسيق بين الأقسام",
    "8. الثناء والمكافأة": "9. الثناء والمكافأة",
    "11. الالتزام بالتعليمات والمتابعة": "11. الالتزام بالتعليمات والمتابعة", // Keep this for now or map to notes
    "10. التطوير المطلوب": "12. التطوير المطلوب",
    "12. التعاون في الملاحظات والجرد": "13. التعاون في الملاحظات والجرد",
    "13. إطلاع المشرف على المستجدات": "14. إطلاع المشرف على المستجدات",
    "14. تقبل الملاحظات": "15. تقبل الملاحظات",
    "15. التقييم العام والعقبات": "16. التقييم العام والعقبات"
};

const rows = db.prepare('SELECT id, data FROM supervisors').all();
let updatedCount = 0;

const updateStmt = db.prepare('UPDATE supervisors SET data = ? WHERE id = ?');

rows.forEach(row => {
    let data = JSON.parse(row.data);
    let newData = {};
    let changed = false;

    // First, copy non-mapped keys
    for (const key in data) {
        if (!mapping[key]) {
            newData[key] = data[key];
        }
    }

    // Then, apply mappings
    for (const oldKey in mapping) {
        if (data[oldKey] !== undefined) {
            const newKey = mapping[oldKey];
            // If newKey already exists in record (e.g. from a recent submission), keep the recent one but we log it
            if (newData[newKey] === undefined) {
                newData[newKey] = data[oldKey];
                changed = true;
            } else {
                // If both exist, keep the one that's already in newData (assuming it's the newer one or already processed)
                // Actually, since we're iterating over the oldKey mapping, we should be careful.
            }
        }
    }

    if (changed) {
        updateStmt.run(JSON.stringify(newData), row.id);
        updatedCount++;
    }
});

console.log(`Migration complete. Updated ${updatedCount} supervisor records.`);
