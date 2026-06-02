const db = require('../api/database');

async function main() {
    try {
        console.log("Fetching all daily reports...");
        const reports = await db.getAllDailyReports('all', 0, '', {});
        console.log(`Total daily reports: ${reports.length}`);
        if (reports.length > 0) {
            console.log("Sample report structure:", JSON.stringify(reports[0], null, 2));
            const months = {};
            reports.forEach(r => {
                const dateStr = r.receivedAt || '';
                const m = dateStr.slice(0, 7);
                months[m] = (months[m] || 0) + 1;
            });
            console.log("Distribution of reports by month (receivedAt):", months);
        } else {
            console.log("No daily reports found in database.");
        }
    } catch (e) {
        console.error("Error:", e);
    }
}

main();
