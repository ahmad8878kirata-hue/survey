const axios = require('axios');
const fs = require('fs');
const path = require('path');

async function main() {
    try {
        console.log("Calling generate_monthly_report API...");
        const response = await axios.get('http://localhost:3000/api/generate_monthly_report', {
            params: { month: '2026-05' },
            responseType: 'arraybuffer'
        });
        
        const outputPath = path.join(__dirname, 'Monthly_Report_2026-05.docx');
        fs.writeFileSync(outputPath, response.data);
        console.log(`Report generated successfully! Saved to: ${outputPath}`);
    } catch (e) {
        if (e.response && e.response.data) {
            console.error("API Error Response:", Buffer.from(e.response.data).toString('utf8'));
        } else {
            console.error("Error:", e.message);
        }
    }
}

main();
