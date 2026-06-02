const htmlToDocx = require('html-to-docx');

// Test with minimal HTML similar to what the report generates
const testCases = [
  {
    name: 'Simple table no width',
    html: `<html><body><table style="border-collapse: collapse;"><tr><td style="border: 1px solid #a5d6a7; padding: 10px;">Test</td></tr></table></body></html>`
  },
  {
    name: 'Table with background-color on tr',
    html: `<html><body><table><tr style="background-color: #2e7d32; color: white;"><th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: center; font-size: 12pt;">Header</th></tr><tr><td style="border: 1px solid #a5d6a7; padding: 10px;">Data</td></tr></table></body></html>`
  },
  {
    name: 'Table with background-color on th',
    html: `<html><body><table><tr><th style="background-color: #e8f5e9; border: 1px solid #a5d6a7; padding: 12px;">Header</th></tr><tr><td>Data</td></tr></table></body></html>`
  },
  {
    name: 'Table with page-break-before',
    html: `<html><body><div style="page-break-before: always;"></div><p>Test</p></body></html>`
  },
  {
    name: 'Table with border-top on td',
    html: `<html><body><table style="border-collapse: collapse; border: none; margin-top: 40px;"><tr style="border: none;"><td style="font-weight: bold; text-align: center; border: none; font-size: 14pt; padding: 10px; border-top: 1px solid #333;">Test</td></tr></table></body></html>`
  },
  {
    name: 'Full RTL doc',
    html: `<!DOCTYPE html><html lang="ar" dir="rtl"><head><meta charset="UTF-8"><style>body{font-family:Arial;} table{border-collapse:collapse;margin:15px 0;} th,td{border:1px solid #a5d6a7;padding:10px;text-align:center;font-size:12pt;} th{background-color:#e8f5e9;font-weight:bold;color:#2e7d32;}</style></head><body><p>Test</p><table><tr><th>H1</th><th>H2</th></tr><tr><td>D1</td><td>D2</td></tr></table></body></html>`
  },
  {
    name: 'Colspan test',
    html: `<html><body><table><tr><td colspan="4" style="border: 1px solid #a5d6a7; padding: 14px; text-align: center;">Spanning</td></tr></table></body></html>`
  }
];

(async () => {
  for (const tc of testCases) {
    try {
      const buffer = await htmlToDocx(tc.html, null, { table: { row: { cantSplit: true } } });
      console.log(`✅ PASS: ${tc.name} (${buffer.byteLength} bytes)`);
    } catch (err) {
      console.error(`❌ FAIL: ${tc.name} => ${err.message}`);
    }
  }
})();
