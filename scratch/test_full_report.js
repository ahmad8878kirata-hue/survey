// This script simulates the exact report generation logic from index.js
// and tries to convert it to DOCX, catching the exact error.

const db = require('../api/database');
const htmlToDocx = require('html-to-docx');

const month = '2026-05';

(async () => {
  try {
    const dailyReports = await db.getRecordsByMonth('daily_reports', month);
    console.log(`Found ${dailyReports.length} reports for ${month}`);

    if (dailyReports.length === 0) {
      console.log('No reports found. Trying other months...');
      // Try to find any month with data
      for (const m of ['2026-04', '2026-03', '2026-02', '2026-01', '2025-12', '2025-11']) {
        const r = await db.getRecordsByMonth('daily_reports', m);
        if (r.length > 0) {
          console.log(`Found ${r.length} reports for ${m}`);
          break;
        }
      }
      return;
    }

    // Group reports by branch
    const branchReports = {};
    for (const report of dailyReports) {
      const branch = report['اسم الفرع'] || 'غير محدد';
      if (!branchReports[branch]) {
        branchReports[branch] = { reports: [], stations: [], issues: [], gasPlants: [] };
      }
      branchReports[branch].reports.push(report);

      if (Array.isArray(report['المحطات'])) {
        const reportDate = report['تاريخ الجولة'] || report['receivedAt']?.slice(0, 10) || 'غير محدد';
        const stationsWithDate = report['المحطات'].map(st => ({
          ...st,
          reportDate: reportDate
        }));
        branchReports[branch].stations.push(...stationsWithDate);
      }
      if (Array.isArray(report['القضايا'])) {
        const reportDate = report['تاريخ الجولة'] || report['receivedAt']?.slice(0, 10) || '';
        const issuesWithDate = report['القضايا'].map(issue => ({ ...issue, reportDate: reportDate }));
        branchReports[branch].issues.push(...issuesWithDate);
      }
      if (Array.isArray(report['معامل الغاز'])) {
        branchReports[branch].gasPlants.push(...report['معامل الغاز']);
      }
    }

    const monthDisplay = month;
    const branchNames = Object.keys(branchReports).sort();
    console.log(`Branches: ${branchNames.join(', ')}`);

    const sanitizeText = (text) => {
      if (!text) return '';
      return String(text)
        .replace(/\(/g, ' - ')
        .replace(/\)/g, ' - ')
        .replace(/\s*-\s*-\s*/g, ' - ')
        .trim();
    };

    let isFirstBranch = true;
    let html = `<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head><meta charset="UTF-8"><style>
  body { font-family: 'Traditional Arabic', Arial, sans-serif; font-size: 14pt; line-height: 1.8; color: #333; }
  table { border-collapse: collapse; margin: 15px 0; }
  th, td { border: 1px solid #a5d6a7; padding: 10px 12px; text-align: center; font-size: 12pt; }
  th { background-color: #e8f5e9; font-weight: bold; color: #2e7d32; font-size: 13pt; }
  ul { margin: 8px 0; padding-right: 30px; }
  li { margin: 5px 0; line-height: 1.6; }
</style></head>
<body>
<p style="text-align: center; font-weight: bold; font-size: 24pt; color: #1b5e20;">السيد المدير العام</p>
<p style="text-align: center; font-size: 16pt; color: #555; margin-bottom: 30px;">التقرير الشهري المجمع لجولات الرقابة والمتابعة - شهر ${monthDisplay}</p>
`;

    for (const branch of branchNames) {
      const data = branchReports[branch];
      const { reports, stations, issues } = data;

      if (!isFirstBranch) {
        html += `<div style="page-break-before: always;"></div>`;
      }
      isFirstBranch = false;

      html += `<p style="color: #c62828; font-size: 20pt; font-weight: bold; text-align: center; margin-top: 35px; margin-bottom: 25px;">تقرير فرع: ${branch}</p>`;

      html += `<p style="color: #2e7d32; font-weight: bold; font-size: 16pt; margin-top: 25px;">أولاً: عدد الجولات التفتيشية</p>
<ul>
  <li>عدد الجولات المنفذة في فرع ${branch}: ${reports.length}</li>
</ul>`;

      const evalScores = { 'ممتاز': 5, 'جيد جداً': 4, 'جيد جدا': 4, 'جيد': 3, 'مقبول': 2, 'ضعيف': 1 };
      const evalTexts = { 5: 'ممتاز', 4: 'جيد جداً', 3: 'جيد', 2: 'مقبول', 1: 'ضعيف' };
      const branchEvaluations = [];
      const freqMap = {};
      
      for (const report of reports) {
        const val = report['التقييم اليومي'];
        if (val) {
          branchEvaluations.push(val);
          freqMap[val] = (freqMap[val] || 0) + 1;
        }
      }

      let averageScoreText = 'لا يوجد بيانات';
      let mostFrequentEval = 'لا يوجد بيانات';
      let avgScoreValue = 0;
      
      if (branchEvaluations.length > 0) {
        let totalScore = 0;
        let validCount = 0;
        for (const evalVal of branchEvaluations) {
          const score = evalScores[evalVal];
          if (score !== undefined) {
            totalScore += score;
            validCount++;
          }
        }
        if (validCount > 0) {
          avgScoreValue = totalScore / validCount;
          const roundedScore = Math.round(avgScoreValue);
          averageScoreText = evalTexts[roundedScore] || 'غير محدد';
        }
        let maxCount = 0;
        for (const [evalVal, count] of Object.entries(freqMap)) {
          if (count > maxCount) { maxCount = count; mostFrequentEval = evalVal; }
        }
      }

      html += `<p style="color: #2e7d32; font-weight: bold; font-size: 16pt; margin-top: 25px;">ثانياً: تقييم الأداء العام للفرع خلال الشهر</p>
<table style="border-collapse: collapse; margin-top: 10px; margin-bottom: 20px;">
  <thead>
    <tr style="background-color: #e8f5e9;">
      <th style="border: 1px solid #a5d6a7; padding: 12px; text-align: right; font-weight: bold; color: #2e7d32;">مؤشر تقييم الفرع</th>
      <th style="border: 1px solid #a5d6a7; padding: 12px; text-align: center; font-weight: bold; color: #2e7d32;">التقييم والنتيجة</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td style="border: 1px solid #a5d6a7; padding: 12px; text-align: right; font-weight: bold;">التقييم الحسابي العام للفرع</td>
      <td style="border: 1px solid #a5d6a7; padding: 12px; text-align: center; color: #2e7d32; font-weight: bold; font-size: 13pt;">${sanitizeText(averageScoreText)} - النتيجة: ${avgScoreValue.toFixed(1)} من 5</td>
    </tr>
    <tr>
      <td style="border: 1px solid #a5d6a7; padding: 12px; text-align: right; font-weight: bold;">التقييم الأكثر تكراراً</td>
      <td style="border: 1px solid #a5d6a7; padding: 12px; text-align: center; color: #2e7d32; font-weight: bold; font-size: 13pt;">${sanitizeText(mostFrequentEval)} - الأكثر شيوعاً</td>
    </tr>
  </tbody>
</table>`;

      // Third: Maintenance
      const badMaintenanceItems = [];
      if (Array.isArray(stations)) {
        for (const st of stations) {
          const stName = st['اسم المحطة'] || 'محطة غير مسماة';
          const reportDate = st.reportDate || 'غير محدد';
          if (st['الصندوق'] === 'عجز') badMaintenanceItems.push({ location: stName, date: reportDate, type: 'عجز في الصندوق', note: st['ملاحظات الصندوق'] || '' });
          if (st['متابعة القضايا'] === 'يوجد قضايا سابقة') badMaintenanceItems.push({ location: stName, date: reportDate, type: 'قضايا سابقة', note: st['ملاحظات متابعة القضايا'] || '' });
          if (st['مخالفات في المحطة'] === 'نعم') badMaintenanceItems.push({ location: stName, date: reportDate, type: 'وجود مخالفات', note: st['ملاحظات المخالفات'] || '' });
          if (st['جودة المضخات'] === 'سيئة') badMaintenanceItems.push({ location: stName, date: reportDate, type: 'سوء جودة المضخات', note: st['ملاحظات جودة المضخات'] || '' });
          if (st['منظومة المراقبة'] === 'لا تعمل' || st['منظومة المراقبة'] === 'لا يوجد') badMaintenanceItems.push({ location: stName, date: reportDate, type: st['منظومة المراقبة'] === 'لا تعمل' ? 'منظومة المراقبة لا تعمل' : 'منظومة المراقبة غير متوفرة', note: st['ملاحظات منظومة المراقبة'] || '' });
          if (st['الطفايات'] === 'بحاجة صيانة' || st['الطفايات'] === 'غير متوفرة') badMaintenanceItems.push({ location: stName, date: reportDate, type: st['الطفايات'] === 'بحاجة صيانة' ? 'الطفايات بحاجة صيانة' : 'الطفايات غير متوفرة', note: st['ملاحظات الطفايات'] || '' });
        }
      }

      html += `<p style="color: #2e7d32; font-weight: bold; font-size: 16pt; margin-top: 25px;">ثالثاً: الصيانة والتحسينات التشغيلية</p>
<table style="border-collapse: collapse; margin-top: 10px;">
  <thead>
    <tr style="background-color: #2e7d32; color: white;">
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: center; font-size: 12pt;">التاريخ</th>
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: right; font-size: 12pt;">الموقع</th>
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: right; font-size: 12pt;">النوع</th>
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: right; font-size: 12pt;">الملاحظات</th>
    </tr>
  </thead>
  <tbody>`;

      if (badMaintenanceItems.length === 0) {
        html += `<tr><td colspan="4" style="border: 1px solid #a5d6a7; padding: 14px; text-align: center; color: #2e7d32; font-weight: bold;">لا توجد سلبيات</td></tr>`;
      } else {
        for (const item of badMaintenanceItems) {
          html += `<tr>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: center;">${sanitizeText(item.date)}</td>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: right; font-weight: bold;">${sanitizeText(item.location)}</td>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: right; color: #b71c1c; font-weight: bold;">${sanitizeText(item.type)}</td>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: right;">${sanitizeText(item.note) || '—'}</td>
  </tr>`;
        }
      }

      html += `</tbody></table>`;

      // Fourth: Issues
      html += `<p style="color: #2e7d32; font-weight: bold; font-size: 16pt; margin-top: 25px;">رابعاً: الإنذارات والقضايا</p>
<table style="border-collapse: collapse; margin-top: 10px; margin-bottom: 20px;">
  <thead>
    <tr style="background-color: #2e7d32; color: white;">
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: center; font-size: 12pt;">التاريخ</th>
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: right; font-size: 12pt;">الموقع</th>
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: right; font-size: 12pt;">الموضوع</th>
      <th style="border: 1px solid #2e7d32; padding: 12px; font-weight: bold; text-align: right; font-size: 12pt;">الإجراء</th>
    </tr>
  </thead>
  <tbody>`;

      if (issues.length === 0) {
        html += `<tr><td colspan="4" style="border: 1px solid #a5d6a7; padding: 14px; text-align: center; color: #2e7d32; font-weight: bold;">لا توجد قضايا</td></tr>`;
      } else {
        for (const issue of issues) {
          const rawDate = issue['تاريخ القضية'] || issue.reportDate || 'غير محدد';
          const location = issue['الجهة المعنية'] || 'غير محدد';
          const subject = issue['موضوع القضية'] || 'غير محدد';
          const action = issue['الإجراء المتخذ'] || '—';
          html += `<tr>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: center;">${sanitizeText(rawDate)}</td>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: right; font-weight: bold;">${sanitizeText(location)}</td>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: right;">${sanitizeText(subject)}</td>
    <td style="border: 1px solid #a5d6a7; padding: 10px; text-align: right;">${sanitizeText(action)}</td>
  </tr>`;
        }
      }

      html += `</tbody></table>`;

      // Fifth: Notes
      html += `<p style="color: #2e7d32; font-weight: bold; font-size: 16pt; margin-top: 25px;">خامساً: الاقتراحات والملاحظات العامة</p><ul>`;

      const allNotes = [];
      for (const report of reports) {
        if (report['ملاحظات'] && report['ملاحظات'].trim()) {
          allNotes.push(`ملاحظة الجولة: ${sanitizeText(report['ملاحظات'])}`);
        }
        if (Array.isArray(report['المحطات'])) {
          for (const st of report['المحطات']) {
            const note = st['ملاحظات عامة للمحطة'];
            if (note && note.trim()) {
              allNotes.push(`${sanitizeText(st['اسم المحطة']) || 'محطة'}: ${sanitizeText(note)}`);
            }
          }
        }
      }
      for (const gp of data.gasPlants) {
        if (gp['ملاحظات عامة وتوصيات'] && gp['ملاحظات عامة وتوصيات'].trim()) {
          allNotes.push(`معمل غاز: ${sanitizeText(gp['ملاحظات عامة وتوصيات'])}`);
        }
      }

      if (allNotes.length === 0) {
        html += `<li>لا يوجد اقتراحات مسجلة</li>`;
      } else {
        for (const note of allNotes) {
          if (note) html += `<li>${note}</li>`;
        }
      }

      html += `</ul><br><br>`;
    }

    // Footer
    html += `
<br><br><br>
<table style="border-collapse: collapse; border: none; margin-top: 40px;">
  <tr style="border: none;">
    <td style="font-weight: bold; text-align: center; border: none; font-size: 14pt; padding: 10px; border-top: 1px solid #333;">مدير قسم الرقابة والمتابعة</td>
    <td style="font-weight: bold; text-align: center; border: none; font-size: 14pt; padding: 10px; border-top: 1px solid #333;">المدير العام</td>
  </tr>
</table>
<br><br>
  <p style="margin: 0; font-weight: bold;">نسخة الـ:</p>
  <p style="margin: 5px 0 0 20px;">_ مكتب الديوان.</p>
</body>
</html>`;

    // Log the HTML length and check for any 'width' in the generated HTML
    console.log(`HTML length: ${html.length}`);
    const widthMatches = html.match(/width/gi);
    console.log(`width occurrences in HTML: ${widthMatches ? widthMatches.length : 0}`);
    
    // Save the HTML for inspection
    require('fs').writeFileSync('./scratch/debug_report.html', html, 'utf-8');
    console.log('Saved debug_report.html');

    // Now try to convert
    console.log('Converting to DOCX...');
    const docxBuffer = await htmlToDocx(html, null, {
      table: { row: { cantSplit: true } }
    });
    console.log(`✅ SUCCESS! DOCX size: ${docxBuffer.byteLength} bytes`);
    
    require('fs').writeFileSync('./scratch/test_report.docx', Buffer.from(docxBuffer));
    console.log('Saved test_report.docx');
  } catch (err) {
    console.error(`❌ FAILED: ${err.message}`);
    console.error(err.stack);
  }
})();
