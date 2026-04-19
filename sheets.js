const { google } = require("googleapis");

async function clearSheet(sheets, spreadsheetId) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId,
    range: "Sheet2!A2:Z", // keep header row safe
  });

  console.log("🧹 Old data cleared");
}

async function writeToSheet(jobs) {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_CREDS),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  const sheets = google.sheets({ version: "v4", auth });
  const spreadsheetId = process.env.SHEET_ID;

  // ✅ STEP 1: clear old entries
  await clearSheet(sheets, spreadsheetId);

  console.log(`📦 Total jobs to insert: ${jobs.length}`);

  const values = jobs.map(job => [
    job.title,
    job.company,
    job.location,
    job.experience,
    job.companyProfile,
    job.fullDesc,
    job.link,
    job.keyword,
    job.searchLocation,
    new Date().toLocaleDateString()
  ]);

  if (values.length === 0) return;

  // ✅ STEP 2: insert fresh data
  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: "Sheet2!A2",
    valueInputOption: "RAW",
    resource: { values }
  });

  console.log("✅ Fresh jobs inserted");
}

module.exports = writeToSheet;
