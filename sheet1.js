const { google } = require("googleapis");

async function clearSheet(sheets, spreadsheetId) {
  try {
    await sheets.spreadsheets.values.clear({
      spreadsheetId,
      range: "Sheet2!A2:Z",
    });
    console.log("🧹 Old data cleared from Sheet2");
  } catch (err) {
    console.log("ℹ️ Sheet clear skipped or failed (might already be empty):", err.message);
  }
}

async function writeToSheet(jobs) {
  try {
    const auth = new google.auth.GoogleAuth({
      credentials: JSON.parse(process.env.GOOGLE_CREDS),
      scopes: ["https://www.googleapis.com/auth/spreadsheets"]
    });

    const sheets = google.sheets({ version: "v4", auth });
    const spreadsheetId = process.env.SHEET_ID;

    await clearSheet(sheets, spreadsheetId);

    if (!jobs || jobs.length === 0) {
      console.log("⚠️ No jobs found to write.");
      return;
    }

    const values = jobs.map(job => [
      job.title || "",
      job.company || "",
      job.location || "",
      job.experience || "",
      job.companyProfile || "",
      job.fullDesc || "",
      job.link || "",
      job.keyword || "",
      job.searchLocation || "",
      new Date().toLocaleDateString()
    ]);

    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: "Sheet2!A2",
      valueInputOption: "RAW",
      resource: { values }
    });

    console.log(`✅ ${values.length} jobs inserted into Google Sheets successfully.`);
  } catch (err) {
    console.error("❌ Error writing to Google Sheets:", err.message);
  }
}

module.exports = writeToSheet;
