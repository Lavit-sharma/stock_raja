const { google } = require("googleapis");

async function clearSheet(sheets, spreadsheetId) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId,
    range: "Sheet2!A2:Z", // Keep header row
  });

  console.log("🧹 Old data cleared");
}

async function writeToSheet(jobs) {
  try {
    const auth = new google.auth.GoogleAuth({
      credentials: JSON.parse(process.env.GOOGLE_CREDS),
      scopes: ["https://www.googleapis.com/auth/spreadsheets"]
    });

    const sheets = google.sheets({
      version: "v4",
      auth
    });

    const spreadsheetId = process.env.SHEET_ID;

    // Clear previous data
    await clearSheet(sheets, spreadsheetId);

    console.log(`📦 Total jobs to insert: ${jobs.length}`);

    if (!jobs || jobs.length === 0) {
      console.log("⚠️ No jobs found.");
      return;
    }

    const values = jobs.map(job => [
      job.title || "",
      job.company || "",
      job.location || "",
      job.experience || "",
      job.companyProfile || "",
      job.fullDesc || "",     // <-- Job Description
      job.link || "",
      job.keyword || "",
      job.searchLocation || "",
      new Date().toLocaleDateString()
    ]);

    await sheets.spreadsheets.values.append({
      spreadsheetId,
      range: "Sheet2!A2",
      valueInputOption: "RAW",
      resource: {
        values
      }
    });

    console.log(`✅ ${values.length} jobs inserted successfully.`);
  } catch (err) {
    console.error("❌ Error writing to Google Sheets:", err.message);
  }
}

module.exports = writeToSheet;
