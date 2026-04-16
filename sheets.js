const { google } = require("googleapis");

async function writeToSheet(jobs) {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_CREDS),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  const sheets = google.sheets({ version: "v4", auth });

  const spreadsheetId = process.env.SHEET_ID;

  const values = jobs.map(job => [
    job.title,
    job.company,
    job.location,
    job.link,
    job.keyword,
    job.searchLocation,
    new Date().toLocaleDateString()
  ]);

  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: "Sheet2!A1",
    valueInputOption: "RAW",
    resource: { values }
  });

  console.log("✅ Data pushed to Sheet2");
}

module.exports = writeToSheet;
