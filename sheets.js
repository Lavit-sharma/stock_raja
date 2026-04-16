const { google } = require("googleapis");

async function addHeaders(sheets, spreadsheetId) {
  const headers = [[
    "Title",
    "Company",
    "Location",
    "Experience",
    "Salary",
    "Posted",
    "Description",
    "Link",
    "Keyword",
    "Search Location",
    "Date"
  ]];

  await sheets.spreadsheets.values.update({
    spreadsheetId,
    range: "Sheet2!A1",
    valueInputOption: "RAW",
    resource: { values: headers }
  });
}

async function writeToSheet(jobs) {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_CREDS),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  const sheets = google.sheets({ version: "v4", auth });
  const spreadsheetId = process.env.SHEET_ID;

  // ✅ Add headers
  await addHeaders(sheets, spreadsheetId);

  const values = jobs.map(job => [
    job.title,
    job.company,
    job.location,
    job.experience,
    job.salary,
    job.posted,
    job.description,
    job.link,
    job.keyword,
    job.searchLocation,
    new Date().toLocaleDateString()
  ]);

  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: "Sheet2!A2",
    valueInputOption: "RAW",
    resource: { values }
  });

  console.log("✅ Data pushed with headers");
}

module.exports = writeToSheet;
