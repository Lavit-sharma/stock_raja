const { google } = require("googleapis");

async function getExistingLinks(sheets, spreadsheetId) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: "Sheet2!H:H" // link column
  });

  return new Set((res.data.values || []).flat());
}

async function writeToSheet(jobs) {
  const auth = new google.auth.GoogleAuth({
    credentials: JSON.parse(process.env.GOOGLE_CREDS),
    scopes: ["https://www.googleapis.com/auth/spreadsheets"]
  });

  const sheets = google.sheets({ version: "v4", auth });
  const spreadsheetId = process.env.SHEET_ID;

  const existingLinks = await getExistingLinks(sheets, spreadsheetId);

  const newJobs = jobs.filter(j => j.link && !existingLinks.has(j.link));

  console.log(`✅ New jobs found: ${newJobs.length}`);

  const values = newJobs.map(job => [
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

  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: "Sheet2!A2",
    valueInputOption: "RAW",
    resource: { values }
  });

  console.log("✅ Only NEW jobs inserted");
}

module.exports = writeToSheet;
