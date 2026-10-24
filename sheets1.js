const { google } = require("googleapis");

async function clearSheet(sheets, spreadsheetId) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId,
    range: "Sheet3!A2:Z",
  });

  console.log("🧹 Old data cleared from Sheet3");
}

async function writeToSheet(jobs) {
  if (!Array.isArray(jobs) || jobs.length === 0) {
    console.log(
      "⚠️ No jobs found. Existing Sheet3 data has been preserved."
    );
    return;
  }

  try {
    if (!process.env.GOOGLE_CREDS || !process.env.SHEET_ID) {
      throw new Error(
        "Missing GOOGLE_CREDS or SHEET_ID environment variable."
      );
    }

    const credentials = JSON.parse(process.env.GOOGLE_CREDS);

    const auth = new google.auth.GoogleAuth({
      credentials,
      scopes: [
        "https://www.googleapis.com/auth/spreadsheets",
      ],
    });

    const sheets = google.sheets({
      version: "v4",
      auth,
    });

    const spreadsheetId = process.env.SHEET_ID;

    const values = jobs.map((job) => [
      job.title || "",
      job.company || "",
      job.location || "",
      job.experience || "",
      job.companyProfile || "",
      job.fullDesc || "",
      job.link || "",
      job.keyword || "",
      job.searchLocation || "",
      job.platform || "",
      new Date().toLocaleDateString("en-IN", {
        timeZone: "Asia/Kolkata",
      }),
    ]);

    await clearSheet(sheets, spreadsheetId);

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: "Sheet3!A2",
      valueInputOption: "RAW",
      requestBody: { values },
    });

    console.log(
      `✅ Successfully wrote ${values.length} jobs to Sheet3.`
    );
  } catch (error) {
    console.error(
      "❌ Google Sheets error:",
      error.message
    );
    throw error;
  }
}

module.exports = writeToSheet;
