const { google } = require("googleapis");

async function clearSheet(sheets, spreadsheetId) {
  await sheets.spreadsheets.values.clear({
    spreadsheetId,
    range: "Sheet4!A2:Z",
  });

  console.log("🧹 Old data cleared from Sheet4");
}

async function writeToSheet(jobs) {
  if (!Array.isArray(jobs) || jobs.length === 0) {
    console.log(
      "⚠️ No jobs found. Existing Sheet4 data has been preserved."
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
      new Date().toLocaleDateString("en-IN", {
        timeZone: "Asia/Kolkata",
      }),
    ]);

    // Clear only after confirming we have data to write.
    await clearSheet(sheets, spreadsheetId);

    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: "Sheet4!A2",
      valueInputOption: "RAW",
      requestBody: { values },
    });

    console.log(
      `✅ Successfully wrote ${values.length} jobs to Sheet4.`
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
