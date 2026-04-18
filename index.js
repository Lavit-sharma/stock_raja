const scrapeJobs = require("./scraper");
const writeToSheet = require("./sheets");
const pLimit = require("p-limit");

const limit = pLimit(2); // keep LOW for GitHub Actions

const keywords = ["Product Manager", "Program Manager"];
const locations = ["delhi", "bangalore", "remote"];

(async () => {
  try {
    const tasks = [];

    for (let keyword of keywords) {
      for (let location of locations) {

        tasks.push(
          limit(async () => {
            console.log(`Starting: ${keyword} | ${location}`);

            try {
              const jobs = await scrapeJobs(keyword, location, 5);

              console.log(`Done: ${keyword} | ${location} → ${jobs.length}`);

              return jobs.map(j => ({
                ...j,
                keyword,
                searchLocation: location
              }));

            } catch (err) {
              console.log(`Failed: ${keyword} | ${location}`, err.message);
              return []; // prevent crash
            }
          })
        );

      }
    }

    const results = await Promise.all(tasks);
    const finalJobs = results.flat();

    console.log("Total Jobs:", finalJobs.length);

    await writeToSheet(finalJobs);

    console.log("✅ Sheet Updated Successfully");

  } catch (err) {
    console.log("❌ Main Error:", err);
  }
})();
