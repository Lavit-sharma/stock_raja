const scrapeJobs = require("./scraper");
const writeToSheet = require("./sheets");
const pLimit = require("p-limit");

const limit = pLimit(2); // Keep LOW for GitHub Actions

const keywords = ["Product-Manager", "Program-Manager"];
const locations = ["delhi", "bangalore", "remote"];

(async () => {
  try {
    const tasks = [];

    for (const keyword of keywords) {
      for (const location of locations) {

        tasks.push(
          limit(async () => {
            console.log(`🚀 Starting: ${keyword} | ${location}`);

            try {
              const jobs = await scrapeJobs(keyword, location, 5);

              console.log(`✅ Done: ${keyword} | ${location} → ${jobs.length} jobs`);

              return jobs.map(job => ({
                title: job.title || "",
                company: job.company || "",
                location: job.location || "",
                experience: job.experience || "",
                companyProfile: job.companyProfile || "",
                fullDesc: job.fullDesc || "", // Job Description
                link: job.link || "",
                keyword,
                searchLocation: location
              }));

            } catch (err) {
              console.error(`❌ Failed: ${keyword} | ${location}`);
              console.error(err.message);
              return [];
            }
          })
        );

      }
    }

    const results = await Promise.all(tasks);
    const finalJobs = results.flat();

    console.log(`📦 Total Jobs Collected: ${finalJobs.length}`);

    await writeToSheet(finalJobs);

    console.log("✅ Google Sheet Updated Successfully");

  } catch (err) {
    console.error("❌ Main Error:", err);
  }
})();
