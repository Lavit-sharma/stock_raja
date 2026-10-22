
const scrapeJobs = require("./scraper1");
const writeToSheet = require("./sheets1");
const pLimit = require("p-limit");

const limit = pLimit(2);

const keywords = [
  "product-manager",
  "engineering-manager",
  "edge-ai",
];

const locations = [
  "delhi",
  "bengaluru",
  "mumbai",
  "hyderabad",
];

(async () => {
  try {
    console.log("🚀 Starting Wellfound Job Scraping Pipeline...");

    const tasks = [];

    for (const keyword of keywords) {
      for (const location of locations) {
        tasks.push(
          limit(async () => {
            console.log(
              `\n🚀 Starting Task: ${keyword} | ${location}`
            );

            try {
              const jobs = await scrapeJobs(
                keyword,
                location,
                2
              );

              console.log(
                `✅ Finished: ${keyword} | ${location} → ${jobs.length} jobs`
              );

              return jobs.map((job) => ({
                title: job.title || "",
                company: job.company || "",
                location: job.location || "",
                experience: job.experience || "",
                companyProfile: job.companyProfile || "",
                fullDesc: job.fullDesc || "",
                link: job.link || "",
                keyword,
                searchLocation: location,
              }));
            } catch (error) {
              console.error(
                `❌ Task failed: ${keyword} | ${location}: ${error.message}`
              );

              return [];
            }
          })
        );
      }
    }

    const results = await Promise.all(tasks);
    const collectedJobs = results.flat();

    // Deduplicate across all keywords and locations.
    const uniqueJobs = new Map();

    for (const job of collectedJobs) {
      if (!job.link) continue;

      if (!uniqueJobs.has(job.link)) {
        uniqueJobs.set(job.link, job);
      }
    }

    const finalJobs = Array.from(uniqueJobs.values());

    console.log("\n======================================");
    console.log(`📦 Total collected: ${collectedJobs.length}`);
    console.log(`🔗 Unique jobs: ${finalJobs.length}`);
    console.log("======================================\n");

    // sheets1.js preserves old data when this array is empty.
    await writeToSheet(finalJobs);

    console.log("🎉 Pipeline completed.");
  } catch (error) {
    console.error("❌ Main pipeline error:", error);
    process.exitCode = 1;
  }
})();
