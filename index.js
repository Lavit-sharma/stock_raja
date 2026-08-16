const scrapeJobs = require("./scraper");
const writeToSheet = require("./sheets");
const pLimit = require("p-limit");

const limit = pLimit(2); // Kept low to prevent GitHub Actions rate limiting/bans

const keywords = ["Product-Manager", "Product-Head","Director-of-Product","New-Product-Development", "Engineering-Manager","Edge-Compute","Edge-AI" ];
const locations = ["delhi", "bangalore", "ahmedabad","mumbai","all"];

(async () => {
  try {
    console.log("🚀 Starting Job Scraping Pipeline...");
    const tasks = [];

    for (const keyword of keywords) {
      for (const location of locations) {
        tasks.push(
          limit(async () => {
            console.log(`🚀 Starting Task: ${keyword} | ${location}`);

            try {
              // Reduced page limit per query to 2-3 for faster runtime & stability
              const jobs = await scrapeJobs(keyword, location, 2);

              console.log(`✅ Done: ${keyword} | ${location} → ${jobs.length} jobs collected`);

              return jobs.map(job => ({
                title: job.title || "",
                company: job.company || "",
                location: job.location || "",
                experience: job.experience || "",
                companyProfile: job.companyProfile || "",
                fullDesc: job.fullDesc || "",
                link: job.link || "",
                keyword,
                searchLocation: location
              }));

            } catch (err) {
              console.error(`❌ Failed Task: ${keyword} | ${location} -> ${err.message}`);
              return [];
            }
          })
        );
      }
    }

    const results = await Promise.all(tasks);
    const finalJobs = results.flat();

    console.log(`📦 Total Unique Jobs Collected Across All Queries: ${finalJobs.length}`);

    await writeToSheet(finalJobs);

    console.log("🎉 Process Completed Successfully!");
  } catch (err) {
    console.error("❌ Main Error:", err);
    process.exit(1);
  }
})();
