const scrapeJobs = require("./scraper");
const writeToSheet = require("./sheets");

const keywords = ["developer", "engineer", "analyst"];
const locations = ["delhi", "mumbai", "bangalore"];

(async () => {
  let finalJobs = [];
  const seen = new Set();

  for (let keyword of keywords) {
    for (let location of locations) {
      const jobs = await scrapeJobs(keyword, location, 3);

      const enriched = jobs.map(j => ({
        ...j,
        keyword,
        searchLocation: location
      }));

      const unique = enriched.filter(j => {
        if (seen.has(j.link)) return false;
        seen.add(j.link);
        return true;
      });

      finalJobs.push(...unique);
    }
  }

  await writeToSheet(finalJobs);
})();
