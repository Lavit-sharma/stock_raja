const scrapeJobs = require("./scraper");
const writeToSheet = require("./sheets");
const pLimit = require("p-limit");

const limit = pLimit(2); // keyword parallel

const keywords = ["developer", "engineer", "python", "java"];
const locations = ["delhi", "bangalore", "remote"];

(async () => {
  const tasks = [];

  for (let keyword of keywords) {
    for (let location of locations) {
      tasks.push(
        limit(async () => {
          const jobs = await scrapeJobs(keyword, location, 5);

          return jobs.map(j => ({
            ...j,
            keyword,
            searchLocation: location
          }));
        })
      );
    }
  }

  const results = await Promise.all(tasks);
  const finalJobs = results.flat();

  await writeToSheet(finalJobs);
})();
