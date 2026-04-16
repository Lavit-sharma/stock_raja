const puppeteer = require("puppeteer");

async function scrapeJobs(keyword, location, pages = 3) {
  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox", "--disable-setuid-sandbox"]
  });

  const page = await browser.newPage();

  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
  );

  let allJobs = [];

  for (let p = 1; p <= pages; p++) {
    const url = `https://www.naukri.com/${keyword}-jobs-in-${location}-${p}`;
    console.log("Opening:", url);

    try {
      await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });

      await page.waitForSelector(".srp-jobtuple-wrapper", { timeout: 15000 });

      const jobs = await page.evaluate(() => {
        return Array.from(document.querySelectorAll(".srp-jobtuple-wrapper")).map(job => ({
          title: job.querySelector(".title")?.innerText || "",
          company: job.querySelector(".comp-name")?.innerText || "",
          location: job.querySelector(".locWdth")?.innerText || "",
          link: job.querySelector("a.title")?.href || ""
        }));
      });

      allJobs.push(...jobs);

      await new Promise(r => setTimeout(r, 3000));
    } catch (err) {
      console.log("Error on page:", p);
    }
  }

  await browser.close();
  return allJobs;
}

module.exports = scrapeJobs;
