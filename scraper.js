const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());

const limit = pLimit(3); // parallel browsers

async function delay(min = 2000, max = 5000) {
  return new Promise(r => setTimeout(r, Math.random() * (max - min) + min));
}

async function scrapeJobDetail(page, url) {
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

    await delay();

    return await page.evaluate(() => {
      const companyProfile =
        document.querySelector(".company-profile")?.innerText || "";

      const fullDesc =
        document.querySelector(".dang-inner-html")?.innerText || "";

      return { companyProfile, fullDesc };
    });
  } catch {
    return { companyProfile: "", fullDesc: "" };
  }
}

async function scrapeJobs(keyword, location, pages = 5) {
  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox"]
  });

  const page = await browser.newPage();

  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"
  );

  let allJobs = [];

  for (let p = 1; p <= pages; p++) {
    const url = `https://www.naukri.com/${keyword}-jobs-in-${location}-${p}?jobAge=3`;

    console.log("Opening:", url);

    try {
      await page.goto(url, { waitUntil: "networkidle2" });

      await page.waitForSelector(".srp-jobtuple-wrapper");

      const jobs = await page.evaluate(() => {
        return Array.from(document.querySelectorAll(".srp-jobtuple-wrapper")).map(job => ({
          title: job.querySelector(".title")?.innerText || "",
          company: job.querySelector(".comp-name")?.innerText || "",
          location: job.querySelector(".locWdth")?.innerText || "",
          experience: job.querySelector(".expwdth")?.innerText || "",
          link: job.querySelector("a.title")?.href || ""
        }));
      });

      allJobs.push(...jobs);

      await delay();
    } catch {
      console.log("Page failed:", p);
    }
  }

  // 🔥 Parallel deep scraping
  const detailedJobs = await Promise.all(
    allJobs.map(job =>
      limit(async () => {
        const newPage = await browser.newPage();
        const details = await scrapeJobDetail(newPage, job.link);
        await newPage.close();

        return { ...job, ...details };
      })
    )
  );

  await browser.close();
  return detailedJobs;
}

module.exports = scrapeJobs;
