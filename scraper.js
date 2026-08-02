const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());

const limit = pLimit(3); // Parallel deep scraping limit

async function delay(min = 2000, max = 5000) {
  return new Promise(r => setTimeout(r, Math.random() * (max - min) + min));
}

async function scrapeJobDetail(page, url) {
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 35000 });
    await delay(1500, 3000);

    return await page.evaluate(() => {
      // Multiple fallback selectors for Naukri's layout changes
      const companyProfile =
        document.querySelector(".company-description")?.innerText ||
        document.querySelector(".comp-desc")?.innerText ||
        document.querySelector(".styles_JDC__comp-profile__...")?.innerText || "";

      const fullDesc =
        document.querySelector(".job-desc")?.innerText ||
        document.querySelector(".dang-inner-html")?.innerText ||
        document.querySelector(".styles_JDC__dang-inner-html__...")?.innerText || "";

      return { companyProfile, fullDesc };
    });
  } catch (err) {
    return { companyProfile: "", fullDesc: "" };
  }
}

async function scrapeJobs(keyword, location, pages = 3) {
  const browser = await puppeteer.launch({
    headless: "new",
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-accelerated-2d-canvas",
      "--disable-gpu"
    ]
  });

  const page = await browser.newPage();

  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
  );

  let allJobs = [];

  for (let p = 1; p <= pages; p++) {
    const url = `https://www.naukri.com/${keyword}-jobs-in-${location}-${p}?jobAge=3`;
    console.log(`🔍 Opening: ${url}`);

    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 40000 });
      
      // Wait for listing container with fallback timeout
      await page.waitForSelector(".srp-jobtuple-wrapper", { timeout: 10000 }).catch(() => {});

      const jobs = await page.evaluate(() => {
        const cards = document.querySelectorAll(".srp-jobtuple-wrapper");
        return Array.from(cards).map(job => ({
          title: job.querySelector(".title")?.innerText?.trim() || "",
          company: job.querySelector(".comp-name")?.innerText?.trim() || "",
          location: job.querySelector(".locWdth")?.innerText?.trim() || "",
          experience: job.querySelector(".expwdth")?.innerText?.trim() || "",
          link: job.querySelector("a.title")?.href || ""
        })).filter(j => j.link !== "");
      });

      allJobs.push(...jobs);
      console.log(`📄 Page ${p}: Found ${jobs.length} jobs`);

      await delay();
    } catch (err) {
      console.log(`⚠️ Page failed or blocked: ${p} (${err.message})`);
    }
  }

  // Deduplicate jobs by link
  const uniqueJobs = Array.from(new Map(allJobs.map(j => [j.link, j])).values());
  console.log(`🔗 Total unique jobs to deep-scrape: ${uniqueJobs.length}`);

  // Parallel deep scraping of individual job detail pages
  const detailedJobs = await Promise.all(
    uniqueJobs.map(job =>
      limit(async () => {
        const newPage = await browser.newPage();
        await newPage.setUserAgent(
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        );
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
