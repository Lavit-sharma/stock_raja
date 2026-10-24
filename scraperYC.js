const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());
const limit = pLimit(1);
const USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const randomSleep = (min, max) =>
  new Promise((resolve) => setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min));

async function scrapeYC(keyword, location) {
  const query = keyword.replace("-", " ");
  const url = `https://www.workatastartup.com/jobs?query=${encodeURIComponent(query)}`;

  console.log(`🔍 [YC] Opening: ${url}`);
  const browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox", "--disable-setuid-sandbox"] });

  try {
    const page = await browser.newPage();
    await page.setUserAgent(USER_AGENT);
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
    await randomSleep(4000, 6000);

    const jobs = await page.evaluate(() => {
      const clean = (v) => (v || "").replace(/\s+/g, " ").trim();
      const rows = Array.from(document.querySelectorAll("div.job-card, div[class*='job']"));
      const output = [];

      for (const row of rows) {
        const linkEl = row.querySelector("a[href*='/jobs/']");
        if (!linkEl) continue;
        const title = clean(linkEl.innerText);
        const link = linkEl.href;
        const company = clean(row.querySelector(".company-name, [class*='company']")?.innerText);
        const locationText = clean(row.querySelector(".location, [class*='location']")?.innerText);

        if (title && link) {
          output.push({ title, company, location: locationText, experience: "", link: link.startsWith("http") ? link : `https://www.workatastartup.com${link}` });
        }
      }
      return output;
    });

    await page.close().catch(() => {});
    console.log(`🔗 [YC] ${keyword}: ${jobs.length} jobs found.`);

    return await Promise.all(
      jobs.map((job) =>
        limit(async () => {
          const detailPage = await browser.newPage();
          await detailPage.setUserAgent(USER_AGENT);
          try {
            await detailPage.goto(job.link, { waitUntil: "domcontentloaded", timeout: 30000 });
            await randomSleep(2000, 3000);
            const fullDesc = await detailPage.evaluate(() => {
              const el = document.querySelector("div.job-description, div[class*='description']");
              return el ? el.innerText.replace(/\s+/g, " ").trim() : "";
            });
            return { ...job, companyProfile: "", fullDesc: fullDesc || "No description found", platform: "YC" };
          } catch {
            return { ...job, companyProfile: "", fullDesc: "", platform: "YC" };
          } finally {
            await detailPage.close().catch(() => {});
          }
        })
      )
    );
  } finally {
    await browser.close().catch(() => {});
  }
}

module.exports = scrapeYC;
