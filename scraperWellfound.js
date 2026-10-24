const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const pLimit = require("p-limit");

puppeteer.use(StealthPlugin());

const DETAIL_CONCURRENCY = 1;
const limit = pLimit(DETAIL_CONCURRENCY);

const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const LOCATION_SLUGS = {
  delhi: "new-delhi",
  bengaluru: "bangalore",
  bangalore: "bangalore",
  mumbai: "mumbai",
  hyderabad: "hyderabad",
};

const ROLE_SLUGS = {
  "product-manager": "product-manager",
  "engineering-manager": "engineering-manager",
  "edge-ai": "artificial-intelligence-engineer",
};

const randomSleep = (min, max) =>
  new Promise((resolve) =>
    setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min)
  );

function buildSearchUrl(keyword, location, pageNumber = 1) {
  const role = ROLE_SLUGS[keyword];
  const place = LOCATION_SLUGS[location];
  if (!role || !place) return null;

  const base = `https://wellfound.com/role/l/${role}/${place}`;
  return pageNumber > 1 ? `${base}?page=${pageNumber}` : base;
}

async function scrapeSearchPage(page, url, keyword) {
  console.log(`🔍 [Wellfound] Opening: ${url}`);
  const response = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
  if (response?.status() === 403) return [];

  await randomSleep(3000, 5000);

  const jobs = await page.evaluate(() => {
    const clean = (v) => (v || "").replace(/\s+/g, " ").trim();
    const anchors = Array.from(document.querySelectorAll("a[href]"));
    const output = [];

    for (const anchor of anchors) {
      const link = anchor.href;
      if (!/^https:\/\/wellfound\.com\/jobs\/[^/?#]+/i.test(link)) continue;

      const title = clean(anchor.innerText);
      if (!title || title.length > 180) continue;

      let card = anchor;
      let cardText = "";
      for (let level = 0; level < 7 && card; level++) {
        const text = clean(card.innerText);
        if (text.length >= title.length && text.length < 3500 && /exp\b|full.?time|remote|salary/i.test(text)) {
          cardText = text;
          break;
        }
        card = card.parentElement;
      }
      if (!cardText) cardText = clean(anchor.parentElement?.innerText);

      const headings = card ? Array.from(card.querySelectorAll("h2, h3, h4")).map((el) => clean(el.innerText)).filter(Boolean) : [];
      const company = headings.find((h) => h !== title) || "";
      const experienceMatch = cardText.match(/\b\d+\+?\s+years?\s+of\s+exp(?:erience)?\b/i);
      const locationMatch = cardText.match(/(?:Remote only|Remote|In office|Hybrid)\s*[•·-]?\s*([^•·\n]{2,100})/i);

      output.push({
        title,
        company,
        location: locationMatch ? clean(locationMatch[0]) : "",
        experience: experienceMatch ? experienceMatch[0] : "",
        link,
      });
    }

    const unique = new Map();
    for (const j of output) if (!unique.has(j.link)) unique.set(j.link, j);
    return Array.from(unique.values());
  });

  return keyword === "edge-ai"
    ? jobs.filter((j) => /\bedge\b|\bai\b|artificial intelligence|machine learning|computer vision|embedded/i.test(j.title))
    : jobs;
}

async function scrapeJobDetail(page, url) {
  try {
    await page.goto(url, { waitUntil: "domcontentloaded", timeout: 40000 });
    await randomSleep(2000, 3500);

    return await page.evaluate(() => {
      const clean = (v) => (v || "").replace(/\s+/g, " ").trim();
      const pick = (selectors) => {
        for (const s of selectors) {
          const el = document.querySelector(s);
          const t = clean(el?.innerText);
          if (t && t.length > 20) return t;
        }
        return "";
      };

      const companyProfile = pick(["[data-test='CompanyProfile']", "[class*='companyOverview']", "section[class*='company']"]);
      let fullDesc = pick(["[data-test='JobDescription']", "[class*='jobDescription']", "div[class*='styles_jobDescription']", "section[class*='description']"]);

      if (!fullDesc) {
        const text = clean((document.querySelector("main") || document.body)?.innerText);
        const marker = text.toLowerCase().indexOf("about the job");
        fullDesc = marker >= 0 ? text.slice(marker, marker + 15000) : text.slice(0, 15000);
      }

      return { companyProfile, fullDesc: fullDesc || "No detailed JD found." };
    });
  } catch {
    return { companyProfile: "", fullDesc: "" };
  }
}

async function scrapeWellfound(keyword, location, pages = 1) {
  const url = buildSearchUrl(keyword, location, pages);
  if (!url) return [];

  const browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox", "--disable-setuid-sandbox"] });
  try {
    const searchPage = await browser.newPage();
    await searchPage.setUserAgent(USER_AGENT);
    await searchPage.setViewport({ width: 1366, height: 768 });

    const jobs = await scrapeSearchPage(searchPage, url, keyword);
    await searchPage.close().catch(() => {});

    console.log(`🔗 [Wellfound] ${keyword} | ${location}: ${jobs.length} jobs found. Fetching JDs...`);

    return await Promise.all(
      jobs.map((job) =>
        limit(async () => {
          const detailPage = await browser.newPage();
          await detailPage.setUserAgent(USER_AGENT);
          const details = await scrapeJobDetail(detailPage, job.link);
          await detailPage.close().catch(() => {});
          return { ...job, ...details, platform: "Wellfound" };
        })
      )
    );
  } finally {
    await browser.close().catch(() => {});
  }
}

module.exports = scrapeWellfound;
