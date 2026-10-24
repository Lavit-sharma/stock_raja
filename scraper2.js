const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");

puppeteer.use(StealthPlugin());

const WORKER_POOL_SIZE = 2; // Fixed browser page worker pool
const USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36";

const LOCATION_SLUGS = {
  delhi: "delhi-ncr",
  bengaluru: "bengaluru",
  bangalore: "bengaluru",
  mumbai: "mumbai",
  hyderabad: "hyderabad",
};

const ROLE_SLUGS = {
  "product-manager": "product-manager-jobs",
  "engineering-manager": "engineering-manager-jobs",
  "edge-ai": "artificial-intelligence-ai-jobs",
};

const randomSleep = (min, max) =>
  new Promise((resolve) =>
    setTimeout(resolve, Math.floor(Math.random() * (max - min + 1)) + min)
  );

function buildSearchUrl(keyword, location) {
  const role = ROLE_SLUGS[keyword];
  const place = LOCATION_SLUGS[location];

  if (!role) throw new Error(`No Cutshort role mapping for: ${keyword}`);
  if (!place) throw new Error(`No Cutshort location mapping for: ${location}`);

  return `https://cutshort.io/jobs/${role}-in-${place}`;
}

async function scrapeSearchPage(page, url, keyword) {
  console.log(`🔍 Opening Cutshort URL: ${url}`);

  const response = await page.goto(url, {
    waitUntil: "domcontentloaded",
    timeout: 45000,
  });

  console.log(`🌐 HTTP: ${response?.status() ?? "No response"} | ${page.url()}`);

  if (response?.status() === 403) {
    console.error("🛡️ Cloudflare/Security Block encountered on search page.");
    return [];
  }

  try {
    await page.evaluate(async () => {
      await new Promise((resolve) => {
        let total = 0;
        const timer = setInterval(() => {
          window.scrollBy(0, 350);
          total += 350;
          if (total >= 2100 || total >= document.body.scrollHeight) {
            clearInterval(timer);
            resolve();
          }
        }, 150);
      });
    });
  } catch (e) {}

  await randomSleep(2000, 3500);

  const result = await page.evaluate(() => {
    const clean = (value) => (value || "").replace(/\s+/g, " ").trim();
    const output = [];

    try {
      const nextDataEl = document.getElementById("__NEXT_DATA__");
      if (nextDataEl && nextDataEl.innerText) {
        const parsed = JSON.parse(nextDataEl.innerText);
        const pageProps = parsed?.props?.pageProps;
        const rawJobs = pageProps?.jobs || pageProps?.initialData?.jobs || [];

        if (Array.isArray(rawJobs) && rawJobs.length > 0) {
          for (const j of rawJobs) {
            if (j.canonicalUrl || j.slug) {
              const link = j.canonicalUrl || `https://cutshort.io/job/${j.slug}`;
              output.push({
                title: clean(j.title || j.designation),
                company: clean(j.companyName || j.company?.name),
                location: clean(j.location || j.jobLocations?.join(", ")),
                experience: clean(j.experience || `${j.minExp || 0}-${j.maxExp || ""} yrs`),
                link,
              });
            }
          }
        }
      }
    } catch (err) {}

    if (output.length === 0) {
      const anchors = Array.from(document.querySelectorAll("a[href]"));

      for (const anchor of anchors) {
        const link = anchor.href;
        if (!/^https:\/\/(www\.)?cutshort\.io\/job\/[^/?#]+/i.test(link)) {
          continue;
        }

        const title = clean(anchor.innerText);
        if (!title || title.length > 180) continue;

        let card = anchor;
        let cardText = "";

        for (let level = 0; level < 7 && card; level++) {
          const text = clean(card.innerText);
          if (
            text.length >= title.length &&
            text.length < 3500 &&
            /exp\b|years|full.?time|remote|hybrid|onsite|salary|lac|lpa/i.test(text)
          ) {
            cardText = text;
            break;
          }
          card = card.parentElement;
        }

        if (!cardText) {
          cardText = clean(anchor.parentElement?.innerText);
        }

        const headings = card
          ? Array.from(card.querySelectorAll("h2, h3, h4, span, div"))
              .map((el) => clean(el.innerText))
              .filter(Boolean)
          : [];

        const company = headings.find((h) => h !== title && h.length < 60) || "";
        const experienceMatch = cardText.match(/\b\d+(?:-\d+)?\s*(?:\+)?\s*years?\b/i);
        const locationMatch = cardText.match(/(?:Remote|Hybrid|Onsite|In office)\s*[•·|-]?\s*([^•·|\n]{2,100})/i);

        output.push({
          title,
          company,
          location: locationMatch ? clean(locationMatch[0]) : "",
          experience: experienceMatch ? experienceMatch[0] : "",
          link,
        });
      }
    }

    const unique = new Map();
    for (const job of output) {
      if (job.link && !unique.has(job.link)) unique.set(job.link, job);
    }

    return {
      pageTitle: document.title,
      jobs: Array.from(unique.values()),
    };
  });

  const filtered =
    keyword === "edge-ai"
      ? result.jobs.filter((job) =>
          /\bedge\b|\bai\b|artificial intelligence|machine learning|computer vision|embedded/i.test(
            job.title
          )
        )
      : result.jobs;

  return filtered;
}

async function scrapeJobDetail(page, url) {
  try {
    console.log(`📄 Fetching Job Description & Metadata: ${url}`);

    const response = await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 35000,
    });

    if (response && response.status() === 403) {
      return { 
        companyProfile: "", 
        workMode: "",
        interviewRounds: "",
        fullDesc: "Cloudflare 403 Security Check Blocked", 
        keyResponsibilities: [], 
        detailError: "403 Forbidden" 
      };
    }

    await randomSleep(1500, 2500);

    return await page.evaluate(() => {
      const clean = (value) => (value || "").replace(/\s+/g, " ").trim();
      const bodyText = clean(document.body?.innerText);

      // Extract Work Mode (Remote / Hybrid / Onsite)
      let workMode = "";
      const workModeMatch = bodyText.match(/(?:Work\s*Mode|Location\s*Type)\s*[:|-]?\s*(Remote|Hybrid|Onsite|In[- ]office)/i);
      if (workModeMatch) {
        workMode = workModeMatch[1];
      } else if (/hybrid/i.test(bodyText)) {
        workMode = "Hybrid";
      } else if (/remote/i.test(bodyText)) {
        workMode = "Remote";
      } else if (/onsite|in[- ]office/i.test(bodyText)) {
        workMode = "Onsite";
      }

      // Extract Interview Rounds
      let interviewRounds = "";
      const roundsMatch = bodyText.match(/Interview\s*Rounds?\s*[:|-]?\s*([^\n]+(?:\n[^\n]+){0,3})/i);
      if (roundsMatch) {
        interviewRounds = clean(roundsMatch[1]);
      }

      // Extract Company Profile / About Company
      const pick = (selectors) => {
        for (const selector of selectors) {
          const element = document.querySelector(selector);
          const text = clean(element?.innerText);
          if (text && text.length > 20) return text;
        }
        return "";
      };

      const companyProfile = pick([
        "[class*='company-info']",
        "[class*='aboutCompany']",
        "[class*='companyOverview']",
        "section[class*='company']",
      ]);

      // Extract Full Job Description
      let fullDesc = pick([
        "[class*='job-description']",
        "[class*='jobDescription']",
        "[class*='description']",
        "div[class*='jd']",
        "section[class*='description']",
      ]);

      if (!fullDesc) {
        const main = document.querySelector("main") || document.body;
        const text = clean(main?.innerText);
        const lowerText = text.toLowerCase();
        const marker = lowerText.indexOf("about the job");
        const roleMarker = lowerText.indexOf("about the role");
        const startIdx = marker !== -1 ? marker : roleMarker;

        if (startIdx >= 0) {
          fullDesc = text.slice(startIdx, startIdx + 15000);
        } else if (text.length > 200) {
          fullDesc = text;
        }
      }

      // Extract Key Responsibilities / Qualifications (Bullet points structure)
      const keyResponsibilities = [];
      const headings = Array.from(document.querySelectorAll("h2, h3, h4, h5, strong, b"));
      let respHeader = null;

      for (const h of headings) {
        const text = (h.innerText || "").toLowerCase();
        if (
          text.includes("responsibility") ||
          text.includes("what you will do") ||
          text.includes("what you'll do") ||
          text.includes("roles & responsibilities") ||
          text.includes("key responsibilities") ||
          text.includes("key qualifications") ||
          text.includes("requirements")
        ) {
          respHeader = h;
          break;
        }
      }

      if (respHeader) {
        let nextEl = respHeader.nextElementSibling;
        let limitCount = 0;
        while (nextEl && limitCount < 6) {
          if (nextEl.tagName === "UL" || nextEl.tagName === "OL") {
            const items = nextEl.querySelectorAll("li");
            items.forEach((li) => {
              const val = clean(li.innerText);
              if (val.length > 5) keyResponsibilities.push(val);
            });
            break;
          } else if (nextEl.tagName === "P" || nextEl.tagName === "DIV") {
            const textVal = clean(nextEl.innerText);
            if (textVal.length > 10) {
              keyResponsibilities.push(textVal);
            }
          }
          nextEl = nextEl.nextElementSibling;
          limitCount++;
        }
      }

      if (keyResponsibilities.length === 0 && fullDesc) {
        const lines = fullDesc.split(/(?:\r?\n)+|[•\-\*]\s+/);
        for (const line of lines) {
          const cleanedLine = clean(line);
          if (cleanedLine.length > 20 && /manage|develop|build|design|lead|create|implement|collaborate|test|deploy|maintain/i.test(cleanedLine)) {
            keyResponsibilities.push(cleanedLine);
          }
        }
      }

      return {
        companyProfile,
        workMode,
        interviewRounds,
        fullDesc: fullDesc || "No detailed JD found.",
        keyResponsibilities: Array.from(new Set(keyResponsibilities)),
      };
    });
  } catch (error) {
    console.error(`⚠️ Detail page failed: ${url} - ${error.message}`);
    return {
      companyProfile: "",
      workMode: "",
      interviewRounds: "",
      fullDesc: "",
      keyResponsibilities: [],
      detailError: error.message,
    };
  }
}

async function scrapeJobs(keyword, location) {
  const browser = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--window-size=1366,768",
    ],
  });

  try {
    const pages = [];
    for (let i = 0; i < WORKER_POOL_SIZE; i++) {
      const p = await browser.newPage();
      await p.setCacheEnabled(false);
      await p.setUserAgent(USER_AGENT);
      await p.setViewport({ width: 1366, height: 768 });
      pages.push(p);
    }

    await new Promise((resolve) => setTimeout(resolve, 500));

    const mainPage = pages[0];
    let uniqueJobs = [];

    try {
      const url = buildSearchUrl(keyword, location);
      uniqueJobs = await scrapeSearchPage(mainPage, url, keyword);
    } catch (error) {
      console.error(`❌ Search failed (${keyword} | ${location}): ${error.message}`);
    }

    console.log(
      `🔗 ${keyword} | ${location}: ${uniqueJobs.length} unique Cutshort jobs found. Scraping detailed metadata & JDs...`
    );

    const detailedJobs = [];
    for (let i = 0; i < uniqueJobs.length; i += WORKER_POOL_SIZE) {
      const chunk = uniqueJobs.slice(i, i + WORKER_POOL_SIZE);
      const chunkResults = await Promise.all(
        chunk.map((job, idx) => {
          const workerPage = pages[idx % WORKER_POOL_SIZE];
          return (async () => {
            const details = await scrapeJobDetail(workerPage, job.link);
            return { ...job, ...details };
          })();
        })
      );
      detailedJobs.push(...chunkResults);
    }

    await Promise.all(pages.map((p) => p.close().catch(() => {})));

    return detailedJobs;
  } finally {
    await browser.close().catch(() => {});
  }
}

module.exports = scrapeJobs;
