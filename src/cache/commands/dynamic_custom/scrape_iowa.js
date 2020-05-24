const fs = require('fs').promises;
const puppeteer = require('puppeteer');

// Process arguments
const args = require('yargs').parse();

(async () => {
    const browser = await puppeteer.launch();
    const page = await browser.newPage();
    await page.goto(args.url).catch(exc => void exc);
    const response = await page.waitForResponse(response => {
        return response.url().indexOf('/cards/1308116147/render') !== -1;
    }, { timeout: 10000 });
    try {
        await fs.writeFile(args.output, JSON.stringify((await response.json())));
    } catch (exc) {
        console.error(`Error parsing response: ${exc}`);
    }
    await browser.close();
})();