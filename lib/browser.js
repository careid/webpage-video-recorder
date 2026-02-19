/**
 * Puppeteer browser automation
 * Handles browser lifecycle, navigation, and video playback
 */

import puppeteer from 'puppeteer';
import { log, sleep } from './cleanup.js';

/**
 * Launch headful browser with display configuration
 * @param {number} displayNumber - X display number to use
 * @param {Object} options - Additional launch options
 * @returns {Promise<Browser>}
 */
export async function launchBrowser(displayNumber = 99, options = {}) {
  log(`Launching Puppeteer browser on display :${displayNumber}`);

  const {
    width = 1920,
    height = 1080,
    executablePath = null,
    additionalArgs = [],
    pulseServer = null,
    defaultSinkName = null
  } = options;

  const launchOptions = {
    headless: false,  // Must be false to work with Xvfb
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      `--display=:${displayNumber}`,
      '--autoplay-policy=no-user-gesture-required',  // Allow video autoplay
      '--disable-web-security',                      // Allow cross-origin content
      '--disable-features=IsolateOrigins,site-per-process',
      `--window-size=${width},${height}`,
      '--start-maximized',
      '--disable-infobars',                          // Hide "controlled by automation" bar
      '--disable-background-timer-throttling',       // Prevent tab throttling
      '--disable-backgrounding-occluded-windows',
      '--disable-renderer-backgrounding',
      '--use-fake-ui-for-media-stream',             // Auto-accept media permissions
      '--use-fake-device-for-media-stream',
      '--enable-features=NetworkService,NetworkServiceInProcess',
      ...additionalArgs
    ],
    defaultViewport: {
      width,
      height
    },
    ignoreDefaultArgs: ['--mute-audio', '--enable-automation'],  // Don't mute audio; hide automation bar
    env: {
      ...process.env,
      DISPLAY: `:${displayNumber}`,
      ...(pulseServer ? { PULSE_SERVER: pulseServer } : {}),
      ...(defaultSinkName ? { PULSE_SINK: defaultSinkName } : {})
    }
  };

  // Use custom Chrome/Chromium path if provided
  if (executablePath) {
    launchOptions.executablePath = executablePath;
    log(`Using custom browser executable: ${executablePath}`);
  }

  try {
    const browser = await puppeteer.launch(launchOptions);
    const version = await browser.version();
    log(`Browser launched successfully: ${version}`);

    return browser;
  } catch (err) {
    throw new Error(`Failed to launch browser: ${err.message}`);
  }
}

/**
 * Create new page with configuration
 * @param {Browser} browser - Puppeteer browser instance
 * @param {Object} options - Page options
 * @returns {Promise<Page>}
 */
export async function createPage(browser, options = {}) {
  const {
    width = 1920,
    height = 1080,
    userAgent = null
  } = options;

  log('Creating new browser page');

  const page = await browser.newPage();

  // Set viewport
  await page.setViewport({ width, height });

  // Set custom user agent if provided
  if (userAgent) {
    await page.setUserAgent(userAgent);
  }

  // Enable request interception for debugging if needed
  if (options.logRequests) {
    await page.setRequestInterception(true);
    page.on('request', (request) => {
      log(`Request: ${request.method()} ${request.url()}`);
      request.continue();
    });
  }

  // Log console messages from page
  if (options.logConsole) {
    page.on('console', (msg) => {
      log(`[Browser Console] ${msg.type()}: ${msg.text()}`);
    });
  }

  // Log page errors
  page.on('pageerror', (error) => {
    log(`[Page Error] ${error.message}`);
  });

  return page;
}

/**
 * Navigate to URL and wait for page load
 * @param {Page} page - Puppeteer page instance
 * @param {string} url - URL to navigate to
 * @param {Object} options - Navigation options
 * @returns {Promise<void>}
 */
export async function navigateToUrl(page, url, options = {}) {
  const {
    timeout = 60000,
    waitUntil = 'networkidle2'
  } = options;

  log(`Navigating to: ${url}`);

  try {
    await page.goto(url, {
      timeout,
      waitUntil
    });

    log(`Successfully loaded: ${url}`);
  } catch (err) {
    throw new Error(`Failed to navigate to ${url}: ${err.message}`);
  }
}

/**
 * Find video element on page
 * @param {Page} page - Puppeteer page instance
 * @param {string} selector - Video element selector
 * @param {number} timeout - Timeout in milliseconds
 * @returns {Promise<ElementHandle>}
 */
export async function findVideoElement(page, selector = 'video', timeout = 30000) {
  log(`Looking for video element: ${selector}`);

  try {
    await page.waitForSelector(selector, { timeout });
    log(`Video element found: ${selector}`);

    const videoElement = await page.$(selector);
    return videoElement;
  } catch (err) {
    throw new Error(`Video element not found within ${timeout}ms: ${selector}\n${err.message}`);
  }
}

/**
 * Get video metadata
 * @param {Page} page - Puppeteer page instance
 * @param {string} selector - Video element selector
 * @returns {Promise<Object>}
 */
export async function getVideoMetadata(page, selector = 'video') {
  log('Retrieving video metadata...');

  try {
    const metadata = await page.evaluate((sel) => {
      const video = document.querySelector(sel);
      if (!video) {
        return null;
      }

      return {
        duration: video.duration,
        currentTime: video.currentTime,
        paused: video.paused,
        ended: video.ended,
        readyState: video.readyState,
        videoWidth: video.videoWidth,
        videoHeight: video.videoHeight,
        src: video.src || video.currentSrc
      };
    }, selector);

    if (!metadata) {
      throw new Error('Video element not found or inaccessible');
    }

    log(`Video metadata: duration=${metadata.duration}s, resolution=${metadata.videoWidth}x${metadata.videoHeight}`);
    return metadata;
  } catch (err) {
    throw new Error(`Failed to get video metadata: ${err.message}`);
  }
}

/**
 * Play video element
 * @param {Page} page - Puppeteer page instance
 * @param {string} selector - Video element selector
 * @param {Object} options - Playback options
 * @returns {Promise<void>}
 */
export async function playVideo(page, selector = 'video', options = {}) {
  const {
    clickToPlay = true,
    waitForPlay = true,
    maxAttempts = 3
  } = options;

  log(`Attempting to play video: ${selector}`);

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      // Try clicking the video first (handles some video players)
      if (clickToPlay) {
        log(`Attempt ${attempt}: Clicking video element...`);
        await page.click(selector);
        await sleep(500);
      }

      // Call play() method programmatically
      log(`Attempt ${attempt}: Calling video.play()...`);
      const playResult = await page.evaluate((sel) => {
        const video = document.querySelector(sel);
        if (!video) {
          return { success: false, error: 'Video element not found' };
        }

        try {
          const playPromise = video.play();
          if (playPromise !== undefined) {
            return playPromise
              .then(() => ({ success: true, paused: video.paused }))
              .catch(err => ({ success: false, error: err.message }));
          }
          return { success: true, paused: video.paused };
        } catch (err) {
          return { success: false, error: err.message };
        }
      }, selector);

      if (playResult.success) {
        log('Video playback started successfully');

        // Wait to verify video is actually playing
        if (waitForPlay) {
          await sleep(1000);

          const isPlaying = await page.evaluate((sel) => {
            const video = document.querySelector(sel);
            return video && !video.paused && !video.ended && video.readyState > 2;
          }, selector);

          if (isPlaying) {
            log('Verified video is playing');
            return;
          } else {
            log('Warning: Video play() succeeded but video is not playing');
          }
        } else {
          return;
        }
      } else {
        log(`Play attempt ${attempt} failed: ${playResult.error}`);
      }

      // Wait before retry
      if (attempt < maxAttempts) {
        await sleep(1000);
      }
    } catch (err) {
      log(`Play attempt ${attempt} error: ${err.message}`);

      if (attempt === maxAttempts) {
        throw new Error(`Failed to play video after ${maxAttempts} attempts: ${err.message}`);
      }

      await sleep(1000);
    }
  }

  throw new Error(`Could not start video playback after ${maxAttempts} attempts`);
}

/**
 * Click element if exists
 * @param {Page} page - Puppeteer page instance
 * @param {string} selector - Element selector
 * @param {number} timeout - Timeout in milliseconds
 * @returns {Promise<boolean>} True if clicked, false if not found
 */
export async function clickIfExists(page, selector, timeout = 5000) {
  try {
    await page.waitForSelector(selector, { timeout });
    await page.click(selector);
    log(`Clicked element: ${selector}`);
    return true;
  } catch (err) {
    log(`Element not found or not clickable: ${selector}`);
    return false;
  }
}

/**
 * Find and play video with optional click selectors
 * @param {Page} page - Puppeteer page instance
 * @param {Object} options - Options
 * @returns {Promise<Object>} Video metadata
 */
export async function findAndPlayVideo(page, options = {}) {
  const {
    videoSelector = 'video',
    clickSelectors = [],
    autoDetectDuration = true
  } = options;

  log('Finding and playing video...');

  // Wait for video element
  const videoElement = await findVideoElement(page, videoSelector);

  // Click any specified elements first (play buttons, etc.)
  for (const selector of clickSelectors) {
    await clickIfExists(page, selector);
    await sleep(500);
  }

  // Get video metadata before playing
  const metadata = await getVideoMetadata(page, videoSelector);

  // Play the video
  await playVideo(page, videoSelector);

  // Return metadata including duration for auto-detection
  return {
    ...metadata,
    autoDetectedDuration: autoDetectDuration && metadata.duration ? metadata.duration : null
  };
}

/**
 * Close browser gracefully
 * @param {Browser} browser - Puppeteer browser instance
 */
export async function closeBrowser(browser) {
  if (!browser) {
    log('No browser instance to close');
    return;
  }

  log('Closing browser...');

  try {
    await browser.close();
    log('Browser closed successfully');
  } catch (err) {
    log(`Error closing browser: ${err.message}`);
  }
}

/**
 * Wait for video to finish playing
 * @param {Page} page - Puppeteer page instance
 * @param {string} selector - Video element selector
 * @param {number} timeout - Maximum wait time in milliseconds
 * @returns {Promise<void>}
 */
export async function waitForVideoEnd(page, selector = 'video', timeout = 600000) {
  log('Waiting for video to finish...');

  try {
    await page.waitForFunction(
      (sel) => {
        const video = document.querySelector(sel);
        return video && video.ended;
      },
      { timeout },
      selector
    );

    log('Video playback finished');
  } catch (err) {
    throw new Error(`Video did not finish within timeout: ${err.message}`);
  }
}
