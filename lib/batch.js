/**
 * Batch recording utilities
 * Read URL files, generate output paths, run sequential/parallel recordings
 */

import { readFile } from 'fs/promises';
import { existsSync, mkdirSync } from 'fs';
import { join } from 'path';
import { log } from './cleanup.js';

/**
 * Read a URL file and return an array of URLs
 * Skips blank lines and lines starting with #
 * @param {string} filePath - Path to the URL list file
 * @returns {Promise<string[]>} Array of URLs
 */
export async function readUrlFile(filePath) {
  log(`Reading URL file: ${filePath}`);

  if (!existsSync(filePath)) {
    throw new Error(`URL file not found: ${filePath}`);
  }

  const content = await readFile(filePath, 'utf-8');
  const urls = content
    .split('\n')
    .map(line => line.trim())
    .filter(line => line.length > 0 && !line.startsWith('#'));

  if (urls.length === 0) {
    throw new Error(`No URLs found in file: ${filePath}`);
  }

  log(`Found ${urls.length} URL(s) in file`);
  return urls;
}

/**
 * Generate a unique output filename from a URL
 * @param {string} url - The URL to generate a filename for
 * @param {number} index - Zero-based index in the batch
 * @param {string} outputDir - Output directory path
 * @returns {string} Full output file path
 */
export function generateOutputPath(url, index, outputDir) {
  let parsed;
  try {
    parsed = new URL(url);
  } catch {
    // Fallback for invalid URLs
    const slug = url.replace(/[^a-zA-Z0-9]/g, '-').substring(0, 50);
    const prefix = String(index + 1).padStart(3, '0');
    return join(outputDir, `${prefix}-${slug}.mp4`);
  }

  const hostname = parsed.hostname.replace(/^www\./, '');
  const pathSlug = parsed.pathname
    .replace(/^\//, '')
    .replace(/\/$/, '')
    .replace(/[^a-zA-Z0-9-]/g, '-')
    .substring(0, 60);

  const prefix = String(index + 1).padStart(3, '0');
  const hostPart = hostname.replace(/\./g, '-');
  const nameParts = [prefix, hostPart];
  if (pathSlug) {
    nameParts.push(pathSlug);
  }

  return join(outputDir, `${nameParts.join('-')}.mp4`);
}

/**
 * Ensure output directory exists
 * @param {string} dir - Directory path
 */
export function ensureOutputDir(dir) {
  if (!existsSync(dir)) {
    log(`Creating output directory: ${dir}`);
    mkdirSync(dir, { recursive: true });
  }
}

/**
 * Run recordings sequentially
 * @param {string[]} urls - Array of URLs to record
 * @param {string} outputDir - Output directory
 * @param {Function} recordFn - Recording function (url, outputPath, index, opts) => result
 * @param {Object} sharedArgs - Shared CLI arguments to pass through
 * @returns {Promise<Object[]>} Array of results
 */
export async function runSequential(urls, outputDir, recordFn, sharedArgs) {
  ensureOutputDir(outputDir);

  const results = [];

  for (let i = 0; i < urls.length; i++) {
    const url = urls[i];
    const outputPath = generateOutputPath(url, i, outputDir);
    const jobLabel = `[${i + 1}/${urls.length}]`;

    log('='.repeat(70));
    log(`${jobLabel} Starting: ${url}`);
    log(`${jobLabel} Output: ${outputPath}`);
    log('='.repeat(70));

    const result = await recordFn({
      url,
      outputPath,
      jobLabel,
      jobIndex: i,
      parallelMode: false,
      ...sharedArgs
    });

    results.push({ url, outputPath, ...result });

    if (result.success) {
      log(`${jobLabel} Completed successfully`);
    } else {
      log(`${jobLabel} Failed: ${result.error}`);
    }
  }

  return results;
}

/**
 * Run recordings in parallel with concurrency limit
 * @param {string[]} urls - Array of URLs to record
 * @param {string} outputDir - Output directory
 * @param {Function} recordFn - Recording function (url, outputPath, index, opts) => result
 * @param {Object} sharedArgs - Shared CLI arguments to pass through
 * @param {number} concurrency - Maximum concurrent recordings
 * @returns {Promise<Object[]>} Array of results
 */
export async function runParallel(urls, outputDir, recordFn, sharedArgs, concurrency = 2) {
  ensureOutputDir(outputDir);

  const results = new Array(urls.length);
  const queue = urls.map((url, i) => ({ url, index: i }));
  let nextIndex = 0;

  /**
   * Worker that pulls jobs from the shared queue
   * @param {number} workerId - Worker identifier
   */
  async function worker(workerId) {
    while (nextIndex < queue.length) {
      const jobIdx = nextIndex++;
      const { url, index } = queue[jobIdx];
      const outputPath = generateOutputPath(url, index, outputDir);
      const jobLabel = `[W${workerId}|${index + 1}/${urls.length}]`;

      log(`${jobLabel} Starting: ${url}`);
      log(`${jobLabel} Output: ${outputPath}`);

      const result = await recordFn({
        url,
        outputPath,
        jobLabel,
        jobIndex: index,
        parallelMode: true,
        displayStartNumber: 99 + (workerId * 10),
        sinkName: `recording_sink_${workerId}`,
        ...sharedArgs
      });

      results[index] = { url, outputPath, ...result };

      if (result.success) {
        log(`${jobLabel} Completed successfully`);
      } else {
        log(`${jobLabel} Failed: ${result.error}`);
      }
    }
  }

  // Launch workers
  const workers = [];
  for (let i = 0; i < Math.min(concurrency, urls.length); i++) {
    workers.push(worker(i));
  }

  await Promise.all(workers);
  return results;
}

/**
 * Print a summary table of batch results
 * @param {Object[]} results - Array of batch results
 */
export function printBatchSummary(results) {
  log('='.repeat(70));
  log('Batch Recording Summary');
  log('='.repeat(70));

  let successCount = 0;
  let failCount = 0;

  for (const result of results) {
    const status = result.success ? 'OK' : 'FAIL';
    const detail = result.success ? result.outputPath : result.error;
    log(`  [${status}] ${result.url}`);
    log(`         ${detail}`);

    if (result.success) {
      successCount++;
    } else {
      failCount++;
    }
  }

  log('-'.repeat(70));
  log(`Total: ${results.length} | Success: ${successCount} | Failed: ${failCount}`);
  log('='.repeat(70));
}
