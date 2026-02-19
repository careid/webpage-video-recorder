#!/usr/bin/env node

/**
 * Webpage Video Recorder CLI
 * Automated screen recorder for capturing webpage videos with audio
 */

import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { resolve } from 'path';
import { cleanupManager, log, sleep } from './lib/cleanup.js';
import { startDisplay, stopDisplay } from './lib/display.js';
import { setupAudioRecording, cleanupAudioRecording } from './lib/audio.js';
import { startRecording, stopRecording, estimateFileSize } from './lib/recorder.js';
import {
  launchBrowser,
  createPage,
  navigateToUrl,
  findAndPlayVideo,
  closeBrowser
} from './lib/browser.js';
import {
  readUrlFile,
  generateOutputPath,
  runSequential,
  runParallel,
  printBatchSummary
} from './lib/batch.js';

/**
 * Parse CLI arguments
 */
const argv = yargs(hideBin(process.argv))
  .usage('Usage: $0 --url <url> --output <file> [options]\n       $0 --batch <file> --batch-output-dir <dir> [options]')
  .option('url', {
    alias: 'u',
    type: 'string',
    description: 'URL of the webpage to record'
  })
  .option('duration', {
    alias: 'd',
    type: 'number',
    description: 'Recording duration in seconds (optional if auto-detection enabled)',
    demandOption: false
  })
  .option('output', {
    alias: 'o',
    type: 'string',
    description: 'Output file path (e.g., recording.mp4)'
  })
  .option('batch', {
    type: 'string',
    description: 'Path to URL list file (one URL per line)'
  })
  .option('batch-output-dir', {
    type: 'string',
    description: 'Output directory for batch recordings',
    default: './recordings'
  })
  .option('concurrency', {
    type: 'number',
    description: 'Number of parallel recordings (default: 1 = sequential)',
    default: 1
  })
  .option('resolution', {
    alias: 'r',
    type: 'string',
    description: 'Video resolution (e.g., 1920x1080)',
    default: '1920x1080'
  })
  .option('framerate', {
    alias: 'f',
    type: 'number',
    description: 'Video framerate',
    default: 30
  })
  .option('display', {
    type: 'number',
    description: 'X display number to use',
    default: 99
  })
  .option('video-selector', {
    alias: 's',
    type: 'string',
    description: 'CSS selector for video element',
    default: 'video'
  })
  .option('click-selector', {
    alias: 'c',
    type: 'array',
    description: 'CSS selectors to click before recording (e.g., play button)',
    default: []
  })
  .option('auto-detect-duration', {
    type: 'boolean',
    description: 'Auto-detect video duration from DOM',
    default: true
  })
  .option('buffer', {
    alias: 'b',
    type: 'number',
    description: 'Extra buffer time after duration (seconds)',
    default: 2
  })
  .option('quality', {
    alias: 'q',
    type: 'number',
    description: 'Video quality (CRF: 0-51, lower is better)',
    default: 23
  })
  .option('preset', {
    type: 'string',
    description: 'ffmpeg encoding preset (ultrafast, fast, medium, slow)',
    default: 'fast',
    choices: ['ultrafast', 'superfast', 'veryfast', 'faster', 'fast', 'medium', 'slow', 'slower', 'veryslow']
  })
  .option('audio-bitrate', {
    type: 'string',
    description: 'Audio bitrate (e.g., 128k, 192k)',
    default: '128k'
  })
  .option('log-console', {
    type: 'boolean',
    description: 'Log browser console messages',
    default: false
  })
  .option('log-requests', {
    type: 'boolean',
    description: 'Log network requests',
    default: false
  })
  .example('$0 --url "https://example.com/video" --output recording.mp4', 'Auto-detect video duration')
  .example('$0 -u "https://example.com/video" -d 30 -o recording.mp4', 'Record with manual 30-second duration')
  .example('$0 --batch urls.txt --batch-output-dir ./recordings', 'Batch record from file')
  .example('$0 --batch urls.txt --batch-output-dir ./recordings --concurrency 3', 'Batch parallel')
  .check((args) => {
    if (!args.batch && !args.url) {
      throw new Error('Either --url or --batch is required');
    }
    if (args.batch && args.url) {
      throw new Error('Cannot use both --url and --batch');
    }
    if (args.url && !args.output) {
      throw new Error('--output is required when using --url');
    }
    return true;
  })
  .help('h')
  .alias('h', 'help')
  .version('1.0.0')
  .alias('v', 'version')
  .strict()
  .parseSync();

/**
 * Record a single URL
 * @param {Object} options - Recording options
 * @param {string} options.url - URL to record
 * @param {string} options.outputPath - Output file path
 * @param {string} options.jobLabel - Label prefix for log messages
 * @param {number} options.jobIndex - Job index in batch
 * @param {boolean} options.parallelMode - Whether running in parallel
 * @param {number} options.displayStartNumber - Starting display number
 * @param {string} options.sinkName - PulseAudio sink name
 * @param {Object} options - Remaining shared CLI args
 * @returns {Promise<{success: boolean, outputPath: string, error?: string}>}
 */
async function recordUrl(options) {
  const {
    url,
    outputPath,
    jobLabel = '',
    jobIndex = 0,
    parallelMode = false,
    displayStartNumber = argv.display,
    sinkName = 'recording_sink',
    duration = argv.duration,
    resolution = argv.resolution,
    framerate = argv.framerate,
    quality = argv.quality,
    preset = argv.preset,
    audioBitrate = argv['audio-bitrate'],
    bufferTime = argv.buffer,
    videoSelector = argv['video-selector'],
    clickSelectors = argv['click-selector'],
    autoDetectDuration = argv['auto-detect-duration'],
    logConsole = argv['log-console'],
    logRequests = argv['log-requests']
  } = options;

  const prefix = jobLabel ? `${jobLabel} ` : '';
  const jobId = `job${jobIndex}`;

  const jlog = (msg) => log(`${prefix}${msg}`);

  // Validate duration arguments
  if (!duration && !autoDetectDuration) {
    return {
      success: false,
      outputPath,
      error: 'Either --duration must be provided or --auto-detect-duration must be enabled'
    };
  }

  jlog(`URL: ${url}`);
  jlog(`Output: ${outputPath}`);
  jlog(`Resolution: ${resolution}`);

  try {
  await cleanupManager.withCleanup(async (cleanup) => {
    let displayInfo, audioInfo, ffmpegProcess, browser;

    // Step 1: Start virtual display
    jlog('Starting virtual display...');
    displayInfo = await startDisplay(displayStartNumber, resolution + 'x24', true);
    cleanup.trackProcess(`${jobId}-xvfb`, displayInfo.process);
    jlog(`Display :${displayInfo.displayNumber} started`);

    // Step 2: Setup audio recording
    jlog('Setting up audio environment...');
    audioInfo = await setupAudioRecording(sinkName, {
      skipDefault: parallelMode
    });
    jlog(`Audio sink: ${audioInfo.sinkName}`);
    jlog(`Audio monitor: ${audioInfo.monitorName}`);

    // Register audio cleanup handler
    cleanup.registerCleanupHandler(async () => {
      await cleanupAudioRecording(audioInfo.sinkName);
    });

    // Step 3: Start ffmpeg recording BEFORE browser
    jlog('Starting ffmpeg recording...');
    ffmpegProcess = await startRecording({
      displayNumber: displayInfo.displayNumber,
      audioSource: audioInfo.monitorName,
      outputPath,
      resolution,
      framerate,
      crf: quality,
      preset,
      audioBitrate
    });
    cleanup.trackProcess(`${jobId}-ffmpeg`, ffmpegProcess);
    jlog('Recording started');

    // Wait a moment to ensure recording is stable
    await sleep(2000);

    // Step 4: Launch browser
    jlog('Launching browser...');
    browser = await launchBrowser(displayInfo.displayNumber, {
      width: parseInt(resolution.split('x')[0]),
      height: parseInt(resolution.split('x')[1]),
      pulseServer: audioInfo.pulseServer,
      defaultSinkName: parallelMode ? sinkName : null
    });

    // Step 5: Navigate and setup page
    jlog('Navigating to URL...');
    const page = await createPage(browser, {
      width: parseInt(resolution.split('x')[0]),
      height: parseInt(resolution.split('x')[1]),
      logConsole,
      logRequests
    });

    await navigateToUrl(page, url, {
      timeout: 60000,
      waitUntil: 'networkidle2'
    });

    // Step 6: Find and play video
    jlog('Finding and playing video...');
    const videoMetadata = await findAndPlayVideo(page, {
      videoSelector,
      clickSelectors,
      autoDetectDuration
    });

    jlog(`Video found: ${videoMetadata.videoWidth}x${videoMetadata.videoHeight}`);
    jlog(`Video source: ${videoMetadata.src}`);

    // Auto-detect duration if enabled
    let actualDuration = duration;
    if (autoDetectDuration) {
      if (videoMetadata.autoDetectedDuration && videoMetadata.autoDetectedDuration > 0) {
        actualDuration = Math.ceil(videoMetadata.autoDetectedDuration);
        jlog(`Auto-detected video duration: ${actualDuration}s`);
        jlog(`Recording will capture: ${actualDuration}s (+ ${bufferTime}s buffer)`);
      } else {
        // Auto-detection failed
        if (duration) {
          jlog('Auto-detection failed, using fallback manual duration');
          actualDuration = duration;
        } else {
          throw new Error(
            'Failed to auto-detect video duration. ' +
            'The video element may not have duration metadata (e.g., live stream). ' +
            'Please provide a manual duration with --duration <seconds>.'
          );
        }
      }
    }

    jlog('Video is playing');

    // Step 7: Wait for recording duration
    jlog('Recording in progress...');
    const totalTime = actualDuration + bufferTime;

    for (let i = 1; i <= totalTime; i++) {
      await sleep(1000);
      const remaining = totalTime - i;
      if (i % 5 === 0 || remaining <= 5) {
        jlog(`Recording... ${i}/${totalTime}s elapsed (${remaining}s remaining)`);
      }
    }

    jlog('Recording duration completed');

    // Stop recording gracefully
    jlog('Stopping recording...');
    await stopRecording(ffmpegProcess, 10000);
    cleanup.untrackProcess(`${jobId}-ffmpeg`);

    // Close browser
    jlog('Closing browser...');
    await closeBrowser(browser);

    // Stop display
    jlog('Stopping display...');
    await stopDisplay(displayInfo);
    cleanup.untrackProcess(`${jobId}-xvfb`);

    // Cleanup audio
    jlog('Cleaning up audio...');
    await cleanupAudioRecording(audioInfo.sinkName);
  });

  return { success: true, outputPath };
  } catch (error) {
    jlog(`ERROR: ${error.message}`);
    return { success: false, outputPath, error: error.message };
  }
}

/**
 * Run single URL recording (original behavior)
 */
async function runSingle() {
  log('='.repeat(70));
  log('Webpage Video Recorder');
  log('='.repeat(70));

  const outputPath = resolve(argv.output);
  const duration = argv.duration;
  const bufferTime = argv.buffer;

  // Validate duration arguments
  if (!argv.duration && !argv['auto-detect-duration']) {
    log('ERROR: Either --duration must be provided or --auto-detect-duration must be enabled');
    process.exit(1);
  }

  if (argv['auto-detect-duration']) {
    log('Auto-detection enabled: video duration will be extracted from DOM');
    if (argv.duration) {
      log(`Manual duration (${argv.duration}s) will be used as fallback if auto-detection fails`);
    }
  } else {
    log(`Manual duration: ${argv.duration}s`);
  }

  log(`URL: ${argv.url}`);
  if (argv.duration) {
    log(`Duration: ${duration}s (+ ${bufferTime}s buffer = ${duration + bufferTime}s total)`);
  } else {
    log(`Duration: Auto-detect from video element (+ ${bufferTime}s buffer)`);
  }
  log(`Output: ${outputPath}`);
  log(`Resolution: ${argv.resolution}`);
  log(`Framerate: ${argv.framerate} fps`);
  log(`Quality (CRF): ${argv.quality}`);
  log(`Preset: ${argv.preset}`);

  // Estimate file size (only if duration is known)
  if (argv.duration) {
    const estimatedSize = estimateFileSize(duration + bufferTime, argv.resolution, argv.framerate, argv.quality);
    log(`Estimated file size: ~${estimatedSize} MB`);
  } else {
    log('File size: Will be determined after auto-detecting video duration');
  }

  log('='.repeat(70));

  try {
    const result = await recordUrl({
      url: argv.url,
      outputPath
    });

    if (result.success) {
      log('='.repeat(70));
      log('Recording completed successfully!');
      log(`Output file: ${outputPath}`);
      log('='.repeat(70));
    } else {
      log('='.repeat(70));
      log(`ERROR: ${result.error}`);
      log('='.repeat(70));
      process.exit(1);
    }
  } catch (error) {
    log('='.repeat(70));
    log(`ERROR: ${error.message}`);
    log('='.repeat(70));

    if (error.stack) {
      log('Stack trace:');
      log(error.stack);
    }

    process.exit(1);
  }
}

/**
 * Run batch recording from URL file
 */
async function runBatch() {
  log('='.repeat(70));
  log('Webpage Video Recorder — Batch Mode');
  log('='.repeat(70));

  const batchFile = resolve(argv.batch);
  const outputDir = resolve(argv['batch-output-dir']);
  const concurrency = argv.concurrency;

  // Read URLs
  const urls = await readUrlFile(batchFile);

  log(`Batch file: ${batchFile}`);
  log(`Output directory: ${outputDir}`);
  log(`URLs to record: ${urls.length}`);
  log(`Concurrency: ${concurrency === 1 ? '1 (sequential)' : concurrency}`);
  log(`Resolution: ${argv.resolution}`);
  log(`Quality (CRF): ${argv.quality}`);
  log(`Preset: ${argv.preset}`);
  log('='.repeat(70));

  // Preview planned outputs
  log('Planned recordings:');
  for (let i = 0; i < urls.length; i++) {
    const outPath = generateOutputPath(urls[i], i, outputDir);
    log(`  ${i + 1}. ${urls[i]}`);
    log(`     -> ${outPath}`);
  }
  log('='.repeat(70));

  let results;

  try {
    if (concurrency <= 1) {
      results = await runSequential(urls, outputDir, recordUrl, {});
    } else {
      results = await runParallel(urls, outputDir, recordUrl, {}, concurrency);
    }
  } catch (error) {
    log(`FATAL: Batch processing error: ${error.message}`);
    if (error.stack) {
      log(error.stack);
    }
    process.exit(1);
  }

  // Print summary
  printBatchSummary(results);

  // Exit with error if any failed
  const failures = results.filter(r => !r.success);
  if (failures.length > 0) {
    process.exit(1);
  }
}

/**
 * Main entry point — dispatches to single or batch mode
 */
async function main() {
  if (argv.batch) {
    await runBatch();
  } else {
    await runSingle();
  }
}

// Run main function
main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
