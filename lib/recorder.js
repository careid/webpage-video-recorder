/**
 * ffmpeg recording orchestration
 * Handles screen and audio capture using ffmpeg
 */

import { spawn } from 'child_process';
import { existsSync, mkdirSync } from 'fs';
import { dirname, resolve } from 'path';
import { log } from './cleanup.js';

/**
 * Check if ffmpeg is available
 */
export async function checkFfmpegAvailable() {
  return new Promise((resolve) => {
    const check = spawn('which', ['ffmpeg'], { stdio: 'pipe' });
    check.on('exit', (code) => resolve(code === 0));
  });
}

/**
 * Get ffmpeg version
 */
export async function getFfmpegVersion() {
  return new Promise((resolve, reject) => {
    const ffmpeg = spawn('ffmpeg', ['-version'], { stdio: 'pipe' });

    let output = '';
    ffmpeg.stdout.on('data', (data) => {
      output += data.toString();
    });

    ffmpeg.on('exit', (code) => {
      if (code === 0) {
        const match = output.match(/ffmpeg version ([^\s]+)/);
        resolve(match ? match[1] : 'unknown');
      } else {
        reject(new Error('Could not get ffmpeg version'));
      }
    });
  });
}

/**
 * Parse resolution string to width and height
 */
function parseResolution(resolution) {
  const match = resolution.match(/(\d+)x(\d+)/);
  if (!match) {
    throw new Error(`Invalid resolution format: ${resolution}. Expected format: WIDTHxHEIGHT (e.g., 1920x1080)`);
  }
  return {
    width: parseInt(match[1], 10),
    height: parseInt(match[2], 10)
  };
}

/**
 * Start recording screen and audio with ffmpeg
 * @param {Object} options - Recording options
 * @param {number} options.displayNumber - X display number
 * @param {string} options.audioSource - PulseAudio monitor source
 * @param {string} options.outputPath - Output file path
 * @param {string} options.resolution - Video resolution (e.g., '1920x1080')
 * @param {number} options.framerate - Video framerate (default: 30)
 * @param {string} options.format - Output format (default: 'mp4')
 * @param {number} options.crf - Video quality (0-51, lower is better, default: 23)
 * @param {string} options.preset - ffmpeg preset (default: 'fast')
 * @param {string} options.audioBitrate - Audio bitrate (default: '128k')
 * @returns {Promise<ChildProcess>}
 */
export async function startRecording(options) {
  const {
    displayNumber = 99,
    audioSource,
    outputPath,
    resolution = '1920x1080',
    framerate = 30,
    format = 'mp4',
    crf = 23,
    preset = 'fast',
    audioBitrate = '128k'
  } = options;

  log('Starting ffmpeg recording...');

  // Validate required options
  if (!audioSource) {
    throw new Error('audioSource is required');
  }

  if (!outputPath) {
    throw new Error('outputPath is required');
  }

  // Check if ffmpeg is available
  const available = await checkFfmpegAvailable();
  if (!available) {
    throw new Error('ffmpeg is not installed. Install with: apt-get install ffmpeg');
  }

  // Log ffmpeg version
  try {
    const version = await getFfmpegVersion();
    log(`Using ffmpeg version: ${version}`);
  } catch (err) {
    log('Could not determine ffmpeg version');
  }

  // Ensure output directory exists
  const outputDir = dirname(resolve(outputPath));
  if (!existsSync(outputDir)) {
    log(`Creating output directory: ${outputDir}`);
    mkdirSync(outputDir, { recursive: true });
  }

  // Parse resolution
  const { width, height } = parseResolution(resolution);

  // Build ffmpeg command
  const ffmpegArgs = [
    '-y',                              // Overwrite output file
    // Use wall clock timestamps so captured frames match real elapsed time.
    // Without this, x11grab assumes perfect constant-rate delivery; any jitter
    // causes the output to play back slightly fast or slow.
    '-use_wallclock_as_timestamps', '1',
    '-thread_queue_size', '1024',      // Prevent video buffer underruns
    '-video_size', `${width}x${height}`,
    '-framerate', framerate.toString(),
    '-f', 'x11grab',
    '-i', `:${displayNumber}`,         // X11 display input
    '-thread_queue_size', '1024',      // Prevent audio buffer underruns
    '-itsoffset', '1.125',              // Delay audio ~1125ms to compensate for x11grab pipeline latency
    '-f', 'pulse',
    '-i', audioSource,                 // PulseAudio input
    '-c:v', 'libx264',                 // H.264 video codec
    '-preset', preset,                 // Encoding preset
    '-crf', crf.toString(),            // Quality level
    '-r', framerate.toString(),        // Output framerate (re-timestamp to constant rate)
    '-c:a', 'aac',                     // AAC audio codec
    '-b:a', audioBitrate,              // Audio bitrate
    '-pix_fmt', 'yuv420p',             // Pixel format (universal compatibility)
    '-movflags', '+faststart',         // Enable streaming
    outputPath
  ];

  log(`ffmpeg command: ffmpeg ${ffmpegArgs.join(' ')}`);

  // Start ffmpeg process
  const ffmpegProcess = spawn('ffmpeg', ffmpegArgs, {
    stdio: ['pipe', 'pipe', 'pipe'],
    env: { ...process.env, DISPLAY: `:${displayNumber}` }
  });

  // Track startup
  let started = false;
  let errorOutput = '';

  // Capture stderr for debugging (ffmpeg outputs to stderr)
  ffmpegProcess.stderr.on('data', (data) => {
    const output = data.toString();
    errorOutput += output;

    // ffmpeg prints progress info to stderr
    if (output.includes('frame=') || output.includes('time=')) {
      if (!started) {
        started = true;
        log('ffmpeg recording started successfully');
      }
      // Log progress periodically
      const timeMatch = output.match(/time=(\S+)/);
      if (timeMatch) {
        log(`Recording progress: ${timeMatch[1]}`);
      }
    }
  });

  // Handle ffmpeg exit
  ffmpegProcess.on('exit', (code, signal) => {
    if (code === 0) {
      log('ffmpeg recording completed successfully');
    } else if (signal === 'SIGINT' || signal === 'SIGTERM') {
      log('ffmpeg recording stopped gracefully');
    } else {
      log(`ffmpeg exited with code ${code}, signal ${signal}`);
      if (errorOutput) {
        log(`ffmpeg stderr:\n${errorOutput}`);
      }
    }
  });

  // Wait a moment to ensure ffmpeg starts successfully
  await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      if (!started) {
        ffmpegProcess.kill('SIGTERM');
        reject(new Error(`ffmpeg failed to start within 5 seconds\nStderr: ${errorOutput}`));
      } else {
        resolve();
      }
    }, 5000);

    ffmpegProcess.on('exit', (code) => {
      clearTimeout(timeout);
      if (code !== 0 && code !== null) {
        reject(new Error(`ffmpeg exited prematurely with code ${code}\nStderr: ${errorOutput}`));
      }
    });

    // Check for startup in stderr
    const startupCheckInterval = setInterval(() => {
      if (started) {
        clearInterval(startupCheckInterval);
        clearTimeout(timeout);
        resolve();
      }
    }, 200);
  });

  return ffmpegProcess;
}

/**
 * Stop ffmpeg recording gracefully
 * @param {ChildProcess} ffmpegProcess - The ffmpeg process to stop
 * @param {number} gracePeriod - Time to wait for graceful exit (ms)
 */
export async function stopRecording(ffmpegProcess, gracePeriod = 5000) {
  if (!ffmpegProcess || !ffmpegProcess.pid) {
    log('No ffmpeg process to stop');
    return;
  }

  log('Stopping ffmpeg recording...');

  return new Promise((resolve) => {
    let resolved = false;

    const cleanup = () => {
      if (!resolved) {
        resolved = true;
        resolve();
      }
    };

    // Listen for exit
    ffmpegProcess.on('exit', (code, signal) => {
      log(`ffmpeg exited with code ${code}, signal ${signal}`);
      cleanup();
    });

    // Send SIGINT for graceful shutdown (ffmpeg finalizes file on SIGINT)
    try {
      log('Sending SIGINT to ffmpeg for graceful shutdown...');
      ffmpegProcess.kill('SIGINT');
    } catch (err) {
      log(`Error sending SIGINT: ${err.message}`);
      cleanup();
      return;
    }

    // Force kill if graceful shutdown fails
    const forceTimeout = setTimeout(() => {
      if (!resolved && ffmpegProcess.killed === false) {
        log('ffmpeg did not exit gracefully, sending SIGKILL...');
        try {
          ffmpegProcess.kill('SIGKILL');
        } catch (err) {
          log(`Error sending SIGKILL: ${err.message}`);
        }
        cleanup();
      }
    }, gracePeriod);

    // Cleanup timeout on exit
    ffmpegProcess.on('exit', () => {
      clearTimeout(forceTimeout);
    });
  });
}

/**
 * Estimate recording file size
 * @param {number} durationSeconds - Recording duration
 * @param {string} resolution - Video resolution
 * @param {number} framerate - Video framerate
 * @param {number} crf - Video quality
 * @returns {number} Estimated file size in MB
 */
export function estimateFileSize(durationSeconds, resolution = '1920x1080', framerate = 30, crf = 23) {
  const { width, height } = parseResolution(resolution);
  const pixels = width * height;

  // Rough estimation based on H.264 encoding
  // Lower CRF = higher bitrate
  const crf_factor = Math.pow(2, (51 - crf) / 6);
  const base_bitrate = (pixels * framerate * 0.07) / 1000; // kbps
  const video_bitrate = base_bitrate * crf_factor;
  const audio_bitrate = 128; // kbps

  const total_bitrate = video_bitrate + audio_bitrate;
  const file_size_mb = (total_bitrate * durationSeconds) / (8 * 1024);

  return Math.round(file_size_mb * 100) / 100;
}
