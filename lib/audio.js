/**
 * PulseAudio virtual sink management
 * Creates virtual audio sinks for capturing webpage audio
 */

import { spawn, exec } from 'child_process';
import { promisify } from 'util';
import { log } from './cleanup.js';

const execAsync = promisify(exec);

/**
 * Check if PulseAudio is running
 */
export async function checkPulseAudioRunning() {
  try {
    await execAsync('pulseaudio --check');
    return true;
  } catch (err) {
    return false;
  }
}

/**
 * Start PulseAudio daemon if not running
 */
export async function startPulseAudio() {
  log('Checking PulseAudio status...');

  const running = await checkPulseAudioRunning();

  if (running) {
    log('PulseAudio is already running');
    return;
  }

  log('Starting PulseAudio daemon...');

  try {
    // Kill any existing PulseAudio processes first
    try {
      await execAsync('pulseaudio --kill 2>/dev/null || true');
      await new Promise(resolve => setTimeout(resolve, 500));
    } catch (_) {
      // Ignore kill errors
    }

    // Start PulseAudio in daemon mode with permissive settings
    await execAsync('pulseaudio --start --exit-idle-time=-1 --load="module-always-sink" 2>/dev/null || pulseaudio -D --exit-idle-time=-1 2>/dev/null || true');
    log('PulseAudio started successfully');

    // Wait for daemon to be ready
    await new Promise(resolve => setTimeout(resolve, 1500));

    // Verify it's actually running
    const isRunning = await checkPulseAudioRunning();
    if (!isRunning) {
      throw new Error('PulseAudio failed to start');
    }
  } catch (err) {
    throw new Error(`Failed to start PulseAudio: ${err.message}`);
  }
}

/**
 * Execute pactl command
 */
async function pactl(command) {
  try {
    const { stdout, stderr } = await execAsync(`pactl ${command}`);
    return { stdout: stdout.trim(), stderr: stderr.trim() };
  } catch (err) {
    throw new Error(`pactl command failed: ${err.message}\nCommand: ${command}`);
  }
}

/**
 * List all audio sinks
 */
export async function listSinks() {
  try {
    const { stdout } = await execAsync('pactl list short sinks');
    return stdout.split('\n')
      .filter(line => line.trim())
      .map(line => {
        const parts = line.split('\t');
        return {
          index: parts[0],
          name: parts[1],
          module: parts[2],
          state: parts[3]
        };
      });
  } catch (err) {
    throw new Error(`Failed to list sinks: ${err.message}`);
  }
}

/**
 * Check if a sink exists
 */
export async function sinkExists(sinkName) {
  const sinks = await listSinks();
  return sinks.some(sink => sink.name === sinkName);
}

/**
 * Create a virtual null sink for audio recording
 * @param {string} sinkName - Name for the virtual sink
 * @param {string} description - Human-readable description
 * @returns {Promise<{sinkName: string, monitorName: string}>}
 */
export async function createVirtualSink(sinkName = 'recording_sink', description = 'Virtual Sink for Recording') {
  log(`Creating virtual audio sink: ${sinkName}`);

  // Check if PulseAudio is running
  await startPulseAudio();

  // Check if sink already exists
  const exists = await sinkExists(sinkName);
  if (exists) {
    log(`Virtual sink ${sinkName} already exists, removing it first...`);
    await removeVirtualSink(sinkName);
  }

  try {
    // Load null sink module (avoid sink_properties quoting issues in containers)
    const command = `load-module module-null-sink sink_name=${sinkName}`;
    const { stdout } = await pactl(command);

    const moduleId = stdout.trim();
    log(`Virtual sink created with module ID: ${moduleId}`);

    // The monitor source name is typically sink_name.monitor
    const monitorName = `${sinkName}.monitor`;

    // Verify sink was created
    await new Promise(resolve => setTimeout(resolve, 500));
    const verified = await sinkExists(sinkName);

    if (!verified) {
      throw new Error('Sink was not created successfully');
    }

    log(`Virtual sink monitor: ${monitorName}`);

    return {
      sinkName,
      monitorName,
      moduleId
    };
  } catch (err) {
    throw new Error(`Failed to create virtual sink: ${err.message}`);
  }
}

/**
 * Set default audio sink
 * @param {string} sinkName - Sink name to set as default
 */
export async function setDefaultSink(sinkName) {
  log(`Setting default sink to: ${sinkName}`);

  try {
    // Verify sink exists
    const exists = await sinkExists(sinkName);
    if (!exists) {
      throw new Error(`Sink ${sinkName} does not exist`);
    }

    // Set as default sink
    await pactl(`set-default-sink ${sinkName}`);
    log(`Default sink set to: ${sinkName}`);
  } catch (err) {
    throw new Error(`Failed to set default sink: ${err.message}`);
  }
}

/**
 * Get current default sink
 */
export async function getDefaultSink() {
  try {
    const { stdout } = await execAsync('pactl info');
    const match = stdout.match(/Default Sink: (.+)/);
    return match ? match[1].trim() : null;
  } catch (err) {
    throw new Error(`Failed to get default sink: ${err.message}`);
  }
}

/**
 * Remove a virtual sink
 * @param {string} sinkName - Sink name to remove
 */
export async function removeVirtualSink(sinkName) {
  log(`Removing virtual sink: ${sinkName}`);

  try {
    // Check if sink exists
    const exists = await sinkExists(sinkName);
    if (!exists) {
      log(`Virtual sink ${sinkName} does not exist, nothing to remove`);
      return;
    }

    // Get module ID for the sink
    const sinks = await listSinks();
    const sink = sinks.find(s => s.name === sinkName);

    if (!sink) {
      log(`Could not find sink ${sinkName} to remove`);
      return;
    }

    // Get full sink info to find module
    const { stdout } = await execAsync(`pactl list sinks`);
    const sinkBlocks = stdout.split('\n\n');

    let moduleId = null;
    for (const block of sinkBlocks) {
      if (block.includes(`Name: ${sinkName}`)) {
        const moduleMatch = block.match(/Owner Module: (\d+)/);
        if (moduleMatch) {
          moduleId = moduleMatch[1];
          break;
        }
      }
    }

    if (moduleId) {
      await pactl(`unload-module ${moduleId}`);
      log(`Virtual sink ${sinkName} removed (module ${moduleId})`);
    } else {
      log(`Could not find module ID for sink ${sinkName}`);
    }
  } catch (err) {
    // Don't throw on cleanup errors
    log(`Error removing virtual sink: ${err.message}`);
  }
}

/**
 * Setup audio environment for recording
 * Creates sink, sets as default, and returns monitor name for ffmpeg
 * @param {string} sinkName - Name for the virtual sink
 * @param {Object} options - Setup options
 * @param {boolean} options.skipDefault - Skip setting as default sink (for parallel mode)
 * @returns {Promise<{sinkName: string, monitorName: string}>}
 */
export async function setupAudioRecording(sinkName = 'recording_sink', options = {}) {
  const { skipDefault = false } = options;

  log('Setting up audio recording environment...');

  // Create virtual sink
  const { sinkName: createdSink, monitorName } = await createVirtualSink(sinkName);

  // Set as default sink (so browser audio goes here)
  // In parallel mode, skip this to avoid conflicts; use PULSE_SINK env var instead
  if (!skipDefault) {
    await setDefaultSink(createdSink);
  } else {
    log('Skipping default sink (parallel mode — using PULSE_SINK env var)');
  }

  // Get actual PulseAudio server string for browser env
  let pulseServer = null;
  try {
    const { stdout: infoOut } = await execAsync('pactl info');
    const serverMatch = infoOut.match(/Server String: (.+)/);
    if (serverMatch) {
      pulseServer = serverMatch[1].trim();
      log(`PulseAudio server: ${pulseServer}`);
    }
  } catch (_) {
    // Non-fatal, browser will try auto-discovery
  }

  log('Audio recording environment ready');
  log(`Browser audio will be captured from: ${monitorName}`);

  return {
    sinkName: createdSink,
    monitorName,
    pulseServer
  };
}

/**
 * Cleanup audio recording environment
 * @param {string} sinkName - Sink name to remove
 */
export async function cleanupAudioRecording(sinkName = 'recording_sink') {
  log('Cleaning up audio recording environment...');

  try {
    await removeVirtualSink(sinkName);
    log('Audio cleanup complete');
  } catch (err) {
    log(`Error during audio cleanup: ${err.message}`);
  }
}
