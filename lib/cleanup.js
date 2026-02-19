/**
 * Resource cleanup coordinator
 * Tracks all spawned processes and ensures proper cleanup on exit
 */

import { spawn } from 'child_process';

class CleanupManager {
  constructor() {
    this.processes = new Map();
    this.cleanupHandlers = [];
    this.isCleaningUp = false;

    // Register signal handlers
    process.on('SIGINT', () => this.cleanupAll('SIGINT'));
    process.on('SIGTERM', () => this.cleanupAll('SIGTERM'));
    process.on('uncaughtException', (error) => {
      console.error('[cleanup] Uncaught exception:', error);
      this.cleanupAll('uncaughtException');
      process.exit(1);
    });
  }

  /**
   * Track a spawned process
   */
  trackProcess(name, process) {
    if (!process || !process.pid) {
      console.warn(`[cleanup] Cannot track process "${name}" - invalid process object`);
      return;
    }

    this.processes.set(name, process);
    console.log(`[cleanup] Tracking process: ${name} (PID: ${process.pid})`);
  }

  /**
   * Untrack a process
   */
  untrackProcess(name) {
    if (this.processes.has(name)) {
      console.log(`[cleanup] Untracking process: ${name}`);
      this.processes.delete(name);
    }
  }

  /**
   * Register a custom cleanup handler
   */
  registerCleanupHandler(handler) {
    if (typeof handler === 'function') {
      this.cleanupHandlers.push(handler);
    }
  }

  /**
   * Kill a specific process gracefully
   */
  async killProcess(name, process, signal = 'SIGTERM') {
    if (!process || !process.pid) {
      console.warn(`[cleanup] Cannot kill process "${name}" - invalid process`);
      return;
    }

    return new Promise((resolve) => {
      console.log(`[cleanup] Sending ${signal} to ${name} (PID: ${process.pid})`);

      try {
        process.kill(signal);

        // Give process 5 seconds to exit gracefully
        const timeout = setTimeout(() => {
          if (process.killed === false) {
            console.warn(`[cleanup] Force killing ${name} with SIGKILL`);
            try {
              process.kill('SIGKILL');
            } catch (err) {
              console.error(`[cleanup] Error force killing ${name}:`, err.message);
            }
          }
          resolve();
        }, 5000);

        process.on('exit', () => {
          clearTimeout(timeout);
          console.log(`[cleanup] Process ${name} exited`);
          resolve();
        });
      } catch (err) {
        console.error(`[cleanup] Error killing ${name}:`, err.message);
        resolve();
      }
    });
  }

  /**
   * Clean up all tracked resources
   */
  async cleanupAll(reason = 'cleanup') {
    if (this.isCleaningUp) {
      console.log('[cleanup] Already cleaning up, skipping...');
      return;
    }

    this.isCleaningUp = true;
    console.log(`\n[cleanup] Starting cleanup (reason: ${reason})...`);

    // Run custom cleanup handlers first
    for (const handler of this.cleanupHandlers) {
      try {
        await handler();
      } catch (err) {
        console.error('[cleanup] Error in custom handler:', err.message);
      }
    }

    // Kill all tracked processes
    const killPromises = [];
    for (const [name, process] of this.processes.entries()) {
      killPromises.push(this.killProcess(name, process, 'SIGTERM'));
    }

    await Promise.all(killPromises);

    this.processes.clear();
    this.cleanupHandlers = [];
    this.isCleaningUp = false;
    console.log('[cleanup] Cleanup complete');
  }

  /**
   * Wrapper to run code with automatic cleanup
   */
  async withCleanup(fn) {
    try {
      return await fn(this);
    } finally {
      await this.cleanupAll('withCleanup');
    }
  }
}

// Export singleton instance
export const cleanupManager = new CleanupManager();

/**
 * Execute a shell command and track the process
 */
export function execTracked(name, command, args = [], options = {}) {
  const process = spawn(command, args, {
    stdio: options.stdio || 'inherit',
    env: { ...process.env, ...options.env },
    ...options
  });

  cleanupManager.trackProcess(name, process);

  // Auto-untrack when process exits
  process.on('exit', (code, signal) => {
    cleanupManager.untrackProcess(name);
    if (options.onExit) {
      options.onExit(code, signal);
    }
  });

  return process;
}

/**
 * Sleep utility
 */
export function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Log with timestamp
 */
export function log(message, ...args) {
  const timestamp = new Date().toISOString();
  console.log(`[${timestamp}] ${message}`, ...args);
}
