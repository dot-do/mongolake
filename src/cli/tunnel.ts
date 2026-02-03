/**
 * MongoLake Tunnel
 *
 * Creates a Cloudflare tunnel to expose the local MongoLake dev server for
 * remote access. Uses cloudflared for quick tunnels without configuration.
 *
 * Usage:
 *   mongolake tunnel              # Create tunnel to local dev server (port 3456)
 *   mongolake tunnel --port 8080  # Create tunnel to specific port
 */

import { spawn, type ChildProcess } from 'node:child_process';
import { connect } from 'node:net';
import { EventEmitter } from 'node:events';

// ============================================================================
// Constants
// ============================================================================

/** Default port for MongoLake dev server */
const DEFAULT_PORT = 3456;

/** Default host for tunnel target */
const DEFAULT_HOST = 'localhost';

/** Default timeout for tunnel URL detection (30 seconds) */
const DEFAULT_TIMEOUT = 30000;

/** Default timeout for stop operation (5 seconds) */
const DEFAULT_STOP_TIMEOUT = 5000;

/** Default number of retries for transient errors */
const DEFAULT_RETRIES = 1;

/** Timeout for cloudflared version check (5 seconds) */
const CLOUDFLARED_CHECK_TIMEOUT = 5000;

/** Port check connection timeout (1 second) */
const PORT_CHECK_TIMEOUT = 1000;

// ============================================================================
// Types
// ============================================================================

export interface TunnelOptions {
  /** Port to tunnel (default: 3456) */
  port?: number;

  /** Host to tunnel to (default: localhost) */
  host?: string;

  /** Enable verbose logging */
  verbose?: boolean;

  /** Timeout for waiting for URL (in ms) */
  timeout?: number;

  /** Timeout for stop operation (in ms) */
  stopTimeout?: number;

  /** Number of retries for transient errors */
  retries?: number;
}

export interface TunnelInfo {
  url: string;
  localUrl: string;
  metricsUrl?: string;
}

export interface TunnelStats {
  uptime: number;
  startTime: Date | null;
  reconnectCount: number;
}

export enum TunnelStatus {
  CONNECTING = 'connecting',
  CONNECTED = 'connected',
  DISCONNECTED = 'disconnected',
  ERROR = 'error',
}

/**
 * Event map for TunnelManager events.
 * Defines the signature for each event type emitted by TunnelManager.
 */
export interface TunnelEventMap {
  /** Emitted when tunnel connection is being established */
  connecting: [];
  /** Emitted when tunnel is ready with connection info */
  ready: [TunnelInfo];
  /** Emitted when tunnel is stopped */
  stopped: [];
  /** Emitted on errors */
  error: [Error];
  /** Emitted for non-fatal warnings */
  warning: [{ message: string }];
  /** Emitted for cloudflared log output */
  log: [{ message: string }];
  /** Emitted during cleanup before shutdown */
  cleanup: [];
}

// ============================================================================
// Configuration
// ============================================================================

/**
 * Resolved tunnel configuration with all defaults applied.
 * This ensures type safety and consistent access to configuration values.
 */
interface TunnelConfig {
  readonly port: number;
  readonly host: string;
  readonly verbose: boolean;
  readonly timeout: number;
  readonly stopTimeout: number;
  readonly retries: number;
}

/**
 * Creates a resolved configuration from user options with defaults applied.
 *
 * @param options - User-provided options
 * @returns Resolved configuration with all defaults
 * @throws Error if port is invalid
 */
function createTunnelConfig(options: TunnelOptions): TunnelConfig {
  const port = options.port ?? DEFAULT_PORT;

  // Validate port range
  if (port <= 0 || port > 65535) {
    throw new Error('Invalid port: must be between 1 and 65535');
  }

  return {
    port,
    host: options.host ?? DEFAULT_HOST,
    verbose: options.verbose ?? false,
    timeout: options.timeout ?? DEFAULT_TIMEOUT,
    stopTimeout: options.stopTimeout ?? DEFAULT_STOP_TIMEOUT,
    retries: options.retries ?? DEFAULT_RETRIES,
  };
}

// ============================================================================
// Error Factories
// ============================================================================

/**
 * Creates a user-friendly error for when cloudflared is not installed.
 */
function createCloudflaredNotFoundError(): Error {
  return new Error(
    'cloudflared not found. Please install cloudflared:\n' +
    '  brew install cloudflare/cloudflare/cloudflared\n' +
    'or visit: https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation/'
  );
}

/**
 * Creates an error for tunnel process exit.
 *
 * @param code - Exit code from the process
 * @param output - Captured output from the process
 */
function createTunnelExitError(code: number | null, output: string): Error {
  return new Error(`Tunnel exited with code ${code}. Output: ${output}`);
}

/**
 * Creates an error for tunnel URL timeout.
 */
function createTimeoutError(): Error {
  return new Error('Timeout waiting for tunnel URL');
}

// ============================================================================
// Internal Types
// ============================================================================

/**
 * State for a single tunnel connection attempt.
 * Encapsulates all mutable state needed during tunnel establishment.
 */
interface ConnectionState {
  /** Accumulated output from cloudflared */
  output: string;
  /** Detected metrics URL, if any */
  metricsUrl: string | undefined;
  /** Whether this attempt has been resolved (success or failure) */
  resolved: boolean;
  /** Interval for checking URL in output */
  urlCheckInterval: NodeJS.Timeout | null;
  /** Timeout for URL detection */
  timeoutId: NodeJS.Timeout | null;
  /** Promise resolve callback */
  resolve: (info: TunnelInfo) => void;
  /** Promise reject callback */
  reject: (error: Error) => void;
}

// ============================================================================
// TunnelManager Class
// ============================================================================

/**
 * Manages a Cloudflare tunnel for exposing local services.
 *
 * TunnelManager handles the lifecycle of a cloudflared process, including:
 * - Starting and stopping the tunnel
 * - Monitoring connection status
 * - Handling signals for graceful shutdown
 * - Emitting events for connection state changes
 *
 * @example
 * ```ts
 * const manager = new TunnelManager({ port: 3000 });
 *
 * manager.on('ready', (info) => {
 *   console.log(`Tunnel ready at ${info.url}`);
 * });
 *
 * await manager.start();
 * // ... later
 * await manager.stop();
 * ```
 */
export class TunnelManager extends EventEmitter {
  /** Port being tunneled */
  public readonly port: number;

  /** Host being tunneled to */
  public readonly host: string;

  /** Whether verbose logging is enabled */
  public readonly verbose: boolean;

  /** Resolved configuration */
  private readonly config: TunnelConfig;

  /** Active cloudflared process */
  private process: ChildProcess | null = null;

  /** Current tunnel status */
  private status: TunnelStatus = TunnelStatus.DISCONNECTED;

  /** Current tunnel connection info */
  private tunnelInfo: TunnelInfo | null = null;

  /** When the tunnel was started */
  private startTime: Date | null = null;

  /** Number of reconnection attempts */
  private reconnectCount: number = 0;

  /** Whether the tunnel has ever been started successfully */
  private hasEverStarted: boolean = false;

  /** Registered SIGINT handler */
  private sigintHandler: (() => void) | null = null;

  /** Registered SIGTERM handler */
  private sigtermHandler: (() => void) | null = null;

  /**
   * Creates a new TunnelManager instance.
   *
   * @param options - Configuration options for the tunnel
   * @throws Error if port is invalid (must be 1-65535)
   */
  constructor(options: TunnelOptions) {
    super();

    this.config = createTunnelConfig(options);
    this.port = this.config.port;
    this.host = this.config.host;
    this.verbose = this.config.verbose;
  }

  /**
   * Starts the cloudflared tunnel.
   *
   * This method will:
   * 1. Check if a service is listening on the target port (emits warning if not)
   * 2. Spawn cloudflared with appropriate arguments
   * 3. Wait for the tunnel URL to be detected
   * 4. Set up signal handlers for graceful shutdown
   *
   * @returns Promise resolving to tunnel connection info
   * @throws Error if tunnel is already running
   * @throws Error if cloudflared is not installed
   * @throws Error if tunnel fails to start after all retries
   *
   * @example
   * ```ts
   * const info = await manager.start();
   * console.log(`Tunnel URL: ${info.url}`);
   * ```
   */
  async start(): Promise<TunnelInfo> {
    if (this.process && this.status === TunnelStatus.CONNECTED) {
      throw new Error('Tunnel already running');
    }

    // Track reconnects (if we've started before, this is a reconnect)
    if (this.hasEverStarted) {
      this.reconnectCount++;
    }

    // Check port listening and emit warning if nothing is listening
    const isListening = await this.checkPortListening();
    if (!isListening) {
      this.emit('warning', { message: `No service detected on port ${this.port}` });
    }

    return this.executeWithRetry();
  }

  /**
   * Executes the tunnel start with retry logic for transient errors.
   *
   * @returns Promise resolving to tunnel info on success
   * @throws Last error encountered after all retries exhausted
   */
  private async executeWithRetry(): Promise<TunnelInfo> {
    let lastError: Error | null = null;
    let attempt = 0;

    while (attempt < this.config.retries) {
      attempt++;
      try {
        return await this.attemptStart();
      } catch (error) {
        lastError = error as Error;

        // Non-retryable errors should fail immediately
        if (this.isNonRetryableError(error)) {
          this.status = TunnelStatus.ERROR;
          this.emit('error', error as Error);
          throw error;
        }

        // Continue if we have more retries
        if (attempt < this.config.retries) {
          continue;
        }
      }
    }

    this.status = TunnelStatus.ERROR;
    this.emit('error', lastError!);
    throw lastError;
  }

  /**
   * Checks if an error is non-retryable (e.g., cloudflared not installed).
   *
   * @param error - Error to check
   * @returns true if the error should not be retried
   */
  private isNonRetryableError(error: unknown): boolean {
    const err = error as NodeJS.ErrnoException;
    return err.code === 'ENOENT' || err.message?.includes('cloudflared not found');
  }

  /**
   * Attempts a single tunnel start operation.
   *
   * @returns Promise resolving to tunnel info on success
   */
  private async attemptStart(): Promise<TunnelInfo> {
    this.status = TunnelStatus.CONNECTING;
    this.emit('connecting');

    const args = this.buildCloudflaredArgs();

    return new Promise<TunnelInfo>((resolve, reject) => {
      this.process = spawn('cloudflared', args, {
        stdio: ['ignore', 'pipe', 'pipe'],
      });

      // Create state manager for this connection attempt
      const state = this.createConnectionState(resolve, reject);

      // Set up stream handlers
      this.setupStreamHandlers(state);

      // Set up process event handlers
      this.setupProcessEventHandlers(state);

      // Start URL check interval and timeout
      state.urlCheckInterval = setInterval(() => this.checkForUrl(state), 100);
      state.timeoutId = setTimeout(
        () => this.handleTimeout(state),
        this.config.timeout
      );
    });
  }

  /**
   * Builds the cloudflared command arguments.
   *
   * @returns Array of command line arguments
   */
  private buildCloudflaredArgs(): string[] {
    const args = [
      'tunnel',
      '--url', `http://${this.host}:${this.port}`,
      '--no-autoupdate',
    ];

    if (this.verbose) {
      args.push('--loglevel', 'debug');
    }

    return args;
  }

  /**
   * State for a single connection attempt.
   * Encapsulates all mutable state needed during tunnel establishment.
   */
  private createConnectionState(
    resolve: (info: TunnelInfo) => void,
    reject: (error: Error) => void
  ): ConnectionState {
    return {
      output: '',
      metricsUrl: undefined,
      resolved: false,
      urlCheckInterval: null,
      timeoutId: null,
      resolve,
      reject,
    };
  }

  /**
   * Cleans up timers for a connection attempt.
   */
  private cleanupConnectionState(state: ConnectionState): void {
    if (state.urlCheckInterval) {
      clearInterval(state.urlCheckInterval);
      state.urlCheckInterval = null;
    }
    if (state.timeoutId) {
      clearTimeout(state.timeoutId);
      state.timeoutId = null;
    }
  }

  /**
   * Checks accumulated output for a tunnel URL.
   */
  private checkForUrl(state: ConnectionState): void {
    if (state.resolved) return;

    const url = parseTunnelUrl(state.output);
    if (url) {
      state.resolved = true;
      this.cleanupConnectionState(state);

      const localUrl = `http://${this.host}:${this.port}`;
      this.tunnelInfo = { url, localUrl, metricsUrl: state.metricsUrl };
      this.status = TunnelStatus.CONNECTED;
      this.startTime = new Date();
      this.hasEverStarted = true;

      this.setupSignalHandlers();

      this.emit('ready', { url, localUrl, metricsUrl: state.metricsUrl });
      state.resolve(this.tunnelInfo);
    }
  }

  /**
   * Sets up handlers for stdout and stderr streams.
   */
  private setupStreamHandlers(state: ConnectionState): void {
    // Handle stderr (cloudflared writes most output here)
    this.process?.stderr?.on('data', (data: Buffer) => {
      const text = data.toString();
      state.output += text;

      // Extract metrics URL
      const metricsMatch = text.match(/Starting metrics server on (http:\/\/[^\s]+)/);
      if (metricsMatch) {
        state.metricsUrl = metricsMatch[1];
      }

      this.emit('log', { message: text.trim() });

      if (this.verbose) {
        console.log(text.trim());
      }

      this.checkForUrl(state);
    });

    // Handle stdout
    this.process?.stdout?.on('data', (data: Buffer) => {
      const text = data.toString();
      state.output += text;
      this.checkForUrl(state);
    });
  }

  /**
   * Sets up error and close handlers for the cloudflared process.
   */
  private setupProcessEventHandlers(state: ConnectionState): void {
    this.process?.on('error', (error: NodeJS.ErrnoException) => {
      this.handleProcessError(state, error);
    });

    this.process?.on('close', (code: number | null) => {
      this.handleProcessClose(state, code);
    });
  }

  /**
   * Handles cloudflared process errors.
   */
  private handleProcessError(state: ConnectionState, error: NodeJS.ErrnoException): void {
    this.cleanupConnectionState(state);

    if (!state.resolved) {
      state.resolved = true;
      this.status = TunnelStatus.ERROR;

      if (error.code === 'ENOENT') {
        const notFoundError = createCloudflaredNotFoundError();
        this.emit('error', notFoundError);
        state.reject(notFoundError);
      } else {
        this.emit('error', error);
        state.reject(error);
      }
    } else {
      // Emit error event even after connection is established
      this.emit('error', error);
    }
  }

  /**
   * Handles cloudflared process close events.
   */
  private handleProcessClose(state: ConnectionState, code: number | null): void {
    this.cleanupConnectionState(state);

    if (!state.resolved) {
      state.resolved = true;
      this.status = TunnelStatus.ERROR;
      const closeError = createTunnelExitError(code, state.output);
      this.emit('error', closeError);
      state.reject(closeError);
    } else if (code !== 0 && code !== null && this.status === TunnelStatus.CONNECTED) {
      // Unexpected exit after connected
      this.status = TunnelStatus.DISCONNECTED;
      this.emit('error', new Error(`Tunnel process exited unexpectedly with code ${code}`));
    } else {
      this.status = TunnelStatus.DISCONNECTED;
    }
  }

  /**
   * Handles tunnel URL timeout.
   */
  private handleTimeout(state: ConnectionState): void {
    if (!state.resolved) {
      state.resolved = true;
      this.cleanupConnectionState(state);
      this.status = TunnelStatus.ERROR;
      const timeoutError = createTimeoutError();
      this.emit('error', timeoutError);
      if (this.process) {
        this.process.kill('SIGKILL');
      }
      state.reject(timeoutError);
    }
  }

  /**
   * Stops the tunnel gracefully.
   *
   * Sends SIGTERM to the cloudflared process and waits for it to exit.
   * If the process doesn't exit within the stop timeout, SIGKILL is sent.
   *
   * @returns Promise that resolves when the tunnel is stopped
   *
   * @example
   * ```ts
   * await manager.stop();
   * console.log('Tunnel stopped');
   * ```
   */
  async stop(): Promise<void> {
    if (!this.process) {
      return;
    }

    return new Promise<void>((resolve) => {
      this.emit('cleanup');
      this.removeSignalHandlers();

      const proc = this.process!;
      let killed = false;

      // Set up force kill timeout
      const forceKillTimeout = setTimeout(() => {
        if (!killed) {
          proc.kill('SIGKILL');
        }
      }, this.config.stopTimeout);

      proc.on('close', () => {
        killed = true;
        clearTimeout(forceKillTimeout);
        this.resetState();
        this.emit('stopped');
        resolve();
      });

      // Send SIGTERM first for graceful shutdown
      proc.kill('SIGTERM');
    });
  }

  /**
   * Resets the tunnel state after stopping.
   */
  private resetState(): void {
    this.process = null;
    this.tunnelInfo = null;
    this.status = TunnelStatus.DISCONNECTED;
    this.startTime = null;
  }

  /**
   * Checks if the tunnel is currently running and connected.
   *
   * @returns true if the tunnel process is active and in connected state
   */
  isRunning(): boolean {
    return this.process !== null && this.status === TunnelStatus.CONNECTED;
  }

  /**
   * Gets the current tunnel status.
   *
   * @returns Current status string ('connecting', 'connected', 'disconnected', or 'error')
   */
  getStatus(): string {
    return this.status;
  }

  /**
   * Gets the current tunnel connection info.
   *
   * @returns Tunnel info if connected, null otherwise
   */
  getTunnelInfo(): TunnelInfo | null {
    return this.tunnelInfo;
  }

  /**
   * Gets tunnel statistics including uptime and reconnect count.
   *
   * @returns Object containing uptime (ms), start time, and reconnect count
   */
  getStats(): TunnelStats {
    const uptime = this.calculateUptime();

    return {
      uptime,
      startTime: this.startTime,
      reconnectCount: this.reconnectCount,
    };
  }

  /**
   * Calculates current uptime in milliseconds.
   *
   * @returns Uptime in ms, or 0 if not connected
   */
  private calculateUptime(): number {
    if (this.startTime && this.status === TunnelStatus.CONNECTED) {
      return Date.now() - this.startTime.getTime();
    }
    return 0;
  }

  /**
   * Checks if a service is listening on the target port.
   * Useful for warning users if no service is available to tunnel.
   *
   * @returns Promise resolving to true if a service is listening
   */
  async checkPortListening(): Promise<boolean> {
    return new Promise((resolve) => {
      const socket = connect(this.port, this.host);

      socket.setTimeout(PORT_CHECK_TIMEOUT);

      socket.on('connect', () => {
        socket.destroy();
        resolve(true);
      });

      socket.on('error', () => {
        socket.destroy();
        resolve(false);
      });

      socket.on('timeout', () => {
        socket.destroy();
        resolve(false);
      });
    });
  }

  /**
   * Sets up signal handlers for graceful shutdown.
   * Called after tunnel connection is established.
   */
  private setupSignalHandlers(): void {
    const handler = this.createSignalHandler();
    this.sigintHandler = handler;
    this.sigtermHandler = handler;

    process.on('SIGINT', this.sigintHandler);
    process.on('SIGTERM', this.sigtermHandler);
  }

  /**
   * Creates a signal handler that triggers tunnel cleanup.
   *
   * @returns Handler function for SIGINT/SIGTERM
   */
  private createSignalHandler(): () => void {
    return () => {
      this.emit('cleanup');
      if (this.process) {
        this.process.kill('SIGTERM');
      }
    };
  }

  /**
   * Removes registered signal handlers.
   * Called during stop to prevent handler leaks.
   */
  private removeSignalHandlers(): void {
    if (this.sigintHandler) {
      process.off('SIGINT', this.sigintHandler);
      this.sigintHandler = null;
    }
    if (this.sigtermHandler) {
      process.off('SIGTERM', this.sigtermHandler);
      this.sigtermHandler = null;
    }
  }
}

// ============================================================================
// Cloudflared Detection
// ============================================================================

/**
 * Checks if cloudflared is available on the system.
 *
 * Spawns `cloudflared --version` and checks for successful exit.
 * Times out after 5 seconds if cloudflared doesn't respond.
 *
 * @returns Promise resolving to true if cloudflared is installed and accessible
 *
 * @example
 * ```ts
 * if (await isCloudflaredInstalled()) {
 *   console.log('cloudflared is available');
 * } else {
 *   console.log('Please install cloudflared');
 * }
 * ```
 */
export async function isCloudflaredInstalled(): Promise<boolean> {
  return new Promise((resolve) => {
    const proc = spawn('cloudflared', ['--version'], {
      stdio: ['ignore', 'pipe', 'ignore'],
    });

    const timeout = setTimeout(() => {
      proc.kill();
      resolve(false);
    }, CLOUDFLARED_CHECK_TIMEOUT);

    proc.on('close', (code) => {
      clearTimeout(timeout);
      resolve(code === 0);
    });

    proc.on('error', () => {
      clearTimeout(timeout);
      resolve(false);
    });
  });
}

/**
 * Gets the installed cloudflared version.
 *
 * Parses version from output like "cloudflared version 2024.1.0 (built 2024-01-15)"
 *
 * @returns Promise resolving to version string, or null if not available
 *
 * @example
 * ```ts
 * const version = await getCloudflaredVersion();
 * if (version) {
 *   console.log(`Using cloudflared ${version}`);
 * }
 * ```
 */
export async function getCloudflaredVersion(): Promise<string | null> {
  return new Promise((resolve) => {
    const proc = spawn('cloudflared', ['--version'], {
      stdio: ['ignore', 'pipe', 'ignore'],
    });

    let output = '';

    proc.stdout?.on('data', (data) => {
      output += data.toString();
    });

    proc.on('close', (code) => {
      if (code === 0) {
        const version = parseVersionFromOutput(output);
        resolve(version);
      } else {
        resolve(null);
      }
    });

    proc.on('error', () => {
      resolve(null);
    });
  });
}

/**
 * Parses version number from cloudflared output.
 *
 * @param output - Raw output from cloudflared --version
 * @returns Parsed version string or trimmed output as fallback
 */
function parseVersionFromOutput(output: string): string {
  const match = output.match(/version\s+([\d.]+)/);
  return match ? match[1]! : output.trim();
}

// ============================================================================
// URL Parsing
// ============================================================================

/** Pattern for trycloudflare.com URLs (quick tunnels) */
const TRYCLOUDFLARE_URL_PATTERN = /https:\/\/[a-z0-9-]+\.trycloudflare\.com/i;

/** Pattern for URLs in cloudflared's box format output */
const BOX_FORMAT_URL_PATTERN = /\|\s*(https:\/\/[^\s|]+)\s*\|/;

/**
 * Parses tunnel URL from cloudflared output.
 *
 * Cloudflared outputs the tunnel URL in various formats:
 * - Direct URL: https://abc123.trycloudflare.com
 * - Box format:
 *   +-------------------------------------+
 *   |  https://abc123.trycloudflare.com  |
 *   +-------------------------------------+
 *
 * @param output - Raw output from cloudflared process
 * @returns Extracted tunnel URL, or null if not found
 *
 * @example
 * ```ts
 * const url = parseTunnelUrl('INF | https://my-tunnel.trycloudflare.com |');
 * // Returns: 'https://my-tunnel.trycloudflare.com'
 * ```
 */
export function parseTunnelUrl(output: string): string | null {
  if (!output || output.trim() === '') {
    return null;
  }

  // Primary pattern: trycloudflare.com URLs (quick tunnels)
  const trycloudflareMatch = output.match(TRYCLOUDFLARE_URL_PATTERN);
  if (trycloudflareMatch) {
    return trycloudflareMatch[0];
  }

  // Fallback: Look for URLs in the box format
  const boxMatch = output.match(BOX_FORMAT_URL_PATTERN);
  if (boxMatch) {
    return boxMatch[1]!.trim();
  }

  return null;
}

/**
 * Installation instructions for cloudflared, organized by platform.
 */
const INSTALL_INSTRUCTIONS = `
  cloudflared not found!

  To use MongoLake tunnel, you need to install cloudflared.

  Installation instructions:

  macOS (Homebrew):
    brew install cloudflare/cloudflare/cloudflared

  macOS (Direct download):
    curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-darwin-amd64.tgz | tar xz
    sudo mv cloudflared /usr/local/bin/

  Linux (Debian/Ubuntu):
    curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64.deb -o cloudflared.deb
    sudo dpkg -i cloudflared.deb

  Linux (Red Hat/CentOS):
    curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-x86_64.rpm -o cloudflared.rpm
    sudo rpm -i cloudflared.rpm

  Linux (Direct download):
    curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o cloudflared
    chmod +x cloudflared
    sudo mv cloudflared /usr/local/bin/

  Windows (Scoop):
    scoop install cloudflared

  Windows (Direct download):
    Download from https://github.com/cloudflare/cloudflared/releases/latest

  Docker:
    docker pull cloudflare/cloudflared

  After installation, run 'mongolake tunnel' again.

  For more information: https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation/
`;

/**
 * Prints installation instructions for cloudflared.
 */
function printInstallInstructions(): void {
  console.log(INSTALL_INSTRUCTIONS);
}

// ============================================================================
// Main Entry Point
// ============================================================================

/**
 * Starts a Cloudflare tunnel to expose local MongoLake.
 *
 * This is the main CLI entry point for the tunnel command. It:
 * 1. Checks for cloudflared availability
 * 2. Displays version info (if verbose)
 * 3. Creates and manages the tunnel
 * 4. Handles graceful shutdown
 *
 * @param options - Tunnel configuration options
 * @throws Error if cloudflared is not installed
 * @throws Error if tunnel fails to start
 *
 * @example
 * ```ts
 * // From CLI: mongolake tunnel --port 3000
 * await startTunnelCommand({ port: 3000 });
 * ```
 */
export async function startTunnelCommand(options: TunnelOptions): Promise<void> {
  const port = options.port ?? DEFAULT_PORT;
  const verbose = options.verbose ?? false;

  printStartupBanner(port);

  // Verify cloudflared is available
  await verifyCloudflaredInstalled();

  // Display version info in verbose mode
  if (verbose) {
    await displayVersionInfo();
  }

  const manager = new TunnelManager({ port, verbose });

  setupWarningHandler(manager, port);

  try {
    const info = await manager.start();
    printSuccessBanner(info, port);
    return waitForShutdown(manager);
  } catch (error) {
    console.error(`\n  Error: ${(error as Error).message}`);
    throw error;
  }
}

/**
 * Prints the startup banner.
 */
function printStartupBanner(port: number): void {
  console.log(`
  MongoLake Tunnel

  Creating tunnel to localhost:${port}...
`);
}

/**
 * Verifies cloudflared is installed, prints instructions if not.
 *
 * @throws Error if cloudflared is not available
 */
async function verifyCloudflaredInstalled(): Promise<void> {
  const available = await isCloudflaredInstalled();

  if (!available) {
    console.log(`
  cloudflared not found!

  To use MongoLake tunnel, you need to install cloudflared.

  Installation instructions:

  macOS (Homebrew):
    brew install cloudflare/cloudflare/cloudflared
`);
    printInstallInstructions();
    throw new Error('cloudflared not found');
  }
}

/**
 * Displays cloudflared version information.
 */
async function displayVersionInfo(): Promise<void> {
  const version = await getCloudflaredVersion();
  if (version) {
    console.log(`  Using cloudflared version ${version}\n`);
  }
}

/**
 * Sets up warning event handler for the tunnel manager.
 */
function setupWarningHandler(manager: TunnelManager, port: number): void {
  manager.on('warning', (warn) => {
    console.log(`
  Warning: ${warn.message}

  Make sure you have a MongoLake dev server running:
    mongolake dev --port ${port}

  Starting tunnel anyway...
`);
  });
}

/**
 * Prints the success banner with tunnel URL and instructions.
 */
function printSuccessBanner(info: TunnelInfo, port: number): void {
  console.log(`
  MongoLake Tunnel

  Tunnel created successfully!

  Public URL: ${info.url}

  Your local MongoLake server at localhost:${port} is now accessible at:
  ${info.url}

  Example API call:
  curl ${info.url}/health

  Press Ctrl+C to stop the tunnel
`);
}

/**
 * Waits for the tunnel to be stopped or encounter an error.
 *
 * @param manager - Tunnel manager instance
 * @returns Promise that resolves when tunnel stops
 */
function waitForShutdown(manager: TunnelManager): Promise<void> {
  return new Promise((resolve, reject) => {
    manager.on('stopped', () => {
      console.log('\n  Tunnel closed.');
      resolve();
    });

    manager.on('error', (error) => {
      if (manager.getStatus() === TunnelStatus.CONNECTED) {
        console.log(`\n  Tunnel closed unexpectedly: ${error.message}`);
      }
      reject(error);
    });
  });
}
