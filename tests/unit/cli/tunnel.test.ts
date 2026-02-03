/**
 * Tests for MongoLake Tunnel Integration
 *
 * Comprehensive tests for the Cloudflare tunnel functionality including:
 * - Tunnel creation and lifecycle management
 * - URL generation and parsing
 * - Connection status monitoring
 * - Cleanup and graceful shutdown on exit
 * - Error handling and recovery
 *
 * RED PHASE: These tests define the expected behavior for the
 * tunnel integration that needs to be fully implemented.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { ChildProcess } from 'node:child_process';

// ============================================================================
// Mock Setup
// ============================================================================

// Mock child_process for spawning cloudflared
const mockStdout = {
  on: vi.fn().mockReturnThis(),
  pipe: vi.fn().mockReturnThis(),
  removeListener: vi.fn(),
};

const mockStderr = {
  on: vi.fn().mockReturnThis(),
  pipe: vi.fn().mockReturnThis(),
  removeListener: vi.fn(),
};

// Track process event handlers for triggering them
const processHandlers: Map<string, Function[]> = new Map();

// Control whether kill triggers close event
let killTriggersClose = true;

const mockProcess = {
  on: vi.fn((event: string, handler: Function) => {
    const handlers = processHandlers.get(event) || [];
    handlers.push(handler);
    processHandlers.set(event, handlers);
    return mockProcess;
  }),
  once: vi.fn().mockReturnThis(),
  stdout: mockStdout,
  stderr: mockStderr,
  stdin: { write: vi.fn(), end: vi.fn() },
  kill: vi.fn((signal?: string) => {
    // Only trigger 'close' event if killTriggersClose is true OR if it's a SIGKILL
    if (killTriggersClose || signal === 'SIGKILL') {
      setTimeout(() => {
        const closeHandlers = processHandlers.get('close') || [];
        closeHandlers.forEach(handler => handler(signal === 'SIGKILL' ? 137 : 0));
      }, 10);
    }
    return true;
  }),
  killed: false,
  pid: 12345,
  exitCode: null,
  signalCode: null,
};

// Helper to reset process handlers between tests
function resetProcessHandlers() {
  processHandlers.clear();
  killTriggersClose = true; // Reset to default
}

// Helper to simulate a stuck process that doesn't respond to SIGTERM
function simulateStuckProcess() {
  killTriggersClose = false;
}

vi.mock('node:child_process', () => ({
  spawn: vi.fn(() => mockProcess),
  execSync: vi.fn(),
}));

// Mock net module for port checking
vi.mock('node:net', () => ({
  connect: vi.fn((port, host) => {
    const mockSocket = {
      on: vi.fn((event, handler) => {
        if (event === 'connect') {
          setTimeout(() => handler(), 10);
        }
        return mockSocket;
      }),
      destroy: vi.fn(),
      setTimeout: vi.fn(),
    };
    return mockSocket;
  }),
}));

// ============================================================================
// Test Fixtures
// ============================================================================

const DEFAULT_TUNNEL_OPTIONS = {
  port: 3456,
  verbose: false,
};

const MOCK_TUNNEL_URL = 'https://test-tunnel-abc123.trycloudflare.com';
const MOCK_METRICS_URL = 'http://127.0.0.1:60123/metrics';

// ============================================================================
// Module Export Tests
// ============================================================================

describe('CLI Tunnel - Module Exports', () => {
  it('should export startTunnelCommand function', async () => {
    const module = await import('../../../src/cli/tunnel.js');
    expect(typeof module.startTunnelCommand).toBe('function');
  });

  it('should export TunnelManager class', async () => {
    const module = await import('../../../src/cli/tunnel.js');
    expect(module.TunnelManager).toBeDefined();
    expect(typeof module.TunnelManager).toBe('function');
  });

  it('should export TunnelOptions type (type check)', async () => {
    const module = await import('../../../src/cli/tunnel.js');
    expect(module.startTunnelCommand).toBeDefined();
  });

  it('should export TunnelInfo type (type check)', async () => {
    const module = await import('../../../src/cli/tunnel.js');
    expect(module.TunnelManager).toBeDefined();
  });

  it('should export TunnelStatus enum', async () => {
    const module = await import('../../../src/cli/tunnel.js');
    expect(module.TunnelStatus).toBeDefined();
    expect(module.TunnelStatus.CONNECTING).toBeDefined();
    expect(module.TunnelStatus.CONNECTED).toBeDefined();
    expect(module.TunnelStatus.DISCONNECTED).toBeDefined();
    expect(module.TunnelStatus.ERROR).toBeDefined();
  });

  it('should export isCloudflaredInstalled function', async () => {
    const module = await import('../../../src/cli/tunnel.js');
    expect(typeof module.isCloudflaredInstalled).toBe('function');
  });

  it('should export getCloudflaredVersion function', async () => {
    const module = await import('../../../src/cli/tunnel.js');
    expect(typeof module.getCloudflaredVersion).toBe('function');
  });
});

// ============================================================================
// TunnelManager Class Tests
// ============================================================================

describe('CLI Tunnel - TunnelManager Class', () => {
  let TunnelManager: typeof import('../../../src/cli/tunnel.js').TunnelManager;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/tunnel.js');
    TunnelManager = module.TunnelManager;
    vi.clearAllMocks();
    resetProcessHandlers();

    // Reset spawn to default implementation
    const { spawn } = await import('node:child_process');
    vi.mocked(spawn).mockImplementation(() => mockProcess as unknown as ChildProcess);
  });

  describe('constructor', () => {
    it('should create TunnelManager instance with options', () => {
      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);

      expect(manager.port).toBe(3456);
      expect(manager.verbose).toBe(false);
    });

    it('should use default port when not provided', () => {
      const manager = new TunnelManager({});

      expect(manager.port).toBe(3456);
    });

    it('should validate port range', () => {
      expect(() => new TunnelManager({ port: 0 })).toThrow('Invalid port');
      expect(() => new TunnelManager({ port: 70000 })).toThrow('Invalid port');
    });

    it('should accept custom host option', () => {
      const manager = new TunnelManager({ port: 3456, host: '0.0.0.0' });

      expect(manager.host).toBe('0.0.0.0');
    });

    it('should default host to localhost', () => {
      const manager = new TunnelManager({ port: 3456 });

      expect(manager.host).toBe('localhost');
    });
  });

  describe('start', () => {
    it('should start a cloudflared tunnel', async () => {
      const { spawn } = await import('node:child_process');

      // Mock cloudflared output with tunnel URL
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => {
            handler(Buffer.from(`INF Requesting new quick Tunnel on trycloudflare.com...\n`));
            handler(Buffer.from(`INF +--------------------------------------------------------------------------------------------+\n`));
            handler(Buffer.from(`INF |  Your quick Tunnel has been created! Visit it at (it may take some time to be reachable):  |\n`));
            handler(Buffer.from(`INF |  ${MOCK_TUNNEL_URL}                                                                |\n`));
            handler(Buffer.from(`INF +--------------------------------------------------------------------------------------------+\n`));
          }, 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();

      expect(spawn).toHaveBeenCalledWith(
        'cloudflared',
        expect.arrayContaining(['tunnel', '--url', 'http://localhost:3456']),
        expect.any(Object)
      );
    });

    it('should return tunnel info after starting', async () => {
      // Mock cloudflared output with tunnel URL
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => {
            handler(Buffer.from(`INF |  ${MOCK_TUNNEL_URL}  |\n`));
          }, 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      const info = await manager.start();

      expect(info.url).toBe(MOCK_TUNNEL_URL);
    });

    it('should emit "ready" event when tunnel is established', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => {
            handler(Buffer.from(`INF |  ${MOCK_TUNNEL_URL}  |\n`));
          }, 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      const readyHandler = vi.fn();
      manager.on('ready', readyHandler);

      await manager.start();

      expect(readyHandler).toHaveBeenCalledWith(
        expect.objectContaining({
          url: MOCK_TUNNEL_URL,
        })
      );
    });

    it('should throw error if already running', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => {
            handler(Buffer.from(`INF |  ${MOCK_TUNNEL_URL}  |\n`));
          }, 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();

      await expect(manager.start()).rejects.toThrow('Tunnel already running');
    });

    it('should throw error if cloudflared is not installed', async () => {
      const { spawn } = await import('node:child_process');

      let errorHandler: ((err: Error) => void) | null = null;

      vi.mocked(spawn).mockImplementation(() => {
        const proc = {
          ...mockProcess,
          on: vi.fn((event: string, handler: Function) => {
            if (event === 'error') {
              errorHandler = handler as (err: Error) => void;
            }
            return proc;
          }),
        };
        setTimeout(() => {
          if (errorHandler) {
            const error = new Error('ENOENT') as NodeJS.ErrnoException;
            error.code = 'ENOENT';
            errorHandler(error);
          }
        }, 50);
        return proc as unknown as ChildProcess;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);

      // Attach error listener to prevent uncaught error event
      manager.on('error', () => {});

      await expect(manager.start()).rejects.toThrow(
        expect.objectContaining({
          message: expect.stringContaining('cloudflared'),
        })
      );
    });

    it('should timeout if URL is not received', async () => {
      // Mock cloudflared that never outputs a URL
      mockStderr.on.mockImplementation(() => mockStderr);

      const manager = new TunnelManager({ ...DEFAULT_TUNNEL_OPTIONS, timeout: 100 });

      // Attach error listener to prevent uncaught error event
      manager.on('error', () => {});

      await expect(manager.start()).rejects.toThrow(/timeout/i);
    }, 10000);

    it('should pass --no-autoupdate flag to cloudflared', async () => {
      const { spawn } = await import('node:child_process');
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();

      expect(spawn).toHaveBeenCalledWith(
        'cloudflared',
        expect.arrayContaining(['--no-autoupdate']),
        expect.any(Object)
      );
    });

    it('should pass --loglevel debug when verbose', async () => {
      const { spawn } = await import('node:child_process');
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager({ ...DEFAULT_TUNNEL_OPTIONS, verbose: true });
      await manager.start();

      expect(spawn).toHaveBeenCalledWith(
        'cloudflared',
        expect.arrayContaining(['--loglevel', 'debug']),
        expect.any(Object)
      );
    });
  });

  describe('stop', () => {
    it('should stop the running tunnel', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();
      await manager.stop();

      expect(manager.isRunning()).toBe(false);
    });

    it('should kill the cloudflared process', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();
      await manager.stop();

      expect(mockProcess.kill).toHaveBeenCalledWith('SIGTERM');
    });

    it('should emit "stopped" event', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      const stoppedHandler = vi.fn();
      manager.on('stopped', stoppedHandler);

      await manager.start();
      await manager.stop();

      expect(stoppedHandler).toHaveBeenCalled();
    });

    it('should handle stop when not running', async () => {
      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await expect(manager.stop()).resolves.not.toThrow();
    });

    it('should force kill after timeout', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager({ ...DEFAULT_TUNNEL_OPTIONS, stopTimeout: 100 });

      // Attach error listener to prevent uncaught error event (for exit code 137)
      manager.on('error', () => {});

      await manager.start();

      // Simulate a stuck process that doesn't respond to SIGTERM
      simulateStuckProcess();

      await manager.stop();

      // Should have called kill twice - once with SIGTERM, once with SIGKILL
      expect(mockProcess.kill).toHaveBeenCalledWith('SIGTERM');
      expect(mockProcess.kill).toHaveBeenCalledWith('SIGKILL');
    });
  });

  describe('status', () => {
    it('should return DISCONNECTED initially', () => {
      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);

      expect(manager.getStatus()).toBe('disconnected');
    });

    it('should return CONNECTING while starting', async () => {
      // Don't resolve the URL yet
      mockStderr.on.mockImplementation(() => mockStderr);

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);

      // Attach error listener to prevent uncaught error event (for timeout)
      manager.on('error', () => {});

      const startPromise = manager.start();

      // Wait for checkPortListening to complete and status to change
      await new Promise((resolve) => setTimeout(resolve, 30));

      // Check status during startup
      expect(manager.getStatus()).toBe('connecting');

      // Clean up
      manager.stop();
      try {
        await startPromise;
      } catch {
        // Expected timeout
      }
    });

    it('should return CONNECTED after successful start', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();

      expect(manager.getStatus()).toBe('connected');
    });

    it('should return DISCONNECTED after stop', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();
      await manager.stop();

      expect(manager.getStatus()).toBe('disconnected');
    });

    it('should return ERROR on failure', async () => {
      const { spawn } = await import('node:child_process');

      let errorHandler: ((err: Error) => void) | null = null;

      vi.mocked(spawn).mockImplementation(() => {
        const proc = {
          ...mockProcess,
          on: vi.fn((event: string, handler: Function) => {
            if (event === 'error') {
              errorHandler = handler as (err: Error) => void;
            }
            return proc;
          }),
        };
        setTimeout(() => {
          if (errorHandler) {
            errorHandler(new Error('Connection failed'));
          }
        }, 50);
        return proc as unknown as ChildProcess;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);

      // Attach error listener to prevent uncaught error event
      manager.on('error', () => {});

      try {
        await manager.start();
      } catch {
        // Expected
      }

      expect(manager.getStatus()).toBe('error');
    });
  });

  describe('getTunnelInfo', () => {
    it('should return null before start', () => {
      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);

      expect(manager.getTunnelInfo()).toBeNull();
    });

    it('should return tunnel info after start', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();

      const info = manager.getTunnelInfo();
      expect(info).not.toBeNull();
      expect(info?.url).toBe(MOCK_TUNNEL_URL);
    });

    it('should include metrics URL if available', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => {
            handler(Buffer.from(`INF Starting metrics server on ${MOCK_METRICS_URL}\n`));
            handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`));
          }, 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();

      const info = manager.getTunnelInfo();
      expect(info?.metricsUrl).toBe(MOCK_METRICS_URL);
    });

    it('should include local URL', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();

      const info = manager.getTunnelInfo();
      expect(info?.localUrl).toBe('http://localhost:3456');
    });

    it('should return null after stop', async () => {
      mockStderr.on.mockImplementation((event: string, handler: Function) => {
        if (event === 'data') {
          setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
        }
        return mockStderr;
      });

      const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
      await manager.start();
      await manager.stop();

      expect(manager.getTunnelInfo()).toBeNull();
    });
  });
});

// ============================================================================
// URL Parsing Tests
// ============================================================================

describe('CLI Tunnel - URL Parsing', () => {
  let parseTunnelUrl: (output: string) => string | null;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/tunnel.js');
    parseTunnelUrl = module.parseTunnelUrl;
  });

  it('should parse standard trycloudflare.com URL', () => {
    const output = 'https://test-abc123.trycloudflare.com';
    expect(parseTunnelUrl(output)).toBe('https://test-abc123.trycloudflare.com');
  });

  it('should parse URL from cloudflared box output', () => {
    const output = `
+--------------------------------------------------------------------------------------------+
|  Your quick Tunnel has been created! Visit it at (it may take some time to be reachable):  |
|  https://msg-sufficiently-graphical-pearl.trycloudflare.com                                |
+--------------------------------------------------------------------------------------------+
`;
    expect(parseTunnelUrl(output)).toBe('https://msg-sufficiently-graphical-pearl.trycloudflare.com');
  });

  it('should handle URL with trailing whitespace', () => {
    const output = '|  https://test-tunnel.trycloudflare.com   |';
    expect(parseTunnelUrl(output)).toBe('https://test-tunnel.trycloudflare.com');
  });

  it('should return null for invalid output', () => {
    const output = 'some random log output without a URL';
    expect(parseTunnelUrl(output)).toBeNull();
  });

  it('should return null for empty output', () => {
    expect(parseTunnelUrl('')).toBeNull();
  });

  it('should handle multiple URLs and return first trycloudflare URL', () => {
    const output = `
http://127.0.0.1:60123/metrics
https://my-tunnel.trycloudflare.com
https://other-tunnel.trycloudflare.com
`;
    expect(parseTunnelUrl(output)).toBe('https://my-tunnel.trycloudflare.com');
  });

  it('should handle URL with complex subdomain', () => {
    const output = 'https://very-long-subdomain-with-numbers-123.trycloudflare.com';
    expect(parseTunnelUrl(output)).toBe('https://very-long-subdomain-with-numbers-123.trycloudflare.com');
  });

  it('should parse URL from INF log prefix', () => {
    const output = 'INF | https://my-tunnel.trycloudflare.com |';
    expect(parseTunnelUrl(output)).toBe('https://my-tunnel.trycloudflare.com');
  });
});

// ============================================================================
// Cloudflared Detection Tests
// ============================================================================

describe('CLI Tunnel - Cloudflared Detection', () => {
  let isCloudflaredInstalled: () => Promise<boolean>;
  let getCloudflaredVersion: () => Promise<string | null>;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/tunnel.js');
    isCloudflaredInstalled = module.isCloudflaredInstalled;
    getCloudflaredVersion = module.getCloudflaredVersion;
    vi.clearAllMocks();
    resetProcessHandlers();

    // Reset mock implementations
    mockStdout.on.mockImplementation(() => mockStdout);
    mockStderr.on.mockImplementation(() => mockStderr);
    mockProcess.on.mockImplementation((event: string, handler: Function) => {
      const handlers = processHandlers.get(event) || [];
      handlers.push(handler);
      processHandlers.set(event, handlers);
      return mockProcess;
    });
  });

  describe('isCloudflaredInstalled', () => {
    it('should return true when cloudflared is available', async () => {
      const { spawn } = await import('node:child_process');

      let closeHandler: ((code: number) => void) | null = null;

      vi.mocked(spawn).mockImplementation(() => {
        const proc = {
          ...mockProcess,
          on: vi.fn((event: string, handler: Function) => {
            if (event === 'close') {
              closeHandler = handler as (code: number) => void;
            }
            return proc;
          }),
        };
        setTimeout(() => {
          if (closeHandler) closeHandler(0);
        }, 10);
        return proc as unknown as ChildProcess;
      });

      const result = await isCloudflaredInstalled();
      expect(result).toBe(true);
    });

    it('should return false when cloudflared is not found', async () => {
      const { spawn } = await import('node:child_process');

      let errorHandler: ((err: Error) => void) | null = null;

      vi.mocked(spawn).mockImplementation(() => {
        const proc = {
          ...mockProcess,
          on: vi.fn((event: string, handler: Function) => {
            if (event === 'error') {
              errorHandler = handler as (err: Error) => void;
            }
            return proc;
          }),
        };
        setTimeout(() => {
          if (errorHandler) errorHandler(new Error('ENOENT'));
        }, 10);
        return proc as unknown as ChildProcess;
      });

      const result = await isCloudflaredInstalled();
      expect(result).toBe(false);
    });

    it('should return false when cloudflared exits with non-zero', async () => {
      const { spawn } = await import('node:child_process');

      let closeHandler: ((code: number) => void) | null = null;

      vi.mocked(spawn).mockImplementation(() => {
        const proc = {
          ...mockProcess,
          on: vi.fn((event: string, handler: Function) => {
            if (event === 'close') {
              closeHandler = handler as (code: number) => void;
            }
            return proc;
          }),
        };
        setTimeout(() => {
          if (closeHandler) closeHandler(1);
        }, 10);
        return proc as unknown as ChildProcess;
      });

      const result = await isCloudflaredInstalled();
      expect(result).toBe(false);
    });

    it('should timeout and return false', async () => {
      const { spawn } = await import('node:child_process');
      vi.mocked(spawn).mockImplementation(() => {
        // Never resolves - process doesn't emit any events
        return mockProcess as unknown as ChildProcess;
      });

      const result = await isCloudflaredInstalled();
      expect(result).toBe(false);
    }, 10000);
  });

  describe('getCloudflaredVersion', () => {
    it('should return version string', async () => {
      const { spawn } = await import('node:child_process');

      let closeHandler: ((code: number) => void) | null = null;

      vi.mocked(spawn).mockImplementation(() => {
        const proc = {
          ...mockProcess,
          on: vi.fn((event: string, handler: Function) => {
            if (event === 'close') {
              closeHandler = handler as (code: number) => void;
            }
            return proc;
          }),
        };
        mockStdout.on.mockImplementation((event: string, handler: Function) => {
          if (event === 'data') {
            handler(Buffer.from('cloudflared version 2024.1.0 (built 2024-01-15)\n'));
          }
          return mockStdout;
        });
        setTimeout(() => {
          if (closeHandler) closeHandler(0);
        }, 10);
        return proc as unknown as ChildProcess;
      });

      const version = await getCloudflaredVersion();
      expect(version).toBe('2024.1.0');
    });

    it('should return null when cloudflared not found', async () => {
      const { spawn } = await import('node:child_process');

      let errorHandler: ((err: Error) => void) | null = null;

      vi.mocked(spawn).mockImplementation(() => {
        const proc = {
          ...mockProcess,
          on: vi.fn((event: string, handler: Function) => {
            if (event === 'error') {
              errorHandler = handler as (err: Error) => void;
            }
            return proc;
          }),
        };
        setTimeout(() => {
          if (errorHandler) errorHandler(new Error('ENOENT'));
        }, 10);
        return proc as unknown as ChildProcess;
      });

      const version = await getCloudflaredVersion();
      expect(version).toBeNull();
    });

    it('should handle version without build info', async () => {
      const { spawn } = await import('node:child_process');

      let closeHandler: ((code: number) => void) | null = null;

      vi.mocked(spawn).mockImplementation(() => {
        const proc = {
          ...mockProcess,
          on: vi.fn((event: string, handler: Function) => {
            if (event === 'close') {
              closeHandler = handler as (code: number) => void;
            }
            return proc;
          }),
        };
        mockStdout.on.mockImplementation((event: string, handler: Function) => {
          if (event === 'data') {
            handler(Buffer.from('cloudflared version 2024.2.1\n'));
          }
          return mockStdout;
        });
        setTimeout(() => {
          if (closeHandler) closeHandler(0);
        }, 10);
        return proc as unknown as ChildProcess;
      });

      const version = await getCloudflaredVersion();
      expect(version).toBe('2024.2.1');
    });
  });
});

// ============================================================================
// Port Checking Tests
// ============================================================================

describe('CLI Tunnel - Port Checking', () => {
  let TunnelManager: typeof import('../../../src/cli/tunnel.js').TunnelManager;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/tunnel.js');
    TunnelManager = module.TunnelManager;
    vi.clearAllMocks();
    resetProcessHandlers();

    // Reset mock implementations
    const { spawn } = await import('node:child_process');
    vi.mocked(spawn).mockImplementation(() => mockProcess as unknown as ChildProcess);
    mockStderr.on.mockImplementation(() => mockStderr);
    mockProcess.on.mockImplementation((event: string, handler: Function) => {
      const handlers = processHandlers.get(event) || [];
      handlers.push(handler);
      processHandlers.set(event, handlers);
      return mockProcess;
    });
  });

  it('should warn if no service is listening on target port', async () => {
    const { connect } = await import('node:net');
    vi.mocked(connect).mockImplementation(() => {
      const mockSocket = {
        on: vi.fn((event, handler) => {
          if (event === 'error') {
            setTimeout(() => handler(new Error('ECONNREFUSED')), 10);
          }
          return mockSocket;
        }),
        destroy: vi.fn(),
        setTimeout: vi.fn(),
      };
      return mockSocket as any;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const warningHandler = vi.fn();
    manager.on('warning', warningHandler);

    // Start should still proceed but emit a warning
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    await manager.start();

    expect(warningHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringContaining('No service detected'),
      })
    );
  });

  it('should not warn if service is listening on target port', async () => {
    const { connect } = await import('node:net');
    vi.mocked(connect).mockImplementation(() => {
      const mockSocket = {
        on: vi.fn((event, handler) => {
          if (event === 'connect') {
            setTimeout(() => handler(), 10);
          }
          return mockSocket;
        }),
        destroy: vi.fn(),
        setTimeout: vi.fn(),
      };
      return mockSocket as any;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const warningHandler = vi.fn();
    manager.on('warning', warningHandler);

    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    await manager.start();

    expect(warningHandler).not.toHaveBeenCalled();
  });

  it('should expose checkPortListening method', async () => {
    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    expect(typeof manager.checkPortListening).toBe('function');
  });
});

// ============================================================================
// Cleanup and Signal Handling Tests
// ============================================================================

describe('CLI Tunnel - Cleanup and Signal Handling', () => {
  let TunnelManager: typeof import('../../../src/cli/tunnel.js').TunnelManager;
  let processOnSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/tunnel.js');
    TunnelManager = module.TunnelManager;
    processOnSpy = vi.spyOn(process, 'on').mockImplementation(() => process);
    vi.clearAllMocks();
    resetProcessHandlers();

    // Reset mock implementations
    const { spawn } = await import('node:child_process');
    vi.mocked(spawn).mockImplementation(() => mockProcess as unknown as ChildProcess);
    mockStderr.on.mockImplementation(() => mockStderr);
    mockProcess.on.mockImplementation((event: string, handler: Function) => {
      const handlers = processHandlers.get(event) || [];
      handlers.push(handler);
      processHandlers.set(event, handlers);
      return mockProcess;
    });
  });

  afterEach(() => {
    processOnSpy.mockRestore();
  });

  it('should register SIGINT handler', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    await manager.start();

    expect(processOnSpy).toHaveBeenCalledWith('SIGINT', expect.any(Function));
  });

  it('should register SIGTERM handler', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    await manager.start();

    expect(processOnSpy).toHaveBeenCalledWith('SIGTERM', expect.any(Function));
  });

  it('should cleanup on SIGINT', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    let sigintHandler: () => void;
    processOnSpy.mockImplementation((event: string, handler: () => void) => {
      if (event === 'SIGINT') {
        sigintHandler = handler;
      }
      return process;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    await manager.start();

    // Trigger SIGINT
    sigintHandler!();

    // Process should be killed
    expect(mockProcess.kill).toHaveBeenCalled();
  });

  it('should cleanup on SIGTERM', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    let sigtermHandler: () => void;
    processOnSpy.mockImplementation((event: string, handler: () => void) => {
      if (event === 'SIGTERM') {
        sigtermHandler = handler;
      }
      return process;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    await manager.start();

    // Trigger SIGTERM
    sigtermHandler!();

    // Process should be killed
    expect(mockProcess.kill).toHaveBeenCalled();
  });

  it('should emit "cleanup" event on shutdown', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    let sigintHandler: () => void;
    processOnSpy.mockImplementation((event: string, handler: () => void) => {
      if (event === 'SIGINT') {
        sigintHandler = handler;
      }
      return process;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const cleanupHandler = vi.fn();
    manager.on('cleanup', cleanupHandler);

    await manager.start();

    // Trigger SIGINT
    sigintHandler!();

    expect(cleanupHandler).toHaveBeenCalled();
  });

  it('should remove signal handlers on stop', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const processOffSpy = vi.spyOn(process, 'off').mockImplementation(() => process);

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    await manager.start();
    await manager.stop();

    expect(processOffSpy).toHaveBeenCalledWith('SIGINT', expect.any(Function));
    expect(processOffSpy).toHaveBeenCalledWith('SIGTERM', expect.any(Function));

    processOffSpy.mockRestore();
  });

  it('should handle process close event', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    await manager.start();

    // Simulate process closing
    const closeHandler = mockProcess.on.mock.calls.find(
      (call: [string, Function]) => call[0] === 'close'
    )?.[1];
    closeHandler?.(0);

    expect(manager.getStatus()).toBe('disconnected');
  });

  it('should handle unexpected process exit', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const errorHandler = vi.fn();
    manager.on('error', errorHandler);

    await manager.start();

    // Simulate unexpected exit
    const closeHandler = mockProcess.on.mock.calls.find(
      (call: [string, Function]) => call[0] === 'close'
    )?.[1];
    closeHandler?.(1);

    expect(errorHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringContaining('exit'),
      })
    );
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('CLI Tunnel - Error Handling', () => {
  let TunnelManager: typeof import('../../../src/cli/tunnel.js').TunnelManager;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/tunnel.js');
    TunnelManager = module.TunnelManager;
    consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.clearAllMocks();
    resetProcessHandlers();

    // Reset mock implementations
    const { spawn } = await import('node:child_process');
    vi.mocked(spawn).mockImplementation(() => mockProcess as unknown as ChildProcess);
    mockStderr.on.mockImplementation(() => mockStderr);
    mockProcess.on.mockImplementation((event: string, handler: Function) => {
      const handlers = processHandlers.get(event) || [];
      handlers.push(handler);
      processHandlers.set(event, handlers);
      return mockProcess;
    });
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should handle cloudflared connection errors', async () => {
    const { spawn } = await import('node:child_process');

    let stderrDataHandler: ((data: Buffer) => void) | null = null;
    let closeHandler: ((code: number) => void) | null = null;

    vi.mocked(spawn).mockImplementation(() => {
      const proc = {
        ...mockProcess,
        on: vi.fn((event: string, handler: Function) => {
          if (event === 'close') {
            closeHandler = handler as (code: number) => void;
          }
          return proc;
        }),
        stderr: {
          ...mockStderr,
          on: vi.fn((event: string, handler: Function) => {
            if (event === 'data') {
              stderrDataHandler = handler as (data: Buffer) => void;
            }
            return mockStderr;
          }),
        },
      };
      setTimeout(() => {
        if (stderrDataHandler) stderrDataHandler(Buffer.from('ERR Failed to connect to localhost:3456\n'));
        if (closeHandler) closeHandler(1);
      }, 50);
      return proc as unknown as ChildProcess;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);

    // Attach error listener to prevent uncaught error event
    manager.on('error', () => {});

    await expect(manager.start()).rejects.toThrow();
  });

  it('should emit error event on cloudflared errors', async () => {
    // Track error handler directly
    let procErrorHandler: ((err: Error) => void) | null = null;

    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    mockProcess.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'error') {
        procErrorHandler = handler as (err: Error) => void;
      }
      return mockProcess;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const errorHandler = vi.fn();
    manager.on('error', errorHandler);

    await manager.start();

    // Simulate error after connected
    if (procErrorHandler) {
      procErrorHandler(new Error('Connection lost'));
    }

    expect(errorHandler).toHaveBeenCalled();
  });

  it('should provide helpful error message when cloudflared not installed', async () => {
    const { spawn } = await import('node:child_process');

    let errorHandler: ((err: Error) => void) | null = null;

    vi.mocked(spawn).mockImplementation(() => {
      const proc = {
        ...mockProcess,
        on: vi.fn((event: string, handler: Function) => {
          if (event === 'error') {
            errorHandler = handler as (err: Error) => void;
          }
          return proc;
        }),
      };
      setTimeout(() => {
        if (errorHandler) {
          const error = new Error('ENOENT') as NodeJS.ErrnoException;
          error.code = 'ENOENT';
          errorHandler(error);
        }
      }, 50);
      return proc as unknown as ChildProcess;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);

    // Attach error listener to prevent uncaught error event
    manager.on('error', () => {});

    try {
      await manager.start();
      expect.fail('Should have thrown');
    } catch (error) {
      expect((error as Error).message).toContain('cloudflared not found');
      expect((error as Error).message).toMatch(/install/i);
    }
  });

  it('should retry connection on transient errors', async () => {
    const { spawn } = await import('node:child_process');
    let attempts = 0;

    vi.mocked(spawn).mockImplementation(() => {
      let errorHandler: ((err: Error) => void) | null = null;

      const proc = {
        ...mockProcess,
        on: vi.fn((event: string, handler: Function) => {
          if (event === 'error') {
            errorHandler = handler as (err: Error) => void;
          }
          return proc;
        }),
      };
      attempts++;

      if (attempts < 3) {
        setTimeout(() => {
          if (errorHandler) errorHandler(new Error('Transient error'));
        }, 50);
      } else {
        mockStderr.on.mockImplementation((event: string, handler: Function) => {
          if (event === 'data') {
            setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
          }
          return mockStderr;
        });
      }

      return proc as unknown as ChildProcess;
    });

    const manager = new TunnelManager({ ...DEFAULT_TUNNEL_OPTIONS, retries: 3 });

    // Attach error listener to prevent uncaught error event
    manager.on('error', () => {});

    await manager.start();

    expect(attempts).toBe(3);
  });

  it('should respect retry limit', async () => {
    const { spawn } = await import('node:child_process');
    let attempts = 0;

    vi.mocked(spawn).mockImplementation(() => {
      let errorHandler: ((err: Error) => void) | null = null;

      const proc = {
        ...mockProcess,
        on: vi.fn((event: string, handler: Function) => {
          if (event === 'error') {
            errorHandler = handler as (err: Error) => void;
          }
          return proc;
        }),
      };
      attempts++;
      setTimeout(() => {
        if (errorHandler) errorHandler(new Error('Persistent error'));
      }, 50);
      return proc as unknown as ChildProcess;
    });

    const manager = new TunnelManager({ ...DEFAULT_TUNNEL_OPTIONS, retries: 2 });

    // Attach error listener to prevent uncaught error event
    manager.on('error', () => {});

    await expect(manager.start()).rejects.toThrow();
    expect(attempts).toBe(2);
  });
});

// ============================================================================
// Verbose Logging Tests
// ============================================================================

describe('CLI Tunnel - Verbose Logging', () => {
  let TunnelManager: typeof import('../../../src/cli/tunnel.js').TunnelManager;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/tunnel.js');
    TunnelManager = module.TunnelManager;
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.clearAllMocks();
    resetProcessHandlers();

    // Reset mock implementations
    const { spawn } = await import('node:child_process');
    vi.mocked(spawn).mockImplementation(() => mockProcess as unknown as ChildProcess);
    mockStderr.on.mockImplementation(() => mockStderr);
    mockProcess.on.mockImplementation((event: string, handler: Function) => {
      const handlers = processHandlers.get(event) || [];
      handlers.push(handler);
      processHandlers.set(event, handlers);
      return mockProcess;
    });
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should log cloudflared output when verbose', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => {
          handler(Buffer.from('INF Starting tunnel...\n'));
          handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`));
        }, 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager({ ...DEFAULT_TUNNEL_OPTIONS, verbose: true });
    await manager.start();

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('Starting tunnel')
    );
  });

  it('should not log cloudflared output when not verbose', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => {
          handler(Buffer.from('INF Starting tunnel...\n'));
          handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`));
        }, 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager({ ...DEFAULT_TUNNEL_OPTIONS, verbose: false });
    await manager.start();

    expect(consoleSpy).not.toHaveBeenCalledWith(
      expect.stringContaining('Starting tunnel')
    );
  });

  it('should emit "log" event for cloudflared output', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => {
          handler(Buffer.from('INF Some log message\n'));
          handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`));
        }, 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const logHandler = vi.fn();
    manager.on('log', logHandler);

    await manager.start();

    expect(logHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringContaining('Some log message'),
      })
    );
  });
});

// ============================================================================
// CLI Integration Tests
// ============================================================================

describe('CLI Tunnel - CLI Integration', () => {
  let startTunnelCommand: typeof import('../../../src/cli/tunnel.js').startTunnelCommand;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/tunnel.js');
    startTunnelCommand = module.startTunnelCommand;
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.clearAllMocks();
    resetProcessHandlers();

    // Reset mock implementations
    const { spawn } = await import('node:child_process');

    // Spawn needs to handle both version checks (isCloudflaredInstalled) and tunnel creation
    vi.mocked(spawn).mockImplementation((command, args) => {
      // Track handlers locally for this process instance
      const localHandlers: Map<string, Function[]> = new Map();

      const proc = {
        ...mockProcess,
        on: vi.fn((event: string, handler: Function) => {
          const handlers = localHandlers.get(event) || [];
          handlers.push(handler);
          localHandlers.set(event, handlers);

          // Also track in global processHandlers for tunnel tests
          const globalHandlers = processHandlers.get(event) || [];
          globalHandlers.push(handler);
          processHandlers.set(event, globalHandlers);

          return proc;
        }),
      };

      // For version check, emit close after handlers are registered
      if (args && Array.isArray(args) && args.includes('--version')) {
        // Use setImmediate to let the caller register handlers first, then setTimeout
        setImmediate(() => {
          setTimeout(() => {
            const closeHandlers = localHandlers.get('close') || [];
            closeHandlers.forEach(h => h(0));
          }, 5);
        });
      }

      return proc as unknown as ChildProcess;
    });

    // Don't override mockStderr.on here - let individual tests set it up
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should print startup banner', async () => {
    // The startTunnelCommand will print the banner immediately before waiting for tunnel
    // We need to let it start but not await the full promise (which waits for tunnel ready)
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    // Start but don't await - just check the banner is printed
    startTunnelCommand(DEFAULT_TUNNEL_OPTIONS).catch(() => {});

    await new Promise((resolve) => setTimeout(resolve, 30));

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('MongoLake Tunnel')
    );
  });

  it('should print tunnel URL when ready', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    // Start but don't await the full promise
    startTunnelCommand(DEFAULT_TUNNEL_OPTIONS).catch(() => {});

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining(MOCK_TUNNEL_URL)
    );
  });

  it('should print installation instructions when cloudflared not found', async () => {
    const { spawn } = await import('node:child_process');
    vi.mocked(spawn).mockImplementation(() => {
      const proc = { ...mockProcess };
      setTimeout(() => {
        const error = new Error('ENOENT') as NodeJS.ErrnoException;
        error.code = 'ENOENT';
        proc.on.mock.calls.find((call: [string, Function]) => call[0] === 'error')?.[1](error);
      }, 10);
      return proc as unknown as ChildProcess;
    });

    try {
      await startTunnelCommand(DEFAULT_TUNNEL_OPTIONS);
    } catch {
      // Expected
    }

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('cloudflared not found')
    );
    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('brew install')
    );
  });

  it('should print example curl command', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    // Start but don't await the full promise
    startTunnelCommand(DEFAULT_TUNNEL_OPTIONS).catch(() => {});

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringMatching(/curl.*trycloudflare\.com/i)
    );
  });

  it('should print Ctrl+C instruction', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    // Start but don't await the full promise
    startTunnelCommand(DEFAULT_TUNNEL_OPTIONS).catch(() => {});

    await new Promise((resolve) => setTimeout(resolve, 200));

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('Ctrl+C')
    );
  });
});

// ============================================================================
// Event Emitter Tests
// ============================================================================

describe('CLI Tunnel - Event Emitter', () => {
  let TunnelManager: typeof import('../../../src/cli/tunnel.js').TunnelManager;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/tunnel.js');
    TunnelManager = module.TunnelManager;
    vi.clearAllMocks();
    resetProcessHandlers();

    // Reset mock implementations
    const { spawn } = await import('node:child_process');
    vi.mocked(spawn).mockImplementation(() => mockProcess as unknown as ChildProcess);
    mockStderr.on.mockImplementation(() => mockStderr);
    mockProcess.on.mockImplementation((event: string, handler: Function) => {
      const handlers = processHandlers.get(event) || [];
      handlers.push(handler);
      processHandlers.set(event, handlers);
      return mockProcess;
    });
  });

  it('should emit "connecting" event when starting', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 100);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const connectingHandler = vi.fn();
    manager.on('connecting', connectingHandler);

    await manager.start();

    expect(connectingHandler).toHaveBeenCalled();
  });

  it('should emit "ready" event with tunnel info', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const readyHandler = vi.fn();
    manager.on('ready', readyHandler);

    await manager.start();

    expect(readyHandler).toHaveBeenCalledWith({
      url: MOCK_TUNNEL_URL,
      localUrl: 'http://localhost:3456',
    });
  });

  it('should emit "stopped" event on stop', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const stoppedHandler = vi.fn();
    manager.on('stopped', stoppedHandler);

    await manager.start();
    await manager.stop();

    expect(stoppedHandler).toHaveBeenCalled();
  });

  it('should emit "error" event on errors', async () => {
    const { spawn } = await import('node:child_process');
    vi.mocked(spawn).mockImplementation(() => {
      const proc = { ...mockProcess };
      setTimeout(() => {
        proc.on.mock.calls.find((call: [string, Function]) => call[0] === 'error')?.[1](
          new Error('Test error')
        );
      }, 10);
      return proc as unknown as ChildProcess;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const errorHandler = vi.fn();
    manager.on('error', errorHandler);

    try {
      await manager.start();
    } catch {
      // Expected
    }

    expect(errorHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        message: expect.stringContaining('Test error'),
      })
    );
  });

  it('should support removeListener', async () => {
    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const handler = vi.fn();

    manager.on('ready', handler);
    manager.removeListener('ready', handler);

    mockStderr.on.mockImplementation((event: string, h: Function) => {
      if (event === 'data') {
        setTimeout(() => h(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    await manager.start();

    expect(handler).not.toHaveBeenCalled();
  });

  it('should support once listeners', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const readyHandler = vi.fn();
    manager.once('ready', readyHandler);

    await manager.start();
    await manager.stop();
    await manager.start();

    expect(readyHandler).toHaveBeenCalledTimes(1);
  });
});

// ============================================================================
// Statistics Tests
// ============================================================================

describe('CLI Tunnel - Statistics', () => {
  let TunnelManager: typeof import('../../../src/cli/tunnel.js').TunnelManager;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/tunnel.js');
    TunnelManager = module.TunnelManager;
    vi.clearAllMocks();
    resetProcessHandlers();

    // Reset mock implementations
    const { spawn } = await import('node:child_process');
    vi.mocked(spawn).mockImplementation(() => mockProcess as unknown as ChildProcess);
    mockStderr.on.mockImplementation(() => mockStderr);
    mockProcess.on.mockImplementation((event: string, handler: Function) => {
      const handlers = processHandlers.get(event) || [];
      handlers.push(handler);
      processHandlers.set(event, handlers);
      return mockProcess;
    });
  });

  it('should track uptime', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    await manager.start();

    // Wait a bit
    await new Promise((resolve) => setTimeout(resolve, 100));

    const stats = manager.getStats();
    expect(stats.uptime).toBeGreaterThanOrEqual(100);
  });

  it('should track start time', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    const beforeStart = Date.now();
    await manager.start();

    const stats = manager.getStats();
    expect(stats.startTime).toBeDefined();
    expect(stats.startTime!.getTime()).toBeGreaterThanOrEqual(beforeStart);
  });

  it('should track reconnect count', async () => {
    // Note: reconnectCount tracks reconnects without stop() being called
    // (e.g., auto-reconnects). Manual stop()/start() cycles reset startTime
    // so they don't count as reconnects.
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);

    // First start - not a reconnect
    await manager.start();

    // After stop(), startTime is reset to null, so next start won't count as reconnect
    await manager.stop();

    // Verify reconnectCount is 0 for manual stop/start cycles
    const stats = manager.getStats();
    expect(stats.reconnectCount).toBe(0);
  });

  it('should reset stats on stop', async () => {
    mockStderr.on.mockImplementation((event: string, handler: Function) => {
      if (event === 'data') {
        setTimeout(() => handler(Buffer.from(`${MOCK_TUNNEL_URL}\n`)), 50);
      }
      return mockStderr;
    });

    const manager = new TunnelManager(DEFAULT_TUNNEL_OPTIONS);
    await manager.start();
    await manager.stop();

    const stats = manager.getStats();
    expect(stats.uptime).toBe(0);
    expect(stats.startTime).toBeNull();
  });
});
