/**
 * Tests for MongoLake Dev Command (Wrangler/Miniflare-based)
 *
 * Tests the local development server functionality including:
 * - Wrangler/Miniflare integration for Workers simulation
 * - Hot reloading support
 * - Local R2 bucket configuration
 * - Development server lifecycle management
 *
 * RED PHASE: These tests define the expected behavior for the
 * Wrangler/Miniflare-based dev server that needs to be implemented.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { ChildProcess } from 'node:child_process';

// ============================================================================
// Mock Setup
// ============================================================================

// Mock child_process for spawning wrangler
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

const mockProcess = {
  on: vi.fn().mockReturnThis(),
  once: vi.fn().mockReturnThis(),
  stdout: mockStdout,
  stderr: mockStderr,
  stdin: { write: vi.fn(), end: vi.fn() },
  kill: vi.fn().mockReturnValue(true),
  killed: false,
  pid: 12345,
  exitCode: null,
  signalCode: null,
};

vi.mock('node:child_process', () => ({
  spawn: vi.fn(() => mockProcess),
  execSync: vi.fn(),
}));

// Mock fs for config file operations
vi.mock('node:fs', async () => {
  const actual = await vi.importActual('node:fs');
  return {
    ...actual,
    existsSync: vi.fn(),
    readFileSync: vi.fn(),
    writeFileSync: vi.fn(),
    mkdirSync: vi.fn(),
    rmSync: vi.fn(),
    watchFile: vi.fn(),
    unwatchFile: vi.fn(),
    readdirSync: vi.fn(() => []),
    statSync: vi.fn(() => ({ size: 0, mtime: new Date() })),
  };
});

// Mock chokidar for file watching
vi.mock('chokidar', () => ({
  watch: vi.fn(() => ({
    on: vi.fn().mockReturnThis(),
    close: vi.fn().mockResolvedValue(undefined),
  })),
}));

// ============================================================================
// Test Fixtures
// ============================================================================

const DEFAULT_DEV_OPTIONS = {
  port: 3456,
  host: 'localhost',
  path: '.mongolake',
  verbose: false,
  watch: true,
};

const mockWranglerConfig = {
  name: 'mongolake-dev',
  main: 'src/worker/index.ts',
  compatibility_date: '2024-01-01',
  r2_buckets: [
    {
      binding: 'BUCKET',
      bucket_name: 'mongolake-local',
    },
  ],
  durable_objects: {
    bindings: [
      {
        name: 'SHARD',
        class_name: 'ShardDO',
      },
    ],
  },
};

// ============================================================================
// Module Export Tests
// ============================================================================

describe('CLI Dev Command - Module Exports', () => {
  it('should export startDevServer function', async () => {
    const module = await import('../../../src/cli/dev.js');
    expect(typeof module.startDevServer).toBe('function');
  });

  it('should export DevServerOptions type (type check)', async () => {
    const module = await import('../../../src/cli/dev.js');
    expect(module.startDevServer).toBeDefined();
  });

  it('should export createWranglerConfig function', async () => {
    const module = await import('../../../src/cli/dev.js');
    expect(typeof module.createWranglerConfig).toBe('function');
  });

  it('should export DevServer class', async () => {
    const module = await import('../../../src/cli/dev.js');
    expect(module.DevServer).toBeDefined();
    expect(typeof module.DevServer).toBe('function');
  });

  it('should export HotReloader class', async () => {
    const module = await import('../../../src/cli/dev.js');
    expect(module.HotReloader).toBeDefined();
    expect(typeof module.HotReloader).toBe('function');
  });

  it('should export R2LocalBucket class', async () => {
    const module = await import('../../../src/cli/dev.js');
    expect(module.R2LocalBucket).toBeDefined();
    expect(typeof module.R2LocalBucket).toBe('function');
  });
});

// ============================================================================
// DevServer Class Tests
// ============================================================================

describe('CLI Dev Command - DevServer Class', () => {
  let DevServer: typeof import('../../../src/cli/dev.js').DevServer;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/dev.js');
    DevServer = module.DevServer;
    vi.clearAllMocks();
  });

  describe('constructor', () => {
    it('should create DevServer instance with options', () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);

      expect(server.port).toBe(3456);
      expect(server.host).toBe('localhost');
      expect(server.path).toBe('.mongolake');
    });

    it('should use default options when not provided', () => {
      const server = new DevServer({});

      expect(server.port).toBe(3456);
      expect(server.host).toBe('localhost');
      expect(server.path).toBe('.mongolake');
      expect(server.watch).toBe(true);
    });

    it('should validate port range', () => {
      expect(() => new DevServer({ port: 0 })).toThrow('Invalid port');
      expect(() => new DevServer({ port: 70000 })).toThrow('Invalid port');
    });
  });

  describe('start', () => {
    it('should start the Wrangler dev server', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await server.start();

      expect(server.isRunning()).toBe(true);
    });

    it('should create wrangler.toml configuration', async () => {
      const fs = await import('node:fs');
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await server.start();

      expect(fs.writeFileSync).toHaveBeenCalledWith(
        expect.stringContaining('wrangler.toml'),
        expect.any(String)
      );
    });

    it('should spawn wrangler dev process', async () => {
      const { spawn } = await import('node:child_process');
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await server.start();

      expect(spawn).toHaveBeenCalledWith(
        'npx',
        expect.arrayContaining(['wrangler', 'dev']),
        expect.any(Object)
      );
    });

    it('should configure local R2 bucket', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await server.start();

      const config = server.getWranglerConfig();
      expect(config.r2_buckets).toBeDefined();
      expect(config.r2_buckets[0].binding).toBe('BUCKET');
    });

    it('should configure durable objects', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await server.start();

      const config = server.getWranglerConfig();
      expect(config.durable_objects).toBeDefined();
      expect(config.durable_objects.bindings).toContainEqual(
        expect.objectContaining({ class_name: 'ShardDO' })
      );
    });

    it('should throw error if already running', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await server.start();

      await expect(server.start()).rejects.toThrow('Server already running');
    });

    it('should emit "ready" event when server is ready', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      const readyHandler = vi.fn();
      server.on('ready', readyHandler);

      await server.start();

      expect(readyHandler).toHaveBeenCalledWith(
        expect.objectContaining({
          url: expect.stringContaining('localhost:3456'),
        })
      );
    });
  });

  describe('stop', () => {
    it('should stop the running server', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await server.start();
      await server.stop();

      expect(server.isRunning()).toBe(false);
    });

    it('should kill the wrangler process', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await server.start();
      const process = server.getProcess();
      await server.stop();

      expect(process?.kill).toHaveBeenCalled();
    });

    it('should clean up temporary files', async () => {
      const fs = await import('node:fs');
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await server.start();
      await server.stop();

      expect(fs.rmSync).toHaveBeenCalledWith(
        expect.stringContaining('.wrangler'),
        expect.objectContaining({ recursive: true, force: true })
      );
    });

    it('should emit "stopped" event', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      const stoppedHandler = vi.fn();
      server.on('stopped', stoppedHandler);

      await server.start();
      await server.stop();

      expect(stoppedHandler).toHaveBeenCalled();
    });

    it('should handle stop when not running', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await expect(server.stop()).resolves.not.toThrow();
    });
  });

  describe('restart', () => {
    it('should restart the server', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      await server.start();
      await server.restart();

      expect(server.isRunning()).toBe(true);
    });

    it('should emit "restarted" event', async () => {
      const server = new DevServer(DEFAULT_DEV_OPTIONS);
      const restartedHandler = vi.fn();
      server.on('restarted', restartedHandler);

      await server.start();
      await server.restart();

      expect(restartedHandler).toHaveBeenCalled();
    });
  });
});

// ============================================================================
// Wrangler Configuration Tests
// ============================================================================

describe('CLI Dev Command - Wrangler Configuration', () => {
  let createWranglerConfig: typeof import('../../../src/cli/dev.js').createWranglerConfig;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/dev.js');
    createWranglerConfig = module.createWranglerConfig;
    vi.clearAllMocks();
  });

  describe('createWranglerConfig', () => {
    it('should generate valid wrangler configuration', () => {
      const config = createWranglerConfig({
        name: 'mongolake-dev',
        port: 3456,
        path: '.mongolake',
      });

      expect(config).toMatchObject({
        name: 'mongolake-dev',
        main: expect.stringContaining('worker'),
        compatibility_date: expect.any(String),
      });
    });

    it('should include R2 bucket binding', () => {
      const config = createWranglerConfig({
        name: 'mongolake-dev',
        port: 3456,
        path: '.mongolake',
      });

      expect(config.r2_buckets).toEqual([
        expect.objectContaining({
          binding: 'BUCKET',
          bucket_name: 'mongolake-local',
        }),
      ]);
    });

    it('should include local R2 persistence path', () => {
      const config = createWranglerConfig({
        name: 'mongolake-dev',
        port: 3456,
        path: '.mongolake',
      });

      expect(config.r2_buckets[0].preview_bucket_name).toBe('.mongolake/r2');
    });

    it('should include Durable Objects binding', () => {
      const config = createWranglerConfig({
        name: 'mongolake-dev',
        port: 3456,
        path: '.mongolake',
      });

      expect(config.durable_objects).toEqual({
        bindings: [
          expect.objectContaining({
            name: 'SHARD',
            class_name: 'ShardDO',
          }),
        ],
      });
    });

    it('should configure local DO persistence', () => {
      const config = createWranglerConfig({
        name: 'mongolake-dev',
        port: 3456,
        path: '.mongolake',
      });

      expect(config.miniflare).toEqual(
        expect.objectContaining({
          d1_persist: expect.stringContaining('.mongolake'),
          kv_persist: expect.stringContaining('.mongolake'),
          r2_persist: expect.stringContaining('.mongolake'),
        })
      );
    });

    it('should set dev server port', () => {
      const config = createWranglerConfig({
        name: 'mongolake-dev',
        port: 8080,
        path: '.mongolake',
      });

      expect(config.dev).toEqual(
        expect.objectContaining({
          port: 8080,
        })
      );
    });

    it('should set local mode for development', () => {
      const config = createWranglerConfig({
        name: 'mongolake-dev',
        port: 3456,
        path: '.mongolake',
      });

      expect(config.dev.local).toBe(true);
    });

    it('should include vars for local configuration', () => {
      const config = createWranglerConfig({
        name: 'mongolake-dev',
        port: 3456,
        path: '.mongolake',
        vars: {
          ENVIRONMENT: 'development',
          DEBUG: 'true',
        },
      });

      expect(config.vars).toEqual({
        ENVIRONMENT: 'development',
        DEBUG: 'true',
      });
    });

    it('should serialize to valid TOML', () => {
      const config = createWranglerConfig({
        name: 'mongolake-dev',
        port: 3456,
        path: '.mongolake',
      });

      const toml = config.toTOML();
      expect(toml).toContain('name = "mongolake-dev"');
      expect(toml).toContain('[[r2_buckets]]');
      expect(toml).toContain('[[durable_objects.bindings]]');
    });
  });
});

// ============================================================================
// Hot Reloading Tests
// ============================================================================

describe('CLI Dev Command - Hot Reloading', () => {
  let HotReloader: typeof import('../../../src/cli/dev.js').HotReloader;
  let chokidar: typeof import('chokidar');

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/dev.js');
    HotReloader = module.HotReloader;
    chokidar = await import('chokidar');
    vi.clearAllMocks();
  });

  describe('constructor', () => {
    it('should create HotReloader with watch paths', () => {
      const reloader = new HotReloader({
        paths: ['src/**/*.ts', 'wrangler.toml'],
        debounceMs: 100,
      });

      expect(reloader.paths).toEqual(['src/**/*.ts', 'wrangler.toml']);
      expect(reloader.debounceMs).toBe(100);
    });

    it('should use default debounce time', () => {
      const reloader = new HotReloader({
        paths: ['src/**/*.ts'],
      });

      expect(reloader.debounceMs).toBe(300);
    });
  });

  describe('start', () => {
    it('should start watching files', async () => {
      const reloader = new HotReloader({
        paths: ['src/**/*.ts'],
      });

      await reloader.start();

      expect(chokidar.watch).toHaveBeenCalledWith(
        ['src/**/*.ts'],
        expect.any(Object)
      );
    });

    it('should ignore node_modules and dist', async () => {
      const reloader = new HotReloader({
        paths: ['src/**/*.ts'],
      });

      await reloader.start();

      expect(chokidar.watch).toHaveBeenCalledWith(
        expect.any(Array),
        expect.objectContaining({
          ignored: expect.arrayContaining([
            '**/node_modules/**',
            '**/dist/**',
            '**/.git/**',
          ]),
        })
      );
    });

    it('should emit "change" event on file change', async () => {
      const mockWatcher = {
        on: vi.fn((event: string, handler: Function) => {
          if (event === 'change') {
            // Simulate file change
            setTimeout(() => handler('src/test.ts'), 0);
          }
          return mockWatcher;
        }),
        close: vi.fn(),
      };
      vi.mocked(chokidar.watch).mockReturnValue(mockWatcher as any);

      const reloader = new HotReloader({
        paths: ['src/**/*.ts'],
      });
      const changeHandler = vi.fn();
      reloader.on('change', changeHandler);

      await reloader.start();
      await new Promise((resolve) => setTimeout(resolve, 10));

      expect(changeHandler).toHaveBeenCalledWith(
        expect.objectContaining({
          path: 'src/test.ts',
        })
      );
    });

    it('should debounce rapid file changes', async () => {
      const mockWatcher = {
        on: vi.fn((event: string, handler: Function) => {
          if (event === 'change') {
            // Simulate rapid file changes
            setTimeout(() => handler('src/test1.ts'), 0);
            setTimeout(() => handler('src/test2.ts'), 5);
            setTimeout(() => handler('src/test3.ts'), 10);
          }
          return mockWatcher;
        }),
        close: vi.fn(),
      };
      vi.mocked(chokidar.watch).mockReturnValue(mockWatcher as any);

      const reloader = new HotReloader({
        paths: ['src/**/*.ts'],
        debounceMs: 50,
      });
      const reloadHandler = vi.fn();
      reloader.on('reload', reloadHandler);

      await reloader.start();
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Should only trigger one reload after debounce
      expect(reloadHandler).toHaveBeenCalledTimes(1);
    });

    it('should emit "reload" event with changed files', async () => {
      const mockWatcher = {
        on: vi.fn((event: string, handler: Function) => {
          if (event === 'change') {
            setTimeout(() => handler('src/test.ts'), 0);
          }
          return mockWatcher;
        }),
        close: vi.fn(),
      };
      vi.mocked(chokidar.watch).mockReturnValue(mockWatcher as any);

      const reloader = new HotReloader({
        paths: ['src/**/*.ts'],
        debounceMs: 10,
      });
      const reloadHandler = vi.fn();
      reloader.on('reload', reloadHandler);

      await reloader.start();
      await new Promise((resolve) => setTimeout(resolve, 50));

      expect(reloadHandler).toHaveBeenCalledWith(
        expect.objectContaining({
          files: expect.arrayContaining(['src/test.ts']),
        })
      );
    });
  });

  describe('stop', () => {
    it('should stop watching files', async () => {
      const mockWatcher = {
        on: vi.fn().mockReturnThis(),
        close: vi.fn().mockResolvedValue(undefined),
      };
      vi.mocked(chokidar.watch).mockReturnValue(mockWatcher as any);

      const reloader = new HotReloader({
        paths: ['src/**/*.ts'],
      });

      await reloader.start();
      await reloader.stop();

      expect(mockWatcher.close).toHaveBeenCalled();
    });
  });

  describe('add/remove paths', () => {
    it('should add watch paths dynamically', async () => {
      const mockWatcher = {
        on: vi.fn().mockReturnThis(),
        add: vi.fn(),
        close: vi.fn(),
      };
      vi.mocked(chokidar.watch).mockReturnValue(mockWatcher as any);

      const reloader = new HotReloader({
        paths: ['src/**/*.ts'],
      });

      await reloader.start();
      reloader.addPath('lib/**/*.ts');

      expect(mockWatcher.add).toHaveBeenCalledWith('lib/**/*.ts');
    });

    it('should remove watch paths dynamically', async () => {
      const mockWatcher = {
        on: vi.fn().mockReturnThis(),
        unwatch: vi.fn(),
        close: vi.fn(),
      };
      vi.mocked(chokidar.watch).mockReturnValue(mockWatcher as any);

      const reloader = new HotReloader({
        paths: ['src/**/*.ts', 'lib/**/*.ts'],
      });

      await reloader.start();
      reloader.removePath('lib/**/*.ts');

      expect(mockWatcher.unwatch).toHaveBeenCalledWith('lib/**/*.ts');
    });
  });
});

// ============================================================================
// Local R2 Bucket Tests
// ============================================================================

describe('CLI Dev Command - Local R2 Bucket', () => {
  let R2LocalBucket: typeof import('../../../src/cli/dev.js').R2LocalBucket;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/dev.js');
    R2LocalBucket = module.R2LocalBucket;
    vi.clearAllMocks();
  });

  describe('constructor', () => {
    it('should create R2LocalBucket with path', () => {
      const bucket = new R2LocalBucket({
        path: '.mongolake/r2',
        bucketName: 'mongolake-local',
      });

      expect(bucket.path).toBe('.mongolake/r2');
      expect(bucket.bucketName).toBe('mongolake-local');
    });
  });

  describe('initialize', () => {
    it('should create the bucket directory', async () => {
      const fs = await import('node:fs');
      vi.mocked(fs.existsSync).mockReturnValue(false);

      const bucket = new R2LocalBucket({
        path: '.mongolake/r2',
        bucketName: 'mongolake-local',
      });

      await bucket.initialize();

      expect(fs.mkdirSync).toHaveBeenCalledWith('.mongolake/r2', {
        recursive: true,
      });
    });

    it('should skip creation if directory exists', async () => {
      const fs = await import('node:fs');
      vi.mocked(fs.existsSync).mockReturnValue(true);

      const bucket = new R2LocalBucket({
        path: '.mongolake/r2',
        bucketName: 'mongolake-local',
      });

      await bucket.initialize();

      expect(fs.mkdirSync).not.toHaveBeenCalled();
    });
  });

  describe('put', () => {
    it('should write object to local filesystem', async () => {
      const fs = await import('node:fs');
      const bucket = new R2LocalBucket({
        path: '.mongolake/r2',
        bucketName: 'mongolake-local',
      });

      await bucket.put('test-key', Buffer.from('test content'));

      expect(fs.writeFileSync).toHaveBeenCalledWith(
        expect.stringContaining('test-key'),
        expect.any(Buffer)
      );
    });

    it('should create subdirectories for nested keys', async () => {
      const fs = await import('node:fs');
      vi.mocked(fs.existsSync).mockReturnValue(false);

      const bucket = new R2LocalBucket({
        path: '.mongolake/r2',
        bucketName: 'mongolake-local',
      });

      await bucket.put('db/collection/doc.json', Buffer.from('{}'));

      expect(fs.mkdirSync).toHaveBeenCalledWith(
        expect.stringContaining('db/collection'),
        { recursive: true }
      );
    });
  });

  describe('get', () => {
    it('should read object from local filesystem', async () => {
      const fs = await import('node:fs');
      vi.mocked(fs.existsSync).mockReturnValue(true);
      vi.mocked(fs.readFileSync).mockReturnValue(Buffer.from('test content'));

      const bucket = new R2LocalBucket({
        path: '.mongolake/r2',
        bucketName: 'mongolake-local',
      });

      const result = await bucket.get('test-key');

      expect(result).toEqual(Buffer.from('test content'));
    });

    it('should return null for non-existent object', async () => {
      const fs = await import('node:fs');
      vi.mocked(fs.existsSync).mockReturnValue(false);

      const bucket = new R2LocalBucket({
        path: '.mongolake/r2',
        bucketName: 'mongolake-local',
      });

      const result = await bucket.get('non-existent');

      expect(result).toBeNull();
    });
  });

  describe('delete', () => {
    it('should delete object from local filesystem', async () => {
      const fs = await import('node:fs');
      vi.mocked(fs.existsSync).mockReturnValue(true);

      const bucket = new R2LocalBucket({
        path: '.mongolake/r2',
        bucketName: 'mongolake-local',
      });

      await bucket.delete('test-key');

      expect(fs.rmSync).toHaveBeenCalledWith(
        expect.stringContaining('test-key'),
        expect.any(Object)
      );
    });
  });

  describe('list', () => {
    it('should list objects with prefix', async () => {
      const fs = await import('node:fs');
      vi.mocked(fs.existsSync).mockReturnValue(true);

      const bucket = new R2LocalBucket({
        path: '.mongolake/r2',
        bucketName: 'mongolake-local',
      });

      const result = await bucket.list({ prefix: 'db/' });

      expect(result).toEqual(
        expect.objectContaining({
          objects: expect.any(Array),
          truncated: expect.any(Boolean),
        })
      );
    });
  });

  describe('clear', () => {
    it('should clear all objects', async () => {
      const fs = await import('node:fs');
      const bucket = new R2LocalBucket({
        path: '.mongolake/r2',
        bucketName: 'mongolake-local',
      });

      await bucket.clear();

      expect(fs.rmSync).toHaveBeenCalledWith('.mongolake/r2', {
        recursive: true,
        force: true,
      });
    });
  });
});

// ============================================================================
// Miniflare Integration Tests
// ============================================================================

describe('CLI Dev Command - Miniflare Integration', () => {
  let DevServer: typeof import('../../../src/cli/dev.js').DevServer;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/dev.js');
    DevServer = module.DevServer;
    vi.clearAllMocks();
  });

  it('should use Miniflare for local Workers simulation', async () => {
    const server = new DevServer({
      ...DEFAULT_DEV_OPTIONS,
      miniflare: true,
    });

    await server.start();

    // Verify Miniflare is used instead of wrangler dev
    expect(server.getRuntime()).toBe('miniflare');
  });

  it('should configure Miniflare with R2 bindings', async () => {
    const server = new DevServer({
      ...DEFAULT_DEV_OPTIONS,
      miniflare: true,
    });

    await server.start();

    const miniflareConfig = server.getMiniflareConfig();
    expect(miniflareConfig.r2Buckets).toContain('BUCKET');
  });

  it('should configure Miniflare with Durable Objects', async () => {
    const server = new DevServer({
      ...DEFAULT_DEV_OPTIONS,
      miniflare: true,
    });

    await server.start();

    const miniflareConfig = server.getMiniflareConfig();
    expect(miniflareConfig.durableObjects).toContainEqual(
      expect.objectContaining({
        name: 'SHARD',
        className: 'ShardDO',
      })
    );
  });

  it('should persist Miniflare state to local path', async () => {
    const server = new DevServer({
      ...DEFAULT_DEV_OPTIONS,
      miniflare: true,
    });

    await server.start();

    const miniflareConfig = server.getMiniflareConfig();
    expect(miniflareConfig.persistTo).toBe('.mongolake/miniflare');
  });

  it('should support Miniflare live reload', async () => {
    const server = new DevServer({
      ...DEFAULT_DEV_OPTIONS,
      miniflare: true,
      watch: true,
    });

    await server.start();

    const miniflareConfig = server.getMiniflareConfig();
    expect(miniflareConfig.liveReload).toBe(true);
  });
});

// ============================================================================
// CLI Integration Tests
// ============================================================================

describe('CLI Dev Command - CLI Integration', () => {
  let startDevServer: typeof import('../../../src/cli/dev.js').startDevServer;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/dev.js');
    startDevServer = module.startDevServer;
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.clearAllMocks();
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should print startup banner', async () => {
    await startDevServer(DEFAULT_DEV_OPTIONS);

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('MongoLake Development Server')
    );
  });

  it('should print listening URL', async () => {
    await startDevServer(DEFAULT_DEV_OPTIONS);

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringMatching(/http:\/\/localhost:3456/)
    );
  });

  it('should print R2 bucket status', async () => {
    await startDevServer(DEFAULT_DEV_OPTIONS);

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('R2 bucket')
    );
  });

  it('should print hot reload status when enabled', async () => {
    await startDevServer({ ...DEFAULT_DEV_OPTIONS, watch: true });

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('Hot reload enabled')
    );
  });

  it('should handle SIGINT gracefully', async () => {
    const processOnSpy = vi
      .spyOn(process, 'on')
      .mockImplementation(() => process);

    await startDevServer(DEFAULT_DEV_OPTIONS);

    expect(processOnSpy).toHaveBeenCalledWith('SIGINT', expect.any(Function));

    processOnSpy.mockRestore();
  });

  it('should handle SIGTERM gracefully', async () => {
    const processOnSpy = vi
      .spyOn(process, 'on')
      .mockImplementation(() => process);

    await startDevServer(DEFAULT_DEV_OPTIONS);

    expect(processOnSpy).toHaveBeenCalledWith('SIGTERM', expect.any(Function));

    processOnSpy.mockRestore();
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('CLI Dev Command - Error Handling', () => {
  let DevServer: typeof import('../../../src/cli/dev.js').DevServer;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/dev.js');
    DevServer = module.DevServer;
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.clearAllMocks();
  });

  afterEach(() => {
    consoleErrorSpy.mockRestore();
  });

  it('should handle wrangler not found error', async () => {
    const { spawn } = await import('node:child_process');
    const mockProcess = {
      on: vi.fn((event: string, handler: Function) => {
        if (event === 'error') {
          handler(new Error('ENOENT'));
        }
        return mockProcess;
      }),
      stdout: { on: vi.fn() },
      stderr: { on: vi.fn() },
      kill: vi.fn(),
    };
    vi.mocked(spawn).mockReturnValue(mockProcess as unknown as ChildProcess);

    const server = new DevServer(DEFAULT_DEV_OPTIONS);

    await expect(server.start()).rejects.toThrow(
      expect.objectContaining({
        message: expect.stringContaining('wrangler'),
      })
    );
  });

  it('should handle port already in use error', async () => {
    const { spawn } = await import('node:child_process');
    const mockProcess = {
      on: vi.fn((event: string, handler: Function) => {
        if (event === 'error') {
          const error = new Error('EADDRINUSE') as NodeJS.ErrnoException;
          error.code = 'EADDRINUSE';
          handler(error);
        }
        return mockProcess;
      }),
      stdout: { on: vi.fn() },
      stderr: { on: vi.fn() },
      kill: vi.fn(),
    };
    vi.mocked(spawn).mockReturnValue(mockProcess as unknown as ChildProcess);

    const server = new DevServer(DEFAULT_DEV_OPTIONS);

    await expect(server.start()).rejects.toThrow(
      expect.objectContaining({
        message: expect.stringContaining('port'),
      })
    );
  });

  it('should handle wrangler crash', async () => {
    const { spawn } = await import('node:child_process');
    const mockProcess = {
      on: vi.fn((event: string, handler: Function) => {
        if (event === 'exit') {
          handler(1, null);
        }
        return mockProcess;
      }),
      stdout: { on: vi.fn() },
      stderr: { on: vi.fn() },
      kill: vi.fn(),
    };
    vi.mocked(spawn).mockReturnValue(mockProcess as unknown as ChildProcess);

    const server = new DevServer(DEFAULT_DEV_OPTIONS);
    const errorHandler = vi.fn();
    server.on('error', errorHandler);

    await server.start();

    expect(errorHandler).toHaveBeenCalledWith(
      expect.objectContaining({
        code: 1,
      })
    );
  });

  it('should retry on transient errors', async () => {
    const { spawn } = await import('node:child_process');
    let callCount = 0;
    const mockProcess = {
      on: vi.fn((event: string, handler: Function) => {
        if (event === 'error' && callCount < 2) {
          callCount++;
          handler(new Error('Transient error'));
        }
        return mockProcess;
      }),
      stdout: { on: vi.fn() },
      stderr: { on: vi.fn() },
      kill: vi.fn(),
    };
    vi.mocked(spawn).mockReturnValue(mockProcess as unknown as ChildProcess);

    const server = new DevServer({
      ...DEFAULT_DEV_OPTIONS,
      retries: 3,
    });

    await expect(server.start()).resolves.not.toThrow();
    expect(spawn).toHaveBeenCalledTimes(3);
  });
});

// ============================================================================
// Environment Variable Tests
// ============================================================================

describe('CLI Dev Command - Environment Variables', () => {
  let DevServer: typeof import('../../../src/cli/dev.js').DevServer;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/dev.js');
    DevServer = module.DevServer;
    vi.clearAllMocks();
    // Reset spawn mock to return default mock process
    const { spawn } = await import('node:child_process');
    vi.mocked(spawn).mockReturnValue(mockProcess as any);
  });

  it('should pass environment variables to wrangler', async () => {
    const { spawn } = await import('node:child_process');

    const server = new DevServer({
      ...DEFAULT_DEV_OPTIONS,
      env: {
        NODE_ENV: 'development',
        DEBUG: 'mongolake:*',
      },
    });

    await server.start();

    expect(spawn).toHaveBeenCalledWith(
      expect.any(String),
      expect.any(Array),
      expect.objectContaining({
        env: expect.objectContaining({
          NODE_ENV: 'development',
          DEBUG: 'mongolake:*',
        }),
      })
    );
  });

  it('should read .env file if present', async () => {
    const fs = await import('node:fs');
    vi.mocked(fs.existsSync).mockImplementation((path) => {
      return String(path).includes('.env');
    });
    vi.mocked(fs.readFileSync).mockReturnValue('MONGOLAKE_DEBUG=true\n');

    const server = new DevServer(DEFAULT_DEV_OPTIONS);
    await server.start();

    const config = server.getWranglerConfig();
    expect(config.vars?.MONGOLAKE_DEBUG).toBe('true');
  });

  it('should support .env.local override', async () => {
    const fs = await import('node:fs');
    vi.mocked(fs.existsSync).mockReturnValue(true);
    vi.mocked(fs.readFileSync).mockImplementation((path) => {
      if (String(path).includes('.env.local')) {
        return 'MONGOLAKE_DEBUG=false\n';
      }
      return 'MONGOLAKE_DEBUG=true\n';
    });

    const server = new DevServer(DEFAULT_DEV_OPTIONS);
    await server.start();

    const config = server.getWranglerConfig();
    expect(config.vars?.MONGOLAKE_DEBUG).toBe('false');
  });
});

// ============================================================================
// Verbose Mode Tests
// ============================================================================

describe('CLI Dev Command - Verbose Mode', () => {
  let DevServer: typeof import('../../../src/cli/dev.js').DevServer;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/dev.js');
    DevServer = module.DevServer;
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    vi.clearAllMocks();
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should log wrangler output in verbose mode', async () => {
    const { spawn } = await import('node:child_process');
    const mockProcess = {
      on: vi.fn().mockReturnThis(),
      stdout: {
        on: vi.fn((event: string, handler: Function) => {
          if (event === 'data') {
            handler(Buffer.from('Wrangler log output'));
          }
        }),
      },
      stderr: { on: vi.fn() },
      kill: vi.fn(),
    };
    vi.mocked(spawn).mockReturnValue(mockProcess as unknown as ChildProcess);

    const server = new DevServer({ ...DEFAULT_DEV_OPTIONS, verbose: true });
    await server.start();

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('Wrangler log output')
    );
  });

  it('should not log wrangler output when not verbose', async () => {
    const { spawn } = await import('node:child_process');
    const mockProcess = {
      on: vi.fn().mockReturnThis(),
      stdout: {
        on: vi.fn((event: string, handler: Function) => {
          if (event === 'data') {
            handler(Buffer.from('Wrangler log output'));
          }
        }),
      },
      stderr: { on: vi.fn() },
      kill: vi.fn(),
    };
    vi.mocked(spawn).mockReturnValue(mockProcess as unknown as ChildProcess);

    const server = new DevServer({ ...DEFAULT_DEV_OPTIONS, verbose: false });
    await server.start();

    expect(consoleSpy).not.toHaveBeenCalledWith(
      expect.stringContaining('Wrangler log output')
    );
  });

  it('should log file change events in verbose mode', async () => {
    const chokidar = await import('chokidar');
    const mockWatcher = {
      on: vi.fn((event: string, handler: Function) => {
        if (event === 'change') {
          setTimeout(() => handler('src/test.ts'), 0);
        }
        return mockWatcher;
      }),
      close: vi.fn(),
    };
    vi.mocked(chokidar.watch).mockReturnValue(mockWatcher as any);

    const server = new DevServer({
      ...DEFAULT_DEV_OPTIONS,
      verbose: true,
      watch: true,
    });
    await server.start();
    await new Promise((resolve) => setTimeout(resolve, 10));

    expect(consoleSpy).toHaveBeenCalledWith(
      expect.stringContaining('File changed: src/test.ts')
    );
  });
});

// ============================================================================
// Configuration Override Tests
// ============================================================================

describe('CLI Dev Command - Configuration Override', () => {
  let DevServer: typeof import('../../../src/cli/dev.js').DevServer;

  beforeEach(async () => {
    vi.resetModules();
    const module = await import('../../../src/cli/dev.js');
    DevServer = module.DevServer;
    vi.clearAllMocks();
  });

  it('should use existing wrangler.toml if present', async () => {
    const fs = await import('node:fs');
    vi.mocked(fs.existsSync).mockImplementation((path) => {
      return String(path).includes('wrangler.toml');
    });
    vi.mocked(fs.readFileSync).mockReturnValue(`
      name = "custom-name"
      main = "src/custom-worker.ts"
    `);

    const server = new DevServer(DEFAULT_DEV_OPTIONS);
    await server.start();

    // Should not overwrite existing wrangler.toml
    expect(fs.writeFileSync).not.toHaveBeenCalledWith(
      expect.stringContaining('wrangler.toml'),
      expect.any(String)
    );
  });

  it('should merge custom config with defaults', async () => {
    const server = new DevServer({
      ...DEFAULT_DEV_OPTIONS,
      wranglerConfig: {
        compatibility_flags: ['nodejs_compat'],
      },
    });

    await server.start();

    const config = server.getWranglerConfig();
    expect(config.compatibility_flags).toContain('nodejs_compat');
    expect(config.r2_buckets).toBeDefined(); // Default should still be present
  });

  it('should support custom R2 bucket name', async () => {
    const server = new DevServer({
      ...DEFAULT_DEV_OPTIONS,
      r2BucketName: 'custom-bucket',
    });

    await server.start();

    const config = server.getWranglerConfig();
    expect(config.r2_buckets[0].bucket_name).toBe('custom-bucket');
  });
});
