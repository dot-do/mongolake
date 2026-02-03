/**
 * Tests for MongoLake CLI Framework
 *
 * Comprehensive tests for the CLI framework including:
 * - Command parsing and routing
 * - Argument and option handling
 * - Help output generation
 * - Version output
 * - Error handling and validation
 *
 * These tests are designed to drive the implementation of a proper CLI framework
 * that will eventually replace the ad-hoc parsing in src/cli/index.ts.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// ============================================================================
// Test Fixtures
// ============================================================================

const EXPECTED_VERSION = '0.1.0';
const EXPECTED_COMMANDS = ['dev', 'shell', 'proxy', 'tunnel', 'compact', 'push', 'pull', 'auth', 'help', 'version'];

// ============================================================================
// CLI Framework Module Tests
// ============================================================================

describe('CLI Framework - Module Exports', () => {
  it('should export CLI class', async () => {
    const module = await import('../../../src/cli/framework.js');
    expect(module.CLI).toBeDefined();
    expect(typeof module.CLI).toBe('function');
  });

  it('should export Command class', async () => {
    const module = await import('../../../src/cli/framework.js');
    expect(module.Command).toBeDefined();
    expect(typeof module.Command).toBe('function');
  });

  it('should export Option class', async () => {
    const module = await import('../../../src/cli/framework.js');
    expect(module.Option).toBeDefined();
    expect(typeof module.Option).toBe('function');
  });

  it('should export Argument class', async () => {
    const module = await import('../../../src/cli/framework.js');
    expect(module.Argument).toBeDefined();
    expect(typeof module.Argument).toBe('function');
  });

  it('should export createCLI factory function', async () => {
    const module = await import('../../../src/cli/framework.js');
    expect(typeof module.createCLI).toBe('function');
  });

  it('should export CLIError class', async () => {
    const module = await import('../../../src/cli/framework.js');
    expect(module.CLIError).toBeDefined();
    expect(typeof module.CLIError).toBe('function');
  });

  it('should export ParseError class', async () => {
    const module = await import('../../../src/cli/framework.js');
    expect(module.ParseError).toBeDefined();
    expect(typeof module.ParseError).toBe('function');
  });

  it('should export ValidationError class', async () => {
    const module = await import('../../../src/cli/framework.js');
    expect(module.ValidationError).toBeDefined();
    expect(typeof module.ValidationError).toBe('function');
  });
});

// ============================================================================
// CLI Class Tests
// ============================================================================

describe('CLI Framework - CLI Class', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    CLI = module.CLI;
  });

  describe('constructor', () => {
    it('should create a CLI instance with name and version', () => {
      const cli = new CLI({
        name: 'mongolake',
        version: EXPECTED_VERSION,
      });

      expect(cli.name).toBe('mongolake');
      expect(cli.version).toBe(EXPECTED_VERSION);
    });

    it('should accept optional description', () => {
      const cli = new CLI({
        name: 'mongolake',
        version: EXPECTED_VERSION,
        description: 'MongoDB re-imagined for the lakehouse era',
      });

      expect(cli.description).toBe('MongoDB re-imagined for the lakehouse era');
    });

    it('should accept optional usage string', () => {
      const cli = new CLI({
        name: 'mongolake',
        version: EXPECTED_VERSION,
        usage: 'mongolake <command> [options]',
      });

      expect(cli.usage).toBe('mongolake <command> [options]');
    });

    it('should accept optional examples array', () => {
      const examples = [
        'mongolake dev',
        'mongolake dev --port 8080',
        'mongolake shell',
      ];

      const cli = new CLI({
        name: 'mongolake',
        version: EXPECTED_VERSION,
        examples,
      });

      expect(cli.examples).toEqual(examples);
    });
  });

  describe('command registration', () => {
    it('should register a command', () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.command('dev', 'Start a local development server');

      expect(cli.hasCommand('dev')).toBe(true);
    });

    it('should return Command instance for chaining', () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const command = cli.command('dev', 'Start a local development server');

      expect(command).toBeDefined();
      expect(command.name).toBe('dev');
    });

    it('should support command aliases', () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.command('dev', 'Start a local development server').alias('serve').alias('start');

      expect(cli.hasCommand('dev')).toBe(true);
      expect(cli.hasCommand('serve')).toBe(true);
      expect(cli.hasCommand('start')).toBe(true);
    });

    it('should throw error for duplicate command names', () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.command('dev', 'Start a local development server');

      expect(() => cli.command('dev', 'Duplicate command')).toThrow();
    });

    it('should list all registered commands', () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.command('dev', 'Start a local development server');
      cli.command('shell', 'Start interactive shell');

      const commands = cli.getCommands();
      expect(commands).toHaveLength(2);
      expect(commands.map((c) => c.name)).toContain('dev');
      expect(commands.map((c) => c.name)).toContain('shell');
    });
  });

  describe('global options', () => {
    it('should automatically add --help option', () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      expect(cli.hasOption('help')).toBe(true);
    });

    it('should automatically add --version option', () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      expect(cli.hasOption('version')).toBe(true);
    });

    it('should support adding global options', () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.option('-v, --verbose', 'Enable verbose output');

      expect(cli.hasOption('verbose')).toBe(true);
    });

    it('should support global options with default values', () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.option('-c, --config <path>', 'Config file path', { default: './mongolake.config.json' });

      const option = cli.getOption('config');
      expect(option?.defaultValue).toBe('./mongolake.config.json');
    });
  });
});

// ============================================================================
// Command Class Tests
// ============================================================================

describe('CLI Framework - Command Class', () => {
  let Command: typeof import('../../../src/cli/framework.js').Command;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    Command = module.Command;
  });

  describe('constructor', () => {
    it('should create a command with name and description', () => {
      const cmd = new Command('dev', 'Start a local development server');

      expect(cmd.name).toBe('dev');
      expect(cmd.description).toBe('Start a local development server');
    });

    it('should support hidden commands', () => {
      const cmd = new Command('internal', 'Internal command', { hidden: true });

      expect(cmd.hidden).toBe(true);
    });
  });

  describe('arguments', () => {
    it('should add required argument', () => {
      const cmd = new Command('compact', 'Compact a collection');
      cmd.argument('<database>', 'Database name');

      expect(cmd.getArguments()).toHaveLength(1);
      expect(cmd.getArgument('database')?.required).toBe(true);
    });

    it('should add optional argument', () => {
      const cmd = new Command('compact', 'Compact a collection');
      cmd.argument('[collection]', 'Collection name');

      expect(cmd.getArgument('collection')?.required).toBe(false);
    });

    it('should add variadic argument', () => {
      const cmd = new Command('push', 'Push databases');
      cmd.argument('<databases...>', 'Database names');

      expect(cmd.getArgument('databases')?.variadic).toBe(true);
    });

    it('should support argument default values', () => {
      const cmd = new Command('dev', 'Start dev server');
      cmd.argument('[port]', 'Port number', { default: '3456' });

      expect(cmd.getArgument('port')?.defaultValue).toBe('3456');
    });

    it('should throw error if required argument follows optional', () => {
      const cmd = new Command('test', 'Test command');
      cmd.argument('[optional]', 'Optional arg');

      expect(() => cmd.argument('<required>', 'Required arg')).toThrow();
    });

    it('should throw error if argument follows variadic', () => {
      const cmd = new Command('test', 'Test command');
      cmd.argument('<files...>', 'Files');

      expect(() => cmd.argument('<another>', 'Another arg')).toThrow();
    });
  });

  describe('options', () => {
    it('should add boolean option', () => {
      const cmd = new Command('dev', 'Start dev server');
      cmd.option('-v, --verbose', 'Enable verbose output');

      const opt = cmd.getOption('verbose');
      expect(opt?.type).toBe('boolean');
      expect(opt?.short).toBe('v');
    });

    it('should add string option with value', () => {
      const cmd = new Command('dev', 'Start dev server');
      cmd.option('-p, --port <port>', 'Port number');

      const opt = cmd.getOption('port');
      expect(opt?.type).toBe('string');
      expect(opt?.required).toBe(true);
    });

    it('should add option with optional value', () => {
      const cmd = new Command('dev', 'Start dev server');
      cmd.option('-c, --config [path]', 'Config file');

      const opt = cmd.getOption('config');
      expect(opt?.type).toBe('string');
      expect(opt?.required).toBe(false);
    });

    it('should add option with default value', () => {
      const cmd = new Command('dev', 'Start dev server');
      cmd.option('-p, --port <port>', 'Port number', { default: '3456' });

      expect(cmd.getOption('port')?.defaultValue).toBe('3456');
    });

    it('should add negatable boolean option', () => {
      const cmd = new Command('dev', 'Start dev server');
      cmd.option('--no-cors', 'Disable CORS');

      const opt = cmd.getOption('cors');
      expect(opt?.negatable).toBe(true);
      expect(opt?.defaultValue).toBe(true);
    });

    it('should support multiple values option', () => {
      const cmd = new Command('build', 'Build project');
      cmd.option('-e, --env <values...>', 'Environment variables');

      const opt = cmd.getOption('env');
      expect(opt?.variadic).toBe(true);
    });

    it('should support choices constraint', () => {
      const cmd = new Command('dev', 'Start dev server');
      cmd.option('-l, --log-level <level>', 'Log level', {
        choices: ['debug', 'info', 'warn', 'error'],
      });

      const opt = cmd.getOption('log-level');
      expect(opt?.choices).toEqual(['debug', 'info', 'warn', 'error']);
    });

    it('should mark option as required', () => {
      const cmd = new Command('proxy', 'Start proxy');
      cmd.option('-t, --target <uri>', 'Target URI').requiredOption();

      expect(cmd.getOption('target')?.mandatory).toBe(true);
    });

    it('should support environment variable fallback', () => {
      const cmd = new Command('dev', 'Start dev server');
      cmd.option('-p, --port <port>', 'Port number', { env: 'MONGOLAKE_PORT' });

      expect(cmd.getOption('port')?.env).toBe('MONGOLAKE_PORT');
    });
  });

  describe('subcommands', () => {
    it('should add subcommand', () => {
      const cmd = new Command('auth', 'Authentication commands');
      cmd.subcommand('login', 'Login to oauth.do');

      expect(cmd.hasSubcommand('login')).toBe(true);
    });

    it('should return subcommand for chaining', () => {
      const cmd = new Command('auth', 'Authentication commands');
      const sub = cmd.subcommand('login', 'Login to oauth.do');

      expect(sub.name).toBe('login');
    });

    it('should support nested subcommands', () => {
      const cmd = new Command('config', 'Configuration commands');
      const sub = cmd.subcommand('set', 'Set configuration');
      sub.subcommand('global', 'Set global configuration');

      expect(cmd.getSubcommand('set')?.hasSubcommand('global')).toBe(true);
    });
  });

  describe('action handler', () => {
    it('should set action handler', () => {
      const cmd = new Command('dev', 'Start dev server');
      const handler = vi.fn();
      cmd.action(handler);

      expect(cmd.getAction()).toBe(handler);
    });

    it('should return command for chaining', () => {
      const cmd = new Command('dev', 'Start dev server');
      const result = cmd.action(() => {});

      expect(result).toBe(cmd);
    });
  });

  describe('help text', () => {
    it('should generate help text', () => {
      const cmd = new Command('dev', 'Start a local development server');
      cmd.option('-p, --port <port>', 'Port number', { default: '3456' });
      cmd.option('-v, --verbose', 'Enable verbose output');

      const help = cmd.helpText();

      expect(help).toContain('dev');
      expect(help).toContain('Start a local development server');
      expect(help).toContain('--port');
      expect(help).toContain('--verbose');
    });

    it('should include usage in help text', () => {
      const cmd = new Command('compact', 'Compact a collection');
      cmd.argument('<database>', 'Database name');
      cmd.argument('<collection>', 'Collection name');

      const help = cmd.helpText();

      expect(help).toContain('Usage:');
      expect(help).toContain('compact <database> <collection>');
    });

    it('should include examples in help text', () => {
      const cmd = new Command('dev', 'Start dev server');
      cmd.example('mongolake dev');
      cmd.example('mongolake dev --port 8080');

      const help = cmd.helpText();

      expect(help).toContain('Examples:');
      expect(help).toContain('mongolake dev');
      expect(help).toContain('mongolake dev --port 8080');
    });

    it('should not show hidden options in help', () => {
      const cmd = new Command('dev', 'Start dev server');
      cmd.option('--internal', 'Internal option', { hidden: true });

      const help = cmd.helpText();

      expect(help).not.toContain('--internal');
    });
  });
});

// ============================================================================
// Command Parsing Tests
// ============================================================================

describe('CLI Framework - Command Parsing', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    CLI = module.CLI;
  });

  describe('basic parsing', () => {
    it('should parse command name', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const devHandler = vi.fn();
      cli.command('dev', 'Start dev server').action(devHandler);

      await cli.parse(['dev']);

      expect(devHandler).toHaveBeenCalled();
    });

    it('should parse command with boolean flag', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('-v, --verbose', 'Verbose output')
        .action(handler);

      await cli.parse(['dev', '--verbose']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ verbose: true }),
        expect.any(Object)
      );
    });

    it('should parse command with short flag', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('-v, --verbose', 'Verbose output')
        .action(handler);

      await cli.parse(['dev', '-v']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ verbose: true }),
        expect.any(Object)
      );
    });

    it('should parse command with string option', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('-p, --port <port>', 'Port number')
        .action(handler);

      await cli.parse(['dev', '--port', '8080']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ port: '8080' }),
        expect.any(Object)
      );
    });

    it('should parse command with = value syntax', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('-p, --port <port>', 'Port number')
        .action(handler);

      await cli.parse(['dev', '--port=8080']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ port: '8080' }),
        expect.any(Object)
      );
    });

    it('should parse command with combined short flags', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('-v, --verbose', 'Verbose')
        .option('-d, --debug', 'Debug')
        .action(handler);

      await cli.parse(['dev', '-vd']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ verbose: true, debug: true }),
        expect.any(Object)
      );
    });

    it('should parse command with positional arguments', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('compact', 'Compact collection')
        .argument('<database>', 'Database')
        .argument('<collection>', 'Collection')
        .action(handler);

      await cli.parse(['compact', 'mydb', 'users']);

      expect(handler).toHaveBeenCalledWith(
        expect.any(Object),
        expect.objectContaining({ database: 'mydb', collection: 'users' })
      );
    });

    it('should parse command with variadic arguments', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('push', 'Push databases')
        .argument('<databases...>', 'Databases')
        .action(handler);

      await cli.parse(['push', 'db1', 'db2', 'db3']);

      expect(handler).toHaveBeenCalledWith(
        expect.any(Object),
        expect.objectContaining({ databases: ['db1', 'db2', 'db3'] })
      );
    });

    it('should apply default values for missing options', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('-p, --port <port>', 'Port', { default: '3456' })
        .action(handler);

      await cli.parse(['dev']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ port: '3456' }),
        expect.any(Object)
      );
    });

    it('should apply default values for missing arguments', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .argument('[port]', 'Port', { default: '3456' })
        .action(handler);

      await cli.parse(['dev']);

      expect(handler).toHaveBeenCalledWith(
        expect.any(Object),
        expect.objectContaining({ port: '3456' })
      );
    });
  });

  describe('negatable options', () => {
    it('should parse --no-* option as false', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('--no-cors', 'Disable CORS')
        .action(handler);

      await cli.parse(['dev', '--no-cors']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ cors: false }),
        expect.any(Object)
      );
    });

    it('should default negatable option to true', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('--no-cors', 'Disable CORS')
        .action(handler);

      await cli.parse(['dev']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ cors: true }),
        expect.any(Object)
      );
    });
  });

  describe('subcommand parsing', () => {
    it('should parse subcommand', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('auth', 'Authentication')
        .subcommand('login', 'Login')
        .action(handler);

      await cli.parse(['auth', 'login']);

      expect(handler).toHaveBeenCalled();
    });

    it('should parse subcommand with options', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('auth', 'Authentication')
        .subcommand('login', 'Login')
        .option('--profile <name>', 'Profile name')
        .action(handler);

      await cli.parse(['auth', 'login', '--profile', 'dev']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ profile: 'dev' }),
        expect.any(Object)
      );
    });
  });

  describe('option type coercion', () => {
    it('should coerce number option', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('-p, --port <port>', 'Port', { type: 'number' })
        .action(handler);

      await cli.parse(['dev', '--port', '8080']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ port: 8080 }),
        expect.any(Object)
      );
    });

    it('should coerce boolean option value', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('--verbose <value>', 'Verbose', { type: 'boolean' })
        .action(handler);

      await cli.parse(['dev', '--verbose', 'true']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ verbose: true }),
        expect.any(Object)
      );
    });

    it('should support custom parser function', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const handler = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('--filter <json>', 'Filter', {
          parser: (value: string) => JSON.parse(value),
        })
        .action(handler);

      await cli.parse(['dev', '--filter', '{"name":"test"}']);

      expect(handler).toHaveBeenCalledWith(
        expect.objectContaining({ filter: { name: 'test' } }),
        expect.any(Object)
      );
    });
  });

  describe('environment variable fallback', () => {
    it('should use environment variable when option not provided', async () => {
      const originalEnv = process.env.MONGOLAKE_PORT;
      process.env.MONGOLAKE_PORT = '9000';

      try {
        const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
        const handler = vi.fn();
        cli
          .command('dev', 'Start dev server')
          .option('-p, --port <port>', 'Port', { env: 'MONGOLAKE_PORT' })
          .action(handler);

        await cli.parse(['dev']);

        expect(handler).toHaveBeenCalledWith(
          expect.objectContaining({ port: '9000' }),
          expect.any(Object)
        );
      } finally {
        if (originalEnv !== undefined) {
          process.env.MONGOLAKE_PORT = originalEnv;
        } else {
          delete process.env.MONGOLAKE_PORT;
        }
      }
    });

    it('should prefer CLI option over environment variable', async () => {
      const originalEnv = process.env.MONGOLAKE_PORT;
      process.env.MONGOLAKE_PORT = '9000';

      try {
        const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
        const handler = vi.fn();
        cli
          .command('dev', 'Start dev server')
          .option('-p, --port <port>', 'Port', { env: 'MONGOLAKE_PORT' })
          .action(handler);

        await cli.parse(['dev', '--port', '8080']);

        expect(handler).toHaveBeenCalledWith(
          expect.objectContaining({ port: '8080' }),
          expect.any(Object)
        );
      } finally {
        if (originalEnv !== undefined) {
          process.env.MONGOLAKE_PORT = originalEnv;
        } else {
          delete process.env.MONGOLAKE_PORT;
        }
      }
    });
  });
});

// ============================================================================
// Help Output Tests
// ============================================================================

describe('CLI Framework - Help Output', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    CLI = module.CLI;
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should display help with --help flag', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
      description: 'MongoDB re-imagined for the lakehouse era',
    });
    cli.command('dev', 'Start dev server');

    await cli.parse(['--help']);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('mongolake'));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('MongoDB re-imagined'));
  });

  it('should display help with -h flag', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });

    await cli.parse(['-h']);

    expect(consoleSpy).toHaveBeenCalled();
  });

  it('should display help with help command', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });
    cli.command('dev', 'Start dev server');

    await cli.parse(['help']);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('dev'));
  });

  it('should display command-specific help', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });
    cli
      .command('dev', 'Start dev server')
      .option('-p, --port <port>', 'Port number');

    await cli.parse(['dev', '--help']);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('dev'));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('--port'));
  });

  it('should display help for subcommand', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });
    cli
      .command('auth', 'Authentication')
      .subcommand('login', 'Login to oauth.do')
      .option('--profile <name>', 'Profile name');

    await cli.parse(['auth', 'login', '--help']);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('login'));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('--profile'));
  });

  it('should display help with usage examples', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
      examples: [
        'mongolake dev',
        'mongolake dev --port 8080',
      ],
    });

    await cli.parse(['--help']);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('Examples'));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('mongolake dev --port 8080'));
  });

  it('should list all commands in help', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });
    cli.command('dev', 'Start dev server');
    cli.command('shell', 'Start interactive shell');
    cli.command('proxy', 'Start proxy server');

    await cli.parse(['--help']);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('dev'));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('shell'));
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('proxy'));
  });

  it('should not list hidden commands in help', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });
    cli.command('dev', 'Start dev server');
    cli.command('internal', 'Internal command', { hidden: true });

    await cli.parse(['--help']);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('dev'));
    // Check that 'internal' is not in any call
    const allCalls = consoleSpy.mock.calls.flat().join('\n');
    expect(allCalls).not.toContain('internal');
  });

  it('should display help when no command given', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });
    cli.command('dev', 'Start dev server');

    await cli.parse([]);

    expect(consoleSpy).toHaveBeenCalled();
  });
});

// ============================================================================
// Version Output Tests
// ============================================================================

describe('CLI Framework - Version Output', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;
  let consoleSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    CLI = module.CLI;
    consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleSpy.mockRestore();
  });

  it('should display version with --version flag', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });

    await cli.parse(['--version']);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining(EXPECTED_VERSION));
  });

  it('should display version with -V flag', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });

    await cli.parse(['-V']);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining(EXPECTED_VERSION));
  });

  it('should display version with version command', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });

    await cli.parse(['version']);

    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining(EXPECTED_VERSION));
  });

  it('should display version in specific format', async () => {
    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
    });

    await cli.parse(['--version']);

    expect(consoleSpy).toHaveBeenCalledWith(`mongolake v${EXPECTED_VERSION}`);
  });
});

// ============================================================================
// Error Handling Tests
// ============================================================================

describe('CLI Framework - Error Handling', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;
  let ParseError: typeof import('../../../src/cli/framework.js').ParseError;
  let ValidationError: typeof import('../../../src/cli/framework.js').ValidationError;
  let consoleErrorSpy: ReturnType<typeof vi.spyOn>;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    CLI = module.CLI;
    ParseError = module.ParseError;
    ValidationError = module.ValidationError;
    consoleErrorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
  });

  afterEach(() => {
    consoleErrorSpy.mockRestore();
  });

  describe('unknown command', () => {
    it('should throw error for unknown command', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.command('dev', 'Start dev server');

      await expect(cli.parse(['unknown'])).rejects.toThrow();
    });

    it('should suggest similar commands', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.command('dev', 'Start dev server');

      try {
        await cli.parse(['dve']);
      } catch (error) {
        expect((error as Error).message).toContain('dev');
      }
    });
  });

  describe('unknown option', () => {
    it('should throw error for unknown option', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.command('dev', 'Start dev server');

      await expect(cli.parse(['dev', '--unknown'])).rejects.toThrow();
    });

    it('should include option name in error', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.command('dev', 'Start dev server');

      try {
        await cli.parse(['dev', '--unknown']);
      } catch (error) {
        expect((error as Error).message).toContain('--unknown');
      }
    });

    it('should suggest similar options', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('dev', 'Start dev server')
        .option('-v, --verbose', 'Verbose');

      try {
        await cli.parse(['dev', '--verbos']);
      } catch (error) {
        expect((error as Error).message).toContain('--verbose');
      }
    });
  });

  describe('missing required option', () => {
    it('should throw error for missing required option', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('proxy', 'Start proxy')
        .option('-t, --target <uri>', 'Target URI')
        .requiredOption();

      await expect(cli.parse(['proxy'])).rejects.toThrow();
    });

    it('should include option name in error', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('proxy', 'Start proxy')
        .option('-t, --target <uri>', 'Target URI')
        .requiredOption();

      try {
        await cli.parse(['proxy']);
      } catch (error) {
        expect((error as Error).message).toContain('--target');
      }
    });
  });

  describe('missing required argument', () => {
    it('should throw error for missing required argument', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('compact', 'Compact collection')
        .argument('<database>', 'Database')
        .argument('<collection>', 'Collection')
        .action(() => {});

      await expect(cli.parse(['compact', 'mydb'])).rejects.toThrow();
    });

    it('should include argument name in error', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('compact', 'Compact collection')
        .argument('<database>', 'Database')
        .argument('<collection>', 'Collection')
        .action(() => {});

      try {
        await cli.parse(['compact', 'mydb']);
      } catch (error) {
        expect((error as Error).message).toContain('collection');
      }
    });
  });

  describe('missing option value', () => {
    it('should throw error when option value is missing', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('dev', 'Start dev server')
        .option('-p, --port <port>', 'Port')
        .action(() => {});

      await expect(cli.parse(['dev', '--port'])).rejects.toThrow();
    });

    it('should include option name in error', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('dev', 'Start dev server')
        .option('-p, --port <port>', 'Port')
        .action(() => {});

      try {
        await cli.parse(['dev', '--port']);
      } catch (error) {
        expect((error as Error).message).toContain('--port');
      }
    });
  });

  describe('invalid option value', () => {
    it('should throw error for invalid number', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('dev', 'Start dev server')
        .option('-p, --port <port>', 'Port', { type: 'number' })
        .action(() => {});

      await expect(cli.parse(['dev', '--port', 'abc'])).rejects.toThrow();
    });

    it('should throw error for invalid choice', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('dev', 'Start dev server')
        .option('-l, --log-level <level>', 'Log level', {
          choices: ['debug', 'info', 'warn', 'error'],
        })
        .action(() => {});

      await expect(cli.parse(['dev', '--log-level', 'invalid'])).rejects.toThrow();
    });

    it('should list valid choices in error', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('dev', 'Start dev server')
        .option('-l, --log-level <level>', 'Log level', {
          choices: ['debug', 'info', 'warn', 'error'],
        })
        .action(() => {});

      try {
        await cli.parse(['dev', '--log-level', 'invalid']);
      } catch (error) {
        expect((error as Error).message).toContain('debug');
        expect((error as Error).message).toContain('info');
      }
    });

    it('should throw error for port out of range', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('dev', 'Start dev server')
        .option('-p, --port <port>', 'Port', {
          type: 'number',
          validator: (value: number) => {
            if (value < 1 || value > 65535) {
              throw new Error('Port must be between 1 and 65535');
            }
          },
        })
        .action(() => {});

      await expect(cli.parse(['dev', '--port', '70000'])).rejects.toThrow();
    });
  });

  describe('custom validators', () => {
    it('should run custom option validator', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const validator = vi.fn();
      cli
        .command('dev', 'Start dev server')
        .option('-p, --port <port>', 'Port', { validator })
        .action(() => {});

      await cli.parse(['dev', '--port', '8080']);

      expect(validator).toHaveBeenCalledWith('8080');
    });

    it('should throw error when validator throws', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli
        .command('dev', 'Start dev server')
        .option('-p, --port <port>', 'Port', {
          validator: () => {
            throw new Error('Invalid port');
          },
        })
        .action(() => {});

      await expect(cli.parse(['dev', '--port', '8080'])).rejects.toThrow('Invalid port');
    });

    it('should run custom argument validator', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      const validator = vi.fn();
      cli
        .command('compact', 'Compact')
        .argument('<database>', 'Database', { validator })
        .action(() => {});

      await cli.parse(['compact', 'mydb']);

      expect(validator).toHaveBeenCalledWith('mydb');
    });
  });

  describe('error output', () => {
    it('should output error to stderr', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.command('dev', 'Start dev server');

      try {
        await cli.parse(['unknown']);
      } catch {
        // Error thrown
      }

      expect(consoleErrorSpy).toHaveBeenCalled();
    });

    it('should show usage hint on error', async () => {
      const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
      cli.command('dev', 'Start dev server');

      try {
        await cli.parse(['unknown']);
      } catch {
        // Error thrown
      }

      expect(consoleErrorSpy).toHaveBeenCalledWith(
        expect.stringMatching(/try.*--help/i)
      );
    });
  });

  describe('error classes', () => {
    it('should create ParseError with code', () => {
      const error = new ParseError('Unknown command', 'UNKNOWN_COMMAND');
      expect(error.code).toBe('UNKNOWN_COMMAND');
      expect(error.message).toBe('Unknown command');
    });

    it('should create ValidationError with code', () => {
      const error = new ValidationError('Invalid port', 'INVALID_PORT');
      expect(error.code).toBe('INVALID_PORT');
      expect(error.message).toBe('Invalid port');
    });

    it('should include option name in ValidationError', () => {
      const error = new ValidationError('Invalid value', 'INVALID_VALUE', {
        option: 'port',
      });
      expect(error.option).toBe('port');
    });

    it('should include argument name in ValidationError', () => {
      const error = new ValidationError('Missing argument', 'MISSING_ARG', {
        argument: 'database',
      });
      expect(error.argument).toBe('database');
    });
  });
});

// ============================================================================
// Hooks Tests
// ============================================================================

describe('CLI Framework - Hooks', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    CLI = module.CLI;
  });

  it('should call preAction hook before command action', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    const preAction = vi.fn();
    const action = vi.fn();

    cli.hook('preAction', preAction);
    cli.command('dev', 'Start dev server').action(action);

    await cli.parse(['dev']);

    expect(preAction).toHaveBeenCalled();
    expect(action).toHaveBeenCalled();
    expect(preAction.mock.invocationCallOrder[0]).toBeLessThan(
      action.mock.invocationCallOrder[0]
    );
  });

  it('should call postAction hook after command action', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    const postAction = vi.fn();
    const action = vi.fn();

    cli.hook('postAction', postAction);
    cli.command('dev', 'Start dev server').action(action);

    await cli.parse(['dev']);

    expect(action).toHaveBeenCalled();
    expect(postAction).toHaveBeenCalled();
    expect(action.mock.invocationCallOrder[0]).toBeLessThan(
      postAction.mock.invocationCallOrder[0]
    );
  });

  it('should call preError hook on error', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    const preError = vi.fn();

    cli.hook('preError', preError);
    cli.command('dev', 'Start dev server');

    try {
      await cli.parse(['unknown']);
    } catch {
      // Expected
    }

    expect(preError).toHaveBeenCalled();
  });

  it('should pass options and args to preAction hook', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    const preAction = vi.fn();

    cli.hook('preAction', preAction);
    cli
      .command('dev', 'Start dev server')
      .option('-p, --port <port>', 'Port')
      .action(() => {});

    await cli.parse(['dev', '--port', '8080']);

    expect(preAction).toHaveBeenCalledWith(
      expect.objectContaining({
        command: expect.any(Object),
        options: expect.objectContaining({ port: '8080' }),
      })
    );
  });
});

// ============================================================================
// Output Customization Tests
// ============================================================================

describe('CLI Framework - Output Customization', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    CLI = module.CLI;
  });

  it('should support custom output stream', async () => {
    const output: string[] = [];
    const customOutput = {
      write: (str: string) => output.push(str),
    };

    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
      output: customOutput,
    });

    await cli.parse(['--version']);

    expect(output.join('')).toContain(EXPECTED_VERSION);
  });

  it('should support custom error stream', async () => {
    const errors: string[] = [];
    const customError = {
      write: (str: string) => errors.push(str),
    };

    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
      errorOutput: customError,
    });
    cli.command('dev', 'Start dev server');

    try {
      await cli.parse(['unknown']);
    } catch {
      // Expected
    }

    expect(errors.length).toBeGreaterThan(0);
  });

  it('should support custom help formatter', async () => {
    const output: string[] = [];
    const customOutput = {
      write: (str: string) => output.push(str),
    };

    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
      output: customOutput,
      helpFormatter: (help) => `CUSTOM: ${help}`,
    });

    await cli.parse(['--help']);

    expect(output.join('')).toContain('CUSTOM:');
  });

  it('should support custom version formatter', async () => {
    const output: string[] = [];
    const customOutput = {
      write: (str: string) => output.push(str),
    };

    const cli = new CLI({
      name: 'mongolake',
      version: EXPECTED_VERSION,
      output: customOutput,
      versionFormatter: (name, version) => `${name} version ${version} (custom)`,
    });

    await cli.parse(['--version']);

    expect(output.join('')).toContain('(custom)');
  });
});

// ============================================================================
// Async Action Tests
// ============================================================================

describe('CLI Framework - Async Actions', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    CLI = module.CLI;
  });

  it('should await async action handlers', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    const results: number[] = [];

    cli.command('test', 'Test command').action(async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      results.push(1);
    });

    await cli.parse(['test']);
    results.push(2);

    expect(results).toEqual([1, 2]);
  });

  it('should propagate async errors', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });

    cli.command('test', 'Test command').action(async () => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      throw new Error('Async error');
    });

    await expect(cli.parse(['test'])).rejects.toThrow('Async error');
  });
});

// ============================================================================
// Exit Code Tests
// ============================================================================

describe('CLI Framework - Exit Codes', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    CLI = module.CLI;
  });

  it('should return 0 on success', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    cli.command('dev', 'Start dev server').action(() => {});

    const exitCode = await cli.parse(['dev']);

    expect(exitCode).toBe(0);
  });

  it('should return 0 for help', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });

    const exitCode = await cli.parse(['--help']);

    expect(exitCode).toBe(0);
  });

  it('should return 0 for version', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });

    const exitCode = await cli.parse(['--version']);

    expect(exitCode).toBe(0);
  });

  it('should return custom exit code from action', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    cli.command('test', 'Test').action(() => 5);

    const exitCode = await cli.parse(['test']);

    expect(exitCode).toBe(5);
  });

  it('should return 1 on error', async () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    cli.command('test', 'Test').action(() => {
      throw new Error('Test error');
    });

    await expect(cli.parse(['test'])).rejects.toThrow();
  });
});

// ============================================================================
// Completion Tests
// ============================================================================

describe('CLI Framework - Completion Support', () => {
  let CLI: typeof import('../../../src/cli/framework.js').CLI;

  beforeEach(async () => {
    const module = await import('../../../src/cli/framework.js');
    CLI = module.CLI;
  });

  it('should generate bash completion script', () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    cli.command('dev', 'Start dev server');
    cli.command('shell', 'Start shell');

    const completion = cli.completionScript('bash');

    expect(completion).toContain('mongolake');
    expect(completion).toContain('dev');
    expect(completion).toContain('shell');
  });

  it('should generate zsh completion script', () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    cli.command('dev', 'Start dev server');

    const completion = cli.completionScript('zsh');

    expect(completion).toContain('mongolake');
    expect(completion).toContain('dev');
  });

  it('should include options in completion', () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    cli
      .command('dev', 'Start dev server')
      .option('-p, --port <port>', 'Port')
      .option('-v, --verbose', 'Verbose');

    const completion = cli.completionScript('bash');

    expect(completion).toContain('--port');
    expect(completion).toContain('--verbose');
  });

  it('should include choices in completion', () => {
    const cli = new CLI({ name: 'mongolake', version: EXPECTED_VERSION });
    cli
      .command('dev', 'Start dev server')
      .option('-l, --log-level <level>', 'Log level', {
        choices: ['debug', 'info', 'warn', 'error'],
      });

    const completion = cli.completionScript('bash');

    expect(completion).toContain('debug');
    expect(completion).toContain('info');
    expect(completion).toContain('warn');
    expect(completion).toContain('error');
  });
});
