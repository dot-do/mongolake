#!/usr/bin/env node
/**
 * MongoLake CLI
 *
 * Command-line interface for MongoLake operations:
 * - dev: Start a local development server
 * - shell: Interactive shell (coming soon)
 * - push/pull: Sync operations (coming soon)
 * - branch/merge: Version control (coming soon)
 */

import { parseArgs } from 'node:util';
import { startDevServer } from './dev.js';
import { startShell } from './shell.js';
import { startProxy } from './proxy.js';
import { startTunnelCommand } from './tunnel.js';
import { login, logout, whoami } from './auth.js';
import { handleCompactCommand } from './compact.js';
import { handlePushCommand, handlePullCommand } from './sync.js';

// ============================================================================
// Version & Help
// ============================================================================

const VERSION = '0.1.0';

const HELP_TEXT = `
mongolake - MongoDB re-imagined for the lakehouse era

Usage: mongolake <command> [options]

Commands:
  dev       Start a local development server
  shell     Start an interactive MongoDB-like shell
  proxy     Start a wire protocol proxy to a remote MongoDB/MongoLake
  tunnel    Create a Cloudflare tunnel to expose local MongoLake
  compact   Trigger compaction on a collection
  push      Upload local database to remote
  pull      Download remote database to local
  auth      Authentication commands (login, logout, whoami)
  help      Show this help message
  version   Show version information

Options:
  -h, --help      Show help
  -v, --version   Show version

Examples:
  mongolake dev              Start dev server on default port (3456)
  mongolake dev --port 8080  Start dev server on port 8080
  mongolake dev --path ./data  Use custom data directory
  mongolake shell            Start interactive shell
  mongolake shell --path ./data  Use custom data directory
  mongolake proxy --target mongodb://remote:27017  Start proxy
  mongolake tunnel           Create tunnel to local dev server
  mongolake tunnel --port 8080  Create tunnel to specific port
  mongolake auth login       Authenticate with oauth.do
  mongolake auth whoami      Show current user info
  mongolake compact mydb users          Compact a collection
  mongolake compact mydb users --dry-run  Preview compaction
  mongolake push mydb --remote https://api.mongolake.com  Push to remote
  mongolake pull mydb --remote https://api.mongolake.com  Pull from remote

For more information, visit: https://mongolake.com/docs
`;

const DEV_HELP_TEXT = `
mongolake dev - Start a local development server

Usage: mongolake dev [options]

Options:
  -p, --port <port>    Port to listen on (default: 3456)
  -P, --path <path>    Path to data directory (default: .mongolake)
  -h, --host <host>    Host to bind to (default: localhost)
  --no-cors            Disable CORS headers
  -v, --verbose        Enable verbose logging

Examples:
  mongolake dev
  mongolake dev --port 8080
  mongolake dev --path ./data --verbose
`;

const SHELL_HELP_TEXT = `
mongolake shell - Start an interactive MongoDB-like shell

Usage: mongolake shell [options]

Options:
  -P, --path <path>    Path to data directory (default: .mongolake)
  -v, --verbose        Enable verbose logging (show stack traces)

Shell Commands:
  show dbs                    List all databases
  use <database>              Switch to a database
  show collections            List collections in current database
  db.<collection>.find()      Find documents
  db.<collection>.insertOne() Insert a document
  db.<collection>.updateOne() Update a document
  db.<collection>.deleteOne() Delete a document
  help                        Show available commands
  exit / quit                 Exit the shell

Examples:
  mongolake shell
  mongolake shell --path ./data
  mongolake shell --verbose
`;

const PROXY_HELP_TEXT = `
mongolake proxy - Start a wire protocol proxy to a remote MongoDB/MongoLake

Usage: mongolake proxy --target <connection-string> [options]

Options:
  -t, --target <uri>   Target MongoDB/MongoLake connection string (required)
  -p, --port <port>    Local port to listen on (default: 27017)
  -h, --host <host>    Local host to bind to (default: 127.0.0.1)
  --pool               Enable connection pooling
  --pool-size <size>   Maximum pool size (default: 10)
  -v, --verbose        Enable verbose logging (wire protocol details)

Connection String Formats:
  mongodb://host:port/database
  mongodb://user:pass@host:port/database?authSource=admin
  mongolake://host:port
  host:port
  host (defaults to port 27017)

Examples:
  mongolake proxy --target mongodb://localhost:27018
  mongolake proxy --target mongodb://remote.example.com:27017 --port 27018
  mongolake proxy --target mongodb://user:pass@cluster.mongodb.net:27017 --verbose
  mongolake proxy --target mongodb://remote:27017 --pool --pool-size 20
`;

const AUTH_HELP_TEXT = `
mongolake auth - Authentication commands

Usage: mongolake auth <subcommand> [options]

Subcommands:
  login     Start device flow authentication with oauth.do
  logout    Clear stored credentials
  whoami    Show current user information

Options:
  --profile <name>   Use a named profile (default: "default")
  -v, --verbose      Enable verbose output

Examples:
  mongolake auth login           Start device flow authentication
  mongolake auth logout          Clear stored credentials
  mongolake auth whoami          Show current user info
  mongolake auth login --profile dev   Login to a specific profile
`;

const TUNNEL_HELP_TEXT = `
mongolake tunnel - Create a Cloudflare tunnel to expose local MongoLake

Usage: mongolake tunnel [options]

Options:
  -p, --port <port>    Port to tunnel (default: 3456)
  -v, --verbose        Enable verbose logging

Description:
  Creates a quick Cloudflare tunnel using cloudflared to expose your local
  MongoLake dev server to the internet. This is useful for:
  - Testing webhooks that need a public URL
  - Sharing your development environment temporarily
  - Debugging remote client connections

  The tunnel URL is randomly generated and valid until you stop the command.

Requirements:
  - cloudflared must be installed (run 'mongolake tunnel' for install instructions)
  - No Cloudflare account required for quick tunnels

Examples:
  mongolake tunnel                  # Tunnel to localhost:3456
  mongolake tunnel --port 8080      # Tunnel to localhost:8080
  mongolake tunnel --verbose        # Show detailed cloudflared output
`;

// ============================================================================
// Main CLI Entry Point
// ============================================================================

async function main(): Promise<void> {
  const args = process.argv.slice(2);

  // No arguments - show help
  if (args.length === 0) {
    console.log(HELP_TEXT);
    process.exit(0);
  }

  const command = args[0];

  // Handle global flags
  if (command === '-h' || command === '--help' || command === 'help') {
    console.log(HELP_TEXT);
    process.exit(0);
  }

  if (command === '-v' || command === '--version' || command === 'version') {
    console.log(`mongolake v${VERSION}`);
    process.exit(0);
  }

  // Route to command handlers
  switch (command) {
    case 'dev':
      await handleDevCommand(args.slice(1));
      break;

    case 'shell':
      await handleShellCommand(args.slice(1));
      break;

    case 'proxy':
      await handleProxyCommand(args.slice(1));
      break;

    case 'auth':
      await handleAuthCommand(args.slice(1));
      break;

    case 'compact':
      await handleCompactCommand(args.slice(1));
      break;

    case 'tunnel':
      await handleTunnelCommand(args.slice(1));
      break;

    case 'push':
      await handlePushCommand(args.slice(1));
      break;

    case 'pull':
      await handlePullCommand(args.slice(1));
      break;

    case 'branch':
    case 'merge':
      console.log(`${command} command not yet implemented. Coming soon!`);
      process.exit(1);
      break;

    default:
      console.error(`Unknown command: ${command}`);
      console.log(HELP_TEXT);
      process.exit(1);
  }
}

// ============================================================================
// Dev Command Handler
// ============================================================================

interface DevOptions {
  port: number;
  path: string;
  host: string;
  cors: boolean;
  verbose: boolean;
}

async function handleDevCommand(args: string[]): Promise<void> {
  // Check for help flag
  if (args.includes('-h') || args.includes('--help')) {
    console.log(DEV_HELP_TEXT);
    process.exit(0);
  }

  // Parse dev command options
  let options: DevOptions;

  try {
    const { values } = parseArgs({
      args,
      options: {
        port: {
          type: 'string',
          short: 'p',
          default: '3456',
        },
        path: {
          type: 'string',
          short: 'P',
          default: '.mongolake',
        },
        host: {
          type: 'string',
          short: 'h',
          default: 'localhost',
        },
        cors: {
          type: 'boolean',
          default: true,
        },
        verbose: {
          type: 'boolean',
          short: 'v',
          default: false,
        },
      },
      allowPositionals: true,
    });

    options = {
      port: parseInt(values.port as string, 10),
      path: values.path as string,
      host: values.host as string,
      cors: values.cors as boolean,
      verbose: values.verbose as boolean,
    };

    // Validate port
    if (isNaN(options.port) || options.port < 1 || options.port > 65535) {
      console.error(`Invalid port: ${values.port}. Port must be between 1 and 65535.`);
      process.exit(1);
    }
  } catch (error) {
    console.error('Error parsing arguments:', (error as Error).message);
    console.log(DEV_HELP_TEXT);
    process.exit(1);
  }

  // Start the development server
  await startDevServer(options);
}

// ============================================================================
// Shell Command Handler
// ============================================================================

interface ShellOptions {
  path: string;
  verbose: boolean;
}

async function handleShellCommand(args: string[]): Promise<void> {
  // Check for help flag
  if (args.includes('-h') || args.includes('--help')) {
    console.log(SHELL_HELP_TEXT);
    process.exit(0);
  }

  // Parse shell command options
  let options: ShellOptions;

  try {
    const { values } = parseArgs({
      args,
      options: {
        path: {
          type: 'string',
          short: 'P',
          default: '.mongolake',
        },
        verbose: {
          type: 'boolean',
          short: 'v',
          default: false,
        },
      },
      allowPositionals: true,
    });

    options = {
      path: values.path as string,
      verbose: values.verbose as boolean,
    };
  } catch (error) {
    console.error('Error parsing arguments:', (error as Error).message);
    console.log(SHELL_HELP_TEXT);
    process.exit(1);
  }

  // Start the interactive shell
  await startShell(options);
}

// ============================================================================
// Proxy Command Handler
// ============================================================================

interface ProxyOptions {
  target: string;
  port: number;
  host: string;
  pool: boolean;
  poolSize: number;
  verbose: boolean;
}

async function handleProxyCommand(args: string[]): Promise<void> {
  // Check for help flag
  if (args.includes('-h') || args.includes('--help')) {
    console.log(PROXY_HELP_TEXT);
    process.exit(0);
  }

  // Parse proxy command options
  let options: ProxyOptions;

  try {
    const { values } = parseArgs({
      args,
      options: {
        target: {
          type: 'string',
          short: 't',
        },
        port: {
          type: 'string',
          short: 'p',
          default: '27017',
        },
        host: {
          type: 'string',
          short: 'h',
          default: '127.0.0.1',
        },
        pool: {
          type: 'boolean',
          default: false,
        },
        'pool-size': {
          type: 'string',
          default: '10',
        },
        verbose: {
          type: 'boolean',
          short: 'v',
          default: false,
        },
      },
      allowPositionals: true,
    });

    // Target is required
    if (!values.target) {
      console.error('Error: --target is required');
      console.log(PROXY_HELP_TEXT);
      process.exit(1);
    }

    options = {
      target: values.target as string,
      port: parseInt(values.port as string, 10),
      host: values.host as string,
      pool: values.pool as boolean,
      poolSize: parseInt(values['pool-size'] as string, 10),
      verbose: values.verbose as boolean,
    };

    // Validate port
    if (isNaN(options.port) || options.port < 1 || options.port > 65535) {
      console.error(`Invalid port: ${values.port}. Port must be between 1 and 65535.`);
      process.exit(1);
    }

    // Validate pool size
    if (isNaN(options.poolSize) || options.poolSize < 1) {
      console.error(`Invalid pool size: ${values['pool-size']}. Pool size must be at least 1.`);
      process.exit(1);
    }
  } catch (error) {
    console.error('Error parsing arguments:', (error as Error).message);
    console.log(PROXY_HELP_TEXT);
    process.exit(1);
  }

  // Start the proxy server
  await startProxy(options);
}

// ============================================================================
// Auth Command Handler
// ============================================================================

interface AuthOptions {
  profile?: string;
  verbose: boolean;
}

async function handleAuthCommand(args: string[]): Promise<void> {
  // Check for help flag
  if (args.includes('-h') || args.includes('--help') || args.length === 0) {
    console.log(AUTH_HELP_TEXT);
    if (args.length === 0) {
      process.exit(1);
    }
    process.exit(0);
  }

  const subcommand = args[0];

  // Parse auth command options
  let options: AuthOptions;

  try {
    const { values } = parseArgs({
      args: args.slice(1),
      options: {
        profile: {
          type: 'string',
          default: 'default',
        },
        verbose: {
          type: 'boolean',
          short: 'v',
          default: false,
        },
      },
      allowPositionals: true,
    });

    options = {
      profile: values.profile as string,
      verbose: values.verbose as boolean,
    };
  } catch (error) {
    console.error('Error parsing arguments:', (error as Error).message);
    console.log(AUTH_HELP_TEXT);
    process.exit(1);
  }

  // Route to subcommand handlers
  switch (subcommand) {
    case 'login': {
      const result = await login(options);
      process.exit(result.success ? 0 : 1);
      break;
    }

    case 'logout': {
      const success = await logout(options);
      process.exit(success ? 0 : 1);
      break;
    }

    case 'whoami': {
      const result = await whoami(options);
      process.exit(result.authenticated ? 0 : 1);
      break;
    }

    default:
      console.error(`Unknown auth subcommand: ${subcommand}`);
      console.log(AUTH_HELP_TEXT);
      process.exit(1);
  }
}

// ============================================================================
// Tunnel Command Handler
// ============================================================================

interface TunnelOptions {
  port: number;
  verbose: boolean;
}

async function handleTunnelCommand(args: string[]): Promise<void> {
  // Check for help flag
  if (args.includes('-h') || args.includes('--help')) {
    console.log(TUNNEL_HELP_TEXT);
    process.exit(0);
  }

  // Parse tunnel command options
  let options: TunnelOptions;

  try {
    const { values } = parseArgs({
      args,
      options: {
        port: {
          type: 'string',
          short: 'p',
          default: '3456',
        },
        verbose: {
          type: 'boolean',
          short: 'v',
          default: false,
        },
      },
      allowPositionals: true,
    });

    options = {
      port: parseInt(values.port as string, 10),
      verbose: values.verbose as boolean,
    };

    // Validate port
    if (isNaN(options.port) || options.port < 1 || options.port > 65535) {
      console.error(`Invalid port: ${values.port}. Port must be between 1 and 65535.`);
      process.exit(1);
    }
  } catch (error) {
    console.error('Error parsing arguments:', (error as Error).message);
    console.log(TUNNEL_HELP_TEXT);
    process.exit(1);
  }

  // Start the tunnel
  await startTunnelCommand(options);
}

// ============================================================================
// Run
// ============================================================================

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
