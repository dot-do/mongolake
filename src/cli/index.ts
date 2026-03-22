#!/usr/bin/env node
/**
 * MongoLake CLI
 *
 * Command-line interface for MongoLake operations, built on @dotdo/cli framework.
 */

import { createCLI } from '@dotdo/cli/framework'
import { startDevServer } from './dev.js'
import { startShell } from './shell.js'
import { startProxy } from './proxy.js'
import { startTunnelCommand } from './tunnel.js'
import { login, logout, whoami } from './auth.js'
import { runCompact } from './compact.js'
import { runPush, runPull } from './sync.js'

const app = createCLI({
  name: 'mongolake',
  version: '0.1.0',
  description: 'MongoDB re-imagined for the lakehouse era',
  examples: [
    'mongolake dev              Start dev server on default port (3456)',
    'mongolake dev --port 8080  Start dev server on port 8080',
    'mongolake shell            Start interactive shell',
    'mongolake proxy --target mongodb://remote:27017  Start proxy',
    'mongolake tunnel           Create tunnel to local dev server',
    'mongolake auth login       Authenticate with oauth.do',
    'mongolake compact mydb users          Compact a collection',
    'mongolake push mydb --remote https://api.mongolake.com  Push to remote',
    'mongolake pull mydb --remote https://api.mongolake.com  Pull from remote',
  ],
})

// ── dev ──────────────────────────────────────────────────────────────────────

app.command('dev', 'Start a local development server')
  .option('-p, --port <port>', 'Port to listen on', {
    type: 'number', default: 3456,
    validator: (v: unknown) => { const n = Number(v); if (n < 1 || n > 65535) throw new Error('Port must be between 1 and 65535') }
  })
  .option('-P, --path <path>', 'Path to data directory', { default: '.mongolake' })
  .option('--host <host>', 'Host to bind to', { default: 'localhost' })
  .option('--no-cors', 'Disable CORS headers')
  .option('-v, --verbose', 'Enable verbose logging')
  .example('mongolake dev                     Start dev server on default port')
  .example('mongolake dev --port 8080         Start dev server on port 8080')
  .example('mongolake dev --no-cors           Start without CORS headers')
  .action(async (opts) => {
    await startDevServer({
      port: opts.port as number,
      path: opts.path as string,
      host: opts.host as string,
      cors: opts.cors as boolean,
      verbose: opts.verbose as boolean,
    })
  })

// ── shell ────────────────────────────────────────────────────────────────────

app.command('shell', 'Start an interactive MongoDB-like shell')
  .option('-P, --path <path>', 'Path to data directory', { default: '.mongolake' })
  .option('-v, --verbose', 'Enable verbose logging (show stack traces)')
  .action(async (opts) => {
    await startShell({
      path: opts.path as string,
      verbose: opts.verbose as boolean,
    })
  })

// ── proxy ────────────────────────────────────────────────────────────────────

app.command('proxy', 'Start a wire protocol proxy to a remote MongoDB/MongoLake')
  .option('-t, --target <uri>', 'Target MongoDB/MongoLake connection string')
  .requiredOption()
  .option('-p, --port <port>', 'Local port to listen on', {
    type: 'number', default: 27017,
    validator: (v: unknown) => { const n = Number(v); if (n < 1 || n > 65535) throw new Error('Port must be between 1 and 65535') }
  })
  .option('--host <host>', 'Local host to bind to', { default: '127.0.0.1' })
  .option('--pool', 'Enable connection pooling')
  .option('--pool-size <size>', 'Maximum pool size', {
    type: 'number', default: 10,
    validator: (v: unknown) => { if (Number(v) < 1) throw new Error('Pool size must be at least 1') }
  })
  .option('-v, --verbose', 'Enable verbose logging (wire protocol details)')
  .example('mongolake proxy --target mongodb://localhost:27017')
  .example('mongolake proxy --target mongodb://remote:27017 --pool --pool-size 20')
  .action(async (opts) => {
    await startProxy({
      target: opts.target as string,
      port: opts.port as number,
      host: opts.host as string,
      pool: opts.pool as boolean,
      poolSize: opts['pool-size'] as number,
      verbose: opts.verbose as boolean,
    })
  })

// ── compact ──────────────────────────────────────────────────────────────────

app.command('compact', 'Trigger compaction on a collection')
  .argument('<database>', 'Database name')
  .argument('<collection>', 'Collection name')
  .option('-P, --path <path>', 'Path to data directory', { default: '.mongolake' })
  .option('-n, --dry-run', 'Show what would be compacted without making changes')
  .option('-v, --verbose', 'Enable verbose logging')
  .action(async (opts, args) => {
    await runCompact({
      database: args.database as string,
      collection: args.collection as string,
      path: opts.path as string,
      dryRun: opts['dry-run'] as boolean,
      verbose: opts.verbose as boolean,
    })
  })

// ── push ─────────────────────────────────────────────────────────────────────

app.command('push', 'Upload local database to remote')
  .argument('<database>', 'Database name')
  .option('-r, --remote <url>', 'Remote URL to sync with')
  .requiredOption()
  .option('-P, --path <path>', 'Path to data directory', { default: '.mongolake' })
  .option('-n, --dry-run', 'Show what would be synced without making changes')
  .option('-f, --force', 'Force sync even if there are conflicts')
  .option('--profile <name>', 'Authentication profile', { default: 'default' })
  .option('-v, --verbose', 'Enable verbose logging')
  .example('mongolake push mydb --remote https://api.mongolake.com')
  .example('mongolake push mydb --remote https://api.mongolake.com --dry-run')
  .action(async (opts, args) => {
    const result = await runPush({
      database: args.database as string,
      remote: opts.remote as string,
      path: opts.path as string,
      dryRun: opts['dry-run'] as boolean,
      force: opts.force as boolean,
      profile: opts.profile as string,
      verbose: opts.verbose as boolean,
    })
    return result.success ? 0 : 1
  })

// ── pull ─────────────────────────────────────────────────────────────────────

app.command('pull', 'Download remote database to local')
  .argument('<database>', 'Database name')
  .option('-r, --remote <url>', 'Remote URL to sync with')
  .requiredOption()
  .option('-P, --path <path>', 'Path to data directory', { default: '.mongolake' })
  .option('-n, --dry-run', 'Show what would be synced without making changes')
  .option('-f, --force', 'Force sync even if there are conflicts')
  .option('--profile <name>', 'Authentication profile', { default: 'default' })
  .option('-v, --verbose', 'Enable verbose logging')
  .example('mongolake pull mydb --remote https://api.mongolake.com')
  .example('mongolake pull mydb --remote https://api.mongolake.com --force')
  .action(async (opts, args) => {
    const result = await runPull({
      database: args.database as string,
      remote: opts.remote as string,
      path: opts.path as string,
      dryRun: opts['dry-run'] as boolean,
      force: opts.force as boolean,
      profile: opts.profile as string,
      verbose: opts.verbose as boolean,
    })
    return result.success ? 0 : 1
  })

// ── tunnel ───────────────────────────────────────────────────────────────────

app.command('tunnel', 'Create a Cloudflare tunnel to expose local MongoLake')
  .option('-p, --port <port>', 'Port to tunnel', { type: 'number', default: 3456 })
  .option('-v, --verbose', 'Enable verbose logging')
  .action(async (opts) => {
    await startTunnelCommand({
      port: opts.port as number,
      verbose: opts.verbose as boolean,
    })
  })

// ── auth ─────────────────────────────────────────────────────────────────────

const authCmd = app.command('auth', 'Authentication commands (login, logout, whoami)')
  .action(() => {
    console.log('Usage: mongolake auth <command>\n')
    console.log('Commands:')
    console.log('  login     Log in to MongoLake')
    console.log('  logout    Log out')
    console.log('  whoami    Show current user')
    return 1
  })

authCmd.subcommand('login', 'Start device flow authentication with oauth.do')
  .option('--profile <name>', 'Use a named profile', { default: 'default' })
  .option('-v, --verbose', 'Enable verbose output')
  .action(async (opts) => {
    const result = await login({
      profile: opts.profile as string,
      verbose: opts.verbose as boolean,
    })
    return result.success ? 0 : 1
  })

authCmd.subcommand('logout', 'Clear stored credentials')
  .option('--profile <name>', 'Use a named profile', { default: 'default' })
  .option('-v, --verbose', 'Enable verbose output')
  .action(async (opts) => {
    const success = await logout({
      profile: opts.profile as string,
      verbose: opts.verbose as boolean,
    })
    return success ? 0 : 1
  })

authCmd.subcommand('whoami', 'Show current user information')
  .option('--profile <name>', 'Use a named profile', { default: 'default' })
  .option('-v, --verbose', 'Enable verbose output')
  .action(async (opts) => {
    const result = await whoami({
      profile: opts.profile as string,
      verbose: opts.verbose as boolean,
    })
    return result.authenticated ? 0 : 1
  })

// ── branch / merge (stubs) ──────────────────────────────────────────────

app.command('branch', 'Create a branch (coming soon)')
  .action(() => { console.log('branch command not yet implemented. Coming soon!'); return 1 })

app.command('merge', 'Merge a branch (coming soon)')
  .action(() => { console.log('merge command not yet implemented. Coming soon!'); return 1 })

// ── run ──────────────────────────────────────────────────────────────────────

app.parse(process.argv.slice(2)).then((code) => {
  if (code !== 0) process.exit(code)
}).catch((error) => {
  console.error('Fatal error:', error)
  process.exit(1)
})
