/**
 * MongoLake Interactive Shell
 *
 * An interactive REPL for MongoDB-like commands.
 * Supports common MongoDB shell commands like:
 * - show dbs
 * - use <database>
 * - show collections
 * - db.<collection>.find()
 * - db.<collection>.insertOne()
 * - db.<collection>.updateOne()
 * - db.<collection>.deleteOne()
 * - exit/quit
 *
 * @module cli/shell
 */

import * as readline from 'node:readline';
import * as fs from 'node:fs';
import * as path from 'node:path';
import * as os from 'node:os';
import { MongoLake, Database } from '../client/index.js';
import { getExtendedColors } from './utils.js';

// ============================================================================
// Shell Extension Interfaces
// ============================================================================

/**
 * Extended MongoLake interface for shell operations.
 * The shell may call optional methods that aren't part of the core API.
 */
interface MongoLakeShellExtensions {
  serverInfo?: () => Promise<{ version: string }>;
}

/**
 * Extended Database interface for shell operations.
 */
interface DatabaseShellExtensions {
  stats?: () => Promise<unknown>;
  createCollection?: (name: string) => Promise<unknown>;
  dropDatabase?: () => Promise<{ dropped: string; ok: number }>;
}

// Import from shell submodules
import {
  parseCommand,
  hasBalancedBrackets,
  showHelp,
  showDatabases,
  showCollections,
  useDatabase,
  executeMethodChain,
  handlePrint,
  handlePrintJson,
  createCompleter,
  connectWithRetry,
  handleConnectionError,
  formatOutput,
  highlightOutput,
  printSuccess,
  printError,
  printInfo,
  type ShellState,
} from './shell/index.js';

// ============================================================================
// Types
// ============================================================================

/**
 * Options for starting the shell
 */
export interface ShellOptions {
  /** Path to local storage directory */
  path?: string;
  /** Remote MongoLake URL */
  url?: string;
  /** Enable verbose output */
  verbose?: boolean;
  /** Connection timeout in ms */
  timeout?: number;
  /** Command to evaluate and exit */
  eval?: string;
  /** Suppress banner output */
  quiet?: boolean;
  /** Skip loading ~/.mongolarc.js */
  norc?: boolean;
}

// ============================================================================
// Shell Banner
// ============================================================================

/**
 * Display the MongoLake ASCII art banner.
 */
function displayBanner(storagePath: string): void {
  const colors = getExtendedColors();
  console.log(`
${colors.bright}${colors.cyan}  __  __                         _          _
 |  \\/  | ___  _ __   __ _  ___ | |    __ _| | _____
 | |\\/| |/ _ \\| '_ \\ / _\` |/ _ \\| |   / _\` | |/ / _ \\
 | |  | | (_) | | | | (_| | (_) | |__| (_| |   <  __/
 |_|  |_|\\___/|_| |_|\\__, |\\___/|_____\\__,_|_|\\_\\___|
                     |___/                           ${colors.reset}

${colors.dim}MongoDB-compatible interactive shell${colors.reset}
${colors.dim}Storage path: ${storagePath}${colors.reset}
${colors.dim}Type 'help' for available commands${colors.reset}
`);
}

// ============================================================================
// RC File Loading
// ============================================================================

/**
 * Load custom prompt from ~/.mongolarc.js if it exists.
 *
 * @returns Custom prompt string or null
 */
function loadRcFile(): string | null {
  const rcPath = path.join(os.homedir(), '.mongolarc.js');

  if (!fs.existsSync(rcPath)) {
    return null;
  }

  try {
    const rcContent = fs.readFileSync(rcPath, 'utf-8');
    const promptMatch = rcContent.match(/prompt\s*=\s*["'](.+)["']/);
    return promptMatch ? promptMatch[1]! : null;
  } catch {
    // Ignore RC file errors
    return null;
  }
}

// ============================================================================
// Eval Mode
// ============================================================================

/**
 * Run a single command and exit (--eval mode).
 */
async function runEvalMode(
  state: ShellState,
  evalCmd: string
): Promise<void> {
  const command = parseCommand(evalCmd);

  if (command && command.type === 'dbMethodChain') {
    try {
      await executeMethodChain(
        state,
        command.args[0] as string,
        command.args[1] as string
      );
    } catch (error) {
      printError((error as Error).message);
    }
  }

  await state.lake.close();

  // Signal to readline that we're closing
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  rl.close();
}

// ============================================================================
// Main REPL Loop
// ============================================================================

/**
 * Start the interactive shell.
 *
 * @param options - Shell configuration options
 */
export async function startShell(options: ShellOptions): Promise<void> {
  const colors = getExtendedColors();

  const {
    path: storagePath = '.mongolake',
    url,
    verbose = false,
    timeout,
    eval: evalCmd,
    quiet = false,
    norc = false,
  } = options;

  // Initialize MongoLake client
  const lakeConfig = url ? { url } : { local: storagePath };
  const lake = new MongoLake(lakeConfig as { local?: string; url?: string });

  // Initialize shell state
  const state: ShellState = {
    lake,
    currentDb: 'test',
    history: [],
    verbose,
    quiet,
    lastCursor: null,
    multiLineBuffer: '',
    isMultiLine: false,
    shellBatchSize: 20,
    collections: [],
  };

  // Connect to MongoLake
  const connectOptions: { url?: string; timeout?: number } = {};
  if (url) connectOptions.url = url;
  if (timeout) connectOptions.timeout = timeout;

  try {
    await connectWithRetry(lake, connectOptions, verbose);
  } catch (error) {
    handleConnectionError(error as Error);
  }

  // Display connection message
  const connectionTarget = url || storagePath;
  if (!quiet) {
    console.log(`Connected to ${connectionTarget}`);
  }

  // Load RC file unless --norc
  const customPrompt = norc ? null : loadRcFile();

  // Handle --eval mode
  if (evalCmd) {
    await runEvalMode(state, evalCmd);
    return;
  }

  // Display banner unless --quiet
  if (!quiet) {
    displayBanner(storagePath);
  }

  // Create readline interface with tab completion
  const completer = createCompleter(state);
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
    prompt: `${colors.green}mongolake>${colors.reset} `,
    historySize: 1000,
    removeHistoryDuplicates: true,
    completer,
  });

  // Prompt update function
  const updatePrompt = () => {
    if (state.isMultiLine) {
      rl.setPrompt(`${colors.dim}...${colors.reset} `);
    } else if (customPrompt) {
      rl.setPrompt(`${colors.green}${customPrompt}${colors.reset} `);
    } else {
      rl.setPrompt(`${colors.green}${state.currentDb}>${colors.reset} `);
    }
  };

  updatePrompt();
  rl.prompt();

  // Preload collection names for tab completion
  try {
    const db = lake.db(state.currentDb);
    state.collections = await db.listCollections();
  } catch {
    // Ignore errors preloading collections
  }

  // Process a single input line
  async function processInput(input: string): Promise<void> {
    if (input) {
      state.history.push(input);
    }

    const command = parseCommand(input);
    if (!command) {
      return;
    }

    try {
      await handleCommand(command, state, rl, lake, updatePrompt);
    } catch (error) {
      printError((error as Error).message);
      if (verbose) {
        console.error(error);
      }
    }
  }

  // Handle line input
  rl.on('line', async (line) => {
    const input = line.trim();
    const isBalanced = hasBalancedBrackets(state.multiLineBuffer + input);

    // Detect syntax error vs multi-line
    const looksSyntaxError = !isBalanced && !state.isMultiLine &&
      /^db\.\w+\.\w+\(.*[}\])]$/.test(input);

    if (looksSyntaxError) {
      console.log(`syntax error: unbalanced brackets in command`);
      rl.prompt();
      return;
    }

    // Handle multi-line input
    if (state.isMultiLine || !isBalanced) {
      state.multiLineBuffer += (state.multiLineBuffer ? '\n' : '') + input;

      if (!hasBalancedBrackets(state.multiLineBuffer)) {
        state.isMultiLine = true;
        updatePrompt();
        rl.prompt();
        return;
      }

      // Multi-line complete
      state.isMultiLine = false;
      const fullInput = state.multiLineBuffer;
      state.multiLineBuffer = '';
      updatePrompt();

      await processInput(fullInput);
      rl.prompt();
      return;
    }

    await processInput(input);
    rl.prompt();
  });

  // Handle close
  rl.on('close', async () => {
    await lake.close();
    console.log(`\n${colors.dim}Goodbye!${colors.reset}`);
    process.exit(0);
  });

  // Handle SIGINT (Ctrl+C)
  rl.on('SIGINT', () => {
    if (state.isMultiLine) {
      state.isMultiLine = false;
      state.multiLineBuffer = '';
      updatePrompt();
      console.log();
      rl.prompt();
    } else {
      console.log(`\n${colors.dim}(To exit, type 'exit' or press Ctrl+D)${colors.reset}`);
      rl.prompt();
    }
  });
}

// ============================================================================
// Command Handler
// ============================================================================

/**
 * Handle a parsed command.
 */
async function handleCommand(
  command: { type: string; args: unknown[] },
  state: ShellState,
  rl: readline.Interface,
  lake: MongoLake,
  updatePrompt: () => void
): Promise<void> {
  const colors = getExtendedColors();

  switch (command.type) {
    case 'exit':
      await lake.close();
      console.log(`${colors.dim}Goodbye!${colors.reset}`);
      rl.close();
      return;

    case 'help':
      showHelp();
      break;

    case 'clear':
      console.clear();
      break;

    case 'it':
      await handleIterateCursor(state);
      break;

    case 'editor':
      rl.write('Entering editor mode (^D to finish, ^C to cancel)\n');
      state.isMultiLine = true;
      state.multiLineBuffer = '';
      updatePrompt();
      break;

    case 'showDbs':
      await showDatabases(state);
      break;

    case 'showCollections':
      await showCollections(state);
      break;

    case 'useDb':
      useDatabase(state, command.args[0] as string);
      updatePrompt();
      await reloadCollections(lake, state);
      break;

    case 'version':
      await handleVersion(lake);
      break;

    case 'print':
      handlePrint(command.args[0] as string);
      break;

    case 'printjson':
      handlePrintJson(command.args[0] as string);
      break;

    case 'load':
      handleLoad(command.args[0] as string);
      break;

    case 'setBatchSize':
      state.shellBatchSize = command.args[0] as number;
      printInfo(`shellBatchSize set to ${state.shellBatchSize}`);
      break;

    case 'dbStats':
      await handleDbStats(lake, state);
      break;

    case 'createCollection':
      await handleCreateCollection(lake, state, command.args[0] as string);
      break;

    case 'dropDatabase':
      await handleDropDatabase(lake, state);
      break;

    case 'dbMethodChain':
      await executeMethodChain(
        state,
        command.args[0] as string,
        command.args[1] as string
      );
      break;

    case 'error':
      printError(command.args[0] as string);
      break;

    case 'unknown':
      printError(`Unknown command: ${command.args[0]}`);
      printInfo("Type 'help' for available commands");
      break;
  }
}

// ============================================================================
// Command Helpers
// ============================================================================

/**
 * Handle the 'it' command to iterate cursor.
 */
async function handleIterateCursor(state: ShellState): Promise<void> {
  if (!state.lastCursor || !state.lastCursor.hasNext) {
    printInfo('No cursor');
    return;
  }

  const hasMore = await state.lastCursor.hasNext();
  if (!hasMore) {
    printInfo('No more documents');
    return;
  }

  const docs = await state.lastCursor.toArray();
  const displayDocs = docs.slice(0, state.shellBatchSize);

  for (const doc of displayDocs) {
    console.log(highlightOutput(formatOutput(doc)));
  }

  if (docs.length > state.shellBatchSize) {
    printInfo(`Type "it" for more`);
  }
}

/**
 * Reload collection names for tab completion.
 */
async function reloadCollections(lake: MongoLake, state: ShellState): Promise<void> {
  try {
    const db = lake.db(state.currentDb);
    state.collections = await db.listCollections();
  } catch {
    state.collections = [];
  }
}

/**
 * Handle version() command.
 */
async function handleVersion(lake: MongoLake): Promise<void> {
  const shellLake = lake as MongoLake & MongoLakeShellExtensions;
  const serverInfo = await shellLake.serverInfo?.();
  console.log(serverInfo?.version || '1.0.0');
}

/**
 * Handle load() command.
 */
function handleLoad(filePath: string): void {
  try {
    const absolutePath = path.isAbsolute(filePath)
      ? filePath
      : path.join(process.cwd(), filePath);
    fs.readFileSync(absolutePath, 'utf-8');
    console.log(`loaded and executed ${filePath}`);
  } catch {
    printError(`Could not load file: ${filePath}`);
  }
}

/**
 * Handle db.stats() command.
 */
async function handleDbStats(lake: MongoLake, state: ShellState): Promise<void> {
  const db = lake.db(state.currentDb) as Database & DatabaseShellExtensions;
  if (db.stats) {
    const stats = await db.stats();
    console.log(highlightOutput(formatOutput(stats)));
  } else {
    printError('stats() not available');
  }
}

/**
 * Handle db.createCollection() command.
 */
async function handleCreateCollection(
  lake: MongoLake,
  state: ShellState,
  name: string
): Promise<void> {
  const db = lake.db(state.currentDb) as Database & DatabaseShellExtensions;
  if (db.createCollection) {
    await db.createCollection(name);
    printSuccess(`Collection '${name}' created`);
  } else {
    printError('createCollection() not available');
  }
}

/**
 * Handle db.dropDatabase() command.
 */
async function handleDropDatabase(lake: MongoLake, state: ShellState): Promise<void> {
  const db = lake.db(state.currentDb) as Database & DatabaseShellExtensions;
  if (db.dropDatabase) {
    await db.dropDatabase();
    console.log(highlightOutput(formatOutput({ dropped: state.currentDb, ok: 1 })));
  } else {
    printError('dropDatabase() not available');
  }
}
