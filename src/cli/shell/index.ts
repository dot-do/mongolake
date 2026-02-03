/**
 * Shell Module Index
 *
 * Re-exports all shell components for convenient importing.
 *
 * @module cli/shell
 */

// Output formatting
export {
  formatValue,
  formatOutput,
  highlightOutput,
  printSuccess,
  printError,
  printInfo,
  MAX_OUTPUT_LENGTH,
  MAX_STRING_LENGTH,
} from './output.js';

// Command parsing
export {
  parseCommand,
  parseMethodChain,
  parseArgs,
  hasBalancedBrackets,
  type ParsedCommand,
  type CommandType,
  type ParsedMethod,
} from './parser.js';

// Command handlers
export {
  showHelp,
  showDatabases,
  showCollections,
  useDatabase,
  executeMethodChain,
  handlePrint,
  handlePrintJson,
  type ShellState,
  type AsyncCursor,
} from './handlers.js';

// Tab completion
export {
  createCompleter,
  type CompleterFunction,
  type CompletionState,
} from './completer.js';

// Connection management
export {
  connectWithRetry,
  handleConnectionError,
  type ConnectionOptions,
} from './connection.js';
