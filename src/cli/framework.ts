/**
 * MongoLake CLI Framework
 *
 * This module re-exports the shared CLI framework from @dotdo/cli.
 * The canonical implementation lives in @dotdo/cli/framework.
 *
 * @deprecated Import directly from '@dotdo/cli/framework' or '@dotdo/cli' instead.
 */

export {
  // Classes
  CLI,
  Command,
  Option,
  Argument,

  // Factory
  createCLI,

  // Agent mode
  isAgentMode,

  // Errors
  CLIError,
  ParseError,
  ValidationError,
} from '@dotdo/cli/framework'

export type {
  // Types
  CLIConfig,
  CommandConfig,
  OptionConfig,
  ArgumentConfig,
  OptionType,
  ActionHandler,
  HookType,
  HookContext,
  HookHandler,
  OutputStream,
  ParserFunction,
  ValidatorFunction,
} from '@dotdo/cli/framework'
