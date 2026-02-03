/**
 * MongoLake CLI Framework
 *
 * A lightweight, type-safe CLI framework for building command-line applications.
 * Features:
 * - Command and subcommand registration
 * - Option and argument parsing
 * - Help and version generation
 * - Input validation
 * - Hooks for extensibility
 * - Shell completion support
 */

// ============================================================================
// Types
// ============================================================================

/** Option types for type coercion */
export type OptionType = 'string' | 'boolean' | 'number';

/** Custom parser function for options */
export type ParserFunction<T = unknown> = (value: string) => T;

/** Custom validator function */
export type ValidatorFunction<T = unknown> = (value: T) => void;

/** Action handler function */
export type ActionHandler = (
  options: Record<string, unknown>,
  args: Record<string, unknown>
) => void | number | Promise<void | number>;

/** Hook types */
export type HookType = 'preAction' | 'postAction' | 'preError';

/** Hook context */
export interface HookContext {
  command: Command;
  options: Record<string, unknown>;
  args: Record<string, unknown>;
  error?: Error;
}

/** Hook handler function */
export type HookHandler = (context: HookContext) => void | Promise<void>;

/** Output stream interface */
export interface OutputStream {
  write(str: string): void;
}

/** CLI configuration options */
export interface CLIConfig {
  name: string;
  version: string;
  description?: string;
  usage?: string;
  examples?: string[];
  output?: OutputStream;
  errorOutput?: OutputStream;
  helpFormatter?: (help: string) => string;
  versionFormatter?: (name: string, version: string) => string;
}

/** Command configuration options */
export interface CommandConfig {
  hidden?: boolean;
}

/** Option configuration */
export interface OptionConfig {
  default?: unknown;
  type?: OptionType;
  choices?: string[];
  env?: string;
  hidden?: boolean;
  parser?: ParserFunction;
  validator?: ValidatorFunction;
}

/** Argument configuration */
export interface ArgumentConfig {
  default?: string;
  validator?: ValidatorFunction<string>;
}

// ============================================================================
// Error Classes
// ============================================================================

/**
 * Base CLI error class
 */
export class CLIError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CLIError';
  }
}

/**
 * Parse error for command line parsing failures
 */
export class ParseError extends CLIError {
  public readonly code: string;
  public readonly option?: string;
  public readonly argument?: string;

  constructor(
    message: string,
    code: string,
    details?: { option?: string; argument?: string }
  ) {
    super(message);
    this.name = 'ParseError';
    this.code = code;
    this.option = details?.option;
    this.argument = details?.argument;
  }
}

/**
 * Validation error for invalid input values
 */
export class ValidationError extends CLIError {
  public readonly code: string;
  public readonly option?: string;
  public readonly argument?: string;

  constructor(
    message: string,
    code: string,
    details?: { option?: string; argument?: string }
  ) {
    super(message);
    this.name = 'ValidationError';
    this.code = code;
    this.option = details?.option;
    this.argument = details?.argument;
  }
}

// ============================================================================
// Option Class
// ============================================================================

/**
 * Represents a command-line option
 */
export class Option {
  public readonly name: string;
  public readonly description: string;
  public readonly short?: string;
  public readonly type: OptionType;
  public readonly required: boolean;
  public readonly negatable: boolean;
  public readonly variadic: boolean;
  public readonly defaultValue?: unknown;
  public readonly choices?: string[];
  public readonly env?: string;
  public readonly hidden: boolean;
  public readonly parser?: ParserFunction;
  public readonly validator?: ValidatorFunction;
  public mandatory: boolean = false;

  constructor(
    flags: string,
    description: string,
    config: OptionConfig = {}
  ) {
    this.description = description;
    this.hidden = config.hidden ?? false;
    this.defaultValue = config.default;
    this.choices = config.choices;
    this.env = config.env;
    this.parser = config.parser;
    this.validator = config.validator;

    // Parse flags string like "-p, --port <port>" or "--verbose" or "--no-cors"
    const flagParts = flags.split(',').map((f) => f.trim());
    const longFlag = flagParts.find((f) => f.startsWith('--'));
    const shortFlag = flagParts.find((f) => f.startsWith('-') && !f.startsWith('--'));

    // Extract option name from long flag
    const longMatch = longFlag?.match(/^--(?:no-)?([a-zA-Z0-9-]+)(?:\s+([<[].*[>\]])?)?$/);
    if (!longMatch) {
      throw new Error(`Invalid option flags: ${flags}`);
    }

    this.name = longMatch[1]!;
    this.negatable = longFlag?.startsWith('--no-') ?? false;

    // Extract short flag
    const shortMatch = shortFlag?.match(/^-([a-zA-Z])(?:\s+.+)?$/);
    this.short = shortMatch?.[1];

    // Determine value requirement from brackets
    const valuePart = longMatch[2] || (shortFlag?.match(/^-\w\s+(.+)$/)?.[1]);
    if (valuePart) {
      this.required = valuePart.startsWith('<');
      this.variadic = valuePart.includes('...');
      this.type = config.type ?? 'string';
    } else {
      this.required = false;
      this.variadic = false;
      this.type = config.type ?? 'boolean';
    }

    // Set default for negatable options
    if (this.negatable && this.defaultValue === undefined) {
      this.defaultValue = true;
    }
  }

  /**
   * Parse and coerce a value according to option type
   */
  parseValue(value: string | boolean): unknown {
    if (this.parser) {
      return this.parser(value as string);
    }

    if (typeof value === 'boolean') {
      return value;
    }

    switch (this.type) {
      case 'number': {
        const num = Number(value);
        if (isNaN(num)) {
          throw new ValidationError(
            `Invalid number value "${value}" for option --${this.name}`,
            'INVALID_NUMBER',
            { option: this.name }
          );
        }
        return num;
      }
      case 'boolean':
        return value === 'true' || value === '1';
      default:
        return value;
    }
  }

  /**
   * Validate a value against constraints
   */
  validate(value: unknown): void {
    // Check choices
    if (this.choices && !this.choices.includes(value as string)) {
      throw new ValidationError(
        `Invalid value "${value}" for option --${this.name}. Valid choices: ${this.choices.join(', ')}`,
        'INVALID_CHOICE',
        { option: this.name }
      );
    }

    // Run custom validator
    if (this.validator) {
      this.validator(value);
    }
  }
}

// ============================================================================
// Argument Class
// ============================================================================

/**
 * Represents a positional argument
 */
export class Argument {
  public readonly name: string;
  public readonly description: string;
  public readonly required: boolean;
  public readonly variadic: boolean;
  public readonly defaultValue?: string;
  public readonly validator?: ValidatorFunction<string>;

  constructor(
    definition: string,
    description: string,
    config: ArgumentConfig = {}
  ) {
    this.description = description;
    this.defaultValue = config.default;
    this.validator = config.validator;

    // Parse definition like "<database>", "[collection]", or "<files...>"
    const match = definition.match(/^([<[])([a-zA-Z0-9-_]+)(\.\.\.)?([>\]])$/);
    if (!match) {
      throw new Error(`Invalid argument definition: ${definition}`);
    }

    this.name = match[2]!;
    this.required = match[1] === '<';
    this.variadic = match[3]! === '...';
  }

  /**
   * Validate a value
   */
  validate(value: string): void {
    if (this.validator) {
      this.validator(value);
    }
  }
}

// ============================================================================
// Command Class
// ============================================================================

/**
 * Represents a CLI command
 */
export class Command {
  public readonly name: string;
  public readonly description: string;
  public readonly hidden: boolean;

  private _arguments: Argument[] = [];
  private _options: Map<string, Option> = new Map();
  private _shortOptions: Map<string, string> = new Map();
  private _subcommands: Map<string, Command> = new Map();
  private _aliases: Set<string> = new Set();
  private _action?: ActionHandler;
  private _examples: string[] = [];
  // Parent command reference (used for command hierarchy traversal)
  private _parentCmd?: Command;

  /** Get the parent command in the hierarchy */
  get parent(): Command | undefined {
    return this._parentCmd;
  }
  private _lastOption?: Option;
  private _onAlias?: (alias: string, commandName: string) => void;

  constructor(name: string, description: string, config: CommandConfig = {}) {
    this.name = name;
    this.description = description;
    this.hidden = config.hidden ?? false;
  }

  /**
   * Set the callback for alias registration (used internally by CLI)
   */
  _setOnAlias(callback: (alias: string, commandName: string) => void): void {
    this._onAlias = callback;
  }

  /**
   * Add a positional argument
   */
  argument(definition: string, description: string, config: ArgumentConfig = {}): this {
    const arg = new Argument(definition, description, config);

    // Validate argument ordering
    const lastArg = this._arguments[this._arguments.length - 1];
    if (lastArg?.variadic) {
      throw new Error('Cannot add argument after variadic argument');
    }
    if (lastArg && !lastArg.required && arg.required) {
      throw new Error('Cannot add required argument after optional argument');
    }

    this._arguments.push(arg);
    return this;
  }

  /**
   * Add an option
   */
  option(flags: string, description: string, config: OptionConfig = {}): this {
    const opt = new Option(flags, description, config);
    this._options.set(opt.name, opt);
    if (opt.short) {
      this._shortOptions.set(opt.short, opt.name);
    }
    this._lastOption = opt;
    return this;
  }

  /**
   * Mark the last option as required/mandatory
   */
  requiredOption(): this {
    if (this._lastOption) {
      this._lastOption.mandatory = true;
    }
    return this;
  }

  /**
   * Add a subcommand
   */
  subcommand(name: string, description: string, config: CommandConfig = {}): Command {
    const cmd = new Command(name, description, config);
    cmd._parentCmd = this;
    this._subcommands.set(name, cmd);
    return cmd;
  }

  /**
   * Add an alias for this command
   */
  alias(aliasName: string): this {
    this._aliases.add(aliasName);
    if (this._onAlias) {
      this._onAlias(aliasName, this.name);
    }
    return this;
  }

  /**
   * Set the action handler
   */
  action(handler: ActionHandler): this {
    this._action = handler;
    return this;
  }

  /**
   * Add an example
   */
  example(example: string): this {
    this._examples.push(example);
    return this;
  }

  /**
   * Check if command has a subcommand
   */
  hasSubcommand(name: string): boolean {
    return this._subcommands.has(name);
  }

  /**
   * Get a subcommand by name
   */
  getSubcommand(name: string): Command | undefined {
    return this._subcommands.get(name);
  }

  /**
   * Get all subcommands
   */
  getSubcommands(): Command[] {
    return Array.from(this._subcommands.values());
  }

  /**
   * Get all arguments
   */
  getArguments(): Argument[] {
    return this._arguments;
  }

  /**
   * Get an argument by name
   */
  getArgument(name: string): Argument | undefined {
    return this._arguments.find((a) => a.name === name);
  }

  /**
   * Get an option by name
   */
  getOption(name: string): Option | undefined {
    return this._options.get(name);
  }

  /**
   * Get option by short flag
   */
  getOptionByShort(short: string): Option | undefined {
    const name = this._shortOptions.get(short);
    return name ? this._options.get(name) : undefined;
  }

  /**
   * Get all options
   */
  getOptions(): Option[] {
    return Array.from(this._options.values());
  }

  /**
   * Get the action handler
   */
  getAction(): ActionHandler | undefined {
    return this._action;
  }

  /**
   * Get all aliases
   */
  getAliases(): string[] {
    return Array.from(this._aliases);
  }

  /**
   * Generate help text for this command
   */
  helpText(): string {
    const lines: string[] = [];

    // Description
    lines.push(`${this.name} - ${this.description}`);
    lines.push('');

    // Usage
    let usage = this.name;
    if (this._arguments.length > 0) {
      for (const arg of this._arguments) {
        const bracket = arg.required ? '<' : '[';
        const closeBracket = arg.required ? '>' : ']';
        const dots = arg.variadic ? '...' : '';
        usage += ` ${bracket}${arg.name}${dots}${closeBracket}`;
      }
    }
    if (this._options.size > 0) {
      usage += ' [options]';
    }
    lines.push('Usage:');
    lines.push(`  ${usage}`);
    lines.push('');

    // Arguments
    if (this._arguments.length > 0) {
      lines.push('Arguments:');
      for (const arg of this._arguments) {
        const defaultStr = arg.defaultValue ? ` (default: ${arg.defaultValue})` : '';
        lines.push(`  ${arg.name.padEnd(15)} ${arg.description}${defaultStr}`);
      }
      lines.push('');
    }

    // Options
    const visibleOptions = Array.from(this._options.values()).filter((o) => !o.hidden);
    if (visibleOptions.length > 0) {
      lines.push('Options:');
      for (const opt of visibleOptions) {
        const shortStr = opt.short ? `-${opt.short}, ` : '    ';
        const negStr = opt.negatable ? 'no-' : '';
        const valueStr = opt.type === 'boolean' && !opt.negatable ? '' : opt.required ? ' <value>' : ' [value]';
        const defaultStr = opt.defaultValue !== undefined ? ` (default: ${opt.defaultValue})` : '';
        lines.push(`  ${shortStr}--${negStr}${opt.name}${valueStr}`.padEnd(25) + ` ${opt.description}${defaultStr}`);
      }
      lines.push('');
    }

    // Examples
    if (this._examples.length > 0) {
      lines.push('Examples:');
      for (const example of this._examples) {
        lines.push(`  ${example}`);
      }
      lines.push('');
    }

    return lines.join('\n');
  }
}

// ============================================================================
// CLI Class
// ============================================================================

/**
 * Main CLI application class
 */
export class CLI {
  public readonly name: string;
  public readonly version: string;
  public readonly description?: string;
  public readonly usage?: string;
  public readonly examples?: string[];

  private _commands: Map<string, Command> = new Map();
  private _commandAliases: Map<string, string> = new Map();
  private _globalOptions: Map<string, Option> = new Map();
  private _globalShortOptions: Map<string, string> = new Map();
  private _hooks: Map<HookType, HookHandler[]> = new Map();
  private _output: OutputStream;
  private _errorOutput: OutputStream;
  private _helpFormatter?: (help: string) => string;
  private _versionFormatter?: (name: string, version: string) => string;

  constructor(config: CLIConfig) {
    this.name = config.name;
    this.version = config.version;
    this.description = config.description;
    this.usage = config.usage;
    this.examples = config.examples;
    this._output = config.output ?? { write: (s) => console.log(s) };
    this._errorOutput = config.errorOutput ?? { write: (s) => console.error(s) };
    this._helpFormatter = config.helpFormatter;
    this._versionFormatter = config.versionFormatter;

    // Add default global options
    this._addGlobalOption('-h, --help', 'Show help');
    this._addGlobalOption('-V, --version', 'Show version');
  }

  private _addGlobalOption(flags: string, description: string): void {
    const opt = new Option(flags, description);
    this._globalOptions.set(opt.name, opt);
    if (opt.short) {
      this._globalShortOptions.set(opt.short, opt.name);
    }
  }

  /**
   * Register a command
   */
  command(name: string, description: string, config: CommandConfig = {}): Command {
    if (this._commands.has(name) || this._commandAliases.has(name)) {
      throw new Error(`Command "${name}" already exists`);
    }

    const cmd = new Command(name, description, config);
    this._commands.set(name, cmd);

    // Set up alias registration callback
    cmd._setOnAlias((alias, commandName) => {
      this._commandAliases.set(alias, commandName);
    });

    return cmd;
  }

  /**
   * Add a global option
   */
  option(flags: string, description: string, config: OptionConfig = {}): this {
    const opt = new Option(flags, description, config);
    this._globalOptions.set(opt.name, opt);
    if (opt.short) {
      this._globalShortOptions.set(opt.short, opt.name);
    }
    return this;
  }

  /**
   * Check if a command exists
   */
  hasCommand(name: string): boolean {
    return this._commands.has(name) || this._commandAliases.has(name);
  }

  /**
   * Get a command by name or alias
   */
  getCommand(name: string): Command | undefined {
    if (this._commands.has(name)) {
      return this._commands.get(name);
    }
    const realName = this._commandAliases.get(name);
    return realName ? this._commands.get(realName) : undefined;
  }

  /**
   * Get all commands
   */
  getCommands(): Command[] {
    return Array.from(this._commands.values());
  }

  /**
   * Check if a global option exists
   */
  hasOption(name: string): boolean {
    return this._globalOptions.has(name);
  }

  /**
   * Get a global option
   */
  getOption(name: string): Option | undefined {
    return this._globalOptions.get(name);
  }

  /**
   * Register a hook
   */
  hook(type: HookType, handler: HookHandler): this {
    if (!this._hooks.has(type)) {
      this._hooks.set(type, []);
    }
    this._hooks.get(type)!.push(handler);
    return this;
  }

  /**
   * Generate help text
   */
  helpText(): string {
    const lines: string[] = [];

    // Header
    lines.push(`${this.name} v${this.version}`);
    if (this.description) {
      lines.push(this.description);
    }
    lines.push('');

    // Usage
    lines.push('Usage:');
    lines.push(`  ${this.usage ?? `${this.name} <command> [options]`}`);
    lines.push('');

    // Commands
    const visibleCommands = Array.from(this._commands.values()).filter((c) => !c.hidden);
    if (visibleCommands.length > 0) {
      lines.push('Commands:');
      for (const cmd of visibleCommands) {
        lines.push(`  ${cmd.name.padEnd(15)} ${cmd.description}`);
      }
      lines.push('');
    }

    // Global options
    lines.push('Options:');
    for (const opt of this._globalOptions.values()) {
      const shortStr = opt.short ? `-${opt.short}, ` : '    ';
      lines.push(`  ${shortStr}--${opt.name}`.padEnd(20) + ` ${opt.description}`);
    }
    lines.push('');

    // Examples
    if (this.examples && this.examples.length > 0) {
      lines.push('Examples:');
      for (const example of this.examples) {
        lines.push(`  ${example}`);
      }
      lines.push('');
    }

    return lines.join('\n');
  }

  /**
   * Generate completion script
   */
  completionScript(shell: 'bash' | 'zsh'): string {
    const commands = Array.from(this._commands.keys()).join(' ');

    if (shell === 'bash') {
      const optionsForCommands: string[] = [];
      for (const [name, cmd] of this._commands) {
        const opts = cmd.getOptions().map((o) => `--${o.name}`).join(' ');
        const choices = cmd.getOptions()
          .filter((o) => o.choices)
          .map((o) => o.choices!.join(' '))
          .join(' ');
        optionsForCommands.push(`    ${name}) COMPREPLY=( $(compgen -W "${opts} ${choices}" -- "$cur") ) ;;`);
      }

      return `
_${this.name}_completions() {
  local cur prev commands
  COMPREPLY=()
  cur="\${COMP_WORDS[COMP_CWORD]}"
  prev="\${COMP_WORDS[COMP_CWORD-1]}"
  commands="${commands}"

  if [[ \${COMP_CWORD} -eq 1 ]] ; then
    COMPREPLY=( $(compgen -W "$commands --help --version" -- "$cur") )
    return 0
  fi

  case "\${COMP_WORDS[1]}" in
${optionsForCommands.join('\n')}
  esac
}
complete -F _${this.name}_completions ${this.name}
`.trim();
    }

    // ZSH completion
    return `
#compdef ${this.name}

_${this.name}() {
  local -a commands
  commands=(
${Array.from(this._commands.entries()).map(([name, cmd]) => `    '${name}:${cmd.description}'`).join('\n')}
  )

  _arguments -C \\
    '1:command:->command' \\
    '*::arg:->args'

  case "$state" in
    command)
      _describe 'command' commands
      ;;
  esac
}

_${this.name}
`.trim();
  }

  /**
   * Parse command line arguments and execute
   */
  async parse(args: string[]): Promise<number> {
    try {
      // Handle empty args - show help
      if (args.length === 0) {
        this._showHelp();
        return 0;
      }

      // Check for global flags first
      const firstArg = args[0];

      // Handle help
      if (firstArg === '-h' || firstArg === '--help' || firstArg === 'help') {
        if (args.length > 1 && firstArg === 'help') {
          const cmdName = args[1]!;
          const cmd = this.getCommand(cmdName);
          if (cmd) {
            this._showCommandHelp(cmd);
            return 0;
          }
        }
        this._showHelp();
        return 0;
      }

      // Handle version
      if (firstArg === '-V' || firstArg === '--version' || firstArg === 'version') {
        this._showVersion();
        return 0;
      }

      // Find and execute command
      const commandName = args[0]!;
      let command = this.getCommand(commandName);

      if (!command) {
        // Try to find similar command
        const similar = this._findSimilar(commandName, Array.from(this._commands.keys()));
        const suggestion = similar ? ` Did you mean "${similar}"?` : '';
        const errorMessage = `Unknown command: ${commandName}.${suggestion}`;
        this._errorOutput.write(`${errorMessage}\n`);
        this._errorOutput.write(`Try "${this.name} --help" for available commands.\n`);
        throw new ParseError(errorMessage, 'UNKNOWN_COMMAND');
      }

      // Check for subcommand
      let commandArgs = args.slice(1);
      if (commandArgs.length > 0 && command.hasSubcommand(commandArgs[0]!)) {
        const subName = commandArgs[0]!;
        command = command.getSubcommand(subName)!;
        commandArgs = commandArgs.slice(1);
      }

      // Check for command help
      if (commandArgs.includes('--help') || commandArgs.includes('-h')) {
        this._showCommandHelp(command);
        return 0;
      }

      // Parse options and arguments
      const { options, positionals } = this._parseArgs(commandArgs, command);

      // Build arguments object
      const argsObj: Record<string, unknown> = {};
      const cmdArguments = command.getArguments();
      let posIndex = 0;

      for (const arg of cmdArguments) {
        if (arg.variadic) {
          const values = positionals.slice(posIndex);
          if (arg.required && values.length === 0) {
            this._errorOutput.write(`Missing required argument: ${arg.name}\n`);
            throw new ParseError(`Missing required argument: ${arg.name}`, 'MISSING_ARGUMENT', { argument: arg.name });
          }
          argsObj[arg.name] = values;
          for (const v of values) {
            arg.validate(v);
          }
        } else {
          const value = positionals[posIndex] ?? arg.defaultValue;
          if (arg.required && value === undefined) {
            this._errorOutput.write(`Missing required argument: ${arg.name}\n`);
            throw new ParseError(`Missing required argument: ${arg.name}`, 'MISSING_ARGUMENT', { argument: arg.name });
          }
          if (value !== undefined) {
            arg.validate(value);
          }
          argsObj[arg.name] = value;
          posIndex++;
        }
      }

      // Call preAction hooks
      const context: HookContext = { command, options, args: argsObj };
      await this._runHooks('preAction', context);

      // Execute action
      const action = command.getAction();
      let exitCode = 0;
      if (action) {
        const result = await action(options, argsObj);
        if (typeof result === 'number') {
          exitCode = result;
        }
      }

      // Call postAction hooks
      await this._runHooks('postAction', context);

      return exitCode;
    } catch (error) {
      // Call preError hooks
      await this._runHooks('preError', {
        command: new Command('', ''),
        options: {},
        args: {},
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Parse arguments for a command
   */
  private _parseArgs(
    args: string[],
    command: Command
  ): { options: Record<string, unknown>; positionals: string[] } {
    const options: Record<string, unknown> = {};
    const positionals: string[] = [];

    // Initialize with defaults
    for (const opt of command.getOptions()) {
      if (opt.defaultValue !== undefined) {
        options[opt.name] = opt.defaultValue;
      } else if (opt.negatable) {
        options[opt.name] = true;
      }

      // Check environment variable
      if (opt.env && process.env[opt.env] !== undefined) {
        options[opt.name] = opt.parseValue(process.env[opt.env]!);
      }
    }

    let i = 0;
    while (i < args.length) {
      const arg = args[i]!;

      if (arg.startsWith('--')) {
        // Long option
        const eqIndex = arg.indexOf('=');
        let optName: string;
        let optValue: string | undefined;

        if (eqIndex !== -1) {
          optName = arg.slice(2, eqIndex);
          optValue = arg.slice(eqIndex + 1);
        } else {
          optName = arg.slice(2);
        }

        // Handle --no-* options
        if (optName.startsWith('no-')) {
          const baseName = optName.slice(3);
          const opt = command.getOption(baseName);
          if (opt?.negatable) {
            options[baseName] = false;
            i++;
            continue;
          }
        }

        const opt = command.getOption(optName);
        if (!opt) {
          const allOptions = command.getOptions().map((o) => `--${o.name}`);
          const similar = this._findSimilar(`--${optName}`, allOptions);
          const suggestion = similar ? ` Did you mean "${similar}"?` : '';
          const errorMessage = `Unknown option: --${optName}.${suggestion}`;
          this._errorOutput.write(`${errorMessage}\n`);
          this._errorOutput.write(`Try "${this.name} ${command.name} --help" for available options.\n`);
          throw new ParseError(errorMessage, 'UNKNOWN_OPTION', { option: optName });
        }

        if (opt.type === 'boolean' && !optValue) {
          options[opt.name] = true;
        } else {
          if (optValue === undefined) {
            i++;
            if (i >= args.length || args[i]!.startsWith('-')) {
              this._errorOutput.write(`Option --${optName} requires a value\n`);
              throw new ParseError(`Option --${optName} requires a value`, 'MISSING_VALUE', { option: optName });
            }
            optValue = args[i]!;
          }
          const parsed = opt.parseValue(optValue);
          opt.validate(parsed);
          options[opt.name] = parsed;
        }
      } else if (arg.startsWith('-') && arg.length > 1) {
        // Short option(s)
        const chars = arg.slice(1);

        for (let j = 0; j < chars.length; j++) {
          const char = chars[j]!;
          const opt = command.getOptionByShort(char);

          if (!opt) {
            throw new ParseError(`Unknown option: -${char}`, 'UNKNOWN_OPTION', { option: char });
          }

          if (opt.type === 'boolean') {
            options[opt.name] = true;
          } else {
            // Value is the rest of this arg or next arg
            let value: string;
            if (j < chars.length - 1) {
              value = chars.slice(j + 1);
            } else {
              i++;
              if (i >= args.length) {
                this._errorOutput.write(`Option -${char} requires a value\n`);
                throw new ParseError(`Option -${char} requires a value`, 'MISSING_VALUE', { option: char });
              }
              value = args[i]!;
            }
            const parsed = opt.parseValue(value);
            opt.validate(parsed);
            options[opt.name] = parsed;
            break;
          }
        }
      } else {
        // Positional argument
        positionals.push(arg!);
      }

      i++;
    }

    // Check for mandatory options
    for (const opt of command.getOptions()) {
      if (opt.mandatory && options[opt.name] === undefined) {
        this._errorOutput.write(`Missing required option: --${opt.name}\n`);
        throw new ParseError(`Missing required option: --${opt.name}`, 'MISSING_REQUIRED_OPTION', { option: opt.name });
      }
    }

    return { options, positionals };
  }

  /**
   * Show main help
   */
  private _showHelp(): void {
    let help = this.helpText();
    if (this._helpFormatter) {
      help = this._helpFormatter(help);
    }
    this._output.write(help);
  }

  /**
   * Show command help
   */
  private _showCommandHelp(command: Command): void {
    let help = command.helpText();
    if (this._helpFormatter) {
      help = this._helpFormatter(help);
    }
    this._output.write(help);
  }

  /**
   * Show version
   */
  private _showVersion(): void {
    let version: string;
    if (this._versionFormatter) {
      version = this._versionFormatter(this.name, this.version);
    } else {
      version = `${this.name} v${this.version}`;
    }
    this._output.write(version);
  }

  /**
   * Run hooks of a given type
   */
  private async _runHooks(type: HookType, context: HookContext): Promise<void> {
    const handlers = this._hooks.get(type) ?? [];
    for (const handler of handlers) {
      await handler(context);
    }
  }

  /**
   * Find similar string (for suggestions)
   */
  private _findSimilar(input: string, candidates: string[]): string | null {
    let minDistance = Infinity;
    let closest: string | null = null;

    for (const candidate of candidates) {
      const distance = this._levenshteinDistance(input, candidate);
      if (distance < minDistance && distance <= 3) {
        minDistance = distance;
        closest = candidate;
      }
    }

    return closest;
  }

  /**
   * Calculate Levenshtein distance
   */
  private _levenshteinDistance(a: string, b: string): number {
    const matrix: number[][] = [];

    for (let i = 0; i <= b.length; i++) {
      matrix[i] = [i];
    }

    for (let j = 0; j <= a.length; j++) {
      matrix[0]![j] = j;
    }

    for (let i = 1; i <= b.length; i++) {
      for (let j = 1; j <= a.length; j++) {
        if (b[i - 1] === a[j - 1]) {
          matrix[i]![j] = matrix[i - 1]![j - 1]!;
        } else {
          matrix[i]![j] = Math.min(
            matrix[i - 1]![j - 1]! + 1,
            matrix[i]![j - 1]! + 1,
            matrix[i - 1]![j]! + 1
          );
        }
      }
    }

    return matrix[b.length]![a.length]!;
  }
}

// ============================================================================
// Factory Function
// ============================================================================

/**
 * Create a new CLI instance
 */
export function createCLI(config: CLIConfig): CLI {
  return new CLI(config);
}
