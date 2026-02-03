/**
 * MongoDB Connection String Parser
 *
 * Parses standard MongoDB connection strings (mongodb:// and mongodb+srv://)
 * and extracts connection parameters.
 *
 * @module utils/connection-string
 */

// ============================================================================
// Types
// ============================================================================

/**
 * Parsed MongoDB connection string options
 */
export interface ConnectionOptions {
  /** Authentication source database */
  authSource?: string;
  /** Replica set name */
  replicaSet?: string;
  /** Enable SSL/TLS */
  ssl?: boolean;
  /** Enable TLS */
  tls?: boolean;
  /** Read preference */
  readPreference?: 'primary' | 'primaryPreferred' | 'secondary' | 'secondaryPreferred' | 'nearest';
  /** Write concern */
  w?: number | 'majority';
  /** Write concern timeout in milliseconds */
  wtimeoutMS?: number;
  /** Journal write concern */
  journal?: boolean;
  /** Connection timeout in milliseconds */
  connectTimeoutMS?: number;
  /** Socket timeout in milliseconds */
  socketTimeoutMS?: number;
  /** Server selection timeout in milliseconds */
  serverSelectionTimeoutMS?: number;
  /** Max pool size */
  maxPoolSize?: number;
  /** Min pool size */
  minPoolSize?: number;
  /** Max idle time in milliseconds */
  maxIdleTimeMS?: number;
  /** Retry writes */
  retryWrites?: boolean;
  /** Retry reads */
  retryReads?: boolean;
  /** Application name */
  appName?: string;
  /** Compressors */
  compressors?: string[];
  /** Auth mechanism */
  authMechanism?: string;
  /** Direct connection */
  directConnection?: boolean;
  /** Additional options */
  [key: string]: unknown;
}

/**
 * Host information extracted from connection string
 */
export interface HostInfo {
  /** Hostname or IP address */
  host: string;
  /** Port number (default: 27017) */
  port: number;
}

/**
 * Parsed MongoDB connection string
 */
export interface ParsedConnectionString {
  /** Original connection string (with password masked) */
  uri: string;
  /** Connection scheme (mongodb or mongodb+srv) */
  scheme: 'mongodb' | 'mongodb+srv';
  /** Username for authentication */
  username?: string;
  /** Password for authentication */
  password?: string;
  /** List of hosts */
  hosts: HostInfo[];
  /** Default database name */
  database?: string;
  /** Connection options */
  options: ConnectionOptions;
}

// ============================================================================
// Errors
// ============================================================================

/**
 * Error thrown when parsing an invalid connection string
 */
export class ConnectionStringParseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ConnectionStringParseError';
  }
}

// ============================================================================
// Parser
// ============================================================================

/**
 * Parse a MongoDB connection string
 *
 * Supports both standard mongodb:// and DNS seedlist mongodb+srv:// formats.
 *
 * @param connectionString - MongoDB connection string
 * @returns Parsed connection string components
 * @throws ConnectionStringParseError if the connection string is invalid
 *
 * @example
 * ```typescript
 * // Simple connection
 * const parsed = parseConnectionString('mongodb://localhost:27017/mydb');
 *
 * // With authentication
 * const parsed = parseConnectionString('mongodb://user:pass@localhost:27017/mydb?authSource=admin');
 *
 * // Replica set
 * const parsed = parseConnectionString('mongodb://host1:27017,host2:27017,host3:27017/mydb?replicaSet=myrs');
 *
 * // SRV format
 * const parsed = parseConnectionString('mongodb+srv://user:pass@cluster.mongodb.net/mydb');
 * ```
 */
export function parseConnectionString(connectionString: string): ParsedConnectionString {
  if (!connectionString || typeof connectionString !== 'string') {
    throw new ConnectionStringParseError('Connection string must be a non-empty string');
  }

  const trimmed = connectionString.trim();

  // Check for valid scheme
  let scheme: 'mongodb' | 'mongodb+srv';
  let remainder: string;

  if (trimmed.startsWith('mongodb+srv://')) {
    scheme = 'mongodb+srv';
    remainder = trimmed.slice('mongodb+srv://'.length);
  } else if (trimmed.startsWith('mongodb://')) {
    scheme = 'mongodb';
    remainder = trimmed.slice('mongodb://'.length);
  } else {
    throw new ConnectionStringParseError(
      'Invalid connection string: must start with mongodb:// or mongodb+srv://'
    );
  }

  if (!remainder) {
    throw new ConnectionStringParseError('Invalid connection string: missing host information');
  }

  // Split into credentials+hosts and path+query
  let credentialsAndHosts: string;
  let pathAndQuery: string;

  const slashIndex = remainder.indexOf('/');
  if (slashIndex === -1) {
    credentialsAndHosts = remainder;
    pathAndQuery = '';
  } else {
    credentialsAndHosts = remainder.slice(0, slashIndex);
    pathAndQuery = remainder.slice(slashIndex + 1);
  }

  // Extract username and password
  let username: string | undefined;
  let password: string | undefined;
  let hostsString: string;

  const atIndex = credentialsAndHosts.lastIndexOf('@');
  if (atIndex !== -1) {
    const credentials = credentialsAndHosts.slice(0, atIndex);
    hostsString = credentialsAndHosts.slice(atIndex + 1);

    const colonIndex = credentials.indexOf(':');
    if (colonIndex === -1) {
      username = decodeURIComponent(credentials);
    } else {
      username = decodeURIComponent(credentials.slice(0, colonIndex));
      password = decodeURIComponent(credentials.slice(colonIndex + 1));
    }
  } else {
    hostsString = credentialsAndHosts;
  }

  if (!hostsString) {
    throw new ConnectionStringParseError('Invalid connection string: missing host information');
  }

  // Parse hosts
  const hosts = parseHosts(hostsString, scheme);

  // Parse database and options
  let database: string | undefined;
  let optionsString: string | undefined;

  const questionIndex = pathAndQuery.indexOf('?');
  if (questionIndex === -1) {
    database = pathAndQuery || undefined;
  } else {
    database = pathAndQuery.slice(0, questionIndex) || undefined;
    optionsString = pathAndQuery.slice(questionIndex + 1);
  }

  // Decode database name
  if (database) {
    database = decodeURIComponent(database);
  }

  // Parse options
  const options = parseOptions(optionsString);

  // Create masked URI for safe logging
  const maskedUri = createMaskedUri(scheme, username, hostsString, database, optionsString);

  return {
    uri: maskedUri,
    scheme,
    username,
    password,
    hosts,
    database,
    options,
  };
}

/**
 * Parse host string into HostInfo array
 */
function parseHosts(hostsString: string, scheme: 'mongodb' | 'mongodb+srv'): HostInfo[] {
  const hostParts = hostsString.split(',');
  const hosts: HostInfo[] = [];

  for (const hostPart of hostParts) {
    const trimmed = hostPart.trim();
    if (!trimmed) {
      continue;
    }

    // Handle IPv6 addresses in brackets
    let host: string;
    let port: number;

    if (trimmed.startsWith('[')) {
      // IPv6 address
      const bracketEnd = trimmed.indexOf(']');
      if (bracketEnd === -1) {
        throw new ConnectionStringParseError(`Invalid IPv6 address: ${trimmed}`);
      }
      host = trimmed.slice(1, bracketEnd);
      const portPart = trimmed.slice(bracketEnd + 1);
      if (portPart.startsWith(':')) {
        port = parsePort(portPart.slice(1));
      } else if (portPart) {
        throw new ConnectionStringParseError(`Invalid host format: ${trimmed}`);
      } else {
        port = scheme === 'mongodb+srv' ? 27017 : 27017;
      }
    } else {
      // IPv4 or hostname
      const colonIndex = trimmed.lastIndexOf(':');
      if (colonIndex === -1) {
        host = trimmed;
        port = 27017;
      } else {
        host = trimmed.slice(0, colonIndex);
        port = parsePort(trimmed.slice(colonIndex + 1));
      }
    }

    if (!host) {
      throw new ConnectionStringParseError('Invalid connection string: empty host');
    }

    // SRV records should not have ports specified
    if (scheme === 'mongodb+srv' && hostParts.length > 1) {
      throw new ConnectionStringParseError(
        'mongodb+srv:// URIs must have exactly one host without a port'
      );
    }

    hosts.push({ host, port });
  }

  if (hosts.length === 0) {
    throw new ConnectionStringParseError('Invalid connection string: no valid hosts found');
  }

  return hosts;
}

/**
 * Parse port string to number
 */
function parsePort(portString: string): number {
  const port = parseInt(portString, 10);
  if (isNaN(port) || port < 1 || port > 65535) {
    throw new ConnectionStringParseError(`Invalid port: ${portString}`);
  }
  return port;
}

/**
 * Parse query string options
 */
function parseOptions(optionsString: string | undefined): ConnectionOptions {
  if (!optionsString) {
    return {};
  }

  const options: ConnectionOptions = {};
  const pairs = optionsString.split('&');

  for (const pair of pairs) {
    if (!pair) {
      continue;
    }

    const equalIndex = pair.indexOf('=');
    if (equalIndex === -1) {
      continue;
    }

    const key = decodeURIComponent(pair.slice(0, equalIndex));
    const value = decodeURIComponent(pair.slice(equalIndex + 1));

    // Convert known options to their proper types
    switch (key) {
      case 'authSource':
        options.authSource = value;
        break;
      case 'replicaSet':
        options.replicaSet = value;
        break;
      case 'ssl':
      case 'tls':
        options[key] = value === 'true';
        break;
      case 'readPreference':
        options.readPreference = value as ConnectionOptions['readPreference'];
        break;
      case 'w':
        options.w = value === 'majority' ? 'majority' : parseInt(value, 10);
        break;
      case 'wtimeoutMS':
      case 'connectTimeoutMS':
      case 'socketTimeoutMS':
      case 'serverSelectionTimeoutMS':
      case 'maxPoolSize':
      case 'minPoolSize':
      case 'maxIdleTimeMS':
        options[key] = parseInt(value, 10);
        break;
      case 'journal':
      case 'retryWrites':
      case 'retryReads':
      case 'directConnection':
        options[key] = value === 'true';
        break;
      case 'appName':
        options.appName = value;
        break;
      case 'compressors':
        options.compressors = value.split(',');
        break;
      case 'authMechanism':
        options.authMechanism = value;
        break;
      default:
        // Store unknown options as strings
        options[key] = value;
        break;
    }
  }

  return options;
}

/**
 * Create a masked URI for safe logging (password hidden)
 */
function createMaskedUri(
  scheme: 'mongodb' | 'mongodb+srv',
  username: string | undefined,
  hostsString: string,
  database: string | undefined,
  optionsString: string | undefined
): string {
  let uri = `${scheme}://`;

  if (username) {
    uri += `${encodeURIComponent(username)}:****@`;
  }

  uri += hostsString;

  if (database) {
    uri += `/${encodeURIComponent(database)}`;
  }

  if (optionsString) {
    uri += `?${optionsString}`;
  }

  return uri;
}

/**
 * Build a connection string from components
 *
 * @param components - Connection string components
 * @returns MongoDB connection string
 *
 * @example
 * ```typescript
 * const uri = buildConnectionString({
 *   scheme: 'mongodb',
 *   hosts: [{ host: 'localhost', port: 27017 }],
 *   database: 'mydb',
 *   username: 'user',
 *   password: 'pass',
 *   options: { authSource: 'admin' }
 * });
 * // Returns: mongodb://user:pass@localhost:27017/mydb?authSource=admin
 * ```
 */
export function buildConnectionString(
  components: Omit<ParsedConnectionString, 'uri'>
): string {
  const { scheme, username, password, hosts, database, options } = components;

  let uri = `${scheme}://`;

  // Add credentials
  if (username) {
    uri += encodeURIComponent(username);
    if (password) {
      uri += `:${encodeURIComponent(password)}`;
    }
    uri += '@';
  }

  // Add hosts
  const hostStrings = hosts.map((h) => {
    if (h.host.includes(':')) {
      // IPv6
      return `[${h.host}]:${h.port}`;
    }
    return h.port === 27017 ? h.host : `${h.host}:${h.port}`;
  });
  uri += hostStrings.join(',');

  // Add database
  if (database) {
    uri += `/${encodeURIComponent(database)}`;
  } else if (Object.keys(options).length > 0) {
    uri += '/';
  }

  // Add options
  if (Object.keys(options).length > 0) {
    const optionPairs: string[] = [];
    for (const [key, value] of Object.entries(options)) {
      if (value === undefined) {
        continue;
      }
      if (Array.isArray(value)) {
        optionPairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(value.join(','))}`);
      } else {
        optionPairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);
      }
    }
    if (optionPairs.length > 0) {
      uri += `?${optionPairs.join('&')}`;
    }
  }

  return uri;
}

/**
 * Check if a string looks like a MongoDB connection string
 *
 * @param str - String to check
 * @returns True if the string appears to be a MongoDB connection string
 */
export function isConnectionString(str: string): boolean {
  if (!str || typeof str !== 'string') {
    return false;
  }
  const trimmed = str.trim();
  return trimmed.startsWith('mongodb://') || trimmed.startsWith('mongodb+srv://');
}
