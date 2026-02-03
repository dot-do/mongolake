/**
 * Wrangler Configuration Builder
 *
 * Utilities for creating and serializing Wrangler configuration.
 *
 * @module cli/dev/config
 */

import type { WranglerConfig, WranglerConfigOptions } from './types.js';
import {
  DEFAULT_WORKER_MAIN,
  DEFAULT_R2_BUCKET_NAME,
  DEFAULT_R2_BINDING,
  DEFAULT_DO_BINDING,
  DEFAULT_DO_CLASS,
} from './constants.js';

// ============================================================================
// TOML Serialization
// ============================================================================

/**
 * Convert a Wrangler configuration object to TOML format.
 */
function configToTOML(config: Omit<WranglerConfig, 'toTOML'>): string {
  const lines: string[] = [];

  // Basic configuration
  lines.push(`name = "${config.name}"`);
  lines.push(`main = "${config.main}"`);
  lines.push(`compatibility_date = "${config.compatibility_date}"`);

  // Compatibility flags (if any)
  if (config.compatibility_flags?.length) {
    const flags = config.compatibility_flags.map((f) => `"${f}"`).join(', ');
    lines.push(`compatibility_flags = [${flags}]`);
  }

  // R2 bucket configuration
  lines.push('');
  lines.push('[[r2_buckets]]');
  for (const bucket of config.r2_buckets) {
    lines.push(`binding = "${bucket.binding}"`);
    lines.push(`bucket_name = "${bucket.bucket_name}"`);
    if (bucket.preview_bucket_name) {
      lines.push(`preview_bucket_name = "${bucket.preview_bucket_name}"`);
    }
  }

  // Durable Objects configuration
  lines.push('');
  lines.push('[[durable_objects.bindings]]');
  for (const binding of config.durable_objects.bindings) {
    lines.push(`name = "${binding.name}"`);
    lines.push(`class_name = "${binding.class_name}"`);
  }

  // Miniflare persistence configuration
  if (config.miniflare) {
    lines.push('');
    lines.push('[miniflare]');
    lines.push(`d1_persist = "${config.miniflare.d1_persist}"`);
    lines.push(`kv_persist = "${config.miniflare.kv_persist}"`);
    lines.push(`r2_persist = "${config.miniflare.r2_persist}"`);
  }

  // Dev server configuration
  lines.push('');
  lines.push('[dev]');
  lines.push(`port = ${config.dev.port}`);
  lines.push(`local = ${config.dev.local}`);

  // Environment variables
  if (config.vars && Object.keys(config.vars).length > 0) {
    lines.push('');
    lines.push('[vars]');
    for (const [key, value] of Object.entries(config.vars)) {
      lines.push(`${key} = "${value}"`);
    }
  }

  return lines.join('\n');
}

// ============================================================================
// Configuration Factory
// ============================================================================

/**
 * Create a Wrangler configuration object with sensible defaults.
 *
 * @param options - Configuration options
 * @returns Complete Wrangler configuration with TOML serialization
 *
 * @example
 * ```typescript
 * const config = createWranglerConfig({
 *   name: 'my-worker',
 *   port: 8080,
 *   path: '.mongolake',
 *   vars: { DEBUG: 'true' }
 * });
 *
 * // Write to wrangler.toml
 * fs.writeFileSync('wrangler.toml', config.toTOML());
 * ```
 */
export function createWranglerConfig(options: WranglerConfigOptions): WranglerConfig {
  const { name, port, path: basePath, vars } = options;

  const configData = {
    name,
    main: DEFAULT_WORKER_MAIN,
    compatibility_date: new Date().toISOString().split('T')[0]!,
    r2_buckets: [
      {
        binding: DEFAULT_R2_BINDING,
        bucket_name: DEFAULT_R2_BUCKET_NAME,
        preview_bucket_name: `${basePath}/r2`,
      },
    ],
    durable_objects: {
      bindings: [
        {
          name: DEFAULT_DO_BINDING,
          class_name: DEFAULT_DO_CLASS,
        },
      ],
    },
    miniflare: {
      d1_persist: `${basePath}/d1`,
      kv_persist: `${basePath}/kv`,
      r2_persist: `${basePath}/r2`,
    },
    dev: {
      port,
      local: true,
    },
    vars,
  };

  // Create the config object with the toTOML method bound to the data
  const config: WranglerConfig = {
    ...configData,
    toTOML(): string {
      return configToTOML(this);
    },
  };

  return config;
}

// ============================================================================
// Environment File Parsing
// ============================================================================

/**
 * Parse environment variables from a .env file content.
 *
 * @param content - Content of the .env file
 * @returns Parsed environment variables
 *
 * @example
 * ```typescript
 * const vars = parseEnvFile(`
 *   # Comment
 *   KEY1=value1
 *   KEY2=value2
 * `);
 * // { KEY1: 'value1', KEY2: 'value2' }
 * ```
 */
export function parseEnvFile(content: string | Buffer | undefined | null): Record<string, string> {
  const vars: Record<string, string> = {};

  // Handle edge cases (null, undefined, Buffer)
  if (!content) {
    return vars;
  }

  const text = typeof content === 'string' ? content : content.toString();

  for (const line of text.split('\n')) {
    const trimmed = line.trim();

    // Skip empty lines and comments
    if (!trimmed || trimmed.startsWith('#')) {
      continue;
    }

    // Parse KEY=value format
    const eqIndex = trimmed.indexOf('=');
    if (eqIndex !== -1) {
      const key = trimmed.slice(0, eqIndex).trim();
      const value = trimmed.slice(eqIndex + 1).trim();
      vars[key] = value;
    }
  }

  return vars;
}
