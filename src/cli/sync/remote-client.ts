/**
 * Remote Client for MongoLake Sync
 *
 * Handles HTTP communication with the remote MongoLake server.
 */

import { colors, formatBytes } from '../utils.js';
import type { DatabaseState } from './types.js';

export class RemoteClient {
  private baseUrl: string;
  private accessToken: string | null;
  private verbose: boolean;

  constructor(baseUrl: string, accessToken: string | null, verbose: boolean = false) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.accessToken = accessToken;
    this.verbose = verbose;
  }

  private getHeaders(): HeadersInit {
    const headers: HeadersInit = {
      'Content-Type': 'application/octet-stream',
      'User-Agent': 'mongolake-cli/0.1.0',
    };

    if (this.accessToken) {
      headers['Authorization'] = `Bearer ${this.accessToken}`;
    }

    return headers;
  }

  private log(message: string): void {
    if (this.verbose) {
      console.log(`${colors.dim}[remote] ${message}${colors.reset}`);
    }
  }

  async getState(database: string): Promise<DatabaseState | null> {
    try {
      const response = await fetch(`${this.baseUrl}/api/sync/${database}/state`, {
        method: 'GET',
        headers: this.getHeaders(),
      });

      if (response.status === 404) {
        return null;
      }

      if (!response.ok) {
        throw new Error(`Failed to get remote state: ${response.status} ${response.statusText}`);
      }

      return (await response.json()) as DatabaseState;
    } catch (error) {
      if (error instanceof Error && error.message.includes('fetch')) {
        throw new Error(`Cannot connect to remote: ${this.baseUrl}`);
      }
      throw error;
    }
  }

  async uploadFile(database: string, filePath: string, data: Uint8Array): Promise<void> {
    this.log(`Uploading ${filePath} (${formatBytes(data.length)})`);

    const response = await fetch(
      `${this.baseUrl}/api/sync/${database}/files/${encodeURIComponent(filePath)}`,
      {
        method: 'PUT',
        headers: this.getHeaders(),
        body: data,
      }
    );

    if (!response.ok) {
      throw new Error(`Failed to upload file: ${response.status} ${response.statusText}`);
    }
  }

  async downloadFile(database: string, filePath: string): Promise<Uint8Array> {
    this.log(`Downloading ${filePath}`);

    const response = await fetch(
      `${this.baseUrl}/api/sync/${database}/files/${encodeURIComponent(filePath)}`,
      {
        method: 'GET',
        headers: this.getHeaders(),
      }
    );

    if (!response.ok) {
      throw new Error(`Failed to download file: ${response.status} ${response.statusText}`);
    }

    const buffer = await response.arrayBuffer();
    return new Uint8Array(buffer);
  }

  async deleteFile(database: string, filePath: string): Promise<void> {
    this.log(`Deleting ${filePath}`);

    const response = await fetch(
      `${this.baseUrl}/api/sync/${database}/files/${encodeURIComponent(filePath)}`,
      {
        method: 'DELETE',
        headers: this.getHeaders(),
      }
    );

    if (!response.ok && response.status !== 404) {
      throw new Error(`Failed to delete file: ${response.status} ${response.statusText}`);
    }
  }

  async updateState(database: string, state: DatabaseState): Promise<void> {
    const response = await fetch(`${this.baseUrl}/api/sync/${database}/state`, {
      method: 'PUT',
      headers: {
        ...this.getHeaders(),
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(state),
    });

    if (!response.ok) {
      throw new Error(`Failed to update remote state: ${response.status} ${response.statusText}`);
    }
  }

  async listCollections(database: string): Promise<string[]> {
    const response = await fetch(`${this.baseUrl}/api/sync/${database}/collections`, {
      headers: this.accessToken ? { Authorization: `Bearer ${this.accessToken}` } : {},
    });

    if (!response.ok) {
      return [];
    }

    const data = (await response.json()) as { collections: string[] };
    return data.collections || [];
  }

  async checkHealth(): Promise<boolean> {
    try {
      const response = await fetch(`${this.baseUrl}/health`);
      return response.ok;
    } catch {
      return false;
    }
  }
}
