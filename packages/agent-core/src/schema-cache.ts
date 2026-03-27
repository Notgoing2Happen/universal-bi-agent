/**
 * Local Schema Cache
 *
 * Caches Universal Schema concepts locally so the desktop agent
 * can perform column mapping without querying the platform database.
 *
 * Stored at ~/.universal-bi/schema-cache.json
 */

import * as fs from 'fs';
import * as path from 'path';
import { getConfigDir } from './config';

const CACHE_FILE = path.join(getConfigDir(), 'schema-cache.json');

export interface SchemaCacheEntry {
  canonicalName: string;   // e.g., "customer.name"
  displayName: string;
  description: string;
  dataType: string;        // string, number, date, boolean
  semanticType: string;    // dimension, measure, time
  entity: string;          // e.g., "customer"
  field: string;           // e.g., "name"
}

interface SchemaCache {
  concepts: SchemaCacheEntry[];
  lastUpdated: string;
  version: string;
}

/**
 * Load the local schema cache.
 */
export function loadSchemaCache(): SchemaCache {
  if (!fs.existsSync(CACHE_FILE)) {
    return { concepts: [], lastUpdated: '', version: '0' };
  }

  try {
    const raw = fs.readFileSync(CACHE_FILE, 'utf-8');
    return JSON.parse(raw) as SchemaCache;
  } catch {
    return { concepts: [], lastUpdated: '', version: '0' };
  }
}

/**
 * Save the schema cache.
 */
export function saveSchemaCache(cache: SchemaCache): void {
  const dir = getConfigDir();
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2), 'utf-8');
}

/**
 * Get all cached concepts.
 */
export function getCachedConcepts(): SchemaCacheEntry[] {
  return loadSchemaCache().concepts;
}

/**
 * Cache concepts fetched from the platform or loaded from YAML files.
 */
export function cacheConcepts(concepts: SchemaCacheEntry[]): void {
  const cache: SchemaCache = {
    concepts,
    lastUpdated: new Date().toISOString(),
    version: '1',
  };
  saveSchemaCache(cache);
}
