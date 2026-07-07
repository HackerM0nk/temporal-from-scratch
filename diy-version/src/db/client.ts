// ─────────────────────────────────────────────────────────────────────────────
// PostgreSQL Client
//
// Provides a connection pool and transaction helpers.
// We use the raw `pg` library to keep SQL explicit and educational.
// ─────────────────────────────────────────────────────────────────────────────

import { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg';

let _pool: Pool | null = null;

export function getPool(): Pool {
  if (!_pool) {
    _pool = new Pool({
      connectionString: process.env.DATABASE_URL ??
        'postgresql://postgres:postgres@localhost:5432/diy_workflows',
      max: 10,
      idleTimeoutMillis: 30000,
    });

    _pool.on('error', (err) => {
      console.error('[db] Unexpected pool error:', err.message);
    });
  }
  return _pool;
}

// Simple query helper — use for reads and single-statement writes
export async function query<T extends QueryResultRow = any>(sql: string, params?: any[]): Promise<QueryResult<T>> {
  return getPool().query<T>(sql, params);
}

// Transaction helper — wraps multiple statements in BEGIN/COMMIT/ROLLBACK.
// Use this whenever you need atomicity (e.g., update state + insert event).
//
// KEY CONCEPT: This is how we get the same atomicity guarantee that Temporal
// gets by having all state live in a single transactional database.
// If the process crashes mid-transaction, PostgreSQL rolls back automatically.
export async function withTransaction<T>(
  fn: (client: PoolClient) => Promise<T>,
): Promise<T> {
  const client = await getPool().connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// Wait for the database to be ready (used on startup)
export async function waitForDb(maxAttempts = 30): Promise<void> {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await query('SELECT 1');
      console.log('[db] PostgreSQL connection established');
      return;
    } catch (err) {
      console.log(`[db] Waiting for PostgreSQL... (attempt ${attempt}/${maxAttempts})`);
      await new Promise((r) => setTimeout(r, 2000));
    }
  }
  throw new Error('Could not connect to PostgreSQL');
}

export async function closePool(): Promise<void> {
  if (_pool) {
    await _pool.end();
    _pool = null;
  }
}
