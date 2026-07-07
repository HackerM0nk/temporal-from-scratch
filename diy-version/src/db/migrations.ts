// Applies the database schema. Safe to run multiple times (IF NOT EXISTS).
import fs from 'fs';
import path from 'path';
import { query, waitForDb } from './client';

export async function runMigrations(): Promise<void> {
  await waitForDb();
  const schemaPath = path.resolve(__dirname, 'schema.sql');
  const sql = fs.readFileSync(schemaPath, 'utf8');
  console.log('[migrations] Applying schema...');
  await query(sql);
  console.log('[migrations] ✓ Schema applied');
}

// Run directly: ts-node src/db/migrations.ts
if (require.main === module) {
  runMigrations()
    .then(() => process.exit(0))
    .catch((err) => { console.error(err); process.exit(1); });
}
