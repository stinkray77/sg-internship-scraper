"""Read application table definitions only; never print records or credentials."""
import os
import psycopg2

TABLES=('seen_jobs','job_observations','job_lifecycle_events','source_sync_state','canonical_jobs','job_identity_aliases','source_runs','employer_candidates','maintenance_state')
with psycopg2.connect(os.environ['DATABASE_URL']) as conn:
    conn.set_session(readonly=True)
    with conn.cursor() as cur:
        for table in TABLES:
            cur.execute('''SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
                pg_get_expr(d.adbin,d.adrelid) FROM pg_attribute a
                LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
                WHERE a.attrelid=to_regclass(%s) AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum''',(table,))
            print(table,cur.fetchall(),flush=True)
            cur.execute('SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=to_regclass(%s)',(table,))
            print('constraints',cur.fetchall(),flush=True)
