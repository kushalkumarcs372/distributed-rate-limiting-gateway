"""
Idempotency Key Store (Postgres-backed)
-----------------------------------------
WHY THIS EXISTS:
Clients retry requests on timeout/network blips. Without idempotency,
a retried "charge $50" or "create order" request executes twice.

WHY POSTGRES NOT REDIS:
This data needs to survive a restart (Redis with no persistence config
loses it) and needs ACID guarantees for the "only one winner" race when
two identical requests arrive at the same instant. We use
`INSERT ... ON CONFLICT DO NOTHING` as an atomic compare-and-set,
equivalent to Redis SETNX but durable.

FLOW:
1. Client sends request with header `Idempotency-Key: <uuid>`
2. Gateway tries to INSERT the key with status='processing'
3. If INSERT succeeds -> this is the first time, process the request,
   then UPDATE the row with the response body + status='completed'
4. If INSERT fails (conflict) -> key already exists:
     - if status='completed', return the STORED response immediately
       (never re-executes the business logic)
     - if status='processing', another request is in flight right now;
       caller should retry shortly (we return 409)
"""
import json
import time
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool


class _PooledConn:
    """Context manager: borrow a connection from the pool, return it after use."""
    def __init__(self, pool):
        self.pool = pool
        self.conn = None

    def __enter__(self):
        self.conn = self.pool.getconn()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
        self.pool.putconn(self.conn)


class IdempotencyStore:
    def __init__(self, dsn: str, minconn: int = 5, maxconn: int = 30):
        # NOTE: with 3 gateway instances each holding up to `maxconn`
        # connections, total = 3 * maxconn must stay under Postgres's
        # default max_connections (100). 3*30=90 leaves headroom.
        self.dsn = dsn
        # WHY A POOL: opening a fresh TCP+auth connection per request adds
        # real latency (tens of ms) and doesn't scale under concurrency.
        # A pool keeps warm connections ready to reuse.
        self.pool = ThreadedConnectionPool(minconn, maxconn, dsn)
        self._ensure_table()

    def _conn(self):
        return _PooledConn(self.pool)

    def _ensure_table(self):
        # RACE CONDITION FOUND HERE, WORTH KNOWING FOR INTERVIEWS:
        # All 3 gateway containers start simultaneously and each ran
        # `CREATE TABLE IF NOT EXISTS` at roughly the same instant.
        # Postgres's IF NOT EXISTS check is NOT atomic across separate
        # connections -- two sessions can both see "table doesn't exist
        # yet" before either commits, so the second one crashes with a
        # duplicate-key error on Postgres's internal type catalog
        # (pg_type), not a friendly "table already exists" message.
        #
        # Fix: use a Postgres advisory lock (a lightweight, session-scoped
        # mutex backed by the DB itself) to serialize schema creation
        # across all instances. Whichever instance gets the lock first
        # creates the table; the others block on the lock, then see the
        # table already exists once they get their turn, and no-op.
        LOCK_KEY = 918273645  # arbitrary constant, just needs to be consistent
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
                try:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS idempotency_keys (
                            key TEXT PRIMARY KEY,
                            status TEXT NOT NULL,
                            response_body JSONB,
                            response_status INT,
                            created_at TIMESTAMPTZ DEFAULT now()
                        )
                    """)
                    conn.commit()
                finally:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
                    conn.commit()

    def try_begin(self, key: str) -> str:
        """
        Attempt to claim this idempotency key.
        Returns one of: 'new', 'completed', 'in_progress'
        """
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    INSERT INTO idempotency_keys (key, status)
                    VALUES (%s, 'processing')
                    ON CONFLICT (key) DO NOTHING
                    RETURNING key
                """, (key,))
                row = cur.fetchone()
                conn.commit()
                if row is not None:
                    return "new"

                cur.execute("SELECT status FROM idempotency_keys WHERE key = %s", (key,))
                existing = cur.fetchone()
                return existing["status"] if existing else "new"

    def get_stored_response(self, key: str):
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT response_body, response_status FROM idempotency_keys WHERE key = %s",
                    (key,),
                )
                return cur.fetchone()

    def complete(self, key: str, response_body: dict, response_status: int):
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE idempotency_keys
                    SET status = 'completed', response_body = %s, response_status = %s
                    WHERE key = %s
                """, (json.dumps(response_body), response_status, key))
            conn.commit()
