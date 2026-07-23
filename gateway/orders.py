"""
Orders Service (the "real backend" business logic)
-----------------------------------------------------
Replaces the earlier `simulate_backend_work` placeholder with a real
stateful operation: placing an order against a limited inventory. This
makes the idempotency guarantee mean something concrete -- without it,
a retried request could oversell inventory by decrementing stock twice
for what the client believes is one order.

WHY "FOR UPDATE" ROW LOCKING:
Two simultaneous orders for the same product must not both read
"stock=5, quantity requested=5" and both succeed -- that's a classic
race condition that oversells inventory. `SELECT ... FOR UPDATE` locks
the row until the transaction commits, so a second concurrent order for
the same product waits until the first one's stock deduction is
committed, then sees the updated (correct) stock level.
"""
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool


SEED_INVENTORY = {
    "widget-a": 500,
    "widget-b": 500,
    "widget-c": 500,
}


class OrdersService:
    def __init__(self, dsn: str, minconn: int = 5, maxconn: int = 30):
        self.pool = ThreadedConnectionPool(minconn, maxconn, dsn)
        self._ensure_schema()

    def _conn(self):
        return _PooledConn(self.pool)

    def _ensure_schema(self):
        # Same advisory-lock pattern as idempotency.py's table creation --
        # multiple gateway instances start simultaneously and would
        # otherwise race on CREATE TABLE IF NOT EXISTS (see README for the
        # bug this caused earlier in the project).
        LOCK_KEY = 918273646  # different constant than idempotency.py's lock
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
                try:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS inventory (
                            product_id TEXT PRIMARY KEY,
                            stock INT NOT NULL CHECK (stock >= 0)
                        )
                    """)
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS orders (
                            id SERIAL PRIMARY KEY,
                            client_id TEXT NOT NULL,
                            product_id TEXT NOT NULL,
                            quantity INT NOT NULL,
                            status TEXT NOT NULL,
                            created_at TIMESTAMPTZ DEFAULT now()
                        )
                    """)
                    for product_id, stock in SEED_INVENTORY.items():
                        cur.execute("""
                            INSERT INTO inventory (product_id, stock)
                            VALUES (%s, %s)
                            ON CONFLICT (product_id) DO NOTHING
                        """, (product_id, stock))
                    conn.commit()
                finally:
                    cur.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
                    conn.commit()

    def place_order(self, client_id: str, product_id: str, quantity: int) -> dict:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT stock FROM inventory WHERE product_id = %s FOR UPDATE",
                    (product_id,),
                )
                row = cur.fetchone()

                if row is None:
                    conn.commit()
                    return {"status": "rejected", "reason": "unknown_product", "product_id": product_id}

                available = row["stock"]
                if available < quantity:
                    conn.commit()
                    return {
                        "status": "rejected",
                        "reason": "insufficient_stock",
                        "product_id": product_id,
                        "requested": quantity,
                        "available": available,
                    }

                cur.execute(
                    "UPDATE inventory SET stock = stock - %s WHERE product_id = %s",
                    (quantity, product_id),
                )
                cur.execute("""
                    INSERT INTO orders (client_id, product_id, quantity, status)
                    VALUES (%s, %s, %s, 'confirmed')
                    RETURNING id
                """, (client_id, product_id, quantity))
                order_id = cur.fetchone()["id"]
                conn.commit()

                return {
                    "status": "confirmed",
                    "order_id": order_id,
                    "product_id": product_id,
                    "quantity": quantity,
                    "remaining_stock": available - quantity,
                }

    def get_inventory_snapshot(self) -> dict:
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT product_id, stock FROM inventory ORDER BY product_id")
                return {row["product_id"]: row["stock"] for row in cur.fetchall()}


class _PooledConn:
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
