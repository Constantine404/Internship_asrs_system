# Thread-safe PostgreSQL database access layer
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
try:
    from .utils import normalize_basket_id
except ImportError:
    from utils import normalize_basket_id

class Pg:
    """
    Thread-safe PostgreSQL connection manager that creates new connections for each operation
    to prevent concurrency issues between QR listener and mover threads.
    """

    def __init__(self):
        # Store database connection parameters from environment variables
        self.conn_params = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASS'),
            'dbname': os.getenv('DB_NAME'),
        }
        # Ensure operation_history table exists
        self._ensure_operation_history_table()

    @contextmanager
    def cursor(self):
        """Creates a new database connection and cursor for thread-safe operations"""
        conn = None
        cur = None
        try:
            # Create new connection with autocommit for immediate execution
            conn = psycopg2.connect(**self.conn_params)
            conn.autocommit = True
            cur = conn.cursor(cursor_factory=RealDictCursor)
            yield cur
        finally:
            # Close the cursor and connection to free resources.  If either
            # object is None or already closed, ignore any exception.
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    # --- (!!!) ฟังก์ชันที่เพิ่มเข้ามาใหม่ (!!!) ---
    @contextmanager
    def transaction(self):
        """Creates a new database connection and cursor for a transaction"""
        conn = None
        cur = None
        try:
            # Create new connection, *no* autocommit
            conn = psycopg2.connect(**self.conn_params)
            conn.autocommit = False  # Ensure transaction handling
            cur = conn.cursor(cursor_factory=RealDictCursor)
            yield conn, cur
            # If no exception, commit the transaction
            conn.commit()
        except Exception as e:
            # If exception, rollback
            if conn:
                try:
                    conn.rollback()
                except Exception as rb_e:
                    print(f"[DB] Error during rollback: {rb_e}")
            # Re-raise the original exception
            raise e
        finally:
            # Close the cursor and connection
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    # --- (!!!) จบส่วนที่เพิ่มเข้ามาใหม่ (!!!) ---


    # --- Mapping / Coordinates ---
    def get_mapping_for_basket(self, basket_id: str):
        with self.cursor() as c:
            c.execute(
                """
                SELECT b.shelf_id, s.x_column AS x, s.y_row AS y, s.z_depth AS z
                FROM basket_data b
                JOIN shelf_data  s ON s.shelf_id = b.shelf_id
                WHERE b.basket_id = %s
                """,
                (basket_id.strip(),),
            )
            row = c.fetchone()
            return (int(row["shelf_id"]), int(row["x"]), int(row["y"]), int(row["z"])) if row else None

    def get_coords_for_basket(self, basket_id: str):
        m = self.get_mapping_for_basket(basket_id)
        return (m[1], m[2], m[3]) if m else None

    def get_shelf_of_basket(self, basket_id: str):
        with self.cursor() as c:
            c.execute("SELECT shelf_id FROM shelf_data WHERE basket_id = %s", (basket_id.strip(),))
            row = c.fetchone()
            return int(row["shelf_id"]) if row else None

    # --- Queues ---
    def enqueue_put(self, basket: str, x: int, y: int, z: int):
        basket_norm = normalize_basket_id(basket) if basket else None
        with self.transaction() as (conn, c):
            # ไม่แตะ shelf_data ตรงนี้แล้ว
            c.execute(
                "INSERT INTO queue_put (basket, x, y, z) VALUES (%s, %s, %s, %s)",
                (basket_norm, int(x), int(y), int(z)),
            )

    def enqueue_pick(self, basket: str, x: int, y: int, z: int) -> int:
        basket_norm = normalize_basket_id(basket) if basket else None
        with self.transaction() as (conn, c):
            # ไม่แตะ shelf_data ตรงนี้แล้ว
            c.execute(
                "INSERT INTO queue_pick (basket, x, y, z) VALUES (%s, %s, %s, %s) RETURNING id",
                (basket_norm, int(x), int(y), int(z)),
            )
            return int(c.fetchone()["id"])


    def next_command_window(self, limit_each=20):
        with self.cursor() as c:
            c.execute("SELECT * FROM queue_pick ORDER BY created_at ASC LIMIT %s", (limit_each,))
            picks = c.fetchall()
            c.execute("SELECT * FROM queue_put  ORDER BY created_at ASC LIMIT %s", (limit_each,))
            puts  = c.fetchall()
        return picks, puts

    def delete_queue_row(self, methode: str, row_id: int):
        table = "queue_pick" if methode == "PICK" else "queue_put"
        with self.cursor() as c:
            c.execute(f"DELETE FROM {table} WHERE id = %s", (int(row_id),))

    # --- Occupancy updates ---
    def mark_shelf_occupied(self, shelf_id: int, basket_id: str):
        with self.cursor() as c:
            # Update shelf status
            c.execute(
                """
                UPDATE shelf_data 
                SET basket_id = %s, 
                    active = TRUE, 
                    lastupdate_time = NOW() AT TIME ZONE 'Asia/Bangkok'
                WHERE shelf_id = %s
                """,
                (basket_id.strip() if basket_id else None, int(shelf_id)),
            )
            # Add operation history
            c.execute(
                """
                INSERT INTO operation_history 
                (shelf_id, basket_id, operation_type, status, timestamp)
                VALUES (%s, %s, 'PUT', 'success', NOW() AT TIME ZONE 'Asia/Bangkok')
                """,
                (int(shelf_id), basket_id.strip() if basket_id else None)
            )

    def mark_shelf_empty(self, shelf_id: int):
        with self.cursor() as c:
            # Get current basket_id before updating
            c.execute("SELECT basket_id FROM shelf_data WHERE shelf_id = %s", (int(shelf_id),))
            row = c.fetchone()
            old_basket_id = row["basket_id"] if row else None

            # Update shelf status
            c.execute(
                """
                UPDATE shelf_data 
                SET basket_id = NULL, 
                    active = FALSE, 
                    lastupdate_time = NOW() AT TIME ZONE 'Asia/Bangkok'
                WHERE shelf_id = %s
                """,
                (int(shelf_id),),
            )

            # Add operation history
            if old_basket_id:
                c.execute(
                    """
                    INSERT INTO operation_history 
                    (shelf_id, basket_id, operation_type, status, timestamp)
                    VALUES (%s, %s, 'PICK', 'success', NOW() AT TIME ZONE 'Asia/Bangkok')
                    """,
                    (int(shelf_id), old_basket_id)
                )

    # --- Queue management ---
    def clear_all_queues(self) -> None:
        """
        Remove all pending commands from both the pick and put queues. This
        clears any queued work so that the system can stop processing
        commands immediately. Use this when a reset is requested.

        Returns:
            None
        """
        with self.cursor() as c:
            # Delete all rows from queue_pick and queue_put tables
            c.execute("DELETE FROM queue_pick")
            c.execute("DELETE FROM queue_put")

    # --- Helpers ---
    def get_zone_by_xy(self, x:int, y:int, z:int=0) -> int | None:
        with self.cursor() as c:
            c.execute("""SELECT zone FROM shelf_data
                         WHERE x_column=%s AND y_row=%s AND z_depth=%s LIMIT 1""",
                      (int(x),int(y),int(z)))
            r = c.fetchone()
            return int(r["zone"]) if r else None

    # --- Shelf availability ---
    def shelf_can_use(self, shelf_id: int) -> bool:
        """
        Check if a given shelf can be used. Returns True if the shelf's
        `can_use` flag is True, and False otherwise. If the shelf is not
        found, returns False.

        Args:
            shelf_id: The primary key of the shelf to check.

        Returns:
            bool: True if the shelf is usable, False if it is marked
                unusable or does not exist.
        """
        with self.cursor() as c:
            c.execute("SELECT can_use FROM shelf_data WHERE shelf_id = %s", (int(shelf_id),))
            row = c.fetchone()
            # If the shelf does not exist or can_use is None, treat as unusable
            return bool(row["can_use"]) if row and row.get("can_use") is not None else False

    def shelf_can_use_by_xyz(self, x: int, y: int, z: int = 0) -> bool:
        """
        Check if the shelf at the given coordinates can be used. Returns True
        only if a matching shelf exists and its `can_use` flag is True.

        Args:
            x: The x_column coordinate of the shelf.
            y: The y_row coordinate of the shelf.
            z: The z_depth coordinate of the shelf (default 0).

        Returns:
            bool: True if the shelf can be used, False otherwise.
        """
        with self.cursor() as c:
            c.execute(
                "SELECT can_use FROM shelf_data WHERE x_column = %s AND y_row = %s AND z_depth = %s LIMIT 1",
                (int(x), int(y), int(z)),
            )
            row = c.fetchone()
            return bool(row["can_use"]) if row and row.get("can_use") is not None else False

    # --- Queue inspection ---
    def has_pending_put(self, basket: str) -> bool:
        """
        Check if there is already a pending PUT command for the given basket in the
        queue_put table.  Returns True if an entry exists, False otherwise.

        Args:
            basket: The basket ID to check for.

        Returns:
            bool: True if the basket is already in the PUT queue, False otherwise.
        """
        with self.cursor() as c:
            c.execute(
                "SELECT 1 FROM queue_put WHERE basket = %s LIMIT 1",
                (basket.strip(),),
            )
            return c.fetchone() is not None

    # --- Shelf occupancy ---
    def _ensure_operation_history_table(self):
        """Create operation_history table if it doesn't exist"""
        with self.cursor() as c:
            try:
                c.execute("""
                    CREATE TABLE IF NOT EXISTS operation_history (
                        id SERIAL PRIMARY KEY,
                        shelf_id INTEGER NOT NULL,
                        basket_id TEXT,
                        operation_type TEXT NOT NULL,
                        status TEXT NOT NULL,
                        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                        FOREIGN KEY (shelf_id) REFERENCES shelf_data(shelf_id)
                    )
                """)
                print("[DB] Operation history table ready")
            except Exception as e:
                print(f"[DB] Warning: Could not create operation_history table: {e}")

    def get_basket_on_shelf(self, shelf_id: int) -> str | None:
        """
        Retrieve the basket ID currently occupying the specified shelf.  Returns
        None if the shelf is empty or does not exist.

        Args:
            shelf_id: The shelf's primary key.

        Returns:
            Optional[str]: The basket ID occupying the shelf, or None if the
            shelf is empty or missing.
        """
        with self.cursor() as c:
            c.execute(
                "SELECT basket_id FROM shelf_data WHERE shelf_id = %s",
                (int(shelf_id),),
            )
            row = c.fetchone()
            # If no record or basket_id is None, return None
            return str(row["basket_id"]) if row and row.get("basket_id") else None
        # --- Occupancy updates at FINAL completion ---
    
        # --- Occupancy updates at FINAL completion ---
    def mark_pick(self, shelf_id: int):
        """
        PICK: ยกตะกร้าออกจาก shelf (อัปเดตตอนจบงานจริงเท่านั้น)
        -> shelf_data: basket_id=NULL, active=FALSE, lastupdate_time=now(Asia/Bangkok)
        """
        with self.transaction() as (conn, c):
            c.execute(
                """
                UPDATE shelf_data
                   SET basket_id = NULL,
                       active     = FALSE,
                       lastupdate_time = (NOW() AT TIME ZONE 'Asia/Bangkok')
                 WHERE shelf_id = %s
                """,
                (int(shelf_id),),
            )

    def move_put(self, shelf_id: int, basket_id: str, *, allow_overwrite_dest: bool = False):
        """
        PUT (with move-protection) — อัปเดตตอนจบงานจริง:
        1) เคลียร์ตำแหน่งเดิมของ basket_id (ถ้ามี)
        2) ตรวจปลายทาง: ถ้ามีตะกร้าอื่นอยู่ -> ปฏิเสธ (หรือ overwrite ถ้า allow_overwrite_dest=True)
        3) วางตะกร้าลงปลายทาง และอัปเดตเวลา

        คืนค่า dict: {"cleared_from": [old_shelf_ids], "placed_to": shelf_id}
        """
        bid = normalize_basket_id(basket_id) if basket_id else None
        if bid is None:
            raise ValueError("basket_id is required for move_put")

        with self.transaction() as (conn, c):
            # 1) เคลียร์ตำแหน่งเดิมของตะกร้านี้ (ถ้ามี)
            c.execute(
                """
                WITH cleared AS (
                    UPDATE shelf_data
                       SET basket_id = NULL,
                           active     = FALSE,
                           lastupdate_time = (NOW() AT TIME ZONE 'Asia/Bangkok')
                     WHERE basket_id = %s
                 RETURNING shelf_id
                )
                SELECT array_agg(shelf_id) AS cleared_from FROM cleared
                """,
                (bid,),
            )
            cleared_from = (c.fetchone() or {}).get("cleared_from") or []

            # 2) ตรวจปลายทาง + ล็อคแถว
            c.execute(
                "SELECT basket_id, active FROM shelf_data WHERE shelf_id = %s FOR UPDATE",
                (int(shelf_id),),
            )
            row = c.fetchone()
            if not row:
                raise ValueError(f"shelf_id {shelf_id} ไม่มีอยู่ใน shelf_data")

            dest_has_other = row["basket_id"] is not None and row["basket_id"] != bid
            if dest_has_other and not allow_overwrite_dest:
                raise ValueError(
                    f"ปลายทาง shelf_id {shelf_id} มีตะกร้า {row['basket_id']} อยู่แล้ว (allow_overwrite_dest=False)"
                )

            if dest_has_other and allow_overwrite_dest:
                c.execute(
                    """
                    UPDATE shelf_data
                       SET basket_id = NULL,
                           active     = FALSE,
                           lastupdate_time = (NOW() AT TIME ZONE 'Asia/Bangkok')
                     WHERE shelf_id = %s
                    """,
                    (int(shelf_id),),
                )

            # 3) วางตะกร้าลงปลายทาง
            c.execute(
                """
                UPDATE shelf_data
                   SET basket_id = %s,
                       active     = TRUE,
                       lastupdate_time = (NOW() AT TIME ZONE 'Asia/Bangkok')
                 WHERE shelf_id = %s
                """,
                (bid, int(shelf_id)),
            )

        return {"cleared_from": cleared_from, "placed_to": int(shelf_id)}