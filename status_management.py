import psycopg2
from enum import Enum

class BaseStatus(Enum):
    OPEN = 'Open'
    DOING = 'Doing'
    WAITING = 'Waiting'
    CLOSED = 'Closed'

class StatusManager:
    def __init__(self, db_connection):
        self.conn = db_connection

    def create_status(self, name: str, base_status: BaseStatus, description: str = None):
        """Neuen Status erstellen"""
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO status_definitions (name, base_status, description)
                VALUES (%s, %s, %s)
                RETURNING id
            """, (name, base_status.value, description))
            self.conn.commit()
            return cur.fetchone()[0]

    def get_base_status(self, status_name: str) -> BaseStatus:
        """Basis-Status für einen Status-Namen abrufen"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT base_status 
                FROM status_definitions 
                WHERE name = %s
            """, (status_name,))
            result = cur.fetchone()
            return BaseStatus(result[0]) if result else None

    def get_all_status(self):
        """Alle Status mit ihren Basis-Status abrufen"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT name, base_status, description
                FROM status_definitions
                ORDER BY base_status, name
            """)
            return cur.fetchall() 