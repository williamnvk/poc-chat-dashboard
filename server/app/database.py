import sqlite3
from contextlib import contextmanager

class Database:
    def __init__(self, database_url):
        self.database_url = database_url
        self.connection = None

    def connect(self):
        if not self.connection:
            self.connection = sqlite3.connect(
                self.database_url, 
                check_same_thread=False
            )
        return self.connection

    @contextmanager
    def get_cursor(self):
        cursor = self.connection.cursor()
        try:
            yield cursor
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cursor.close()

db = Database(":memory:")
db.connect() 