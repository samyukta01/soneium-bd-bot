import os
import psycopg2
import psycopg2.extras

class Database:
    def __init__(self):
        self.url = os.environ["DATABASE_URL"]
        self._init()

    def _conn(self):
        return psycopg2.connect(self.url, sslmode="require")

    def _init(self):
        with self._conn() as conn:
            with conn.cursor() as c:
                c.execute("""CREATE TABLE IF NOT EXISTS permitted_chats (
                    chat_id BIGINT PRIMARY KEY,
                    chat_name TEXT NOT NULL,
                    added_at TIMESTAMP DEFAULT NOW()
                )""")
                c.execute("""CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    chat_id BIGINT NOT NULL,
                    chat_name TEXT NOT NULL,
                    user_id BIGINT,
                    username TEXT,
                    text TEXT,
                    msg_id BIGINT,
                    timestamp TEXT NOT NULL
                )""")
                c.execute("CREATE INDEX IF NOT EXISTS idx_msg_chat ON messages(chat_id)")

    def permit_chat(self, chat_id, chat_name):
        with self._conn() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO permitted_chats(chat_id,chat_name) VALUES(%s,%s) ON CONFLICT(chat_id) DO UPDATE SET chat_name=EXCLUDED.chat_name", (chat_id, chat_name))

    def unpermit_chat(self, chat_id):
        with self._conn() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM permitted_chats WHERE chat_id=%s", (chat_id,))

    def is_permitted_chat(self, chat_id):
        with self._conn() as conn:
            with conn.cursor() as c:
                c.execute("SELECT 1 FROM permitted_chats WHERE chat_id=%s", (chat_id,))
                return c.fetchone() is not None

    def get_permitted_chats(self):
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
                c.execute("SELECT chat_id, chat_name FROM permitted_chats ORDER BY chat_name")
                return [dict(r) for r in c.fetchall()]

    def get_chat_name(self, chat_id):
        with self._conn() as conn:
            with conn.cursor() as c:
                c.execute("SELECT chat_name FROM permitted_chats WHERE chat_id=%s", (chat_id,))
                row = c.fetchone()
                return row[0] if row else str(chat_id)

    def log_message(self, chat_id, chat_name, user_id, username, text, msg_id, timestamp):
        if not text or not text.strip(): return
        with self._conn() as conn:
            with conn.cursor() as c:
                c.execute("INSERT INTO messages(chat_id,chat_name,user_id,username,text,msg_id,timestamp) VALUES(%s,%s,%s,%s,%s,%s,%s)",
                    (chat_id, chat_name, user_id, username, text, msg_id, timestamp))

    def get_messages(self, chat_id, limit=300):
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
                c.execute("SELECT * FROM messages WHERE chat_id=%s ORDER BY timestamp DESC LIMIT %s", (chat_id, limit))
                return [dict(r) for r in reversed(c.fetchall())]

    def get_message_count(self, chat_id):
        with self._conn() as conn:
            with conn.cursor() as c:
                c.execute("SELECT COUNT(*) FROM messages WHERE chat_id=%s", (chat_id,))
                return c.fetchone()[0]

    def search_messages_across_chats(self, query, limit=300):
        terms = query.lower().split()
        conditions = " AND ".join(["LOWER(m.text) LIKE %s" for _ in terms])
        params = [f"%{t}%" for t in terms] + [limit]
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c:
                c.execute(f"SELECT m.* FROM messages m JOIN permitted_chats p ON m.chat_id=p.chat_id WHERE {conditions} ORDER BY m.timestamp DESC LIMIT %s", params)
                return [dict(r) for r in reversed(c.fetchall())]
