import sqlite3

class Database:
    def __init__(self, path="bd_bot.db"):
        self.path = path
        self._init()

    def _conn(self):
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init(self):
        with self._conn() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS permitted_chats (
                    chat_id   INTEGER PRIMARY KEY,
                    chat_name TEXT NOT NULL,
                    added_at  TEXT DEFAULT (datetime('now'))
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id   INTEGER NOT NULL,
                    chat_name TEXT NOT NULL,
                    user_id   INTEGER,
                    username  TEXT,
                    text      TEXT,
                    msg_id    INTEGER,
                    timestamp TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_chat ON messages(chat_id);
                CREATE INDEX IF NOT EXISTS idx_ts   ON messages(timestamp);
            """)

    def permit_chat(self, chat_id, chat_name):
        with self._conn() as c:
            c.execute("INSERT OR REPLACE INTO permitted_chats (chat_id, chat_name) VALUES (?,?)", (chat_id, chat_name))

    def unpermit_chat(self, chat_id):
        with self._conn() as c:
            c.execute("DELETE FROM permitted_chats WHERE chat_id=?", (chat_id,))

    def is_permitted_chat(self, chat_id):
        with self._conn() as c:
            return c.execute("SELECT 1 FROM permitted_chats WHERE chat_id=?", (chat_id,)).fetchone() is not None

    def get_permitted_chats(self):
        with self._conn() as c:
            return [dict(r) for r in c.execute("SELECT chat_id, chat_name FROM permitted_chats ORDER BY chat_name").fetchall()]

    def get_chat_name(self, chat_id):
        with self._conn() as c:
            r = c.execute("SELECT chat_name FROM permitted_chats WHERE chat_id=?", (chat_id,)).fetchone()
            return r["chat_name"] if r else str(chat_id)

    def log_message(self, chat_id, chat_name, user_id, username, text, msg_id, timestamp):
        if not text: return
        with self._conn() as c:
            c.execute("INSERT INTO messages (chat_id,chat_name,user_id,username,text,msg_id,timestamp) VALUES (?,?,?,?,?,?,?)",
                (chat_id, chat_name, user_id, username, text, msg_id, timestamp))

    def get_messages(self, chat_id, limit=300):
        with self._conn() as c:
            rows = c.execute("SELECT * FROM messages WHERE chat_id=? ORDER BY timestamp DESC LIMIT ?", (chat_id, limit)).fetchall()
            return [dict(r) for r in reversed(rows)]

    def get_message_count(self, chat_id):
        with self._conn() as c:
            return c.execute("SELECT COUNT(*) as n FROM messages WHERE chat_id=?", (chat_id,)).fetchone()["n"]

    def search_messages_across_chats(self, query, limit=300):
        terms   = query.lower().split()
        clauses = " AND ".join(["LOWER(text) LIKE ?" for _ in terms])
        params  = [f"%{t}%" for t in terms] + [limit]
        with self._conn() as c:
            rows = c.execute(
                f"SELECT * FROM messages WHERE {clauses} ORDER BY timestamp DESC LIMIT ?", params
            ).fetchall()
            return [dict(r) for r in reversed(rows)]
