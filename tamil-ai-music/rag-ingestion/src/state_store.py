import sqlite3
from pathlib import Path
from typing import Optional, Tuple


class StateStore:
    def __init__(self, path: str = None):
        # Always anchor to project root (rag-ingestion/)
        project_root = Path(__file__).resolve().parents[1]
        default_path = project_root / "data" / "state_v2.db"

        self.path = Path(path).expanduser().resolve() if path else default_path
        self.path.parent.mkdir(parents=True, exist_ok=True)

        self.conn = sqlite3.connect(str(self.path))
        self._init()

    def _init(self):
        cur = self.conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS song_state (
            song_id TEXT PRIMARY KEY,
            lyrics_hash TEXT,
            meta_hash TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        self.conn.commit()

    def get(self, song_id: str) -> Optional[Tuple[str, str]]:
        cur = self.conn.cursor()
        cur.execute(
            "SELECT lyrics_hash, meta_hash FROM song_state WHERE song_id = ?",
            (song_id,)
        )
        return cur.fetchone()

    def upsert(self, song_id: str, lyrics_hash: str, meta_hash: str):
        cur = self.conn.cursor()
        cur.execute("""
        INSERT INTO song_state (song_id, lyrics_hash, meta_hash)
        VALUES (?, ?, ?)
        ON CONFLICT(song_id)
        DO UPDATE SET
            lyrics_hash=excluded.lyrics_hash,
            meta_hash=excluded.meta_hash,
            updated_at=CURRENT_TIMESTAMP
        """, (song_id, lyrics_hash, meta_hash))
        self.conn.commit()
