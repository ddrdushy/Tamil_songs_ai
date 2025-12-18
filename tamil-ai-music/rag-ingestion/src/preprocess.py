import re
import hashlib
from typing import List, Dict, Any, Optional


_whitespace_re = re.compile(r"\s+")
_punct_re = re.compile(r"[^\w\s\u0B80-\u0BFF]")  # keep Tamil block + word chars


def make_song_id(title: str, singer: str = "", movie: str = "", year: str = "") -> str:
    """
    Stable ID across runs. Uses normalized fields.
    """
    base = f"{(title or '').strip().lower()}|{(singer or '').strip().lower()}|{(movie or '').strip().lower()}|{(year or '').strip()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:16]


def clean_lyrics(text: Optional[str]) -> str:
    """
    Normalize lyrics for embedding:
    - strip
    - collapse whitespace
    - remove most punctuation while keeping Tamil characters
    """
    if not text:
        return ""
    t = text.strip()
    t = _punct_re.sub(" ", t)
    t = _whitespace_re.sub(" ", t)
    return t.strip()


def chunk_text(text: str, chunk_size: int = 1200, overlap: int = 200) -> List[str]:
    """
    Chunk by characters (simple + robust). Good enough for lyrics.
    """
    if not text:
        return []
    if chunk_size <= overlap:
        raise ValueError("chunk_size must be > overlap")

    chunks = []
    start = 0
    n = len(text)
    while start < n:
        end = min(start + chunk_size, n)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if end == n:
            break
        start = end - overlap
    return chunks


def lyrics_hash(cleaned_lyrics: str) -> str:
    return hashlib.sha1((cleaned_lyrics or "").encode("utf-8")).hexdigest()


def pick_lyrics_field(row: Dict[str, Any]) -> str:
    """
    Prefer Tamil lyrics first, then fallback.
    """
    candidates = [
        "tamil_lyrics",
        "lyrics_ta",
        "english_lyrics",
        "lyrics_translit",
    ]
    for k in candidates:
        v = row.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return ""
