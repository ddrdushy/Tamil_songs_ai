import re
import requests
from urllib.parse import quote_plus

def youtube_search_url(title: str, movie: str | None = None, year: str | None = None) -> str | None:
    q = f"{title} {movie or ''} Tamil song".strip()
    url = f"https://www.youtube.com/results?search_query={quote_plus(q)}"

    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        r.raise_for_status()
        m = re.search(r"watch\?v=([a-zA-Z0-9_-]{11})", r.text)
        if not m:
            return None
        vid = m.group(1)
        return f"https://www.youtube.com/watch?v={vid}"
    except Exception:
        return None
