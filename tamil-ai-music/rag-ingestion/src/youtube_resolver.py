from __future__ import annotations
from urllib.parse import quote_plus

def youtube_search_url(title: str, movie: str | None = None, year: str | int | None = None) -> str:
    q = title
    if movie:
        q += f" {movie}"
    if year:
        q += f" {year}"
    q += " Tamil song"
    return "https://www.youtube.com/results?search_query=" + quote_plus(q)
