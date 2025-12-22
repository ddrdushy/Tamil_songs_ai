import type { SongItem } from "./types";

export async function fetchPlaylistByQuery(args: {
  q: string;
  mood?: string;
  k?: number;
}): Promise<{ ok: boolean; items: SongItem[]; count?: number; mood?: string; query?: string }> {
  const url = new URL("/api/player/query", window.location.origin);
  url.searchParams.set("q", args.q);
  if (args.mood) url.searchParams.set("mood", args.mood);
  url.searchParams.set("k", String(args.k ?? 20));

  const r = await fetch(url.toString());
  const data = await r.json();
  // Your FastAPI returns {ok, items,...} for /player/query
  return data;
}

export async function fetchPlaylistBySeed(args: {
  song_id: string;
  k?: number;
}): Promise<{ ok: boolean; items: SongItem[]; count?: number; mood?: string; seed_song_id?: string }> {
  const url = new URL(`/api/player/seed/${args.song_id}`, window.location.origin);
  url.searchParams.set("k", String(args.k ?? 20));

  const r = await fetch(url.toString());
  const data = await r.json();
  return data;
}
