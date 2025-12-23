"use client";

import type { SongItem } from "@/lib/types";
import YouTubePlayer  from "@/components/YouTubePlayer";


import React, { useMemo, useState, useEffect, useRef } from "react";
import { fetchPlaylistByQuery, fetchPlaylistBySeed, fetchItemsBySongIds , requestYoutubeEnrichment} from "@/lib/api"; 




const MOODS = ["romantic", "devotional", "kuthu", "happy", "melancholic", "sad", "angry", "inspirational"];

export default function Home() {
  const [q, setQ] = useState("love and longing");
  const [mood, setMood] = useState<string>("romantic");
  const [k, setK] = useState<number>(20);

  const [loading, setLoading] = useState(false);
  const [items, setItems] = useState<SongItem[]>([]);
  const [activeIndex, setActiveIndex] = useState<number>(0);
  const active = useMemo(() => items[activeIndex], [items, activeIndex]);
  const [autoAdvance, setAutoAdvance] = useState(true);

  async function onSearch() {
    setLoading(true);
    try {
      const data = await fetchPlaylistByQuery({ q, mood, k });
      const next = Array.isArray(data.items) ? data.items : [];
      setItems(next);
      setActiveIndex(0);
    } finally {
      setLoading(false);
    }
  }

  // 2) Add this helper + effect inside Home()

const enrichTimer = useRef<NodeJS.Timeout | null>(null);
const enrichInFlight = useRef(false);

useEffect(() => {
  if (!items.length) return;

  const missing = items
    .filter((it) => it.song_id && !it.youtube_url)
    .map((it) => it.song_id);

  if (!missing.length) return;
  if (enrichInFlight.current) return;

  enrichInFlight.current = true;

  (async () => {
    try {
      const res = await requestYoutubeEnrichment(missing);

      // res.items includes youtube_url for resolved ones
      const map: Record<string, string> = {};
      for (const x of res?.items ?? []) {
        if (x?.song_id && x?.youtube_url) map[x.song_id] = x.youtube_url;
      }

      if (!Object.keys(map).length) return;

      setItems((prev) =>
        prev.map((it) => ({
          ...it,
          youtube_url: it.youtube_url ?? map[it.song_id] ?? it.youtube_url,
        }))
      );
    } catch (e) {
      console.warn("Background youtube_url resolve failed:", e);
    } finally {
      enrichInFlight.current = false;
    }
  })();
}, [items]);


  async function onSeedFromActive() {
    if (!active?.song_id) return;
    setLoading(true);
    try {
      const data = await fetchPlaylistBySeed({ song_id: active.song_id, k });
      const next = Array.isArray(data.items) ? data.items : [];
      setItems(next);
      setActiveIndex(0);
    } finally {
      setLoading(false);
    }
  }

  function next() {
    setActiveIndex((i) => Math.min(i + 1, items.length - 1));
  }
  function prev() {
    setActiveIndex((i) => Math.max(i - 1, 0));
  }

  return (
    <main className="mx-auto max-w-6xl p-6">
      <h1 className="text-2xl font-semibold">Tamil AI Music Player</h1>
      <p className="mt-1 text-sm text-gray-600">
        Search → playlist → play (YouTube URLs come from your API).
      </p>
    
      {/* Controls */}
      <div className="mt-6 grid gap-3 rounded-2xl border p-4 md:grid-cols-12">
        <div className="md:col-span-7">
          <label className="text-sm font-medium">Query</label>
          <input
            className="mt-1 w-full rounded-lg border px-3 py-2"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="love, heartbreak, kuthu, etc..."
          />
        </div>

        <div className="md:col-span-3">
          <label className="text-sm font-medium">Mood</label>
          <select
            className="mt-1 w-full rounded-lg border px-3 py-2"
            value={mood}
            onChange={(e) => setMood(e.target.value)}
          >
            {MOODS.map((m) => (
              <option key={m} value={m}>
                {m}
              </option>
            ))}
            <option value="">(no filter)</option>
          </select>
        </div>

        <div className="md:col-span-2">
          <label className="text-sm font-medium">K</label>
          <input
            className="mt-1 w-full rounded-lg border px-3 py-2"
            type="number"
            min={1}
            max={50}
            value={k}
            onChange={(e) => setK(Number(e.target.value))}
          />
        </div>

        <div className="md:col-span-12 flex gap-2">
          <button
            onClick={onSearch}
            disabled={loading}
            className="rounded-lg bg-black px-4 py-2 text-white disabled:opacity-60"
          >
            {loading ? "Loading..." : "Search Playlist"}
          </button>

          <button
            onClick={onSeedFromActive}
            disabled={loading || !active?.song_id}
            className="rounded-lg border px-4 py-2 disabled:opacity-60"
          >
            Seed from Current Song
          </button>

          <div className="ml-auto flex gap-2">
            <button
              onClick={prev}
              disabled={activeIndex <= 0}
              className="rounded-lg border px-4 py-2 disabled:opacity-60"
            >
              Prev
            </button>
            <button
              onClick={next}
              disabled={activeIndex >= items.length - 1}
              className="rounded-lg border px-4 py-2 disabled:opacity-60"
            >
              Next
            </button>
          </div>
        </div>
      </div>

      {/* Player + list */}
      <div className="mt-6 grid gap-6 md:grid-cols-12">
        <section className="md:col-span-7">
          <div className="rounded-2xl border p-4">
            <div className="mb-3">
              <div className="text-sm text-gray-600">Now playing</div>
              <div className="text-lg font-semibold">
                {active?.title ?? "—"}{" "}
                <span className="text-sm font-normal text-gray-600">
                  {active?.movie ? `• ${active.movie}` : ""}{" "}
                  {active?.year ? `• ${active.year}` : ""}
                </span>
              </div>
              <div className="text-xs text-gray-500">
                song_id: {active?.song_id ?? "—"}
              </div>
            </div>

            <YouTubePlayer
            youtubeUrl={active?.youtube_url}
            onEnded={() => {
              if (!autoAdvance) return;
              // loop back to start if last song
              setActiveIndex((i) => (i + 1 < items.length ? i + 1 : 0));
            }}
          />

            <div className="mt-3 text-xs text-gray-600">
              If you don’t see video yet, it means `youtube_url` isn’t saved for
              that song in Qdrant yet.
            </div>
          </div>
        </section>

        <section className="md:col-span-5">
          <div className="rounded-2xl border">
            <div className="border-b p-3 text-sm font-medium">

                <label className="flex items-center gap-2 text-sm text-gray-700">
                  Playlist ({items.length})
                  <input
                    type="checkbox"
                    checked={autoAdvance}
                    onChange={(e) => setAutoAdvance(e.target.checked)}
                  />
                  Auto-advance
                </label>
            </div>
            <div className="max-h-[520px] overflow-auto">
              {items.map((it, idx) => {
                const selected = idx === activeIndex;
                return (
                  <button
                    key={`${it.song_id}-${idx}`}
                    onClick={() => setActiveIndex(idx)}
                    className={[
                      "flex w-full items-start gap-3 border-b p-3 text-left hover:bg-gray-50",
                      selected ? "bg-gray-100" : "",
                    ].join(" ")}
                  >
                    <div className="mt-0.5 text-xs text-gray-500 w-10">
                      {idx + 1}
                    </div>
                    <div className="min-w-0">
                      <div className="truncate font-medium">
                        {it.title ?? "(no title)"}
                      </div>
                      <div className="truncate text-xs text-gray-600">
                        {it.movie ?? ""} {it.year ? `• ${it.year}` : ""}{" "}
                        {it.mood ? `• ${it.mood}` : ""}
                      </div>
                      <div className="truncate text-[11px] text-gray-500">
                        {it.youtube_url ? "✅ youtube_url" : "⏳ no youtube_url yet"}
                      </div>
                    </div>
                  </button>
                );
              })}
              {items.length === 0 && (
                <div className="p-4 text-sm text-gray-600">
                  No playlist loaded yet. Click <b>Search Playlist</b>.
                </div>
              )}
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}
