"use client";

import React from "react";

function extractVideoId(youtubeUrl?: string): string | null {
  if (!youtubeUrl) return null;

  try {
    const u = new URL(youtubeUrl);

    // youtu.be/<id>
    if (u.hostname.includes("youtu.be")) {
      const id = u.pathname.replace("/", "").trim();
      return id || null;
    }

    // youtube.com/watch?v=<id>
    const v = u.searchParams.get("v");
    if (v) return v;

    // youtube.com/embed/<id>
    if (u.pathname.includes("/embed/")) {
      const id = u.pathname.split("/embed/")[1]?.split("/")[0];
      return id || null;
    }
  } catch {
    // ignore parse errors
  }
  return null;
}

export default function YouTubePlayer({
  youtubeUrl,
}: {
  youtubeUrl?: string;
}) {
  const vid = extractVideoId(youtubeUrl);

  if (!vid) {
    return (
      <div className="w-full rounded-xl border p-4 text-sm text-gray-600">
        No YouTube URL for this song yet.
      </div>
    );
  }

  const embed = `https://www.youtube.com/embed/${vid}?autoplay=1&rel=0`;

  return (
    <div className="w-full overflow-hidden rounded-xl border">
      <iframe
        className="aspect-video w-full"
        src={embed}
        title="YouTube player"
        allow="autoplay; encrypted-media; picture-in-picture"
        allowFullScreen
      />
    </div>
  );
}
