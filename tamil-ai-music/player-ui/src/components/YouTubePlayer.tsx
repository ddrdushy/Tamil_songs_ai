"use client";

import React from "react";

function extractYouTubeVideoId(url?: string): string | null {
  if (!url) return null;

  try {
    const u = new URL(url);

    // https://www.youtube.com/watch?v=VIDEO_ID
    const v = u.searchParams.get("v");
    if (v) return v;

    // https://youtu.be/VIDEO_ID
    if (u.hostname.includes("youtu.be")) {
      const id = u.pathname.replace("/", "").trim();
      return id || null;
    }

    // https://www.youtube.com/embed/VIDEO_ID
    if (u.pathname.startsWith("/embed/")) {
      return u.pathname.split("/embed/")[1]?.split("?")[0] ?? null;
    }

    return null;
  } catch {
    return null;
  }
}

type YouTubePlayerProps = {
  youtubeUrl?: string;
  onEnded?: () => void;
};

export default function YouTubePlayer({ youtubeUrl, onEnded }: YouTubePlayerProps) {
  const containerRef = React.useRef<HTMLDivElement | null>(null);
  const playerRef = React.useRef<any>(null);
  const endedGuardRef = React.useRef(false);

  const videoId = React.useMemo(() => extractYouTubeVideoId(youtubeUrl), [youtubeUrl]);

  React.useEffect(() => {
    endedGuardRef.current = false;
  }, [videoId]);

  React.useEffect(() => {
    if (typeof window === "undefined") return;
    if (!containerRef.current) return;

    // If we can't extract a playable video id, show a friendly message
    if (!videoId) {
      // destroy previous player (if any)
      if (playerRef.current?.destroy) {
        try {
          playerRef.current.destroy();
        } catch {}
      }
      playerRef.current = null;

      // clear container
      containerRef.current.innerHTML = "";
      return;
    }

    const ensureYT = async () => {
      const w = window as any;

      if (!w.YT || !w.YT.Player) {
        await new Promise<void>((resolve) => {
          const existing = document.getElementById("yt-iframe-api");
          if (existing) return resolve();

          const tag = document.createElement("script");
          tag.id = "yt-iframe-api";
          tag.src = "https://www.youtube.com/iframe_api";
          document.body.appendChild(tag);

          w.onYouTubeIframeAPIReady = () => resolve();
        });
      }
    };

    (async () => {
      await ensureYT();

      const w = window as any;

      // destroy previous instance
      if (playerRef.current?.destroy) {
        try {
          playerRef.current.destroy();
        } catch {}
      }
      containerRef.current!.innerHTML = "";

      // create a new div for YT to mount into
      const mount = document.createElement("div");
      containerRef.current!.appendChild(mount);

      playerRef.current = new w.YT.Player(mount, {
        videoId,
        width: "100%",
        height: "360",
        playerVars: {
          autoplay: 1,
          rel: 0,
          modestbranding: 1,
        },
        events: {
          onStateChange: (event: any) => {
            // ENDED === 0
            if (event?.data === 0) {
              if (endedGuardRef.current) return;
              endedGuardRef.current = true;
              onEnded?.();
            }
          },
        },
      });
    })();

    return () => {
      if (playerRef.current?.destroy) {
        try {
          playerRef.current.destroy();
        } catch {}
      }
      playerRef.current = null;
    };
  }, [videoId, onEnded]);

  if (!youtubeUrl) {
    return (
      <div className="rounded-xl border p-4 text-sm text-gray-600">
        No youtube_url yet for this song.
      </div>
    );
  }

  if (!videoId) {
    return (
      <div className="rounded-xl border p-4 text-sm text-gray-600">
        This youtube_url is not a playable YouTube video link. (Expected watch?v=... or youtu.be/...)
      </div>
    );
  }

  return <div ref={containerRef} className="w-full overflow-hidden rounded-xl border" />;
}
