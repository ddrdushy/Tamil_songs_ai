export type SongItem = {
  song_id: string;
  title?: string;
  movie?: string;
  year?: number | string;
  mood?: string;
  decade?: string;
  score?: number;
  youtube_url?: string;

  // optional enrichment fields (later)
  genre?: string;
  rhythm?: string;
  mood_web?: string;
  meta_source?: string;
  meta_confidence?: number;
};
