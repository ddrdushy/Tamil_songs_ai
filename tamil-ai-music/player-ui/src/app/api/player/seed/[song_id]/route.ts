import { NextResponse } from "next/server";

export async function GET(
  req: Request,
  { params }: { params: { song_id: string } }
) {
  const apiBase = process.env.NEXT_PUBLIC_API_BASE!;
  const { searchParams } = new URL(req.url);

  const k = searchParams.get("k") ?? "20";

  const url = new URL(`${apiBase}/player/seed/${params.song_id}`);
  url.searchParams.set("k", k);

  const r = await fetch(url.toString(), { cache: "no-store" });
  const data = await r.json();
  return NextResponse.json(data, { status: r.status });
}
