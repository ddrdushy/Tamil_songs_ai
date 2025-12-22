import { NextResponse } from "next/server";

export async function GET(req: Request) {
  const apiBase = process.env.NEXT_PUBLIC_API_BASE!;
  const { searchParams } = new URL(req.url);

  const q = searchParams.get("q") ?? "";
  const mood = searchParams.get("mood") ?? "";
  const k = searchParams.get("k") ?? "20";

  const url = new URL(`${apiBase}/player/query`);
  url.searchParams.set("q", q);
  if (mood) url.searchParams.set("mood", mood);
  url.searchParams.set("k", k);

  const r = await fetch(url.toString(), { cache: "no-store" });
  const data = await r.json();
  return NextResponse.json(data, { status: r.status });
}
