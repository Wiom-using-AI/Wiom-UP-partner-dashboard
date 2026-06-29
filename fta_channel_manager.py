"""
FTA Channel Manager — wiom.tv channel registry, health monitor, and admin dashboard.

Architecture overview:
  wiom.tv is an FTA aggregator: it does NOT host/transcode video. It maintains a
  registry of publicly available HLS/DASH stream URLs from broadcaster CDNs
  (Akamai, etc.), YouTube live, and DD Free Dish endpoints. The app (Android +
  web) fetches this registry, renders a channel grid, and plays the stream URL
  directly in ExoPlayer (Android) or HLS.js (web).

To add a new FTA channel you need:
  1. A valid HLS/DASH stream URL (m3u8 or mpd) from the broadcaster's CDN
  2. Channel metadata (name, logo, category, language)
  3. An EPG source (XMLTV format) for the program guide

Run: streamlit run fta_channel_manager.py --server.port 8503
"""

import json
import time
import threading
import requests
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime

REGISTRY_FILE = Path(__file__).parent / "fta_channels.json"

CATEGORIES = ["All", "News", "Entertainment", "Movies", "Music",
              "Devotional", "Regional", "Sports", "Agriculture", "Government"]
LANGUAGES   = ["All", "Hindi", "English", "Bhojpuri", "Awadhi", "Urdu",
               "Punjabi", "Tamil", "Telugu", "Bengali"]
SOURCES     = ["broadcaster_cdn", "youtube_live", "dd_freedish", "m3u_playlist"]

# ── Registry I/O ──────────────────────────────────────────────────────────────

def load_registry() -> list[dict]:
    if REGISTRY_FILE.exists():
        return json.loads(REGISTRY_FILE.read_text())
    return []


def save_registry(channels: list[dict]):
    REGISTRY_FILE.write_text(json.dumps(channels, indent=2, ensure_ascii=False))


# ── Stream health check ───────────────────────────────────────────────────────

def check_stream(url: str, timeout: int = 8) -> tuple[bool, str]:
    """HEAD request to the HLS playlist URL — returns (is_live, status_detail)."""
    if not url or not url.startswith("http"):
        return False, "invalid URL"
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True,
                          headers={"User-Agent": "WiomTV/1.0"})
        if r.status_code == 200:
            return True, f"HTTP {r.status_code}"
        return False, f"HTTP {r.status_code}"
    except requests.Timeout:
        return False, "timeout"
    except Exception as e:
        return False, str(e)[:60]


def run_health_check(channels: list[dict]) -> dict[str, tuple[bool, str]]:
    """Check all channels concurrently; returns {channel_id: (is_live, detail)}."""
    results: dict[str, tuple[bool, str]] = {}
    lock = threading.Lock()

    def _check(ch):
        ok, detail = check_stream(ch["stream_url"])
        with lock:
            results[ch["id"]] = (ok, detail)

    threads = [threading.Thread(target=_check, args=(ch,), daemon=True)
               for ch in channels]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=12)
    return results


# ── M3U export ────────────────────────────────────────────────────────────────

def to_m3u(channels: list[dict]) -> str:
    """Export active channels as an M3U8 playlist (importable into VLC/IPTV apps)."""
    lines = ["#EXTM3U x-tvg-url=\"https://wiom.tv/epg.xml\""]
    for ch in channels:
        if not ch.get("is_active"):
            continue
        lines.append(
            f'#EXTINF:-1 tvg-id="{ch.get("epg_id","")}" '
            f'tvg-name="{ch["name"]}" '
            f'tvg-logo="{ch.get("logo","")}" '
            f'group-title="{ch.get("category","")}",'
            f'{ch["name"]}'
        )
        lines.append(ch["stream_url"])
    return "\n".join(lines)


# ── Streamlit UI ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="FTA Channel Manager — wiom.tv",
    page_icon="📺",
    layout="wide"
)

st.title("📺 FTA Channel Manager — wiom.tv")
st.caption(
    "Manage the Free-to-Air channel registry that powers wiom.tv. "
    "Channels are served as HLS/DASH stream URLs to the Android app and web player."
)

tabs = st.tabs(["📋 Channel Registry", "➕ Add Channel", "🏥 Health Check",
                "📁 M3U Export", "🏗️ Architecture Guide"])

channels = load_registry()

# ── Tab 1: Channel Registry ────────────────────────────────────────────────────
with tabs[0]:
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)
    with col_f1:
        cat_filter = st.selectbox("Category", CATEGORIES)
    with col_f2:
        lang_filter = st.selectbox("Language", LANGUAGES)
    with col_f3:
        active_filter = st.selectbox("Status", ["All", "Active", "Inactive"])
    with col_f4:
        src_filter = st.selectbox("Source", ["All"] + SOURCES)

    filtered = channels
    if cat_filter  != "All": filtered = [c for c in filtered if c.get("category") == cat_filter]
    if lang_filter != "All": filtered = [c for c in filtered if c.get("language") == lang_filter]
    if active_filter == "Active":   filtered = [c for c in filtered if c.get("is_active")]
    if active_filter == "Inactive": filtered = [c for c in filtered if not c.get("is_active")]
    if src_filter  != "All": filtered = [c for c in filtered if c.get("source") == src_filter]

    st.metric("Channels shown", f"{len(filtered)} / {len(channels)}")

    if filtered:
        df = pd.DataFrame([{
            "ID":         ch["id"],
            "Name":       ch["name"],
            "Category":   ch.get("category", ""),
            "Language":   ch.get("language", ""),
            "Region":     ch.get("region", ""),
            "Source":     ch.get("source", ""),
            "Stream URL": ch.get("stream_url", ""),
            "Active":     "✅" if ch.get("is_active") else "❌",
        } for ch in filtered])
        st.dataframe(df, use_container_width=True, hide_index=True)
    else:
        st.info("No channels match the selected filters.")

    st.divider()
    st.subheader("Toggle Active Status")
    channel_names = [f"{c['name']} ({c['id']})" for c in channels]
    toggle_sel = st.selectbox("Select channel", [""] + channel_names, key="toggle_sel")
    if toggle_sel:
        sel_id = toggle_sel.split("(")[-1].rstrip(")")
        ch_obj = next((c for c in channels if c["id"] == sel_id), None)
        if ch_obj:
            new_state = not ch_obj.get("is_active", True)
            label = "Activate" if new_state else "Deactivate"
            if st.button(f"{label} '{ch_obj['name']}'"):
                ch_obj["is_active"] = new_state
                save_registry(channels)
                st.success(f"{'Activated' if new_state else 'Deactivated'} {ch_obj['name']}")
                st.rerun()

# ── Tab 2: Add Channel ─────────────────────────────────────────────────────────
with tabs[1]:
    st.subheader("Add a New FTA Channel")
    st.info(
        "To find an FTA stream URL: inspect the broadcaster's website DevTools → "
        "Network tab → filter by '.m3u8' or 'manifest'. "
        "For DD channels use the Akamai endpoints listed in the Architecture Guide tab."
    )

    with st.form("add_channel_form"):
        col1, col2 = st.columns(2)
        with col1:
            new_id   = st.text_input("Channel ID (slug, e.g. dd-national)",
                                     placeholder="dd-national")
            new_name = st.text_input("Channel Name", placeholder="DD National")
            new_cat  = st.selectbox("Category", CATEGORIES[1:])
            new_lang = st.selectbox("Language", LANGUAGES[1:])
            new_region = st.text_input("Region", placeholder="National / Uttar Pradesh")
        with col2:
            new_stream = st.text_input(
                "HLS Stream URL (.m3u8)",
                placeholder="https://example.akamaized.net/live/master.m3u8"
            )
            new_logo   = st.text_input("Logo URL", placeholder="https://...")
            new_epg    = st.text_input("EPG ID (XMLTV)", placeholder="dd-national.in")
            new_source = st.selectbox("Stream Source", SOURCES)
            new_active = st.checkbox("Active", value=True)

        submitted = st.form_submit_button("Add Channel", type="primary")
        if submitted:
            if not new_id or not new_name or not new_stream:
                st.error("ID, Name, and Stream URL are required.")
            elif any(c["id"] == new_id for c in channels):
                st.error(f"Channel ID '{new_id}' already exists.")
            else:
                channels.append({
                    "id":         new_id.strip().lower().replace(" ", "-"),
                    "name":       new_name.strip(),
                    "logo":       new_logo.strip(),
                    "category":   new_cat,
                    "language":   new_lang,
                    "region":     new_region.strip(),
                    "stream_url": new_stream.strip(),
                    "epg_id":     new_epg.strip(),
                    "source":     new_source,
                    "is_active":  new_active,
                })
                save_registry(channels)
                st.success(f"✅ Added '{new_name}' to the registry.")
                st.rerun()

# ── Tab 3: Health Check ────────────────────────────────────────────────────────
with tabs[2]:
    st.subheader("Stream Health Monitor")
    st.caption(
        "Sends an HTTP HEAD request to each stream URL. A 200 OK means the CDN "
        "endpoint is reachable — it does not guarantee the video is playing correctly. "
        "For production, use a proper HLS prober (ffprobe)."
    )

    active_chs = [c for c in channels if c.get("is_active")]
    st.metric("Active channels to check", len(active_chs))

    if st.button("▶ Run Health Check", type="primary"):
        with st.spinner(f"Checking {len(active_chs)} streams (parallel, up to 12s)..."):
            t0 = time.time()
            results = run_health_check(active_chs)
            elapsed = time.time() - t0

        rows = []
        for ch in active_chs:
            ok, detail = results.get(ch["id"], (False, "not checked"))
            rows.append({
                "Channel":  ch["name"],
                "Category": ch.get("category", ""),
                "Stream URL": ch["stream_url"][:70] + "…" if len(ch["stream_url"]) > 70 else ch["stream_url"],
                "Status":   "🟢 Live" if ok else "🔴 Down",
                "Detail":   detail,
            })

        health_df = pd.DataFrame(rows)
        live_count = sum(1 for r in rows if "Live" in r["Status"])
        c1, c2, c3 = st.columns(3)
        c1.metric("Live", live_count, delta=None)
        c2.metric("Down", len(rows) - live_count, delta=None)
        c3.metric("Check duration", f"{elapsed:.1f}s")

        st.dataframe(health_df, use_container_width=True, hide_index=True)

        if st.button("Auto-deactivate dead streams"):
            dead_ids = {ch["id"] for ch in active_chs
                        if not results.get(ch["id"], (False,))[0]}
            for ch in channels:
                if ch["id"] in dead_ids:
                    ch["is_active"] = False
            save_registry(channels)
            st.success(f"Deactivated {len(dead_ids)} dead streams.")
            st.rerun()
    else:
        st.info("Click 'Run Health Check' to probe all active stream URLs.")

# ── Tab 4: M3U Export ──────────────────────────────────────────────────────────
with tabs[3]:
    st.subheader("M3U8 Playlist Export")
    st.caption(
        "Export active channels as a standard M3U8 playlist. "
        "Import this into VLC, IPTV Smarters, TiviMate, or any M3U-compatible player."
    )

    active_only = [c for c in channels if c.get("is_active")]
    m3u_content = to_m3u(active_only)

    st.code(m3u_content, language="text")

    st.download_button(
        label="⬇ Download wiom_fta.m3u8",
        data=m3u_content,
        file_name="wiom_fta.m3u8",
        mime="text/plain",
    )

    st.divider()
    st.subheader("Channel Summary")
    if active_only:
        summary_df = pd.DataFrame([{
            "Category": c["category"],
            "Language": c["language"],
            "Region":   c["region"],
            "Name":     c["name"],
        } for c in active_only])
        by_cat = summary_df.groupby("Category").size().reset_index(name="Count")
        st.bar_chart(by_cat.set_index("Category"))

# ── Tab 5: Architecture Guide ─────────────────────────────────────────────────
with tabs[4]:
    st.subheader("🏗️ How wiom.tv FTA Channels Work — Architecture Guide")

    st.markdown("""
## What wiom.tv Is

wiom.tv is a **channel aggregator**, not a broadcaster. It does NOT:
- Host or store video files
- Transcode or re-encode streams
- Pay licensing fees for most FTA content

It DOES:
- Maintain a **registry of publicly available HLS/DASH stream URLs**
- Serve that registry to the Android app and web player
- Monitor stream health and remove dead links
- Provide an EPG (Electronic Program Guide) in XMLTV format

---

## The Three Stream Source Types

### 1. Broadcaster CDN (best quality, most stable)
Doordarshan (government broadcaster) publishes official HLS endpoints via Akamai CDN.
These are the most stable FTA streams in India.

```
DD National:  https://ddnationalcloudfront.akamaized.net/live/smil:DD_Nat.smil/playlist.m3u8
DD News:      https://ddnewscloudfront.akamaized.net/live/smil:DD_News.smil/playlist.m3u8
DD UP:        https://dduphls.akamaized.net/live/smil:DD_UP.smil/playlist.m3u8
DD Kisan:     https://ddkisanhls.akamaized.net/live/smil:DD_Kisan.smil/playlist.m3u8
```

Private channels (Aaj Tak, NDTV, Zee News) also expose HLS endpoints on their CDNs —
find them by opening DevTools → Network → filter `.m3u8` while watching live on their website.

### 2. YouTube Live
Many Indian channels stream live on YouTube. Use the YouTube Data API v3:
```
GET https://www.googleapis.com/youtube/v3/search
  ?part=snippet&channelId={CHANNEL_ID}&type=video&eventType=live&key={API_KEY}
```
Then construct the embed/stream URL. YouTube live can be played via yt-dlp or
an HLS proxy since YouTube streams are adaptive and require auth cookies.

### 3. DD Free Dish (Satellite → IP)
DD Free Dish is India's largest FTA satellite platform (GSAT-30 @ 83°E).
Channels: 800+. To add these you need:
- A satellite dish + DVB-S2 receiver with IP output (e.g. Dreambox, VU+, Enigma2)
- The receiver transcodes the satellite stream to HLS and serves it on your LAN
- You then push this to a CDN (AWS CloudFront, Akamai) for national delivery

---

## System Architecture Diagram

```
Satellite (GSAT-30/Intelsat-20)
        │ DVB-S2
        ▼
  [IRD/Decoder] ── TS stream ──▶ [Encoder: Elemental/FFMPEG]
                                         │ HLS (.m3u8 + .ts segments)
                                         ▼
                               [Origin Server / CDN]
                           (Akamai / AWS CloudFront / Cloudflare)
                                         │
              ┌──────────────────────────┼─────────────────────────┐
              ▼                          ▼                         ▼
     [wiom.tv Web App]         [wiom.tv Android App]      [M3U Clients]
      HLS.js / Video.js           ExoPlayer                VLC, TiviMate
```

---

## How to Add Your Own FTA Channel

### Step 1 — Find or create the HLS stream URL

**Option A (easiest): Use existing broadcaster CDN**
Open any news channel's website, open DevTools (F12), go to Network tab,
start the live stream, filter by "m3u8" — copy the master playlist URL.

**Option B: Self-encode from satellite**
```bash
# Install FFmpeg and encode a satellite TS feed to HLS
ffmpeg -i udp://@239.0.0.1:1234 \\
  -c:v libx264 -preset veryfast -b:v 1500k \\
  -c:a aac -b:a 128k \\
  -hls_time 6 -hls_list_size 10 -hls_flags delete_segments \\
  -f hls /var/www/html/live/channel_name/index.m3u8
```

**Option C: YouTube live proxy**
Use yt-dlp to get the HLS URL of a YouTube live stream:
```bash
yt-dlp -g "https://www.youtube.com/watch?v=LIVE_VIDEO_ID"
# Returns a direct .m3u8 URL valid for ~6 hours
```

### Step 2 — Add to the registry
Use the **Add Channel** tab above, or edit `fta_channels.json` directly.

### Step 3 — Verify stream health
Use the **Health Check** tab to confirm the stream URL returns HTTP 200.
For deeper validation:
```bash
ffprobe -v quiet -print_format json -show_streams \\
  "https://your.cdn.net/live/channel/master.m3u8"
```

### Step 4 — EPG (Program Guide)
FTA EPG data sources for India:
- `https://www.epgshare01.online/epg_risat.xml.gz` (community-maintained)
- DD Free Dish official EPG (XMLTV format, updated daily)
- Build your own by scraping broadcaster websites

### Step 5 — Push to the wiom.tv app
The Android app (`com.wiomtv.android.prod`) fetches the channel list from the
`labs-api.wiom.in` backend. New channels added to the registry will appear in
the app after the next registry sync (or app restart).

---

## Key Indian FTA Channels & Their CDN Patterns

| Channel         | CDN Pattern                                      | Source         |
|-----------------|--------------------------------------------------|----------------|
| DD National     | `ddnationalcloudfront.akamaized.net`             | Doordarshan    |
| DD News         | `ddnewscloudfront.akamaized.net`                 | Doordarshan    |
| DD UP / Lucknow | `dduphls.akamaized.net`                          | Doordarshan    |
| DD Kisan        | `ddkisanhls.akamaized.net`                       | Doordarshan    |
| DD Sports       | `ddsportshls.akamaized.net`                      | Doordarshan    |
| Aaj Tak         | `linear-intoday.akamaized.net`                   | India Today    |
| NDTV India      | `ndtvgrouphls.akamaized.net`                     | NDTV           |
| Zee News        | `z5linear-ause.akamaized.net`                    | Zee Media      |
| Sansad TV       | `sansad.in/ls/live/`                             | Government     |

---

## Legal Note
FTA (Free-to-Air) channels are legally available for reception by anyone with
suitable equipment. Aggregating and linking to publicly available FTA streams
is the model wiom.tv uses. For channels with DRM or encryption (e.g., most
entertainment channels behind paywalls), you need a valid carriage agreement
with the broadcaster.
    """)

st.divider()
st.caption(f"Registry: {REGISTRY_FILE} · {len(channels)} channels total · "
           f"Last updated: {datetime.now().strftime('%d %b %Y %H:%M')}")
