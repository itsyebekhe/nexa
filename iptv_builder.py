import csv
import requests
import pandas as pd
from io import StringIO

# ==============================
# CONFIG
# ==============================
CHANNELS_URL = "https://raw.githubusercontent.com/iptv-org/database/refs/heads/master/data/channels.csv"
STREAMS_URL = "https://iptv-org.github.io/api/streams.json"

LANGUAGE_FILTER = "fas"

FILTERED_CSV = "filtered_channels.csv"
MERGED_CSV = "final_merged_list.csv"
OUTPUT_M3U = "playlist.m3u"


# ==============================
# STEP 1 — Download & Filter Channels
# ==============================
def download_and_filter_channels():
    print("Downloading channels CSV...")

    df = pd.read_csv(CHANNELS_URL, dtype=str)
    df["languages"] = df["languages"].fillna("")

    if "languages" not in df.columns:
        print("❌ 'languages' column not found!")
        print("Columns:", df.columns)
        return

    filtered_df = df[df["languages"].str.contains(LANGUAGE_FILTER, na=False)]

    print(f"Filtered {len(filtered_df)} channels with language '{LANGUAGE_FILTER}'")

    filtered_df.to_csv(FILTERED_CSV, index=False)



# ==============================
# STEP 2 — Merge with Streams
# ==============================
def merge_streams():
    print("Downloading streams JSON...")
    df_streams = pd.read_json(STREAMS_URL)

    print("Reading filtered channels...")
    df_channels = pd.read_csv(FILTERED_CSV)

    if df_channels.empty:
        print("⚠ No channels found after filtering. Skipping merge.")
        return

    print("Merging data...")
    merged_df = pd.merge(
        df_streams,
        df_channels,
        left_on="channel",
        right_on="id",
        how="inner"
    )

    merged_df.to_csv(MERGED_CSV, index=False)

    print(f"Merged {len(merged_df)} rows")



# ==============================
# STEP 3 — Convert to M3U
# ==============================
def csv_to_m3u():
    print("Generating M3U playlist...")

    with open(MERGED_CSV, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        with open(OUTPUT_M3U, "w", encoding="utf-8") as m3u:
            m3u.write("#EXTM3U\n")

            for row in reader:
                title = row.get("title", "").strip()
                url = row.get("url", "").strip()
                tvg_id = row.get("name", "").strip()
                group = row.get("broadcast_area", "").replace("c/", "").replace(";", ", ")
                language = row.get("languages", "")
                quality = row.get("format", "")
                user_agent = row.get("user_agent", "")
                referrer = row.get("referrer", "")

                if not url:
                    continue

                extinf = (
                    f'#EXTINF:-1 tvg-id="{tvg_id}" '
                    f'group-title="{group}" '
                    f'tvg-language="{language}" '
                    f'tvg-quality="{quality}",{title}\n'
                )

                m3u.write(extinf)

                if user_agent:
                    m3u.write(f"#EXTVLCOPT:http-user-agent={user_agent}\n")
                if referrer:
                    m3u.write(f"#EXTVLCOPT:http-referrer={referrer}\n")

                m3u.write(f"{url}\n\n")

    print(f"✅ IPTV playlist created: {OUTPUT_M3U}")


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    download_and_filter_channels()
    merge_streams()
    csv_to_m3u()
    print("🎉 All done successfully!")
