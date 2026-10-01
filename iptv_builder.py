#!/usr/bin/env python3
"""
IPTV Playlist Generator for iptv-org database.
Fetches feed metadata and stream URLs, merges them, and generates an M3U file.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
import pandas as pd
import requests

# Default Configuration
DEFAULT_FEEDS_URL = "https://raw.githubusercontent.com/iptv-org/database/master/data/feeds.csv"
DEFAULT_STREAMS_URL = "https://iptv-org.github.io/api/streams.json"
DEFAULT_LANG = "fas"          # Persian (ISO 639-3 code)
DEFAULT_OUTPUT = "playlist.m3u"
REQUEST_TIMEOUT = 15          # seconds

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def fetch_dataframe(url: str, file_type: str = "csv") -> pd.DataFrame:
    """Download data using requests to ensure timeouts and proper headers."""
    headers = {"User-Agent": "Mozilla/5.0 (compatible; PlaylistGenerator/2.0)"}
    response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()

    if file_type == "csv":
        # Using io.StringIO prevents writing temp files to disk
        from io import StringIO
        return pd.read_csv(StringIO(response.text))
    elif file_type == "json":
        return pd.DataFrame(response.json())
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def build_playlist(
    lang: str = DEFAULT_LANG,
    output_file: str = DEFAULT_OUTPUT,
    feeds_url: str = DEFAULT_FEEDS_URL,
    streams_url: str = DEFAULT_STREAMS_URL,
    include_offline: bool = False,
) -> None:
    logging.info("Starting playlist generation...")

    # 1. Download Feeds Metadata
    logging.info(f"Downloading feeds metadata from: {feeds_url}")
    try:
        df_feeds = fetch_dataframe(feeds_url, file_type="csv")
    except Exception as e:
        logging.error(f"Failed to retrieve feeds CSV: {e}")
        sys.exit(1)

    if "languages" not in df_feeds.columns:
        logging.error("Column 'languages' was not found in the feeds dataset.")
        sys.exit(1)

    # 2. Filter by Language (Handles multi-languages like 'eng;fas')
    logging.info(f"Filtering feeds for language: '{lang}'...")
    lang_pattern = rf"\b{re.escape(lang)}\b"
    df_filtered = df_feeds[
        df_feeds["languages"].astype(str).str.contains(lang_pattern, regex=True, na=False)
    ].copy()

    # Ensure consistent channel key
    if "channel" not in df_filtered.columns and "id" in df_filtered.columns:
        df_filtered.rename(columns={"id": "channel"}, inplace=True)

    if df_filtered.empty:
        logging.warning(f"No channels found matching language code '{lang}'.")
        return

    logging.info(f"Found {len(df_filtered)} matching feed(s).")

    # 3. Download Streams Data
    logging.info(f"Downloading streams list from: {streams_url}")
    try:
        df_streams = fetch_dataframe(streams_url, file_type="json")
    except Exception as e:
        logging.error(f"Failed to retrieve streams JSON: {e}")
        sys.exit(1)

    # Filter out offline streams if requested
    if not include_offline and "status" in df_streams.columns:
        df_streams = df_streams[df_streams["status"] != "offline"]

    # 4. Merge Feeds and Streams
    logging.info("Merging feeds with streams...")
    merged = pd.merge(
        df_streams,
        df_filtered,
        on="channel",
        how="inner",
        suffixes=("_stream", "_feed"),
    ).fillna("")

    if merged.empty:
        logging.warning("No active streams matched the filtered feeds.")
        return

    logging.info(f"Total matched streams: {len(merged)}")

    # 5. Write M3U Playlist
    output_path = Path(output_file)
    logging.info(f"Writing M3U playlist to: {output_path.resolve()}")

    with output_path.open("w", encoding="utf-8") as m3u:
        m3u.write("#EXTM3U\n")

        # itertuples is significantly faster than iterrows
        for row in merged.itertuples(index=False):
            url = str(getattr(row, "url", "")).strip()
            if not url:
                continue

            channel_id = str(getattr(row, "channel", "")).strip()
            # Title fallback: feed name -> channel_id
            name = str(getattr(row, "name_feed", "")).strip() or channel_id
            
            # Format metadata
            raw_area = str(getattr(row, "broadcast_area", ""))
            group = raw_area.replace("c/", "").replace(";", ", ") if raw_area else "General"
            language = str(getattr(row, "languages", ""))
            quality = str(getattr(row, "format", ""))
            user_agent = str(getattr(row, "user_agent", "")).strip()
            referrer = str(getattr(row, "referrer", "")).strip()

            # Build EXTINF Header
            extinf = (
                f'#EXTINF:-1 tvg-id="{channel_id}" '
                f'tvg-name="{name}" '
                f'group-title="{group}" '
                f'tvg-language="{language}" '
                f'tvg-quality="{quality}",{channel_id}\n'
            )
            m3u.write(extinf)

            # Player compatibility: VLC options & JSON headers
            if user_agent:
                m3u.write(f"#EXTVLCOPT:http-user-agent={user_agent}\n")
            if referrer:
                m3u.write(f"#EXTVLCOPT:http-referrer={referrer}\n")

            m3u.write(f"{url}\n\n")

    logging.info(f"✅ Success! Created {output_path.name} with {len(merged)} stream(s).")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an M3U playlist from iptv-org.")
    parser.add_argument(
        "-l", "--lang", default=DEFAULT_LANG, help=f"ISO 639-3 language code (default: {DEFAULT_LANG})"
    )
    parser.add_argument(
        "-o", "--output", default=DEFAULT_OUTPUT, help=f"Output file path (default: {DEFAULT_OUTPUT})"
    )
    parser.add_argument(
        "--include-offline", action="store_true", help="Include streams marked as offline"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_playlist(
        lang=args.lang,
        output_file=args.output,
        include_offline=args.include_offline,
    )
