#!/usr/bin/env python3
"""
IPTV Playlist Generator for iptv-org database.
Fetches feed metadata and stream URLs, merges them, and generates
All, HD, and SD M3U playlists filtered by language.
"""

from __future__ import annotations

import argparse
from contextlib import ExitStack
from io import StringIO
import logging
from pathlib import Path
import re
import sys
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
        return pd.read_csv(StringIO(response.text))
    elif file_type == "json":
        return pd.DataFrame(response.json())
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def is_stream_hd(height: object, width: object, quality: str, url: str) -> bool:
    """Determine whether a stream is HD (>= 720p) based on dimensions or tags."""
    try:
        h = float(height) if height != "" and height is not None else 0
        w = float(width) if width != "" and width is not None else 0
        if h >= 720 or w >= 1280:
            return True
    except (ValueError, TypeError):
        pass

    q_lower = quality.lower()
    u_lower = url.lower()
    return any(k in q_lower for k in ("hd", "1080", "720")) or any(k in u_lower for k in ("hd", "1080", "720"))


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

    # 2. Filter by Language (Supports multi-languages like 'eng;fas')
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

    # 5. Determine Output Paths (All, HD, SD)
    base_path = Path(output_file)
    path_all = base_path
    path_hd = base_path.with_name(f"{base_path.stem}_hd{base_path.suffix}")
    path_sd = base_path.with_name(f"{base_path.stem}_sd{base_path.suffix}")

    logging.info(f"Writing M3U playlists:\n - All: {path_all}\n - HD:  {path_hd}\n - SD:  {path_sd}")

    # Use ExitStack to cleanly manage all 3 file streams simultaneously
    with ExitStack() as stack:
        f_all = stack.enter_context(path_all.open("w", encoding="utf-8"))
        f_hd = stack.enter_context(path_hd.open("w", encoding="utf-8"))
        f_sd = stack.enter_context(path_sd.open("w", encoding="utf-8"))

        # Write M3U headers
        f_all.write("#EXTM3U\n")
        f_hd.write("#EXTM3U\n")
        f_sd.write("#EXTM3U\n")

        hd_count = 0
        sd_count = 0

        for row in merged.itertuples(index=False):
            url = str(getattr(row, "url", "")).strip()
            if not url:
                continue

            channel_id = str(getattr(row, "channel", "")).strip()
            name = str(getattr(row, "name_feed", "")).strip() or channel_id
            raw_area = str(getattr(row, "broadcast_area", ""))
            group = raw_area.replace("c/", "").replace(";", ", ") if raw_area else "General"
            language = str(getattr(row, "languages", ""))
            quality = str(getattr(row, "format", ""))
            user_agent = str(getattr(row, "user_agent", "")).strip()
            referrer = str(getattr(row, "referrer", "")).strip()

            # Build EXTINF line
            extinf = (
                f'#EXTINF:-1 tvg-id="{channel_id}" '
                f'tvg-name="{name}" '
                f'group-title="{group}" '
                f'tvg-language="{language}" '
                f'tvg-quality="{quality}",{channel_id}\n'
            )

            # Assemble entry block
            entry = extinf
            if user_agent:
                entry += f"#EXTVLCOPT:http-user-agent={user_agent}\n"
            if referrer:
                entry += f"#EXTVLCOPT:http-referrer={referrer}\n"
            entry += f"{url}\n\n"

            # Always write to main playlist
            f_all.write(entry)

            # Quality categorization
            height = getattr(row, "height", 0)
            width = getattr(row, "width", 0)
            if is_stream_hd(height, width, quality, url):
                f_hd.write(entry)
                hd_count += 1
            else:
                f_sd.write(entry)
                sd_count += 1

    logging.info(f"✅ Success! Generated:")
    logging.info(f"   - All ({len(merged)} streams): {path_all.name}")
    logging.info(f"   - HD  ({hd_count} streams): {path_hd.name}")
    logging.info(f"   - SD  ({sd_count} streams): {path_sd.name}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate M3U playlists from iptv-org.")
    parser.add_argument(
        "-l", "--lang", default=DEFAULT_LANG, help=f"ISO 639-3 language code (default: {DEFAULT_LANG})"
    )
    parser.add_argument(
        "-o", "--output", default=DEFAULT_OUTPUT, help=f"Base output file path (default: {DEFAULT_OUTPUT})"
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