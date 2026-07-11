import pandas as pd
import numpy as np
import sys

# --- CONFIGURATION ---
INPUT_CSV_URL = 'https://raw.githubusercontent.com/iptv-org/database/master/data/feeds.csv'
STREAMS_JSON_URL = 'https://iptv-org.github.io/api/streams.json'

# Output files
OUTPUT_ALL = 'playlist.m3u'
OUTPUT_HD = 'playlist_hd.m3u'
OUTPUT_SD = 'playlist_sd.m3u'

# Filter settings
TARGET_COLUMN = 'languages'
SEARCH_TERM = 'eng' # English

def generate_playlist():
    print("--- Starting Process ---")

    # 1. Download and Filter CSV Data
    print(f"1. Downloading metadata from: {INPUT_CSV_URL}")
    try:
        df_csv = pd.read_csv(INPUT_CSV_URL)
    except Exception as e:
        print(f"Error downloading CSV: {e}")
        return

    if TARGET_COLUMN not in df_csv.columns:
        print(f"Error: Column '{TARGET_COLUMN}' not found in CSV.")
        print(f"Available columns: {list(df_csv.columns)}")
        return

    print(f"   Filtering for {TARGET_COLUMN} = '{SEARCH_TERM}'...")
    df_filtered = df_csv[df_csv[TARGET_COLUMN] == SEARCH_TERM].copy()

    if 'channel' not in df_filtered.columns and 'id' in df_filtered.columns:
        print("   Renaming 'id' column to 'channel' for merging...")
        df_filtered.rename(columns={'id': 'channel'}, inplace=True)

    print(f"   Found {len(df_filtered)} rows matching criteria.")

    if df_filtered.empty:
        print("No channels found. Stopping.")
        return

    # 2. Download JSON Data
    print(f"2. Downloading streams from: {STREAMS_JSON_URL}")
    try:
        df_streams = pd.read_json(STREAMS_JSON_URL)
    except Exception as e:
        print(f"Error downloading JSON: {e}")
        return

    # 3. Merge Data
    print("3. Merging streams with csv data...")
    merged_df = pd.merge(
        df_streams, 
        df_filtered, 
        on='channel', 
        how='inner', 
        suffixes=('', '_info')
    )

    merged_df = merged_df.replace({np.nan: ""})
    print(f"   Total streams matched: {len(merged_df)}")

    # 4. Generate M3U Files (All, HD, SD)
    print("4. Writing M3U playlists...")

    # Open all three files at once
    try:
        f_all = open(OUTPUT_ALL, 'w', encoding='utf-8')
        f_hd = open(OUTPUT_HD, 'w', encoding='utf-8')
        f_sd = open(OUTPUT_SD, 'w', encoding='utf-8')

        # Write header for all files
        f_all.write("#EXTM3U\n")
        f_hd.write("#EXTM3U\n")
        f_sd.write("#EXTM3U\n")

        for index, row in merged_df.iterrows():
            url = str(row.get("url", "")).strip()
            if not url:
                continue

            channel_id = str(row.get("channel", "")).strip()
            title = channel_id
            tvg_id = channel_id
            group = str(row.get("broadcast_area", "")).replace("c/", "").replace(";", ", ")
            language = str(row.get("languages", ""))
            quality = str(row.get("format", "")).lower() # Convert to lowercase for easier check
            
            # --- SD / HD DETECTION LOGIC ---
            # We also check height/width if they exist in the row, or look into the format string
            height = row.get("height", 0)
            width = row.get("width", 0)
            
            is_hd = False
            # Check by resolution numbers (720p and above is HD)
            if (isinstance(height, (int, float)) and height >= 720) or (isinstance(width, (int, float)) and width >= 1280):
                is_hd = True
            # Check by text indicators in quality or url
            elif "hd" in quality or "1080" in quality or "720" in quality or "hd" in url.lower():
                is_hd = True

            user_agent = str(row.get("user_agent", ""))
            referrer = str(row.get("referrer", ""))

            # Build EXTINF line
            extinf = (
                f'#EXTINF:-1 tvg-id="{tvg_id}" '
                f'group-title="{group}" '
                f'tvg-language="{language}" '
                f'tvg-quality="{quality}",{title}\n'
            )

            # Helper function to write to a specific file target
            def write_to_file(file_obj):
                file_obj.write(extinf)
                if user_agent:
                    file_obj.write(f"#EXTVLCOPT:http-user-agent={user_agent}\n")
                if referrer:
                    file_obj.write(f"#EXTVLCOPT:http-referrer={referrer}\n")
                file_obj.write(f"{url}\n\n")

            # Always write to the main playlist
            write_to_file(f_all)

            # Separate based on quality
            if is_hd:
                write_to_file(f_hd)
            else:
                write_to_file(f_sd)

    finally:
        # Ensure all files are properly closed
        f_all.close()
        f_hd.close()
        f_sd.close()

    print(f"✅ Success! Created:")
    print(f"   - All channels: {OUTPUT_ALL}")
    print(f"   - HD only: {OUTPUT_HD}")
    print(f"   - SD only: {OUTPUT_SD}")

if __name__ == "__main__":
    generate_playlist()
