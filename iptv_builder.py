import pandas as pd
import numpy as np
import sys

# --- CONFIGURATION ---
INPUT_CSV_URL = 'https://raw.githubusercontent.com/iptv-org/database/master/data/feeds.csv'
STREAMS_JSON_URL = 'https://iptv-org.github.io/api/streams.json'
OUTPUT_FILE = 'playlist.m3u'

# Filter settings
TARGET_COLUMN = 'languages'
SEARCH_TERM = 'fas' # Persian

def generate_playlist():
    print("--- Starting Process ---")

    # 1. Download and Filter CSV Data (feeds.csv)
    print(f"1. Downloading metadata from: {INPUT_CSV_URL}")
    try:
        df_csv = pd.read_csv(INPUT_CSV_URL)
    except Exception as e:
        print(f"Error downloading CSV: {e}")
        return

    # Check if the column exists
    if TARGET_COLUMN not in df_csv.columns:
        print(f"Error: Column '{TARGET_COLUMN}' not found in CSV.")
        return

    # Filter rows (Handle missing values safely)
    print(f"   Filtering for {TARGET_COLUMN} = '{SEARCH_TERM}'...")
    df_filtered = df_csv[df_csv[TARGET_COLUMN] == SEARCH_TERM].copy()
    
    # REMOVED: The logic that renamed 'id' to 'channel'. 
    # feeds.csv already has a 'channel' column.

    print(f"   Found {len(df_filtered)} rows matching criteria.")

    if df_filtered.empty:
        print("No channels found with that language. Stopping.")
        return

    # 2. Download JSON Data (streams.json)
    print(f"2. Downloading streams from: {STREAMS_JSON_URL}")
    try:
        df_streams = pd.read_json(STREAMS_JSON_URL)
    except Exception as e:
        print(f"Error downloading JSON: {e}")
        return

    # 3. Merge Data
    print("3. Merging streams with csv data...")
    
    # Both files have a 'url' column. We must resolve this collision.
    # suffixes=('', '_src') means:
    # - Columns from streams.json (left) keep their names (e.g., 'url')
    # - Columns from feeds.csv (right) get '_src' added (e.g., 'url_src')
    merged_df = pd.merge(
        df_streams, 
        df_filtered, 
        on='channel', 
        how='inner', 
        suffixes=('', '_src')
    )
    
    # Replace NaN values with empty strings
    merged_df = merged_df.replace({np.nan: ""})
    
    print(f"   Total streams matched: {len(merged_df)}")

    # 4. Generate M3U File
    print(f"4. Writing M3U playlist to: {OUTPUT_FILE}")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as m3u:
        m3u.write("#EXTM3U\n")

        for index, row in merged_df.iterrows():
            # Get the Stream URL (from streams.json)
            url = str(row.get("url", "")).strip()
            
            # If the URL is empty or it's just the feed source URL, skip it
            if not url:
                continue

            # Mapping columns based on feeds.csv headers
            # 'name' is usually the channel name in feeds.csv
            title = str(row.get("name", "")).strip() 
            tvg_id = str(row.get("channel", "")).strip()
            
            # Clean up broadcast area
            group = str(row.get("broadcast_area", "")).replace("c/", "").replace(";", ", ")
            
            language = str(row.get("languages", ""))
            
            # Streams.json sometimes has 'format' (mp4, m3u8), feeds.csv does not.
            # We look for it, but default to empty.
            quality = str(row.get("format", ""))
            
            user_agent = str(row.get("user_agent", ""))
            referrer = str(row.get("referrer", ""))

            # Build EXTINF line
            # Format: #EXTINF:-1 tvg-id="ID" group-title="Group",Title
            extinf = (
                f'#EXTINF:-1 tvg-id="{tvg_id}" '
                f'group-title="{group}" '
                f'tvg-language="{language}" '
                f'tvg-quality="{quality}",{title}\n'
            )
            m3u.write(extinf)

            # Add HTTP headers if they exist in streams.json
            if user_agent:
                m3u.write(f"#EXTVLCOPT:http-user-agent={user_agent}\n")
            if referrer:
                m3u.write(f"#EXTVLCOPT:http-referrer={referrer}\n")

            m3u.write(f"{url}\n\n")

    print(f"✅ Success! Playlist created: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_playlist()