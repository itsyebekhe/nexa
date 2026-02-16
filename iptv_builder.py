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

    # 1. Download and Filter CSV Data
    print(f"1. Downloading metadata from: {INPUT_CSV_URL}")
    try:
        df_csv = pd.read_csv(INPUT_CSV_URL)
    except Exception as e:
        print(f"Error downloading CSV: {e}")
        return

    # Check if target column exists
    if TARGET_COLUMN not in df_csv.columns:
        print(f"Error: Column '{TARGET_COLUMN}' not found in CSV.")
        # Optional: Print available columns to help debug
        print(f"Available columns: {list(df_csv.columns)}")
        return

    # Filter rows
    print(f"   Filtering for {TARGET_COLUMN} = '{SEARCH_TERM}'...")
    df_filtered = df_csv[df_csv[TARGET_COLUMN] == SEARCH_TERM].copy()
    
    # --- FIX FOR "CHANNEL NOT UNIQUE" ERROR ---
    # We need a column named 'channel' to merge with streams.json.
    # If the CSV has 'id' but not 'channel', we rename 'id' to 'channel'.
    # If it already has 'channel', we do nothing.
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
    
    # We use suffixes to prevent naming collisions.
    # Columns from streams.json keep their names.
    # Columns from the CSV get '_info' added to them (e.g., 'name' -> 'name_info').
    merged_df = pd.merge(
        df_streams, 
        df_filtered, 
        on='channel', 
        how='inner', 
        suffixes=('', '_info')
    )
    
    # Handle NaN values
    merged_df = merged_df.replace({np.nan: ""})
    
    print(f"   Total streams matched: {len(merged_df)}")

    # 4. Generate M3U File
    print(f"4. Writing M3U playlist to: {OUTPUT_FILE}")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as m3u:
        m3u.write("#EXTM3U\n")

        for index, row in merged_df.iterrows():
            # Get URL
            url = str(row.get("url", "")).strip()
            
            # Skip if URL is empty
            if not url:
                continue

            # --- NAME SELECTION ---
            # You requested to use the Channel ID as the name.
            # 'channel' is the ID used to merge the files.
            channel_id = str(row.get("channel", "")).strip()
            
            # Use Channel ID as the title
            title = channel_id
            
            # OPTIONAL: If you want ID + Name, uncomment the line below:
            # human_name = str(row.get("name_info", "")).strip()
            # title = f"{channel_id} | {human_name}"

            # TVG ID (Guide ID)
            tvg_id = channel_id
            
            # Group (Broadcast Area)
            group = str(row.get("broadcast_area", "")).replace("c/", "").replace(";", ", ")
            
            # Language
            language = str(row.get("languages", ""))
            
            # Quality/Format
            quality = str(row.get("format", ""))
            
            # User Agent / Referrer
            user_agent = str(row.get("user_agent", ""))
            referrer = str(row.get("referrer", ""))

            # Build EXTINF line
            extinf = (
                f'#EXTINF:-1 tvg-id="{tvg_id}" '
                f'group-title="{group}" '
                f'tvg-language="{language}" '
                f'tvg-quality="{quality}",{title}\n'
            )
            m3u.write(extinf)

            # Add Headers if present
            if user_agent:
                m3u.write(f"#EXTVLCOPT:http-user-agent={user_agent}\n")
            if referrer:
                m3u.write(f"#EXTVLCOPT:http-referrer={referrer}\n")

            m3u.write(f"{url}\n\n")

    print(f"✅ Success! Playlist created: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_playlist()