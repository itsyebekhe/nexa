import pandas as pd
import numpy as np

# --- CONFIGURATION ---
# URL for the channels metadata (Database)
INPUT_CSV_URL = 'https://raw.githubusercontent.com/iptv-org/database/master/data/feed.csv'

# URL for the live streams
STREAMS_JSON_URL = 'https://iptv-org.github.io/api/streams.json'

# Output filename
OUTPUT_FILE = 'playlist.m3u'

# Filter settings
TARGET_COLUMN = 'languages'
SEARCH_TERM = 'fas' # Persian

def generate_playlist():
    print("--- Starting Process ---")

    # 1. Download and Filter CSV Data
    print(f"1. Downloading metadata from: {INPUT_CSV_URL}")
    try:
        df_channels = pd.read_csv(INPUT_CSV_URL)
    except Exception as e:
        print(f"Error downloading CSV: {e}")
        return

    # Filter rows where the column matches the search term
    # We use string matching to handle potential missing values safely
    print(f"   Filtering for {TARGET_COLUMN} = '{SEARCH_TERM}'...")
    df_filtered = df_channels[df_channels[TARGET_COLUMN] == SEARCH_TERM].copy()
    
    # Rename 'id' to 'channel' so it matches the streams.json format for merging
    if 'id' in df_filtered.columns:
        df_filtered.rename(columns={'id': 'channel'}, inplace=True)
    
    print(f"   Found {len(df_filtered)} channels matching criteria.")

    if df_filtered.empty:
        print("No channels found. Stopping.")
        return

    # 2. Download and Load JSON Data
    print(f"2. Downloading streams from: {STREAMS_JSON_URL}")
    try:
        df_streams = pd.read_json(STREAMS_JSON_URL)
    except Exception as e:
        print(f"Error downloading JSON: {e}")
        return

    # 3. Merge Data
    print("3. Merging streams with channel metadata...")
    # Inner join: keeps only streams that have a matching channel in our filtered list
    merged_df = pd.merge(df_streams, df_filtered, on='channel', how='inner')
    
    # Replace NaN (empty) values with empty strings to prevent errors in the loop
    merged_df = merged_df.replace({np.nan: ""})
    
    print(f"   Total streams to write: {len(merged_df)}")

    # 4. Generate M3U File
    print(f"4. Writing M3U playlist to: {OUTPUT_FILE}")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as m3u:
        # Write M3U header
        m3u.write("#EXTM3U\n")

        for index, row in merged_df.iterrows():
            # Map columns to M3U fields
            # 'name' comes from channels.csv, 'url' from streams.json
            title = str(row.get("name", "")).strip()
            url = str(row.get("url", "")).strip()
            tvg_id = str(row.get("channel", "")).strip()
            
            # Clean up broadcast area (e.g., "c/US" -> "US")
            group = str(row.get("broadcast_area", "")).replace("c/", "").replace(";", ", ")
            
            language = str(row.get("languages", ""))
            # Some streams have 'format' (e.g. mp4), some don't.
            quality = str(row.get("format", ""))
            
            user_agent = str(row.get("user_agent", ""))
            referrer = str(row.get("referrer", ""))

            # Skip if no URL is present
            if not url:
                continue

            # Build EXTINF line
            extinf = (
                f'#EXTINF:-1 tvg-id="{tvg_id}" '
                f'group-title="{group}" '
                f'tvg-language="{language}" '
                f'tvg-quality="{quality}",{title}\n'
            )
            m3u.write(extinf)

            # Add VLC specific headers if User-Agent or Referrer exists
            if user_agent:
                m3u.write(f"#EXTVLCOPT:http-user-agent={user_agent}\n")
            if referrer:
                m3u.write(f"#EXTVLCOPT:http-referrer={referrer}\n")

            # Write the URL
            m3u.write(f"{url}\n\n")

    print(f"✅ Success! Playlist created: {OUTPUT_FILE}")

if __name__ == "__main__":
    generate_playlist()