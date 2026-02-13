import pandas as pd
import sys

# --- CONFIGURATION ---
CHANNELS_URL = 'https://raw.githubusercontent.com/iptv-org/database/refs/heads/master/data/feeds.csv'
STREAMS_URL = 'https://iptv-org.github.io/api/streams.json'
OUTPUT_FILE = 'playlist.m3u'

# Filter Settings
TARGET_COLUMN = 'languages'
SEARCH_VALUE = 'fas'

def generate_playlist():
    print("--- Starting Playlist Generation ---")

    # 1. LOAD AND FILTER CHANNELS
    print(f"Downloading channels from: {CHANNELS_URL}")
    try:
        df_channels = pd.read_csv(CHANNELS_URL)
        
        # Filter the data
        # We ensure the column is treated as a string to avoid errors
        df_channels[TARGET_COLUMN] = df_channels[TARGET_COLUMN].astype(str)
        df_filtered = df_channels[df_channels[TARGET_COLUMN] == SEARCH_VALUE].copy()
        
        print(f"Found {len(df_filtered)} channels matching '{SEARCH_VALUE}'.")
        
        # Rename 'id' to 'channel' to prepare for merging with streams data
        # (streams.json uses 'channel', channels.csv uses 'id')
        if 'id' in df_filtered.columns:
            df_filtered = df_filtered.rename(columns={'id': 'channel'})
            
    except Exception as e:
        print(f"Error loading channels: {e}")
        sys.exit(1)

    # 2. LOAD AND MERGE STREAMS
    print(f"Downloading streams from: {STREAMS_URL}")
    try:
        df_streams = pd.read_json(STREAMS_URL)
        
        # Merge data (Inner Join)
        # matches streams.channel with the filtered channels.channel
        merged_df = pd.merge(df_streams, df_filtered, on='channel', how='inner')
        
        # Fallback: If 'title' is missing, use 'name' from the channel data
        if 'title' not in merged_df.columns:
            merged_df['title'] = merged_df['name']
            
        print(f"Merged data contains {len(merged_df)} playable streams.")
        
    except Exception as e:
        print(f"Error loading streams or merging: {e}")
        sys.exit(1)

    # 3. CONVERT TO M3U
    print(f"Writing to {OUTPUT_FILE}...")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as m3u:
        m3u.write("#EXTM3U\n")

        for _, row in merged_df.iterrows():
            # Extract data, handling NaN (empty) values safely
            title = str(row.get("title", "")).strip()
            url = str(row.get("url", "")).strip()
            
            # Using 'name' for tvg-id as per your request (usually 'id' is better, but following your logic)
            tvg_id = str(row.get("name", "")).strip() 
            
            # Clean up broadcast area
            group = str(row.get("broadcast_area", "")).replace("c/", "").replace(";", ", ").replace("nan", "")
            
            language = str(row.get("languages", "")).replace("nan", "")
            quality = str(row.get("format", "")).replace("nan", "") # 'format' is sometimes missing
            
            # User Agent and Referrer are often in streams.json as 'user_agent' and 'http_referrer'
            user_agent = str(row.get("user_agent", "")).replace("nan", "")
            referrer = str(row.get("http_referrer", "")).replace("nan", "")
            
            # Skip invalid URLs
            if not url or url.lower() == "nan":
                continue

            # Build EXTINF line
            extinf = (
                f'#EXTINF:-1 tvg-id="{tvg_id}" '
                f'group-title="{group}" '
                f'tvg-language="{language}" '
                f'tvg-quality="{quality}",{title}\n'
            )

            m3u.write(extinf)

            # Add User-Agent or Referrer headers if they exist
            if user_agent:
                m3u.write(f"#EXTVLCOPT:http-user-agent={user_agent}\n")
            if referrer:
                m3u.write(f"#EXTVLCOPT:http-referrer={referrer}\n")

            m3u.write(f"{url}\n\n")

    print(f"✅ Success! Playlist saved to '{OUTPUT_FILE}'")

if __name__ == "__main__":
    generate_playlist()
