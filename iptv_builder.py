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

    # 1. LOAD AND FILTER CHANNELS (feeds.csv)
    print(f"Downloading channels from: {CHANNELS_URL}")
    try:
        df_channels = pd.read_csv(CHANNELS_URL)
        
        # Ensure the target column exists to avoid errors
        if TARGET_COLUMN not in df_channels.columns:
            # Fallback if column missing (just in case), though you said it works.
            print(f"Warning: Column '{TARGET_COLUMN}' not found. Creating empty one.")
            df_channels[TARGET_COLUMN] = ""

        df_channels[TARGET_COLUMN] = df_channels[TARGET_COLUMN].astype(str)
        df_filtered = df_channels[df_channels[TARGET_COLUMN].str.contains(SEARCH_VALUE, na=False)].copy()
        
        print(f"Found {len(df_filtered)} channels matching '{SEARCH_VALUE}'.")
        
        # --- FIX: DO NOT RENAME COLUMNS HERE ---
        # Renaming caused the "label 'channel' is not unique" error
        # because the dataframe likely already had a 'channel' column or conflicted during merge.
            
    except Exception as e:
        print(f"Error loading channels: {e}")
        sys.exit(1)

    # 2. LOAD AND MERGE STREAMS
    print(f"Downloading streams from: {STREAMS_URL}")
    try:
        df_streams = pd.read_json(STREAMS_URL)
        
        # --- FIX: USE LEFT_ON AND RIGHT_ON ---
        # We join 'channel' (from streams.json) with 'id' (from your csv)
        merged_df = pd.merge(
            df_streams, 
            df_filtered, 
            left_on='channel', 
            right_on='id', 
            how='inner'
        )
        
        # Fallback for title
        # If 'title' isn't in streams, use 'name' from the csv
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
            # Extract data
            title = str(row.get("name", "")).strip() 
            url = str(row.get("url", "")).strip()
            tvg_id = str(row.get("id", "")).strip() # Using the ID from csv
            
            # Clean up broadcast area or language for the group title
            group = str(row.get("languages", "")).replace("nan", "")
            
            logo = str(row.get("logo", "")).replace("nan", "")
            
            # User Agent and Referrer
            user_agent = str(row.get("user_agent", "")).replace("nan", "")
            referrer = str(row.get("http_referrer", "")).replace("nan", "")
            
            if not url or url.lower() == "nan":
                continue

            # Build EXTINF line
            extinf = (
                f'#EXTINF:-1 tvg-id="{tvg_id}" '
                f'group-title="{group}" '
                f'tvg-logo="{logo}" '
                f',{title}\n'
            )

            m3u.write(extinf)

            if user_agent:
                m3u.write(f"#EXTVLCOPT:http-user-agent={user_agent}\n")
            if referrer:
                m3u.write(f"#EXTVLCOPT:http-referrer={referrer}\n")

            m3u.write(f"{url}\n\n")

    print(f"✅ Success! Playlist saved to '{OUTPUT_FILE}'")

if __name__ == "__main__":
    generate_playlist()