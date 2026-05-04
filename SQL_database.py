import os
import sqlite3
import pandas as pd

db_name = 'SQL_Database/directory_database.db'
venues_csv = 'data/indieonthemove_with_capacity.csv' 
artists_csv = 'data/artist_enriched.csv' 
to_venues_csv = 'data/thisweekto_indie_venues.csv'

try:
    print("Loading CSV files into pandas...")
    df_venues = pd.read_csv(venues_csv)
    df_artists = pd.read_csv(artists_csv)
    df_to_venues = pd.read_csv(to_venues_csv)

    print("Merging venue datasets into a single master list...")
    # Stack the two venue files on top of each other. 
    # Pandas matches matching columns and adds the missing ones.
    df_combined_venues = pd.concat([df_venues, df_to_venues], ignore_index=True)
    
    # Fill any empty cells across the entire combined dataframe with "NA"
    df_combined_venues = df_combined_venues.fillna("NA")

    # --- DATA NORMALIZATION (Creating the Keys) ---
    print("Normalizing data and generating keys...")
    
    df_combined_venues.insert(0, 'Venue_ID', range(1, 1 + len(df_combined_venues)))

    # Link the datasets
    df_artists = df_artists.merge(
        df_combined_venues[['Venue_ID', 'Name']], 
        left_on='Venue_Name', 
        right_on='Name', 
        how='left'
    )

    # 2. Connect to SQLite — create parent directory if it doesn't exist
    os.makedirs(os.path.dirname(db_name), exist_ok=True)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    print("Building strict relational tables...")
    cursor.execute('DROP TABLE IF EXISTS artists')
    cursor.execute('DROP TABLE IF EXISTS venues')

    
    cursor.execute('''
    CREATE TABLE venues (
        Venue_ID INTEGER PRIMARY KEY,
        "Name" TEXT,
        "City" TEXT,
        "State/Province" TEXT,
        "Zip_Code" TEXT,
        "Address" TEXT,
        "Phone" TEXT,
        "Website" TEXT,
        "Categories" TEXT,
        "Genres" TEXT,
        "Capacity" REAL,
        "Age_Restriction" TEXT,
        "Rating" REAL,
        "Description" TEXT,
        "Booking_Info" TEXT,
        "Facebook" TEXT,
        "Instagram" TEXT,
        "Upcoming_Events" TEXT,
        "Profile_URL" TEXT,
        "venue_type" TEXT,
        "lat" REAL,
        "lon" REAL,
        "Raw_Building_Area_SqFt" REAL,
        "Building_Area_SqFt" REAL,
        "Multi_Tenant_Flag" TEXT,
        "heuristic_capacity" REAL,
        "Estimated_Capacity" REAL,
        "Estimation_Method" TEXT,
        "Confidence" TEXT,
        "Hours" TEXT
    )
    ''')

    cursor.execute('''
    CREATE TABLE artists (
        Artist_ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Venue_ID INTEGER,
        "Venue_Name" TEXT,
        "Venue_City" TEXT,
        "Venue_Genres" TEXT,
        "Scraped_Artist" TEXT,
        "Spotify_Match" TEXT,
        "Spotify_Name" TEXT,
        "Spotify_URL" TEXT,
        "Popularity" REAL,
        "Followers" REAL,
        "Spotify_Genres" TEXT,
        "Name_Similarity" REAL,
        "Lastfm_Match" TEXT,
        "Lastfm_Listeners" REAL,
        "Lastfm_Playcount" REAL,
        "Lastfm_Tags" TEXT,
        "Genre_Overlap_Count" INTEGER,
        "Matched_Genres" TEXT,
        "Validation" TEXT,
        FOREIGN KEY (Venue_ID) REFERENCES venues (Venue_ID)
    )
    ''')

    # 4. Filter the dataframes to match the SQL columns exactly
    venue_cols = [
        "Venue_ID", "Name", "City", "State/Province", "Zip_Code", "Address", 
        "Phone", "Website", "Categories", "Genres", "Capacity", 
        "Age_Restriction", "Rating", "Description", "Booking_Info", "Facebook",
        "Instagram", "Upcoming_Events", "Profile_URL", "venue_type", "lat", 
        "lon", "Raw_Building_Area_SqFt", "Building_Area_SqFt", "Multi_Tenant_Flag",
        "heuristic_capacity", "Estimated_Capacity", "Estimation_Method", "Confidence", 
        "Hours"
    ]
    
    artist_cols = [
        "Venue_ID", "Venue_Name", "Venue_City", "Venue_Genres", "Scraped_Artist", 
        "Spotify_Match", "Spotify_Name", "Spotify_URL", "Popularity", "Followers", 
        "Spotify_Genres", "Name_Similarity", "Lastfm_Match", "Lastfm_Listeners", 
        "Lastfm_Playcount", "Lastfm_Tags", "Genre_Overlap_Count", "Matched_Genres", 
        "Validation"
    ]

    for col in venue_cols:
        if col not in df_combined_venues.columns:
            df_combined_venues[col] = None

    for col in artist_cols:
        if col not in df_artists.columns:
            df_artists[col] = None

    # 5. Insert the data into the strict schema
    print("Pouring data into the database...")
    df_combined_venues[venue_cols].to_sql('venues', conn, if_exists='append', index=False)
    df_artists[artist_cols].to_sql('artists', conn, if_exists='append', index=False)

    conn.commit()
    print(f"Success! Relational database '{db_name}' created with a unified 'venues' and 'artists' table.")

except Exception as e:
    print(f"An unexpected error occurred: {e}")
finally:
    if 'conn' in locals():
        conn.close()
