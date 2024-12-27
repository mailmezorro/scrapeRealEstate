import pandas as pd
import logging
from datetime import datetime
import psycopg2
import re

def gps_update(config_db):
    # Logger
    now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f"./logs/scraping_kleinanzeigen_house_gps_{now}.log", mode='w'),
            logging.StreamHandler()
        ]
    )
    logger = logging.getLogger(__name__)

    # Connection to database
    try:
        conn = psycopg2.connect(
            dbname=config_db.get('dbname'),
            user=config_db.get('user'),
            password=config_db.get('password'),
            host=config_db.get('host'),
            port=config_db.get('port')
        )
        logger.info(f"Connection successful!")
        cur = conn.cursor()
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        return

    # Read locations from database
    try:
        cur.execute(f"""
            SELECT id, location 
            FROM {config_db.get('main_table_name')} 
            WHERE latitude IS NULL OR longitude IS NULL;
        """)
        locations = cur.fetchall()
    except Exception as e:
        logger.error(f"Error fetching data: {e}")
        conn.close()
        return

    # load from csv file
    try:
        plz_data = pd.read_csv('plz_geocoord.csv')  
        plz_data = plz_data.rename(columns={"plz": "plz", "latitude": "lat", "longitude": "lng"})  
    except Exception as e:
        logger.error(f"Error loading CSV file: {e}")
        return

    gps_data = []

    for record_id, location in locations:
        try:
            # Extract ZIP code using regex (first 5-digit number)
            match = re.search(r'\b\d{5}\b', location)
            if match:
                plz = match.group()

                # Search for the coordinates for the ZIP code
                coords = plz_data[plz_data['plz'] == int(plz)]  
                if not coords.empty:
                    lat = coords.iloc[0]['lat']
                    lon = coords.iloc[0]['lng']
                    gps_data.append((record_id, lat, lon))
                else:
                    gps_data.append((record_id, None, None))
                    logger.warning(f"No coordinates found for PLZ: {plz}")
            else:
                gps_data.append((record_id, None, None))
                logger.warning(f"No valid PLZ found in location: {location}")

        except Exception as e:
            gps_data.append((record_id, None, None))
            logger.error(f"Error processing location '{location}': {e}")


    # refresh gps data
    for record_id, lat, lon in gps_data:
        if lat and lon:
            try:
                lat_native = float(lat)  # numpy.float32 → float
                lon_native = float(lon)  # numpy.float32 → float
                
                cur.execute(f"""
                    UPDATE {config_db.get('main_table_name')}
                    SET latitude = %s, longitude = %s
                    WHERE id = %s;
                """, (lat_native, lon_native, record_id))  # use native Python types
            except Exception as e:
                logger.error(f"Error updating GPS for id {record_id} with lat={lat}, lon={lon}: {e}")
                conn.rollback()  



    # store gps data and close connection
    try:
        conn.commit()
        logger.info("GPS update committed successfully.")
    except Exception as e:
        logger.error(f"Error committing changes: {e}")

    cur.close()
    conn.close()
    logger.info("Database connection closed.")

    return