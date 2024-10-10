from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
import logging
import os
from psycopg2 import sql
from selenium.common.exceptions import TimeoutException
from datetime import datetime
# own packages
from scripts import config_utils
from scripts import scrape_houses_kleinanzeigen
import psycopg2
import json
from scripts.utils import convert_to_int, convert_to_float, convert_to_date


def main():
    # Clear terminal
    os.system('cls') 
    
    #datetime for logging name
    now = datetime.now()
    now = now.strftime("%Y_%m_%d_%H_%M_%S")
    
    # Config Logger
    logging.basicConfig(
        level=logging.INFO,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("./logs/scraping_kleinanzeigen_house_"  + now + ".log", mode='w'),  
            logging.StreamHandler()  # console output
        ]
    )

    logger = logging.getLogger(__name__)

    # Load private config for path to driver and ublock extension
    config = config_utils.load_config_file(config_file="./config/config.json")
    driver_path = config.get("driver_path")
    ublock_extension = config.get("ublock_extension")
    service = Service(driver_path)

    # Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_extension(ublock_extension)

    # Start Chrome
    driver = webdriver.Chrome(service=service, options=chrome_options)

    base_url = "https://www.kleinanzeigen.de/s-haus-kaufen/hoesbach/seite:{}/c208l16132r5"
    results = []

    for page in range(1, 99):
        url = base_url.format(page)  
        
        # Open website
        driver.get(url)

        print("Title overview page:", driver.title)

        # Check if page is valid (if no more pages exist, this check will stop the loop)
        if "Website not found" in driver.title or driver.current_url == base_url.format(page - 1):
            print(f"No more pages. Stopping at page {page}.")
            break

        # Find all ad elements
        elements = driver.find_elements(By.CLASS_NAME, "aditem")
        if not elements:
            print(f"No ads found on page {page}. Stopping.")
            break

        # Get all links from ad elements
        links = [element.find_element(By.TAG_NAME, 'a').get_attribute('href') for element in elements]

        for link in links:
            driver.get(link)

            result = {'link': link}
            result.update(scrape_houses_kleinanzeigen.scrape_header(logger, driver))
            result.update(scrape_houses_kleinanzeigen.scrape_attributes(logger, driver, ["Wohnfläche", "Schlafzimmer", "Grundstücksfläche", "Baujahr", "Zimmer", "Badezimmer", "Etagen", "Provision", "Haustyp"]))
            result = convert_to_int(result, ['Schlafzimmer', 'Zimmer', 'Badezimmer', 'Etagen', 'Baujahr'])
            result = convert_to_float(result,['Wohnfläche','Grundstücksfläche'])
            result.update(scrape_houses_kleinanzeigen.scrape_description(logger, driver))
            result.update(scrape_houses_kleinanzeigen.scrape_right_sidebar(logger, driver))
            result["scrape_date"] = datetime.now()
            
            
            results.append(result)

    # Close Chrome
    driver.quit()
    
    
    # Init Database
    config_db = config_utils.load_config_file(config_file="./config/db_config.json")
    try:
        conn = psycopg2.connect(
            dbname=config_db.get('dbname'),
            user=config_db.get('user'),
            password=config_db.get('password'),  
            host=config_db.get('host'),
            port=config_db.get('port')
        )
        print("Connection successful!")
        cur = conn.cursor()
        
        insert_query = """
            INSERT INTO kleinanzeigen_immobilien (
                link, title, price, location, bedrooms, creation_date, view_counter, 
                living_area, plot_area, year_built, rooms, bathrooms, floors, 
                commission, house_type, description, company, author, number_of_ads, id_ad, scrape_date
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for result in results:
            cur.execute(insert_query, (
                result['link'],                # link
                result['title'],               # title
                result['price'],               # price
                result['location'],            # location
                result.get('Schlafzimmer'),    # bedrooms
                result['creation_date'],       # creation_date
                result.get('view_counter'),    # view_counter
                result.get('Wohnfläche'),      # living_area
                result.get('Grundstücksfläche'),# plot_area
                result.get('Baujahr'),         # year_built
                result.get('Zimmer'),          # rooms
                result.get('Badezimmer'),      # bathrooms
                result.get('Etagen'),          # floors
                result.get('Provision'),       # commission
                result.get('Haustyp'),         # house_type
                result.get('description'),     # description
                result.get('Company'),         # company
                result.get('Author'),          # author
                result.get('number_of_ads'),   # number_of_ads
                result.get('id_ad'),            # id_ad
                result['scrape_date']           # scrape_date
            ))


        conn.commit()
        print("Data successfully inserted into the table!")
        
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:

        cur.close()
        conn.close()        

if __name__ == "__main__":
    main() 

    









