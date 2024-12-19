from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
import logging
import os
from selenium.common.exceptions import TimeoutException
import random
import time
from datetime import datetime
# own packages
from scripts import config_utils
from scripts import scrape_houses_kleinanzeigen
import psycopg2
from scripts.utils import convert_to_int, convert_to_float, convert_to_date
import scripts.database_operations as database_operations
import scripts.utils as utils
from webdriver_manager.firefox import GeckoDriverManager
import requests

def main():
    if os.name == 'nt':
        windows_flag = True
    else:
        windows_flag = False
    # Clear terminal
    if windows_flag:
        os.system('cls')
    else:
        os.system('clear') 
    
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
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(current_dir, 'config', 'config.json')
    config = config_utils.load_config_file(config_path)
    UBLOCK_XPI_URL = "https://addons.mozilla.org/firefox/downloads/file/4058632/ublock_origin-1.50.0-an+fx.xpi"
    driver_path = config.get("driver_path")
    service = Service(driver_path)

    ublock_path = os.path.join(os.getcwd(), "ublock_origin.xpi")
    if not os.path.exists(ublock_path):
        import requests
        response = requests.get(UBLOCK_XPI_URL)
        with open(ublock_path, "wb") as f:
            f.write(response.content)

    # Firefox options
    firefox_options = Options()
    firefox_options.add_argument("--headless") 
    firefox_options.add_argument("--no-sandbox")
    firefox_options.add_argument("--disable-dev-shm-usage")
    firefox_options.set_preference("extensions.autoDisableScopes", 0)
    firefox_options.set_preference("extensions.enabledScopes", 15)
    firefox_options.add_argument(f"--load-extension={ublock_path}")
    firefox_options.set_preference("general.useragent.override", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    firefox_options.add_argument("--disable-gpu")

    # Start Firefox
    #service = Service(GeckoDriverManager().install())
    driver = webdriver.Firefox(service=service, options=firefox_options)
    
    base_url = "https://www.kleinanzeigen.de/s-haus-kaufen/aschaffenburg/seite:{}/c208l7421r10"
    results = []

    for page in range(1, 100):
        random_sleep = random.uniform(3.0, 5.0)
        time.sleep(random_sleep)
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

            if scrape_houses_kleinanzeigen.check_url_availability(link):
                print(f"Link there, srapy: {link}")
            else:
                print(f"Link not there, scrapy: {link}")
                continue

            result = {'link': link}
            result.update(scrape_houses_kleinanzeigen.scrape_header(logger, driver))
            result.update(scrape_houses_kleinanzeigen.scrape_attributes(logger, driver, ["Wohnfläche", "Schlafzimmer", "Grundstücksfläche", "Baujahr", "Zimmer", "Badezimmer", "Etagen", "Provision", "Haustyp"]))
            result = convert_to_int(result, ['Schlafzimmer', 'Zimmer', 'Badezimmer', 'Etagen', 'Baujahr'])
            result = convert_to_float(result,['Wohnfläche','Grundstücksfläche'])
            # ToDo: bad pattern here... need do to fix
            utils.rename_key(result, 'Wohnfläche', 'living_area')
            utils.rename_key(result, 'Schlafzimmer', 'bedrooms')
            utils.rename_key(result, 'Grundstücksfläche', 'plot_area')
            utils.rename_key(result, 'Zimmer', 'rooms')
            utils.rename_key(result, 'Badezimmer', 'bathrooms')
            utils.rename_key(result, 'Etagen', 'floors')
            utils.rename_key(result, 'Provision', 'commission')
            utils.rename_key(result, 'Haustyp', 'house_type')
            utils.rename_key(result, 'Baujahr', 'year_built')
            
            
            result.update(scrape_houses_kleinanzeigen.scrape_description(logger, driver))
            result.update(scrape_houses_kleinanzeigen.scrape_right_sidebar(logger, driver))
            result["scrape_date"] = datetime.now()
            result["active_flag"] = True

            results.append(result)
            
            random_sleep = random.uniform(1.0, 2.0)
            time.sleep(random_sleep)

        
    # Close Chrome
    driver.quit()
    
    # Init Database
    config_path = os.path.join(current_dir, 'config', 'db_config.json')
    config_db = config_utils.load_config_file(config_path)

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
        
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return
    
    try:
        columns = database_operations.get_column_names(cur, 'kleinanzeigen_immobilien')    
        for result in results:
            database_operations.check_and_insert_or_update(result, cur, config_db.get('main_table_name'), config_db.get('delta_table_name'), config_db.get('deltaPrice_table_name'), config_db.get('deltaView_counter_name'))
            # Commit changes to the database
            conn.commit()
        
    except Exception as e:
        print(f"Error inserting data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()        

if __name__ == "__main__":
    main() 

    









