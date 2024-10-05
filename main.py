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
# own packages
from scripts import config_utils
from scripts import scrape_houses_kleinanzeigen

def main():
    # Clear terminal
    os.system('cls') 
    
    # Config Logger
    logging.basicConfig(
        level=logging.INFO,  # DEBUG, INFO, WARNING, ERROR, CRITICAL
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("scraping.log", mode='w'),  # log file
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

    # Open website
    driver.get("https://www.kleinanzeigen.de/s-haus-kaufen/hoesbach/c208l16132r5")

    print("Title overview page:", driver.title)

    # Find all ad elements
    elements = driver.find_elements(By.CLASS_NAME, "aditem")
    links = [element.find_element(By.TAG_NAME, 'a').get_attribute('href') for element in elements]

    results =[]
    for link in links:
        driver.get(link)

        result = {'link':  link}
        result = scrape_houses_kleinanzeigen.scrape_header(logger,driver)
        result = scrape_houses_kleinanzeigen.scrape_attributes(logger,driver, ["Wohnfläche","Schlafzimmer", "Grundstücksfläche", "Baujahr", "Zimmer" ,"Badezimmer", "Etagen", "Provision", "Haustyp"])
        result = scrape_houses_kleinanzeigen.scrape_description(logger,driver)
        #result.scrape_images(driver)
        result = scrape_houses_kleinanzeigen.scrape_right_sidebar(logger,driver)
        results.append(result)

    # Close Chrome
    driver.quit()
    
    
if __name__ == "__main__":
    main() 

    









