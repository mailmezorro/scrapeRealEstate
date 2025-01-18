from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from kleinanzeigen_scraper.spiders.houses_kleinanzeigen import HousesKleinanzeigenSpider
import logging
import os

def setup_logging():
    if not os.path.exists("./logs"):
        os.makedirs("./logs")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("./logs/main_scrapy_log.log", mode='w'),
            logging.StreamHandler()
        ]
    )
def main():
    setup_logging()
    logging.info("Starte den Scrapy-Crawler...")

    # Load Scrapy settings
    settings = get_project_settings()

    # Init and Start Crawler
    process = CrawlerProcess(settings)
    process.crawl(HousesKleinanzeigenSpider)
    process.start()

    logging.info("Scrapy-Crawler finished.")

if __name__ == "__main__":
    main()
