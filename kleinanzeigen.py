import scrapy


class KleinanzeigenSpider(scrapy.Spider):
    name = "kleinanzeigen"
    allowed_domains = ["kleinanzeigen.de"]
    start_urls = ["https://kleinanzeigen.de"]

    def parse(self, response):
        pass
