import scrapy
import re
import json
from datetime import datetime
from kleinanzeigen_scraper.items import KleinanzeigenItem

class HousesKleinanzeigenSpider(scrapy.Spider):
    name = "houses_kleinanzeigen"
    allowed_domains = ["kleinanzeigen.de"]
    start_urls = ["https://www.kleinanzeigen.de/s-haus-kaufen/aschaffenburg/seite1/c208l7421r10"]



    def start_requests(self):
        # Definiere den Bereich der Seiten (1 bis 10)
        base_url = "https://www.kleinanzeigen.de/s-haus-kaufen/aschaffenburg/seite:{}/c208l7421r10"
        for page in range(1, 11):  # Von Seite 1 bis einschließlich Seite 10
            page_url = base_url.format(page)
            self.logger.info(f"Queuing page: {page_url}")
            yield scrapy.Request(url=page_url, callback=self.parse)

    def parse(self, response):
        # Verarbeite Listings auf der aktuellen Seite
        self.logger.info(f"Processing page: {response.url}")
        yield from self.parse_listings(response)




    def parse_listings(self, response):
        # Extrahiere die Links zu den einzelnen Anzeigen
        ads = response.css(".aditem .text-module-begin a::attr(href)").getall()
        for ad in ads:
            absolute_url = response.urljoin(ad)
            yield scrapy.Request(absolute_url, callback=self.parse_ad)

    def parse_ad(self, response):
        item = KleinanzeigenItem()


        # header info
        item['link'] = response.url
        title = response.xpath("//*[@id='viewad-main-info']//*[@id='viewad-title']/text()").get()
        if title:
            item['title'] = title.strip()
        else:
            item['title'] = None
            self.logger.warning(f"Title not found for URL: {response.url}")
        item['price'] = self.extract_price(response.xpath("//*[@id='viewad-main-info']//*[@id='viewad-price']/text()").get())
        item['location'] = response.xpath("//*[@id='viewad-main-info']//*[@id='viewad-locality']/text()").get().strip()
        item['creation_date'] = self.parse_date(response.xpath("//*[@id='viewad-extra-info']//span[1]/text()").get())

        # Description
        #item['description'] = response.xpath("//*[@id='viewad-description']/text()").get()
        item['description'] = response.xpath("//meta[@itemprop='description']/@content").get()

        # Attributes
        attributes = response.xpath("//div[@id='viewad-details']//li")
        for attribute in attributes:
            text = attribute.xpath("normalize-space(text())").get()
            value = attribute.xpath("normalize-space(span[@class='addetailslist--detail--value']/text())").get()

            if text and value:
                if "Wohnfläche" in text:
                    item['living_area'] = self.extract_numeric(value)
                if "Schlafzimmer" in text:
                    item['bedrooms'] = self.extract_numeric(value)
                if "Grundstücksfläche" in text:
                    item['plot_area'] = self.extract_numeric(value)
                if "Zimmer" in text:
                    item['rooms'] = self.extract_numeric(value)
                if "Badezimmer" in text:
                    item['bathrooms'] = self.extract_numeric(value)
                if "Etagen" in text:
                    item['floors'] = self.extract_numeric(value)
                if "Provision" in text:
                    item['commission'] = value.strip()
                if "Haustyp" in text:
                    item['house_type'] = value.strip()
                if "Baujahr" in text:
                    item['year_built'] = self.extract_numeric(value)

        seller_names = response.xpath(
            "//div[@id='viewad-contact']//span[contains(@class, 'userprofile-vip')]/a/text() | "
            "//div[@id='viewad-contact']//span[contains(@class, 'userprofile-vip')]/a[2]/text() | "
            "//div[@id='viewad-contact']//span[contains(@class, 'userprofile-vip')]/text()"
        ).getall()
        seller_names = [name.strip() for name in seller_names if name.strip()]
        seller_name = seller_names[0] if seller_names else None
        item['seller_name'] = seller_name
        if len(seller_names) > 1 and "Nutzer" in seller_names[1]:
            item['user_type'] = seller_names[1]
        if len(seller_names) > 2 and"Aktiv seit" in seller_names[2]:  # Prüfe, ob der Text das Datum enthalten könnte
                match = re.search(r'\d{2}\.\d{2}\.\d{4}', seller_names[2])  # Suche das Datumsmuster
                if match:
                    item['active_since'] = self.parse_date(match.group(0))  # Speichere das gefundene Datum

        item['number_of_ads'] = None
        # Anzeigen aus `poster-other-ads-link`
        number_of_ads_poster = response.xpath("//a[@id='poster-other-ads-link']/text()").get()
        if number_of_ads_poster:
            number_of_ads_poster = self.extract_numeric(number_of_ads_poster.strip())

        # Anzeigen aus `bizteaser--numads`
        number_of_ads_bizteaser = response.xpath("//span[contains(@class, 'bizteaser--numads')]/text()").get()
        if number_of_ads_bizteaser:
            number_of_ads_bizteaser = self.extract_numeric(number_of_ads_bizteaser.strip())

        # Kombiniere die beiden Ergebnisse
        if number_of_ads_poster or number_of_ads_bizteaser:
            item['number_of_ads'] = max(number_of_ads_poster or 0, number_of_ads_bizteaser or 0)

        
        item['id_ad'] = None
        id_ad_text = response.xpath("//div[@id='viewad-ad-id-box']//li[2]/text()").get()
        if id_ad_text:
            item['id_ad'] = int(self.extract_numeric(id_ad_text.strip()))

        # Meta data
        item["active_flag"] = True
        item["scrape_date"] = datetime.now()
        # GPS coordinates
        item["latitude"] = None
        item["latitude"] = response.xpath('//meta[@property="og:latitude"]/@content').get()
        item["longitude"] = None
        item["longitude"] = response.xpath('//meta[@property="og:longitude"]/@content').get()
        
        if item['id_ad']:
            api_url = f"https://www.kleinanzeigen.de/s-vac-inc-get.json?adId={int(item['id_ad'])}"
            yield scrapy.Request(
                url=api_url,
                callback=self.parse_api,
                meta={'item': item}
            )
        else:
            # Falls `id_ad` nicht existiert, direkt weitergeben
            yield item


    def parse_api(self, response):
        item = response.meta['item']  # Bestehendes Item abrufen
        try:
            data = json.loads(response.text)
            item['view_counter'] = data.get('numVisits', None)  # Daten hinzufügen
        except json.JSONDecodeError:
            self.logger.warning(f"Invalid JSON response from API: {response.url}")
            item['view_counter'] = None
        yield item  # Aktualisiertes Item zurückgeben



    def extract_price(self, price_text):
        if price_text:
            match = re.search(r'([\d.]+)', price_text)
            if match:
                return float(match.group(1).replace('.', ''))
        return None

    def parse_date(self, date_text):
        try:
            parsed_date = datetime.strptime(date_text, "%d.%m.%Y")
            return parsed_date.strftime("%Y-%m-%d")  # Format für die Datenbank
        except (ValueError, TypeError):
            return None


    def extract_numeric(self, text):
        if not text:
            return None
        match = re.search(r'([\d.]+)', text)
        return float(match.group(1).replace('.', '')) if match else None
