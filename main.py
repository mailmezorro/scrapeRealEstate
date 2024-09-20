from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

# Chrome Driver Pfad anpassen
driver_path = 'C:/Users/danie.DANIEL/Documents/chromedriver-win64/chromedriver.exe'
service = Service(driver_path)

# Chrome Optionen
chrome_options = Options()
chrome_options.add_argument("--headless")  # Für Headless-Betrieb (optional)
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Webdriver initialisieren
driver = webdriver.Chrome(service=service, options=chrome_options)

# Seite laden
driver.get("https://www.kleinanzeigen.de/s-haus-kaufen/hoesbach/c208l16132r5")

print(driver.title)

# Elemente finden
elements = driver.find_elements(By.CLASS_NAME, "aditem")
for element in elements:
    print(element.text)


# Browser schließen
driver.quit()
